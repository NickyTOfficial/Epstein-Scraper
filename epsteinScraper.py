import json
import random
import threading
from anaconda_cli_base import console
import requests
import os
import yaml
import time
from bs4 import BeautifulSoup
import poolDownloader

datasetPattern = "https://www.justice.gov/epstein/doj-disclosures/data-set-{}-files"
filePattern = "https://www.justice.gov/epstein/files/DataSet%20{}/{}"

generating_files = None


# setup code

config = {}

try:
    config = yaml.safe_load(open("config.yaml"))  # load config from yaml file, or create default if it doesn't exist or fails to load
except Exception:
    print("Error loading config.yaml, using default configuration.")

directory = config.get("directory", os.getcwd())
timeBetweenPages = float(config.get("timeBetweenPages", 40))
timeBetweenFiles = float(config.get("timeBetweenFiles", 40))
fetchRetries = int(config.get("fetchRetries", 25))
timeBetween403 = float(config.get("timeBetween403", 4000))
datasets = config.get("datasets", [1])
downloadWorkers = int(config.get("downloadWorkers", 8))
poolSize = int(config.get("poolSize", 600))
trustLocalFiles = config.get("trustLocalFiles",False)

data = {
    "directory": directory,
    "timeBetweenPages": timeBetweenPages,
    "timeBetweenFiles": timeBetweenFiles,
    "fetchRetries": fetchRetries,
    "timeBetween403": timeBetween403,
    "datasets": datasets,
    "downloadWorkers": downloadWorkers,
    "poolSize": poolSize,
    "trustLocalFiles": trustLocalFiles
}

os.makedirs(directory, exist_ok=True)

with open('config.yaml','w') as file:
    yaml.dump(data, file)     ## roundabout way of adding any missing config options to the yaml file while preserving existing ones
try: 
    config = yaml.safe_load(open("config.yaml"))
except Exception: 
    print("file exception when handling config")
    os._exit()


# set up a persistent session to improve network traffic
s = requests.Session()

s.headers.update({ ## Simulating a browser to increase authenticity of requests, reducing scraper detection
    
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    
})

# create a justiceGovAgeVerified cookie that is set to true to bypass age verification
verificationCookie = requests.cookies.create_cookie("justiceGovAgeVerified", "true", domain = "www.justice.gov")
s.cookies.set_cookie(verificationCookie)


#---------------#


def randomDelay(delay):
    delay = delay / 1000  # convert ms to seconds
    time.sleep(delay * (0.5 + random.random()))  # add some randomness to the delay to further reduce scraper detection



# State management for resume functionality

STATE_FILE = "scraper_state.json"

def load_state():
    """Load the last known state (dataset and page)"""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f, parse_int=lambda x: int(x) if x.isdigit() else x)
                return data
        except Exception:
            pass
    return {"last_dataset": None, "last_page": None}  # default state if no file or error

def save_state(location):

    dataset_num = location[0]
    page_num = location[1]

    try:
        with open(STATE_FILE, "w") as f:
            json.dump({
                "last_dataset": dataset_num,
                "last_page": page_num,
            }, f)
    except Exception:
        pass

def reset_state():
    """Clear the saved state"""
    if os.path.exists(STATE_FILE):
        try:
            os.remove(STATE_FILE)
        except Exception:
            pass

#---------------#


def fetch_with_retry(url, session, retries=5, delay=3, timeBetween403 = 4):

    for attempt in range(retries):
        try:
            r = session.get(url, timeout=10)
        except Exception:
            r = None

        if r is None:
            randomDelay(delay + ((delay*0.5) * attempt))  # increase delay with each retry
            continue

        if r.status_code == 200:
            if b"EFTA" in r.content or b"ReportLab" in r.content or len(r.content) > 200:
                return r
            else:
                randomDelay(delay + ((delay*0.5) * attempt))  # increase delay with each retry
                continue

        if r.status_code in (403, 429, 500, 502, 503):
            poolDownloader.incrementForbiddenCount()
            randomDelay(timeBetween403 + ((timeBetween403*0.5) * attempt))  # increase delay with each retry
            continue

    return None


#---------------#



def updatePool(dataset_num, start_page=0):
    page = start_page

    final_page = False

    while True:

        if poolDownloader.poolSize() > poolSize:
            time.sleep(0.2)
            continue

        requested_url = f"{datasetPattern.format(dataset_num)}?page={page}"
        r = fetch_with_retry(requested_url, s, retries=fetchRetries, delay=timeBetweenPages, timeBetween403=timeBetween403)

        if r is None:
            poolDownloader.incrementErrorCount()
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        # --- Extract files ---
        page_files = sorted({
            a["href"].split("/")[-1]
            for a in soup.find_all("a", href=True)
            if "/epstein/files/" in a["href"] and "EFTA" in a["href"]
        })
        # Log inaccessible no-pagination and access denied pages
        pagination = soup.find(class_="usa-pagination")
        access_denied = soup.find(title_="Access Denied")
        generating_files = soup.find_all('link', href= "list%20still%20generating_files/slick.css")

        if access_denied:
            poolDownloader.log_event(
                poolDownloader.failed_log,
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Access denied at https://www.justice.gov/epstein/doj-disclosures/data-set-{dataset_num}-files?page={page}"
            )

            poolDownloader.signalStart()
            poolDownloader.empty_pool(downloadWorkers)
            poolDownloader.producerDone()

        if generating_files:

            poolDownloader.log_event(
                poolDownloader.failed_log,
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Generating files redirect at https://www.justice.gov/epstein/doj-disclosures/data-set-{dataset_num}-files?page={page}"
            )

            randomDelay(timeBetween403)
            continue


        if not pagination and len(page_files) > 40:

            poolDownloader.log_event(
                poolDownloader.failed_log,
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} | "
                f"Dataset {dataset_num} | Page {page} | "
                f"No pagination, inaccessible page | {requested_url}"
            )
            page += 1
            continue

        if(pagination):
            aria_next = pagination.find("a", attrs={"aria-label": "Next page"}) 

            if not (aria_next): # theoretically the end of the dataset should have no "next" button

                final_page = True

        if( 1 <= len(page_files) < 40): #this is specifically to handle dataset 6 and 7
                
            final_page = True
        

        # --- Queue files ---
        pool_objects = [
            ( dataset_num, page, filePattern.format(dataset_num, filename))
            for filename in page_files
        ]

        poolDownloader.updatePool(pool_objects)
        poolDownloader.setDatasetInfo(dataset_num, page)

        if poolDownloader.poolSize() >= poolSize:
            poolDownloader.signalStart()

        if(poolDownloader.isStarted()):
            save_state(poolDownloader.getLastLocation())

        if(final_page): 


            poolDownloader.log_event(
                poolDownloader.failed_log,
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Dataset {dataset_num} reached end condition"
            )

            poolDownloader.log_event(
                poolDownloader.alt_log,
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Dataset {dataset_num} reached end condition"
            )


            poolDownloader.signalStart()
            poolDownloader.producerDone()
            break

        page += 1
        randomDelay(timeBetweenPages)







# Load state and resume from where we left off
state = load_state()
last_dataset = state.get("last_dataset")
last_page = state.get("last_page")


try:


    downloader_thread = threading.Thread(
        target=poolDownloader.downloadFromPool,
        args=(directory, downloadWorkers, timeBetweenFiles, s, trustLocalFiles),
    )
    downloader_thread.start()

    start_index = 0
    if last_dataset is not None:
        try:
            start_index = datasets.index(int(last_dataset))

            poolDownloader.log_event(
                poolDownloader.failed_log,
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Resuming from Dataset {last_dataset}, Page {last_page}"
            )
            poolDownloader.log_event(
                poolDownloader.alt_log,
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Resuming from Dataset {last_dataset}, Page {last_page}"
            )
        except ValueError:
            start_index = 0
            poolDownloader.log_event(
                poolDownloader.failed_log,
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Starting scraping from beginning of configured dataset"
            )
            poolDownloader.log_event(
                poolDownloader.alt_log,
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Starting scraping from beginning of configured dataset"
            )

    # Only iterate from the resume point onward
    for iterand in datasets[start_index:]:
        # If we’re resuming mid-dataset, use last_page, otherwise start at 0
        page_offset = last_page if iterand == last_dataset else 0

        # Set dataset info before enqueuing URLs
        poolDownloader.setDatasetInfo(iterand, page_offset)

        # Enqueue pages starting at the correct offset
        updatePool(iterand, page_offset)

        # Save resume state **after** scraping that dataset’s pages
        save_state(poolDownloader.getLastLocation())
            
except KeyboardInterrupt:

    poolDownloader.signalStart()
    poolDownloader.empty_pool(downloadWorkers)
    poolDownloader.producerDone()
    

    poolDownloader.log_event(
        poolDownloader.failed_log,
        f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Interrupted by user"
    )
    poolDownloader.log_event(
        poolDownloader.alt_log,
        f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Interrupted by user"
    )


finally:        
    poolDownloader.empty_pool(downloadWorkers)

    lastLocation = poolDownloader.getLastLocation()

    poolDownloader.log_event(
        poolDownloader.failed_log,
        f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Log closed, scraper exiting at Dataset {lastLocation[0]}, Page {lastLocation[1]}"
    )
    poolDownloader.log_event(
        poolDownloader.alt_log,
        f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Log closed, scraper exiting at Dataset {lastLocation[0]}, Page {lastLocation[1]}"
    )
