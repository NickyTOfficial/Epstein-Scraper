import json
import threading
import requests
import os
import yaml
import time
import sys
from bs4 import BeautifulSoup
import poolDownloader

datasetPattern = "https://www.justice.gov/epstein/doj-disclosures/data-set-{}-files"
filePattern = "https://www.justice.gov/epstein/files/DataSet%20{}/{}"


# setup code

try:
    config = yaml.safe_load(open("config.yaml"))  # load config from yaml file, or create default if it doesn't exist or fails to load
except Exception:
    print("Error loading config.yaml, using default configuration.")
    config = {
        "directory": os.getcwd(),
        "fetchRetries": 5,
        "timeBetweenFiles": 100,
        "timeBetweenPages": 100,
        "downloadWorkers": 5,
        "datasets": [1, 2, 3, 4],
        "poolSize": 16
    }

directory = config.get("directory")
timeBetweenPages = float(config.get("timeBetweenPages", "20"))
fetchRetries = int(config.get("fetchRetries", "5"))
timeBetweenFiles = float(config.get("timeBetweenFiles", "0.1"))
datasets = config.get("datasets", [1])
downloadWorkers = int(config.get("downloadWorkers", 8))
poolSize = int(config.get("poolSize", 100))

data = {
    "directory": directory,
    "timeBetweenPages": timeBetweenPages,
    "fetchRetries": fetchRetries,
    "timeBetweenFiles": timeBetweenFiles,
    "datasets": datasets,
    "downloadWorkers": downloadWorkers,
    "poolSize": poolSize
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
    
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36>",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.justice.gov/",
    "Connection": "keep-alive",
    
})

# create a justiceGovAgeVerified cookie that is set to true to bypass age verification
verificationCookie = requests.cookies.create_cookie("justiceGovAgeVerified", "true", domain = "www.justice.gov")
s.cookies.set_cookie(verificationCookie)


#---------------#
# State management for resume functionality

STATE_FILE = "scraper_state.json"

def load_state():
    """Load the last known state (dataset and page)"""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {"last_dataset": None, "last_page": None}

def save_state(dataset_num, page_num):
    """Save the current state (dataset and page)"""
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump({"last_dataset": dataset_num, "last_page": page_num}, f)
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


def fetch_with_retry(url, session, retries=5, delay=3, timeout=10):

    for attempt in range(retries):
        try:
            r = session.get(url, timeout=timeout)
        except Exception:
            r = None

        if r is None:
            time.sleep(delay)
            continue

        if r.status_code == 200:
            if b"EFTA" in r.content or b"ReportLab" in r.content or len(r.content) > 200:
                return r
            else:
                time.sleep(delay)
                continue

        if r.status_code in (403, 429, 500, 502, 503):
            time.sleep(delay)
            continue
            # number of files found on this page (removed print)

    return None


#---------------#

def updatePool(dataset_num, dataset_page = 0, timeBetweenPages = timeBetweenPages, fetchRetries = fetchRetries):
    page = dataset_page

    while True:

        if(poolDownloader.poolSize() < poolSize):
            
            url = datasetPattern.format(dataset_num) + f"?page={page}"

            r = fetch_with_retry(url, s, retries=fetchRetries, delay=timeBetweenPages)
            if r is None:
                print(f"Failed to fetch {url} after multiple attempts.")
                break
            soup = BeautifulSoup(r.text, "html.parser")

            page_files = set()

            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/epstein/files/" in href and "EFTA" in href: ## Adds all EFTA files found on this page to the pool
                    filename = href.split("/")[-1]
                    page_files.add(filename)

            page_files = sorted(page_files) # preserve file order
        

            poolDownloader.updatePool([filePattern.format(dataset_num, filename) for filename in page_files])

            page += 1

            poolDownloader.setDatasetInfo(dataset_num, page)

            if poolDownloader.poolSize() >= poolSize - 64: # delay download start until we have a decent buffer of files in the pool to prevent early starvation of download workers
                poolDownloader.signalStart()

            # Save state after each page completes
            save_state(dataset_num, page - 1)

            time.sleep(timeBetweenPages/1000)  # convert ms to seconds

    


# Load state and resume from where we left off
state = load_state()
last_dataset = state.get("last_dataset")
last_page = state.get("last_page")

# Check for command-line argument to reset state
if "--reset" in sys.argv:
    print("Resetting scraper state...")
    reset_state()
    last_dataset = None
    last_page = None

# Initialize pool once
poolDownloader.initPool()

try:
        

    for iterand in datasets:

        if last_dataset is not None and iterand < last_dataset:
            continue

        downloader_thread = threading.Thread(
        target=poolDownloader.downloadFromPool,
            args=(os.path.join(directory, f"Dataset {iterand}"), downloadWorkers, timeBetweenFiles, s),
        )

        downloader_thread.start()

        updatePool(iterand, last_page if iterand == last_dataset else 0)

        poolDownloader.producerDone()

        downloader_thread.join()

        save_state(iterand, 0)
        
except KeyboardInterrupt:
    print("\nInterrupted by user. Shutting down...")
    poolDownloader.forceShutdown()
    sys.exit(0)

    

