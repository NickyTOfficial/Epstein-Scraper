import json
import random
import threading
from anaconda_cli_base import console
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
        "timeBetween403" : 20,
        "downloadWorkers": 10,
        "datasets": [1, 2, 3, 4],
        "poolSize": 450
    }

directory = config.get("directory")
timeBetweenPages = float(config.get("timeBetweenPages", "20"))
fetchRetries = int(config.get("fetchRetries", "5"))
timeBetweenFiles = float(config.get("timeBetweenFiles", "0.1"))
timeBetween403 = float(config.get("timeBetween403", "4"))
datasets = config.get("datasets", [1])
downloadWorkers = int(config.get("downloadWorkers", 8))
poolSize = int(config.get("poolSize", 100))

data = {
    "directory": directory,
    "timeBetweenPages": timeBetweenPages,
    "fetchRetries": fetchRetries,
    "timeBetweenFiles": timeBetweenFiles,
    "timebetween403": timeBetween403,
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
    
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.85 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.justice.gov/",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
    "Sec-CH-UA": "\"Chromium\";v=\"121\", \"Not A(Brand\";v=\"99\"",
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": "\"Linux\""
    
})

# create a justiceGovAgeVerified cookie that is set to true to bypass age verification
verificationCookie = requests.cookies.create_cookie("justiceGovAgeVerified", "true", domain = "www.justice.gov")
s.cookies.set_cookie(verificationCookie)


#---------------#


def randomDelay(delay):
    delay = delay / 1000  # convert ms to seconds
    time.sleep(delay * (0.1 + random.random()))  # add some randomness to the delay to further reduce scraper detection



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

def save_state(dataset_num, page_num):
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
            randomDelay(delay)
            continue

        if r.status_code == 200:
            if b"EFTA" in r.content or b"ReportLab" in r.content or len(r.content) > 200:
                return r
            else:
                randomDelay(delay)
                continue

        if r.status_code in (403, 429, 500, 502, 503):
            poolDownloader.incrementForbiddenCount()
            randomDelay(timeBetween403)
            continue

    return None


#---------------#


def updatePool(dataset_num, start_page=0):
    page = start_page
    seen_signatures = set()

    while True:

        url = f"{datasetPattern.format(dataset_num)}?page={page}"
        r = fetch_with_retry(url, s, retries=fetchRetries)

        if r is None:
            poolDownloader.incrementErrorCount()
            break

        soup = BeautifulSoup(r.text, "html.parser")

        # Collect files on this page
        page_files = {
            (a["href"].split("/")[-1], page)
            for a in soup.find_all("a", href=True)
            if "/epstein/files/" in a["href"] and "EFTA" in a["href"]
        }

        # Signature for duplicate detection
        signature = tuple(sorted(f[0] for f in page_files))
        if signature in seen_signatures:
            poolDownloader.signalStart()
            break
        seen_signatures.add(signature)

        # Build pool objects
        pool_objects = [
            (filePattern.format(dataset_num, filename, page), page)
            for filename, page in page_files
        ]

        poolDownloader.updatePool(pool_objects)
        poolDownloader.setDatasetInfo(dataset_num, page)

        # Start workers once pool is warm
        if poolDownloader.poolSize() >= poolSize:
            poolDownloader.signalStart()

        # Save state
        save_state(dataset_num, page)

        page += 1
        randomDelay(timeBetweenPages)





# Load state and resume from where we left off
state = load_state()
pending = state.get("pending_urls", [])
last_dataset = state.get("last_dataset")
last_page = state.get("last_page")




try:


    downloader_thread = threading.Thread(
        target=poolDownloader.downloadFromPool,
        args=(directory, downloadWorkers, timeBetweenFiles, s),
    )
    downloader_thread.start()


    start_index = 0

    if last_dataset is not None:
        try:
            start_index = datasets.index(last_dataset)
        except ValueError:
            start_index = 0

        # Get list index of where we left off (so we skip earlier datasets entirely)
    start_index = 0
    if last_dataset is not None:
        try:
            start_index = datasets.index(int(last_dataset))
        except ValueError:
            start_index = 0

    # Only iterate from the resume point onward
    for iterand in datasets[start_index:]:
        # If we’re resuming mid-dataset, use last_page, otherwise start at 0
        page_offset = last_page if iterand == last_dataset else 0

        # Set dataset info before enqueuing URLs
        poolDownloader.setDatasetInfo(iterand, page_offset)

        # Enqueue pages starting at the correct offset
        updatePool(iterand, page_offset)

        # Save resume state **after** scraping that dataset’s pages
        save_state(iterand, page_offset)
            
except KeyboardInterrupt:
    sys.exit(0)

poolDownloader.wait_for_completion() ## wait for workers to finish before exiting, allows for graceful shutdown and state saving on interrupt
poolDownloader.producerDone()  # Signal that the producer is done adding URLs
