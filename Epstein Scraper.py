import json
import requests
import concurrent.futures
import magic
import os
import yaml
import mimetypes
import time
import sys
from bs4 import BeautifulSoup
import csv

datasetPattern = "https://www.justice.gov/epstein/doj-disclosures/data-set-{}-files"
filePattern = "https://www.justice.gov/epstein/files/DataSet%20{}/{}"

tryExt = [ # alternate file extensions to use in case a pdf shows "No Images Produced", in order of occurance
    ".avi",
    ".mp4",
    ".mov",
    ".m4a",
    ".m4v",
    ".opus",
    ".wav",
    ".docx",
    ".vob",
    ".wmv",
    ".mp3",
    ".amr",
    ".doc",
    ".3gp",
    ".ts",
    ".xls",
    ".db",
    ".pluginpayloadattachment"
]




# setup code

try:
    config = yaml.safe_load(open("config.yaml"))
except:
    data = {
        "directory" : os.getcwd(), ## output directory
        "timeBetweenPages" : 20, ## time to delay between page requests in ms
        "fetchRetries" : 5, ## number of times to retry a request if it fails
        "fetchDelay" : 3, ## time to delay between fetch retries in seconds
        "timeBetweenFiles" : 0.1, ## time to delay between file downloads in ms
        "downloadWorkers": 8, ## number of concurrent download workers
        "datasets" : [1,2,3,4] ## datasets to scrape as an array # type: ignore
    }
    with open('config.yaml','w') as file:
        yaml.dump(data, file)
    try: 
        config = yaml.safe_load(open("config.yaml"))
    except: 
        print("file exception when handling config")
        os._exit()




directory = config.get("directory")
timeBetweenPages = float(config.get("timeBetweenPages", "20"))
fetchRetries = int(config.get("fetchRetries", "5"))
fetchDelay = float(config.get("fetchDelay", "3"))
timeBetweenFiles = float(config.get("timeBetweenFiles", "0.1"))
datasets = config.get("datasets", [1])
downloadWorkers = int(config.get("downloadWorkers", 8))
os.makedirs(directory, exist_ok=True)


# set up a persistent session to improve network traffic
s = requests.Session()

s.headers.update({ ## Simulating a browser to increase authenticity of requests, reducing scraper detection
    
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
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

def alternate_file(url):

    for ext in tryExt:

        altUrl = url.replace(".pdf", ext)
        altFile = s.get(altUrl)

        if(altFile.status_code == 200 and len(altFile.content) > 1000):
            return altFile
           
    return None
    return None

def download_file(url, output_folder = "", filename = None):

    EFTAfile = s.get(url)

    if(b"ReportLab Generated PDF document" in EFTAfile.content ):
        altFile = alternate_file(url)

        if(altFile is not None):
            EFTAfile = altFile
        else:
            return False
           

    if(b"https://www.justice.gov/doj/page-not-found" in EFTAfile.content):
        return False

    content = EFTAfile.content
    extension = mimetypes.guess_extension(magic.from_buffer(EFTAfile.content, mime=True)) ## Use MIME to identify filetype and use the extensionMap dictionary to convert it to the proper extension

    path = os.path.join(os.path.join(directory, output_folder),filename)

    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "wb") as f:
        f.write(content) 

    return True


#---------------#

def scrape_dataset(dataset_num, dataset_page = 0):
    page = dataset_page

    files = set()
    dataset_failed = []

    # write initial single-line progress for this dataset
    initial_line = f"Dataset {dataset_num} | 0/0 files downloaded on page 0 | 0 total files downloaded"
    sys.stdout.write(initial_line)
    sys.stdout.flush()

    while True:
        url = datasetPattern.format(dataset_num) + f"?page={page}"
        r = fetch_with_retry(url, s)
        if r is None:
            print(f"Failed to fetch {url} after multiple attempts.")
            break
        soup = BeautifulSoup(r.text, "html.parser")
        page_files = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/epstein/files/" in href and "EFTA" in href:
                filename = href.split("/")[-1]
                page_files.add(filename)
        if page_files.issubset(files):
            break
        page_files = sorted(page_files)
        # number of files found on this page
        #print(f"Found {len(page_files)} files on page {page}")

        # go through and download files for the current page in parallel
        new_files = [f for f in page_files if f not in files]
        if new_files:
            jobs = [(filePattern.format(dataset_num, filename), filename) for filename in new_files]
            with concurrent.futures.ThreadPoolExecutor(max_workers=downloadWorkers) as ex:
                future_to_name = {ex.submit(download_file, url, f"Dataset {dataset_num}", name): name for url, name in jobs}
                page_total = len(future_to_name)
                completed = 0

                # update line to show new page and page_total
                sys.stdout.write(f"\rDataset {dataset_num} | 0/{page_total} files downloaded on page {page} | {len(files)} total files downloaded")
                sys.stdout.flush()

                for fut in concurrent.futures.as_completed(future_to_name):
                    name = future_to_name[fut]
                    try:
                        success = fut.result()
                    except Exception:
                        success = False
                        dataset_failed.append(name)

                    completed += 1
                    if success:
                        files.add(name)
                    else:
                        if name not in dataset_failed:
                            dataset_failed.append(name)

                    # update single-line progress (per dataset)
                    sys.stdout.write(f"\rDataset {dataset_num} | {completed}/{page_total} files downloaded on page {page} | {len(files)} total files downloaded")
                    sys.stdout.flush()

        page += 1
        # Save state after each page completes
        save_state(dataset_num, page - 1)
        time.sleep(timeBetweenPages / 1000.0)  # Sleep for configured timeout to avoid overwhelming server
    # finished dataset -- print final newline and any failure summary
    print()
    if dataset_failed:
        short = ', '.join(dataset_failed[:10])
        more = '...' if len(dataset_failed) > 10 else ''
        print(f"Failed to download {len(dataset_failed)} files in dataset {dataset_num}: {short}{more}")


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

for iterand in datasets:
    # Skip datasets that have already been completed
    if last_dataset is not None and iterand < last_dataset:
        continue
    
    scrape_dataset(iterand, last_page if iterand == last_dataset else 0)
    
    # Update state after each dataset completes
    save_state(iterand, 0)
