import os
import random
import threading
import queue
import time
import requests
from rich.progress import Progress, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
from rich.live import Live
from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text


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

    ## some more filetypes, haven't been found yet but are common

    ".txt",
    ".csv",
    ".json",
    ".xml",
    ".log",
    ".sql",
    ".bak",
    ".zip",
    ".rar",
    ".7z",
    ".tar",
    ".gz",
    ".exe",
    ".dll",
    ".bin",
    ".iso",
    ".html",
    ".htm",
    ".jpg",
    ".jpeg",
    ".png",
    ".eml",
    ".cfg",
    "xlsx",
    ".pptx",


]

failed_log = os.path.join("logs", "failed_downloads.log")
alt_log = os.path.join("logs", "alternates.log")

def randomDelay(delay):
    delay = delay / 1000  # convert ms to seconds
    time.sleep(delay * (0.1 + random.random()))  # add some randomness to the delay to further reduce scraper detection

def alternateUrl(poolObject, session, timeBetweenFiles):

    
    _dataset = poolObject[0]
    _filepage = poolObject[1]
    _url = poolObject[2]

    for ext in tryExt:
        altUrl = _url.replace(".pdf", ext)
        try:
            r = session.head(altUrl, allow_redirects=True, timeout=5)
        except requests.RequestException:
            continue

        if r.status_code == 200:
            log_event(
                alt_log,
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Alternate found, Dataset {_globalDataset} | Page {_filepage} | {altUrl}"
            )
            return (_dataset, _filepage, altUrl)  # return as tuple with page and dataset info for state saving

        # Explicitly ignore rate limiting and forbidden during probing
        if r.status_code in (403, 429, 503):
            randomDelay(500)  # Random delay for rate limiting errors
            continue
        randomDelay(timeBetweenFiles)    

    incrementUnknownAlternateCount()

    log_event(
        alt_log,
        f"{time.strftime('%Y-%m-%d %H:%M:%S')} | No alternate found, Dataset {_globalDataset} | Page {_filepage} | {_url}"
    )

    return None



console = Console()

SENTINEL = object()

_producer_done = threading.Event()
_start_event = threading.Event()

_producer_done.clear()
_start_event.clear()

_log_lock = threading.Lock()

def producerDone():
    _producer_done.set()

def signalStart():
    _start_event.set()

def isStarted():
    return _start_event.is_set()

# Thread-safe pool
_pool = queue.Queue()
_workers = []
# Header

_download_count = 0
_globalDataset = None
_globalPage = None
_counter_lock = threading.Lock()
_lastLocation = (-1,-1)


def setLastLocation(lastLocation):
    global _lastLocation
    with _counter_lock:
        _lastLocation = lastLocation

def getLastLocation(): 
    return (_lastLocation)


errors = 0
forbiddens = 0
alternateCount = 0
unknownAlternateCount = 0

def head_with_retry(session, url, retries=3, base_delay=0.5):
    for attempt in range(retries):
        try:
            r = session.head(url, allow_redirects=True, timeout=5)

            # Success
            if r.status_code == 200:
                return r

            # Rate limiting or temporary denial
            if r.status_code in (403, 429, 503):
                randomDelay(base_delay )
                continue

            # Other non-200 responses return immediately
            return r

        except requests.RequestException:
            randomDelay(base_delay)

    return None



def setDatasetInfo(dataset, globalPage):
    global _globalDataset, _globalPage
    with _counter_lock:
        _globalDataset = dataset
        _globalPage = globalPage

def log_event(log_path, message):
    with _log_lock:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(message + "\n")

def incrementDownloadCount():
    global _download_count
    with _counter_lock:
        _download_count += 1

def incrementForbiddenCount():   
    global forbiddens
    with _counter_lock:
        forbiddens += 1

def incrementErrorCount():   
    global errors
    with _counter_lock:
        errors += 1

def incrementAlternateCount():
    global alternateCount
    with _counter_lock:
        alternateCount += 1

def incrementUnknownAlternateCount():
    global unknownAlternateCount
    with _counter_lock:
        unknownAlternateCount += 1


def updatePool(poolObjects): ## force as tuple
    _poolObjects = [(obj[0], obj[1], obj[2]) for obj in poolObjects] # ensure it's a list of triples
    for obj in _poolObjects:
        _pool.put(obj)


def poolSize():
    if _pool is not None:
        return _pool.qsize()
    else:
        return 0
    


def empty_pool(numWorkers):

    ## Immediately remove all pending items from the pool

    removed = 0

    while True:
        try:
            _pool.get_nowait()
            _pool.task_done()
            removed += 1
        except queue.Empty:
            break

    for _ in _workers:
        _pool.put(SENTINEL)

    for t in _workers:
        t.join()

    return removed

def wait_for_completion():
    _pool.join()

def _download_worker(worker_id, out_dir, session, progress, timeBetweenFiles):

    task_id = progress.add_task(f"Worker {worker_id}", total=1)
    _start_event.wait()



    while True:

        try:   
            poolObject = _pool.get(timeout=1)  # get the tuple (url, page)
        except queue.Empty:
            continue

        if poolObject is SENTINEL:
            _pool.task_done()
            break


        _dataset = poolObject[0]
        _filepage = poolObject[1]
        _url = poolObject[2]

        setLastLocation((_dataset,_filepage))

        filename = os.path.basename(_url)
        path = os.path.join(out_dir, f"Dataset {_dataset}", filename)

        # ---- Skip logic ----
        if os.path.exists(path):
            try:
                head = head_with_retry(session, _url, retries=5, base_delay=1)
                if head.status_code == 200:
                    remote_size = head.headers.get("Content-Length")
                    if remote_size:
                        remote_size = int(remote_size)
                        local_size = os.path.getsize(path)

                        # Skip if identical and not the small "No Images Produced" PDF
                        if (
                            local_size == remote_size and
                            not (0.9 < remote_size / 2433 < 1.1)
                        ):
                            progress.update(
                                task_id,
                                total=remote_size,
                                completed=remote_size,
                                description=f"[yellow]W{worker_id}: {filename}[/yellow]"
                            )
                            incrementDownloadCount()
                            _pool.task_done()
                            continue
            except Exception:
                pass

        try:
            with session.get(_url, stream=True) as r:
                r.raise_for_status()
                total = int(r.headers.get("Content-Length", 0))

                progress.update(
                    task_id,
                    total=total,
                    completed=0,
                    description=f"[cyan]W{worker_id}: {filename}[/cyan]"
                )

                os.makedirs(os.path.dirname(path), exist_ok=True)

                bytes_written = 0
                with open(path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if not chunk:
                            continue
                        f.write(chunk)
                        bytes_written += len(chunk)
                        progress.update(task_id, completed=bytes_written)

        except Exception as e:
            incrementErrorCount()
            progress.update(
                task_id,
                description=f"[red]W{worker_id}: {filename}[/red]"
            )

            log_event(
                failed_log,
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Dataset {_dataset} | Page {_filepage} | {_url} | {type(e).__name__} | {str(e)}"
            )

            _pool.task_done()
            continue

        # ---- Download complete at this point ----
        progress.update(
            task_id,
            description=f"[green]W{worker_id}: {filename}[/green]"
        )

        setLastLocation((_dataset,_filepage))
        incrementDownloadCount()

        # Mark task complete immediately
        _pool.task_done()

        # ---- Post-processing OUTSIDE critical path ----
        try:
            with open(path, "rb") as f:
                header = f.read(4096)

            if header.startswith(b"%PDF") and b"ReportLab PDF Library" in header:
                altObject = alternateUrl(poolObject, session, timeBetweenFiles)
                if altObject:
                    incrementAlternateCount()
                    _pool.put(altObject)  # add alternate to pool with page info for state saving

        except Exception:
            pass


        finally:
            if timeBetweenFiles > 0:
                randomDelay(timeBetweenFiles)



def downloadFromPool(out_dir, workers=8, timeBetweenFiles=10, session=None):
    os.makedirs(out_dir, exist_ok=True)

    progress = Progress(
        "[bold blue]{task.description}",
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
    )

    log_dir = os.path.join("logs")
    os.makedirs(log_dir, exist_ok=True)

    global _workers

    layout = Layout()


    layout.split_column(
        Layout(name="header", size=3),
        Layout(progress, name="body")
    )

    with Live(layout, refresh_per_second=30):

        # Start workers
        for i in range(workers):
            t = threading.Thread(
                target=_download_worker,
                args=(i, out_dir, session, progress, timeBetweenFiles),
            )
            t.start()
            _workers.append(t)

        while any(t.is_alive() for t in _workers):
            with _counter_lock:
                header_text = Text(
                    f"Dataset: {_globalDataset} | Page: {_globalPage} | Files Downloaded: {_download_count} | Pool Size: {poolSize()} | Forbiddens: {forbiddens} | Errors: {errors} | Alternates: {alternateCount} | Unknown Alternates: {unknownAlternateCount}",
                    style="bold white"
                )

            layout["header"].update(Panel(header_text))
            layout["body"].size = min(workers, 16)

            randomDelay(200)  # Random delay to avoid busy waiting

