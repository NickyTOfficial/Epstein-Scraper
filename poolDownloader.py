import os
import threading
import queue
import time
import requests
from rich.progress import Progress, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text

console = Console()

SENTINEL = object()

_producer_done = threading.Event()
_start_event = threading.Event()

_producer_done.clear()
_start_event.clear()
forbiddens = 0
errors = 0

def producerDone():
    _producer_done.set()

def signalStart():
    _start_event.set()

# Thread-safe pool
_pool = None
_workers = []
_worker_status = {}
_status_lock = threading.Lock()

# Header

_download_count = 0
_dataset = None
_page = None
_counter_lock = threading.Lock()

def setDatasetInfo(dataset, page):
    global _dataset, _page
    with _counter_lock:
        _dataset = dataset
        _page = page

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

def initPool():
    global _pool
    _pool = queue.Queue()


def updatePool(urls):
    for url in urls:
        _pool.put(url)

def setPool(urls):
    global _pool
    with _pool.mutex:
        _pool.queue.clear()
        for url in urls:
            _pool.queue.append(url)


def poolSize():
    return _pool.qsize()

def exportPool():
    items = []
    with _pool.mutex:
        items = list(_pool.queue)
    return items

def importPool(urls):
    for url in urls:
        _pool.put(url)

def close_pool(num_workers):

    print("Closing pool and waiting for workers to finish...")

    for _ in range(num_workers):
        _pool.put(SENTINEL)

def wait_for_completion():
    _pool.join()

def _download_worker(worker_id, out_dir, session, progress, timeBetweenFiles=10):
    # Create persistent task for this worker
    task_id = progress.add_task(f"Worker {worker_id}", total=1)
    _start_event.wait()

    while True:


        url = _pool.get()
        if url is not None: 

            if url is SENTINEL:
                _pool.task_done()
                break

            filename = os.path.basename(url)
            
            path = os.path.join(os.path.join(out_dir,f"Dataset {_dataset}"), filename)
            # ---- Pre-fetch file size via HEAD ----
            
            total = None
            try:
                head = session.head(url, allow_redirects=True, timeout=10)
                if head.status_code == 200:
                    content_length = head.headers.get("Content-Length")
                    if content_length is not None:
                        total = int(content_length)
            except Exception:
                pass

            if os.path.exists(path):
                local_size = os.path.getsize(path)

                # Try to get remote size
                remote_size = None
                try:
                    head = session.head(url, allow_redirects=True, timeout=10)
                    if head.status_code == 200:
                        cl = head.headers.get("Content-Length")
                        if cl:
                            remote_size = int(cl)
                except Exception:
                    pass

                if remote_size is not None and local_size == remote_size:
                    progress.update(
                        task_id,
                        total=remote_size,
                        completed=remote_size,
                        description=f"[yellow]W{worker_id}: Skipped (complete)[/yellow]"
                    )
                    incrementDownloadCount()
                    time.sleep(timeBetweenFiles/1000)  # Convert ms to seconds
                    _pool.task_done()
                    continue

            try:
                with session.get(url, stream=True) as r:
                    r.raise_for_status()
                    total = int(r.headers.get("Content-Length", 0))

                    # Reset this worker's progress bar
                    progress.update(
                        task_id,
                        total=total,
                        completed=0,
                        description=f"[cyan]W{worker_id}: {filename}[/cyan]"
                    )

                    os.makedirs(os.path.dirname(path), exist_ok=True)

                    with open(path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                progress.update(task_id, advance=len(chunk))

            except Exception as e:
                console.print(f"[red]Download failed:[/red] {url} ({e})")
                progress.update(task_id, advance=len(chunk)) ## file failed, move on to next one without leaving progress bar stuck at 0%

            finally:
                # Leave bar at 100%, just mark idle
                progress.update(
                    task_id,
                    description=f"[green]W{worker_id}: {filename}[/green]"
                )

                incrementDownloadCount()
                time.sleep(timeBetweenFiles/1000)  # Convert ms to seconds

                _pool.task_done()
        else:
            time.sleep(2)  # Sleep briefly if no URL, to avoid busy waiting
            



def forceShutdown():
    global _workers

    _producer_done.set()

    # Empty queue safely
    while not _pool.empty():
        try:
            _pool.get_nowait()
            _pool.task_done()
        except queue.Empty:
            break

    # Send stop signal to all workers
    for _ in _workers:
        _pool.put(SENTINEL)


def downloadFromPool(out_dir, workers=8, timeBetweenFiles=10, session=None):
    os.makedirs(out_dir, exist_ok=True)

    progress = Progress(
        "[bold blue]{task.description}",
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
    )

    global _workers

    layout = Layout()

    layout.split_column(
        Layout(name="header", size=3),
        Layout(progress, name="body")
    )

    with Live(layout, refresh_per_second=10):

        # Start workers
        for i in range(workers):
            t = threading.Thread(
                target=_download_worker,
                args=(i, out_dir, session, progress, timeBetweenFiles),
            )
            t.start()
            _workers.append(t)

        while True:
            with _counter_lock:
                header_text = Text(
                    f"Dataset: {_dataset} | Page: {_page} | Files Downloaded: {_download_count} | Pool Size: {poolSize()} | Forbiddens: {forbiddens} | Errors: {errors}",
                    style="bold white"
                )

            layout["header"].update(Panel(header_text))

            if _producer_done.is_set():
                # Check if all work is finished
                if _pool.unfinished_tasks == 0:
                    break

            time.sleep(0.2)

        _pool.join()

        # Now send SENTINEL to each worker
        for _ in _workers:
            _pool.put(SENTINEL)

        for t in _workers:
            t.join()