import os
import sys
import argparse
import concurrent.futures
from urllib.parse import urlparse, unquote

try:
    import requests
except Exception:
    print("Missing dependency 'requests'. Install with: pip install requests")
    raise

try:
    from tqdm import tqdm
    _HAS_TQDM = True
except Exception:
    _HAS_TQDM = False


def sanitize_filename_from_url(url: str) -> str:
    path = urlparse(url).path
    name = os.path.basename(path) or 'download'
    return unquote(name)


def download_one(session: requests.Session, url: str, out_dir: str, timeout: int = 30) -> tuple:
    filename = sanitize_filename_from_url(url)
    out_path = os.path.join(out_dir, filename)
    try:
        with session.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with open(out_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        return (url, True, out_path)
    except Exception as e:
        return (url, False, str(e))


def download_many(urls, out_dir, workers=8):
    os.makedirs(out_dir, exist_ok=True)
    results = []
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=workers, pool_maxsize=workers)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    if _HAS_TQDM:
        pbar = tqdm(total=len(urls), desc='Downloads')
    else:
        pbar = None

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(download_one, session, u, out_dir) for u in urls]
        for fut in concurrent.futures.as_completed(futures):
            res = fut.result()
            results.append(res)
            if pbar:
                pbar.update(1)
            else:
                print(f"{res[0]} -> {'OK' if res[1] else 'FAIL'}")

    if pbar:
        pbar.close()
    return results


def load_urls_from_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]


def main(argv=None):
    ap = argparse.ArgumentParser(description='Parallel downloader for a list of URLs')
    ap.add_argument('--urls-file', '-u', required=True, help='Path to newline-separated URLs file')
    ap.add_argument('--out', '-o', default='downloads', help='Output directory')
    ap.add_argument('--workers', '-w', type=int, default=8, help='Number of concurrent workers')
    args = ap.parse_args(argv)

    urls = load_urls_from_file(args.urls_file)
    if not urls:
        print('No URLs found in', args.urls_file)
        sys.exit(1)

    results = download_many(urls, args.out, workers=args.workers)
    failed = [r for r in results if not r[1]]
    print(f"Downloaded: {len(results)-len(failed)}  Failed: {len(failed)}")
    if failed:
        print('Failures:')
        for u, ok, info in failed:
            print('-', u, info)


if __name__ == '__main__':
    main()
