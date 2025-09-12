# /// script
# requires-python = "==3.12.*"
# dependencies = [
#   "httpx"
# ]
# ///


"""
Computes total byte-size for a BDR collection and returns the size in both bytes and human-readable form.

Usage:
  uv run ./calc_collection_size.py --collection-pid bdr:bwehb8b8

Tweak page-size if desired (API typically caps at <= 500):
  uv run ./calc_collection_size.py --collection-pid bdr:bwehb8b8 --rows 500
"""

import argparse
import logging
import math
import os
import sys
from collections.abc import Generator
from typing import Any

import httpx

log_level_name: str = os.getenv('LOG_LEVEL', 'INFO').upper()
log_level = getattr(
    logging, log_level_name, logging.INFO
)  # maps the string name to the corresponding logging level constant; defaults to INFO
logging.basicConfig(
    level=log_level,
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S',
)
log = logging.getLogger(__name__)


## prevent httpx from logging
if log_level <= logging.INFO:
    for noisy in ('httpx', 'httpcore'):
        lg = logging.getLogger(noisy)
        lg.setLevel(logging.WARNING)  # or logging.ERROR if you prefer only errors
        lg.propagate = False  # don't bubble up to root


SEARCH_BASE = 'https://repository.library.brown.edu/api/search/'

# Hardcoded fields used for search requests
FIELDS: list[str] = ['pid', 'object_size_lsi', 'fed_object_size_lsi']


## -- secondary helper functions ------------------------------------


def human_bytes(n: int) -> str:
    """
    Formats a byte count into a human-readable string (e.g., KB, MB, GB).

    Called by `print_results()`.
    """
    if n < 1024:
        return f'{n} B'

    # Choose the next lower unit below the threshold
    # < 1 MB -> show KB; < 1 GB -> show MB; < 1 TB -> show GB; etc.
    thresholds = [
        (1024**2, 'KB', 1),  # up to MB threshold, show KB
        (1024**3, 'MB', 2),  # up to GB threshold, show MB
        (1024**4, 'GB', 3),  # up to TB threshold, show GB
        (1024**5, 'TB', 4),  # up to PB threshold, show TB
        (1024**6, 'PB', 5),  # up to EB threshold, show PB
    ]

    for upper, unit, power in thresholds:
        if n < upper:
            val = n / (1024**power)
            return f'{val:.2f} {unit}'

    # For extremely large values (>= 1 EB), show EB
    val = n / (1024**6)
    return f'{val:.2f} EB'


def fetch_search_page(
    client: httpx.Client,
    collection_pid: str,
    start: int,
    rows: int,
) -> dict[str, Any]:
    """
    Fetches one page of BDR Search API results for a collection.

    Called by `iter_collection_docs()` and `calculate_size()`.
    """
    params = {
        'q': f'rel_is_member_of_collection_ssim:"{collection_pid}"',
        'rows': rows,
        'start': start,
        'fl': ','.join(FIELDS),
    }
    r = client.get(SEARCH_BASE, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def iter_collection_docs(
    client: httpx.Client,
    collection_pid: str,
    rows: int,
    first_page: dict[str, Any] | None = None,
) -> Generator[dict[str, Any], None, int]:
    """
    Iterates over all search docs for a collection, yielding docs across pages.

    If ``first_page`` is provided, it will be used as the first page response to
    avoid a duplicate fetch (useful when the caller already requested it to get
    ``numFound``). The generator "returns" ``num_found`` via StopIteration.value,
    but typical callers just iterate docs.

    Called by `calculate_size()`.
    """
    ## processes all docs in first search response ------------------
    first = first_page or fetch_search_page(client, collection_pid, 0, rows)
    response = first.get('response', {})
    num_found = int(response.get('numFound', 0))
    total_pages = math.ceil(num_found / rows) if rows > 0 else 0
    log.debug(f'iter_collection_docs: num_found={num_found}, rows={rows}, expected_pages={total_pages}')
    docs = response.get('docs', [])
    log.info(f'processing: page=1 start=0 docs_returned={len(docs)}')
    log.debug('about to yield docs')
    yield from docs
    log.debug('yielded initial docs; about to start pagination')
    start = rows
    ## processes all docs in subsequent search responses ------------
    while start < num_found:
        page = fetch_search_page(client, collection_pid, start, rows)
        docs = page.get('response', {}).get('docs', [])
        current_page = (start // rows) + 1  # 0-based offset + 1 for human page index
        log.info(f'processing: page={current_page}, start={start} docs_returned={len(docs)}')
        if not docs:
            log.warning('iter_collection_docs: received empty docs list before reaching num_found; stopping pagination')
            break
        log.debug('about to yield docs #2')
        yield from docs
        log.debug('yielded docs #2; about to increment start')
        start += rows
    log.debug(f'iter_collection_docs: finished pagination at start={start} (num_found={num_found})')
    return num_found  # not used directly by caller (generator semantics)


## -- primary helper functions --------------------------------------


def print_results(collection_pid: str, results: dict[str, int], collection_title: str | None = None) -> None:
    """
    Prints a human-friendly summary of the collection size results.

    Called by `main()`.
    """
    print(' ')
    print(f'Collection: {collection_pid}')
    if collection_title:
        print(f'Title: {collection_title}')
    print(f'Items found: {results["num_found"]}')
    print(f'Items with size counted: {results["counted"]}')
    if results['missing']:
        print(f'Items still missing size: {results["missing"]}')
    print(f'Total bytes: {results["total_bytes"]}')
    print(f'Human: {human_bytes(results["total_bytes"])}')


def fetch_collection_title_via_collection_api(client, collection_pid: str) -> str | None:
    """
    Fetches the collection's title using the collection api.

    Called by `main()`.
    """
    url: str = f'https://repository.library.brown.edu/api/collections/{collection_pid}/'
    r: httpx.Response = client.get(url, timeout=30)
    if r.status_code == 403:
        return None
    r.raise_for_status()
    data: dict[str, Any] = r.json()
    title: str | None = data.get('name') or data.get('primary_title')
    log.debug(f'title: ``{title}``')
    return title


def calculate_size(
    collection_pid: str,
    rows: int,
) -> dict[str, int]:
    """
    Calculates total bytes for a collection and returns summary stats.

    Returns a dict with keys: num_found, counted, missing, total_bytes.
    """
    total_bytes = 0
    counted = 0
    missing = 0

    with httpx.Client(headers={'Accept': 'application/json'}) as client:
        ## get first page to learn numFound for reporting
        first: dict[str, Any] = fetch_search_page(client, collection_pid, 0, rows)
        resp: dict[str, Any] = first.get('response', {})
        num_found: int = int(resp.get('numFound', 0))
        log.info(f'num_found: {num_found}')

        ## process all docs via iterator (avoids duplicating pagination logic)
        for d in iter_collection_docs(client, collection_pid, rows, first_page=first):
            log.debug(f'processing doc-pid ``{d.get("pid")}``')
            size: int | None = d.get('object_size_lsi') or d.get('fed_object_size_lsi')
            if size is None:
                missing += 1
                log.debug(f'missing count now, ``{missing}``')
            else:
                total_bytes += int(size)
                counted += 1

    return {
        'num_found': num_found,
        'counted': counted,
        'missing': missing,
        'total_bytes': total_bytes,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Parses command-line arguments for collection PID and rows.

    Called by `main()`.
    """
    parser = argparse.ArgumentParser(description='Sum total bytes for a BDR collection.')
    parser.add_argument('--collection-pid', type=str, required=True, help='e.g., bdr:bwehb8b8')
    parser.add_argument('--rows', type=int, default=500, help='page size (max usually 500)')
    return parser.parse_args(argv)


def main() -> int:
    """
    Main controller function.

    Called by dundermain.
    """
    ## parse args ---------------------------------------------------
    args = parse_args()
    ## calculate size -----------------------------------------------
    collection_pid: str = args.collection_pid
    rows: int = args.rows
    ## output results -----------------------------------------------
    results = calculate_size(collection_pid, rows)
    # fetch title separately to preserve separation-of-concerns (collections API)
    with httpx.Client(headers={'Accept': 'application/json'}) as client:
        collection_title = fetch_collection_title_via_collection_api(client, collection_pid)
    print_results(collection_pid, results, collection_title)
    return 0


if __name__ == '__main__':
    sys.exit(main())
