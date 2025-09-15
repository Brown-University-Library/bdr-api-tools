# /// script
# requires-python = "==3.12.*"
# dependencies = [
#   "httpx",
#   "tqdm",
#   "humanize"
# ]
# ///

"""
Collects extracted_text for a collection.
It's server-friendly, in that it makes synchronous requests with a slight sleep, 
  and saves progress after every item so it can be resumed after a network failure 
  and will continue from where it left off.

Usage:
  uv run ./gather_extracted_text.py --collection-pid bdr:bfttpwkj --output-dir "../output_dir" 

Args:
  --collection-pid (required)
  --output-dir (required)
  --test-limit (optional) -- convenient for testing

Note: 
- this script works, but consists of 30 functions. It'll soon be refactored to use classes for more sane organization.
"""

import argparse
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx
import humanize
from tqdm import tqdm

## setup logging
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


BASE = 'https://repository.library.brown.edu'
SEARCH_URL = f'{BASE}/api/search/'
ITEM_URL_TPL = f'{BASE}/api/items/{{pid}}/'
STORAGE_URL_TPL = f'{BASE}/storage/{{pid}}/EXTRACTED_TEXT/'
COLLECTION_URL_TPL = f'{BASE}/api/collections/{{pid}}/'


def _now_iso() -> str:
    """
    Returns an ISO-8601 local timestamp with timezone info.
    """
    return datetime.now().astimezone().isoformat()


def _now_compact_local() -> str:
    """
    Returns a filesystem-safe local timestamp like YYYYmmddTHHMMSS-0500
    (offset varies by local timezone). Useful for naming directories.
    """
    return datetime.now().astimezone().strftime('%Y%m%dT%H%M%S%z')


def _sleep(backoff_s: float) -> None:
    """
    Sleeps for given seconds; centralizes sleep for easier tweaking.
    """
    time.sleep(backoff_s)


def _retrying_get(client: httpx.Client, url: str, *, max_tries: int = 4, timeout_s: float = 30.0) -> httpx.Response:
    """
    Performs a GET with simple exponential backoff on transient failures.
    """
    last_exc: Exception | None = None
    for attempt in range(1, max_tries + 1):
        try:
            # throttle each request to reduce server load
            _sleep(0.2)
            resp: httpx.Response = client.get(url, timeout=timeout_s, follow_redirects=True)
            if resp.status_code >= 500:
                raise httpx.HTTPStatusError(f'server error {resp.status_code}', request=resp.request, response=resp)
            return resp
        except (httpx.HTTPError, httpx.TransportError) as exc:
            last_exc = exc
            _sleep(min(2 ** attempt, 15))
    assert last_exc is not None
    raise last_exc


def _retrying_stream_text(client: httpx.Client, url: str, *, max_tries: int = 4, timeout_s: float = 60.0) -> str:
    """
    Streams a text response with retries and returns it as a string.
    """
    last_exc: Exception | None = None
    for attempt in range(1, max_tries + 1):
        try:
            # throttle each request to reduce server load
            _sleep(0.2)
            with client.stream('GET', url, timeout=timeout_s, follow_redirects=True) as resp:
                resp.raise_for_status()
                chunks: list[str] = []
                for chunk in resp.iter_text():
                    if chunk:
                        chunks.append(chunk)
                return ''.join(chunks)
        except (httpx.HTTPError, httpx.TransportError) as exc:
            last_exc = exc
            _sleep(min(2 ** attempt, 15))
    assert last_exc is not None
    raise last_exc


def search_collection_pids(client: httpx.Client, collection_pid: str) -> list[dict[str, object]]:
    """
    Uses search-api to list items in a collection, returning docs with pid & primary_title.
    Minimizes calls by paging via rows/start.
    """
    rows: int = 500
    start: int = 0
    docs: list[dict[str, object]] = []
    # Filter by collection membership; request just needed fields
    fq: str = f'rel_is_member_of_collection_ssim:"{collection_pid}"'
    fl: str = 'pid,primary_title'
    while True:
        url: str = f'{SEARCH_URL}?q=*:*&fq={httpx.QueryParams({"fq": fq})["fq"]}&fl={fl}&rows={rows}&start={start}'
        log.debug( f' trying search url, ``{url}``')
        resp: httpx.Response = _retrying_get(client, url)
        data: dict[str, object] = resp.json()
        response: dict[str, object] = data.get('response', {})  # type: ignore[assignment]
        page_docs: list[dict[str, object]] = response.get('docs', [])  # type: ignore[assignment]
        if not page_docs:
            break
        docs.extend(page_docs)
        num_found: int = int(response.get('numFound', 0))
        start += rows
        if start >= num_found:
            break
    return docs


def fetch_item_json(client: httpx.Client, pid: str) -> dict[str, object]:
    """
    Fetches item-api json for a pid.
    """
    url: str = ITEM_URL_TPL.format(pid=pid)
    log.debug( f'trying item url, ``{url}``')
    resp: httpx.Response = _retrying_get(client, url)
    resp.raise_for_status()
    return resp.json()


def fetch_collection_json(client: httpx.Client, pid: str) -> dict[str, object]:
    """
    Fetches collection-api json for a collection pid.
    """
    url: str = COLLECTION_URL_TPL.format(pid=pid)
    resp: httpx.Response = _retrying_get(client, url)
    resp.raise_for_status()
    return resp.json()


def collection_title_from_json(coll_json: dict[str, object]) -> str:
    """
    Computes a human-friendly collection title from a collection's item-api JSON.
    Uses the collection's `name` and, when available, the last ancestor's name/title
    to append a source in the form " -- (from {parent})".
    """
    coll_name: str = coll_json.get('name') or ''  # type: ignore[assignment]
    parent_name: str = ''
    ancestors: object = coll_json.get('ancestors')
    if isinstance(ancestors, list) and ancestors:
        last: object = ancestors[-1]
        if isinstance(last, dict):
            parent_name = last.get('name') or last.get('title') or ''  # type: ignore[assignment]
        elif isinstance(last, str):
            parent_name = last
    coll_title: str = ''
    if coll_name and parent_name:
        coll_title = f"{coll_name} -- (from {parent_name})"
    else:
        coll_title = coll_name or ''
    return coll_title


def _extract_child_pids(item_json: dict[str, object]) -> list[str]:
    """
    Extracts child pids from relations.hasPart, supporting list[str] or list[dict].
    """
    rels: dict[str, object] = item_json.get('relations', {})  # type: ignore[assignment]
    has_part: list[object] = rels.get('hasPart', [])  # type: ignore[assignment]
    child_pids: list[str] = []
    for entry in has_part:
        if isinstance(entry, str):
            child_pids.append(entry)
        elif isinstance(entry, dict):
            pid_val: object = entry.get('pid') or entry.get('id')
            if isinstance(pid_val, str):
                child_pids.append(pid_val)
    return child_pids


def _find_extracted_text_link_and_size(item_json: dict[str, object], pid: str) -> tuple[str, int | None] | None:
    """
    Locates EXTRACTED_TEXT download URL and size if available.
    Checks links.content_datastreams, then links.datastreams, then datastreams size + constructs URL.
    """
    links: dict[str, object] = item_json.get('links', {})  # type: ignore[assignment]
    content_ds: dict[str, object] = links.get('content_datastreams', {}) or {}  # type: ignore[assignment]
    if 'EXTRACTED_TEXT' in content_ds and isinstance(content_ds['EXTRACTED_TEXT'], str):
        url: str = content_ds['EXTRACTED_TEXT']  # type: ignore[index]
        size: int | None = _extract_size_from_datastreams(item_json)
        return (url, size)

    # some records expose under links.datastreams
    alt_ds: dict[str, object] = links.get('datastreams', {}) or {}  # type: ignore[assignment]
    if 'EXTRACTED_TEXT' in alt_ds and isinstance(alt_ds['EXTRACTED_TEXT'], str):
        url = alt_ds['EXTRACTED_TEXT']  # type: ignore[index]
        size = _extract_size_from_datastreams(item_json)
        return (url, size)

    # last resort: datastreams block with size; construct canonical storage URL
    ds_block: dict[str, object] = item_json.get('datastreams', {}) or {}  # type: ignore[assignment]
    if 'EXTRACTED_TEXT' in ds_block and isinstance(ds_block['EXTRACTED_TEXT'], dict):
        url = STORAGE_URL_TPL.format(pid=pid)
        size: object = ds_block['EXTRACTED_TEXT'].get('size')  # type: ignore[index]
        size_int: int | None = int(size) if isinstance(size, int) or (isinstance(size, str) and size.isdigit()) else None
        return (url, size_int)

    return None


def _extract_size_from_datastreams(item_json: dict[str, object]) -> int | None:
    """
    Extracts EXTRACTED_TEXT size from datastreams block if present.
    """
    ds_block: dict[str, object] = item_json.get('datastreams', {}) or {}  # type: ignore[assignment]
    entry: object = ds_block.get('EXTRACTED_TEXT')
    if isinstance(entry, dict):
        val: object = entry.get('size')
        if isinstance(val, int):
            return val
        if isinstance(val, str) and val.isdigit():
            return int(val)
    return None


def ensure_dir(path: Path) -> None:
    """
    Ensures directory exists.
    """
    path.mkdir(parents=True, exist_ok=True)


def load_listing(path: Path) -> dict[str, object]:
    """
    Loads listing json if present; otherwise returns initial structure.
    """
    if path.exists():
        with path.open('r', encoding='utf-8') as fh:
            loaded: dict[str, object] = json.load(fh)
            return loaded
    return {
        'summary': {
            'timestamp': _now_iso(),
            'all_extracted_text_file_size': '0 Bytes',
            'count_of_all_extracted_text_files': 0,
            # paths recorded as "parent-dir/filename" (no full absolute path)
            'combined_text_path': '',
            'listing_path': '',
            # collection metadata
            'collection_pid': '',
            'collection_primary_title': '',
        },
        'items': []
    }


def save_listing(path: Path, listing: dict[str, object]) -> None:
    """
    Saves listing as pretty JSON.
    """
    listing['summary']['timestamp'] = _now_iso()
    with path.open('w', encoding='utf-8') as fh:
        json.dump(listing, fh, ensure_ascii=False, indent=2)


def append_text(out_txt_path: Path, pid: str, text: str) -> None:
    """
    Appends prefixed text for a pid to the combined text file.
    """
    prefix = f'---|||start-of-pid:{pid}|||---\n'
    with out_txt_path.open('a', encoding='utf-8') as fh:
        fh.write(prefix)
        fh.write(text.rstrip('\n'))
        fh.write('\n')


def already_processed(listing: dict[str, object]) -> set[str]:
    """
    Returns set of PIDs already in listing items.
    """
    done: set[str] = set()
    for item in listing.get('items', []):
        pid: object = item.get('item_pid')  # type: ignore[index]
        if isinstance(pid, str):
            done.add(pid)
    return done


def add_listing_entry(listing: dict[str, object], *, item_pid: str, primary_title: str, full_item_api_url: str, full_studio_url: str, extracted_text_file_size: int | None) -> None:
    """
    Adds or replaces a listing entry for an item_pid.
    """
    items: list[dict[str, object]] = listing.setdefault('items', [])  # type: ignore[assignment]
    # replace if exists (idempotent)
    idx: int | None = next((i for i, d in enumerate(items) if d.get('item_pid') == item_pid), None)
    # store human-readable size to align with summary
    human_size: str | None = humanize.naturalsize(extracted_text_file_size) if isinstance(extracted_text_file_size, int) else None
    entry: dict[str, object] = {
        'item_pid': item_pid,
        'primary_title': primary_title,
        'full_item_api_url': full_item_api_url,
        'full_studio_url': full_studio_url,
        'extracted_text_file_size': human_size,
    }
    if idx is None:
        items.append(entry)
    else:
        items[idx] = entry


def _parent_dir_and_name(p: Path) -> str:
    """
    Returns a string formatted as "parent-dir/filename" for the given Path.
    The parent is just the immediate directory name, not the full path.
    """
    return f"{p.parent.name}/{p.name}"


def update_summary(listing: dict[str, object], combined_path: Path, listing_path: Path) -> None:
    """
    Updates summary block based on current items and combined text size.
    """
    count: int = sum(1 for d in listing.get('items', []) if d.get('extracted_text_file_size'))
    size: int = combined_path.stat().st_size if combined_path.exists() else 0
    listing['summary']['count_of_all_extracted_text_files'] = count
    # remove deprecated keys if present
    listing['summary'].pop('all_extracted_text_file_size_bytes', None)
    listing['summary'].pop('all_extracted_text_file_size_human', None)
    listing['summary']['all_extracted_text_file_size'] = humanize.naturalsize(size)
    listing['summary']['timestamp'] = _now_iso()
    # store paths in parent-dir/filename form (no full absolute path)
    listing['summary']['combined_text_path'] = _parent_dir_and_name(combined_path)
    listing['summary']['listing_path'] = _parent_dir_and_name(listing_path)


def processed_set_from_listing(listing: dict[str, object]) -> set[str]:
    """
    Computes set of item_pids present in listing items.
    """
    pids: set[str] = set()
    for d in listing.get('items', []):
        pid: object = d.get('item_pid')  # type: ignore[index]
        if isinstance(pid, str):
            pids.add(pid)
    return pids


def counts_from_listing(listing: dict[str, object], *, total_docs: int = 0) -> dict[str, int]:
    """
    Computes summary counts from listing items.
    """
    items: list[dict[str, object]] = listing.get('items', [])  # type: ignore[assignment]
    processed_count: int = len({d.get('item_pid') for d in items if isinstance(d.get('item_pid'), str)})
    appended_count: int = sum(1 for d in items if d.get('extracted_text_file_size'))
    no_text_count: int = sum(1 for d in items if d.get('extracted_text_file_size') in (None, ''))
    forbidden_count: int = sum(1 for d in items if d.get('status') == 'forbidden')
    return {
        'total_docs': total_docs,
        'processed_count': processed_count,
        'appended_count': appended_count,
        'no_text_count': no_text_count,
        'forbidden_count': forbidden_count,
    }


def _run_dir_name_for(safe_collection_pid: str) -> str:
    """
    Returns a timestamped run directory name for a safe collection pid.
    """
    return f'run-{_now_compact_local()}-{safe_collection_pid}'


def _is_run_dir_for(path: Path, safe_collection_pid: str) -> bool:
    """
    Checks whether a path appears to be a run directory for the safe collection pid.
    """
    name: str = path.name
    return name.startswith('run-') and name.endswith(f'-{safe_collection_pid}') and path.is_dir()


def find_latest_prior_run_dir(out_dir: Path, safe_collection_pid: str) -> Path | None:
    """
    Finds the latest prior run directory for this collection pid that appears usable.
    Prefers an incomplete checkpoint when available; otherwise uses presence of a listing file.
    """
    candidates: list[Path] = [p for p in out_dir.iterdir() if _is_run_dir_for(p, safe_collection_pid)]
    if not candidates:
        return None
    # sort descending by directory name (timestamp prefix ensures correct order locally)
    candidates.sort(key=lambda p: p.name, reverse=True)
    latest: Path = candidates[0]
    ck: Path = latest / f'checkpoint_for_collection_pid-{safe_collection_pid}.json'
    listing_p: Path = latest / f'listing_for_collection_pid-{safe_collection_pid}.json'
    if not ck.exists():
        return None
    try:
        with ck.open('r', encoding='utf-8') as fh:
            data: dict[str, object] = json.load(fh)
        if not bool(data.get('completed', False)) and listing_p.exists():
            return latest
    except Exception:
        return None
    return None


def copy_prior_outputs(prior_dir: Path, new_dir: Path, safe_collection_pid: str) -> None:
    """
    Copies combined text and listing JSON from a prior run into the new run directory.
    """
    prior_combined: Path = prior_dir / f'extracted_text_for_collection_pid-{safe_collection_pid}.txt'
    prior_listing: Path = prior_dir / f'listing_for_collection_pid-{safe_collection_pid}.json'
    new_combined: Path = new_dir / prior_combined.name
    new_listing: Path = new_dir / prior_listing.name
    if prior_combined.exists():
        shutil.copy2(prior_combined, new_combined)
    if prior_listing.exists():
        shutil.copy2(prior_listing, new_listing)


def save_checkpoint(checkpoint_path: Path, *, collection_pid: str, safe_collection_pid: str, run_directory_name: str, listing: dict[str, object], combined_path: Path, listing_path: Path, total_docs: int, completed: bool) -> None:
    """
    Saves minimal checkpoint JSON with counts and paths.
    """
    existing: dict[str, object] | None = None
    if checkpoint_path.exists():
        try:
            with checkpoint_path.open('r', encoding='utf-8') as fh:
                existing = json.load(fh)
        except Exception:
            existing = None
    created_at: str = existing.get('created_at') if isinstance(existing, dict) and isinstance(existing.get('created_at'), str) else _now_iso()
    counts: dict[str, int] = counts_from_listing(listing, total_docs=total_docs)
    data: dict[str, object] = {
        'collection_pid': collection_pid,
        'safe_collection_pid': safe_collection_pid,
        'created_at': created_at,
        'updated_at': _now_iso(),
        'run_directory_name': run_directory_name,
        'completed': completed,
        'counts': counts,
        'paths': {
            'combined_text': _parent_dir_and_name(combined_path),
            'listing_json': _parent_dir_and_name(listing_path),
        },
    }
    with checkpoint_path.open('w', encoding='utf-8') as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)


def process_pid_for_extracted_text(client: httpx.Client, pid: str, out_txt_path: Path, listing: dict[str, object]) -> bool:
    """
    Processes a pid:
    - fetches item json
    - finds EXTRACTED_TEXT link (or checks children)
    - appends text to combined file
    - updates listing
    Returns True if appended, else False.
    """
    item_json: dict[str, object] = fetch_item_json(client, pid)
    primary_title: str = item_json.get('primary_title') or item_json.get('mods_title_full_primary_tsi') or ''  # type: ignore[assignment]
    studio_url: str = item_json.get('uri') or f'{BASE}/studio/item/{pid}/'  # type: ignore[assignment]
    item_api_url: str = ITEM_URL_TPL.format(pid=pid)

    found: tuple[str, int | None] | None = _find_extracted_text_link_and_size(item_json, pid)
    if found:
        url, size = found
        try:
            text: str = _retrying_stream_text(client, url)
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == 403:
                add_listing_entry(
                    listing,
                    item_pid=pid,
                    primary_title=primary_title,
                    full_item_api_url=item_api_url,
                    full_studio_url=studio_url,
                    extracted_text_file_size=None,
                )
                # mark forbidden for counts via optional status
                listing['items'][-1]['status'] = 'forbidden'  # type: ignore[index]
                return False
            raise
        append_text(out_txt_path, pid, text)
        add_listing_entry(
            listing,
            item_pid=pid,
            primary_title=primary_title,
            full_item_api_url=item_api_url,
            full_studio_url=studio_url,
            extracted_text_file_size=size,
        )
        return True

    # try children via hasPart
    child_pids: list[str] = _extract_child_pids(item_json)
    for child_pid in child_pids:
        child_json: dict[str, object] = fetch_item_json(client, child_pid)
        child_title: str = child_json.get('primary_title') or child_json.get('mods_title_full_primary_tsi') or ''  # type: ignore[assignment]
        child_studio_url: str = child_json.get('uri') or f'{BASE}/studio/item/{child_pid}/'  # type: ignore[assignment]
        child_api_url: str = ITEM_URL_TPL.format(pid=child_pid)
        child_found: tuple[str, int | None] | None = _find_extracted_text_link_and_size(child_json, child_pid)
        if child_found:
            url, size = child_found
            try:
                text = _retrying_stream_text(client, url)
            except httpx.HTTPStatusError as exc:
                if exc.response is not None and exc.response.status_code == 403:
                    add_listing_entry(
                        listing,
                        item_pid=child_pid,
                        primary_title=child_title,
                        full_item_api_url=child_api_url,
                        full_studio_url=child_studio_url,
                        extracted_text_file_size=None,
                    )
                    listing['items'][-1]['status'] = 'forbidden'  # type: ignore[index]
                    # also mark parent as handled via child (forbidden)
                    add_listing_entry(
                        listing,
                        item_pid=pid,
                        primary_title=primary_title,
                        full_item_api_url=item_api_url,
                        full_studio_url=studio_url,
                        extracted_text_file_size=None,
                    )
                    listing['items'][-1]['status'] = 'forbidden_via_child'  # type: ignore[index]
                    return False
                raise
            append_text(out_txt_path, child_pid, text)
            add_listing_entry(
                listing,
                item_pid=child_pid,
                primary_title=child_title,
                full_item_api_url=child_api_url,
                full_studio_url=child_studio_url,
                extracted_text_file_size=size,
            )
            # also add an entry for the parent to indicate it was handled via child
            add_listing_entry(
                listing,
                item_pid=pid,
                primary_title=primary_title,
                full_item_api_url=item_api_url,
                full_studio_url=studio_url,
                extracted_text_file_size=None,
            )
            listing['items'][-1]['status'] = 'handled_via_child'  # type: ignore[index]
            return True

    # no extracted text found
    add_listing_entry(
        listing,
        item_pid=pid,
        primary_title=primary_title,
        full_item_api_url=item_api_url,
        full_studio_url=studio_url,
        extracted_text_file_size=None,
    )
    return False


def parse_args() -> argparse.Namespace:
    """
    Parses and returns command-line arguments for this script.
    """
    parser = argparse.ArgumentParser(description='Collect EXTRACTED_TEXT for a collection.')
    parser.add_argument('--collection-pid', required=True, help='Collection PID like bdr:c9fzffs9')
    parser.add_argument('--output-dir', required=True, help='Directory to write outputs')
    parser.add_argument(
        '--test-limit',
        type=int,
        default=None,
        metavar='INTEGER',
        help='Optional. Stop after this many extracted_texts have been successfully appended (useful for testing).'
    )
    return parser.parse_args()


def main() -> int:
    """
    Fetches collection members, finds EXTRACTED_TEXT, writes combined text and JSON listing with resume support.
    """
    args: argparse.Namespace = parse_args()

    collection_pid: str = args.collection_pid.strip()
    safe_collection_pid: str = collection_pid.replace(":", "_")
    out_dir: Path = Path(args.output_dir).expanduser().resolve()
    ensure_dir(out_dir)

    # Determine whether to resume from a prior run BEFORE creating the new run directory
    prior_dir: Path | None = find_latest_prior_run_dir(out_dir, safe_collection_pid)

    # create a timestamped subdirectory within the output directory for this run
    ts_dir_name: str = _run_dir_name_for(safe_collection_pid)
    ts_dir: Path = out_dir / ts_dir_name
    ensure_dir(ts_dir)

    # output files
    combined_txt_path: Path = ts_dir / f'extracted_text_for_collection_pid-{safe_collection_pid}.txt'
    listing_json_path: Path = ts_dir / f'listing_for_collection_pid-{safe_collection_pid}.json'
    checkpoint_json_path: Path = ts_dir / f'checkpoint_for_collection_pid-{safe_collection_pid}.json'

    # attempt resume from latest prior run (skipped if --test-limit provided)
    if prior_dir is not None:
        copy_prior_outputs(prior_dir, ts_dir, safe_collection_pid)
    # load listing (copied or new)
    listing: dict[str, object] = load_listing(listing_json_path)
    # ensure combined exists if resuming or fresh
    combined_txt_path.touch(exist_ok=True)
    # compute effective remaining limit if a test limit is provided, accounting for prior appended items
    effective_limit: int | None = None
    if args.test_limit is not None:
        prior_appended_count: int = sum(1 for d in listing.get('items', []) if d.get('extracted_text_file_size'))
        effective_limit = max(0, args.test_limit - prior_appended_count)
    # initialize a minimal checkpoint with zero counts; will be updated after docs fetch
    save_checkpoint(
        checkpoint_json_path,
        collection_pid=collection_pid,
        safe_collection_pid=safe_collection_pid,
        run_directory_name=ts_dir.name,
        listing=listing,
        combined_path=combined_txt_path,
        listing_path=listing_json_path,
        total_docs=0,
        completed=False,
    )

    # http client
    headers: dict[str, str] = {
        'user-agent': 'bdr-extracted-text-collector/1.0 (+https://repository.library.brown.edu/)'
    }
    timeout: httpx.Timeout = httpx.Timeout(connect=30.0, read=60.0, write=60.0, pool=30.0)
    limits: httpx.Limits = httpx.Limits(max_keepalive_connections=10, max_connections=10)
    with httpx.Client(headers=headers, timeout=timeout, limits=limits) as client:
        # record collection metadata in summary
        try:
            coll_json: dict[str, object] = fetch_collection_json(client, collection_pid)
            coll_title: str = collection_title_from_json(coll_json)
        except Exception:
            coll_title = ''
        listing['summary']['collection_pid'] = collection_pid
        listing['summary']['collection_primary_title'] = coll_title

        # enumerate collection via search-api
        docs: list[dict[str, object]] = search_collection_pids(client, collection_pid)
        # update checkpoint with total_docs
        save_checkpoint(
            checkpoint_json_path,
            collection_pid=collection_pid,
            safe_collection_pid=safe_collection_pid,
            run_directory_name=ts_dir.name,
            listing=listing,
            combined_path=combined_txt_path,
            listing_path=listing_json_path,
            total_docs=len(docs),
            completed=False,
        )
        if not docs:
            print(f'No items found for collection {collection_pid}', file=sys.stderr)

        # If we've already satisfied the limit in a prior run, persist and exit early
        if effective_limit == 0:
            update_summary(listing, combined_txt_path, listing_json_path)
            save_listing(listing_json_path, listing)
            save_checkpoint(
                checkpoint_json_path,
                collection_pid=collection_pid,
                safe_collection_pid=safe_collection_pid,
                run_directory_name=ts_dir.name,
                listing=listing,
                combined_path=combined_txt_path,
                listing_path=listing_json_path,
                total_docs=len(docs),
                completed=False,
            )
            print('Done. Appended text for 0 item(s). (Effective limit reached from prior run.)')
            print(f'Combined text: {combined_txt_path}')
            print(f'Listing JSON:  {listing_json_path}')
            return 0

        appended_count: int = 0
        processed: set[str] = already_processed(listing)
        for i, doc in enumerate(tqdm(docs, total=len(docs), desc="Processing items"), start=1):
            pid: object = doc.get('pid')
            if not isinstance(pid, str):
                continue
            if pid in processed:
                # already listed; skip downloading again
                continue

            try:
                appended: bool = process_pid_for_extracted_text(client, pid, combined_txt_path, listing)
                if appended:
                    appended_count += 1
                    # If an effective limit is provided, stop once we've appended that many texts in THIS run
                    if effective_limit is not None and appended_count >= effective_limit:
                        # persist before stopping
                        update_summary(listing, combined_txt_path, listing_json_path)
                        save_listing(listing_json_path, listing)
                        save_checkpoint(
                            checkpoint_json_path,
                            collection_pid=collection_pid,
                            safe_collection_pid=safe_collection_pid,
                            run_directory_name=ts_dir.name,
                            listing=listing,
                            combined_path=combined_txt_path,
                            listing_path=listing_json_path,
                            total_docs=len(docs),
                            completed=False,
                        )
                        break
            except Exception as exc:
                # record failure stub so resume can continue later without losing context
                add_listing_entry(
                    listing,
                    item_pid=pid,
                    primary_title=doc.get('primary_title') or '',  # type: ignore[index]
                    full_item_api_url=ITEM_URL_TPL.format(pid=pid),
                    full_studio_url=f'{BASE}/studio/item/{pid}/',
                    extracted_text_file_size=None,
                )
                print(f'Error processing {pid}: {exc}', file=sys.stderr)

            # persist after each pid for robust resume
            update_summary(listing, combined_txt_path, listing_json_path)
            save_listing(listing_json_path, listing)
            save_checkpoint(
                checkpoint_json_path,
                collection_pid=collection_pid,
                safe_collection_pid=safe_collection_pid,
                run_directory_name=ts_dir.name,
                listing=listing,
                combined_path=combined_txt_path,
                listing_path=listing_json_path,
                total_docs=len(docs),
                completed=False,
            )

        # final summary update
        update_summary(listing, combined_txt_path, listing_json_path)
        save_listing(listing_json_path, listing)
        save_checkpoint(
            checkpoint_json_path,
            collection_pid=collection_pid,
            safe_collection_pid=safe_collection_pid,
            run_directory_name=ts_dir.name,
            listing=listing,
            combined_path=combined_txt_path,
            listing_path=listing_json_path,
            total_docs=len(docs),
            completed=True,
        )

    print(f'Done. Appended text for {appended_count} item(s).')
    print(f'Combined text: {combined_txt_path}')
    print(f'Listing JSON:  {listing_json_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
