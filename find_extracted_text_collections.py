# /// script
# requires-python = "==3.12.*"
# dependencies = [
#   "httpx"
# ]
# ///

"""
Finds collections that contain items with an EXTRACTED_TEXT datastream, while
minimizing API calls by sampling first and only fully counting "hot" collections.

Output JSON list fields (one entry per matching collection):
- collection_pid
- primary_title
- full_collection_api_url
- full_collection_studio_url
- count_of_extracted_text_files_in_collection

Approach:
1) Enumerates collections via Collections API.
2) For each collection, SAMPLE a few recent parent PDF items using Search API with:
     q = rel_is_member_of_collection_ssim:"<pid>"
         AND rel_content_models_ssim:"pdf"
         AND -rel_is_part_of_ssim:*   (so we only fetch parents)
   If none of the sample parents (nor their children) have EXTRACTED_TEXT, skip.
3) If at least one sample has EXTRACTED_TEXT, switch to COUNT mode for that collection:
   - Page through ALL parent items for the collection with the same filter
   - For each parent, check parent and (if needed) its child items for EXTRACTED_TEXT
   - Attribute each EXTRACTED_TEXT file to this collection
4) Save checkpoints and the user-facing JSON on every step so runs can resume.

All HTTP requests are serial (one-by-one) with a small delay to be Cloudflare-friendly.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import pprint
import sys
import time
from pathlib import Path

import httpx

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


## constants
API_BASE = 'https://repository.library.brown.edu'
SEARCH_URL = f'{API_BASE}/api/search/'
ITEM_URL_TMPL = f'{API_BASE}/api/items/{{pid}}/'
COLL_URL_TMPL = f'{API_BASE}/api/collections/{{pid}}/'
COLL_LIST_URL = f'{API_BASE}/api/collections/'
COLL_STUDIO_TMPL = f'{API_BASE}/studio/collections/{{pid}}/'


## default knobs
DEFAULT_SAMPLE_SIZE = 3         # how many recent parent PDFs to sample per collection
DEFAULT_ROWS = 500              # page size for Search when counting
REQUEST_PAUSE_SECONDS = 0.2     # polite pause between requests


## loads checkpoint
def load_checkpoint(out_path: str) -> dict:
    """
    Loads checkpoint state from `<out_path>.checkpoint` if present; otherwise returns a new state.
    """
    cpath = f'{out_path}.checkpoint'
    if os.path.exists(cpath):
        with open(cpath, 'r', encoding='utf-8') as fh:
            return json.load(fh)
    return {
        'collections_queue': [],        # [{'pid': str, 'title': str | None, 'status': 'pending'|'sampling'|'counting'|'done', ...}, ...]
        'queue_built': False,
        'current': None,               # current collection pid (during sampling/counting)
        'count_paging': {              # per-collection paging state for counting mode
            # pid -> {'start': int, 'num_found': int | None}
        },
        'results': {},                 # coll_pid -> {'count': int, 'title': str | None}
    }


## saves checkpoint and current output
def save_checkpoint(out_path: str, state: dict) -> None:
    """
    Saves checkpoint state to `<out_path>.checkpoint` and writes the user-facing JSON list to `<out_path>`.
    """
    # write checkpoint
    tmp_ckpt = f'{out_path}.checkpoint.tmp'
    with open(tmp_ckpt, 'w', encoding='utf-8') as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)
    os.replace(tmp_ckpt, f'{out_path}.checkpoint')

    # write user-facing JSON (only collections with count > 0)
    entries: list[dict] = []
    for coll_pid, info in sorted(state['results'].items()):
        if int(info.get('count', 0)) <= 0:
            continue
        title = info.get('title') or ''
        entries.append({
            'collection_pid': coll_pid,
            'primary_title': title,
            'full_collection_api_url': COLL_URL_TMPL.format(pid=coll_pid),
            'full_collection_studio_url': COLL_STUDIO_TMPL.format(pid=coll_pid),
            'count_of_extracted_text_files_in_collection': int(info.get('count', 0)),
        })
    tmp_out = f'{out_path}.tmp'
    with open(tmp_out, 'w', encoding='utf-8') as fh:
        json.dump(entries, fh, ensure_ascii=False, indent=2)
    os.replace(tmp_out, out_path)


## simple progress line
def show_progress(message: str) -> None:
    """
    Writes a one-line progress message.
    """
    sys.stdout.write(f'\r{message[:120]:<120}')
    sys.stdout.flush()


## http helpers
def get_json(client: httpx.Client, url: str, params: dict | None = None) -> dict:
    """
    Performs a GET and returns JSON (or {} on error). Serial, with a short delay.
    """
    r = client.get(url, params=params, timeout=30.0)
    time.sleep(REQUEST_PAUSE_SECONDS)
    if r.status_code >= 400:
        return {}
    return r.json()


## collection queue building
def build_collections_queue(client: httpx.Client) -> list[dict]:
    """
    Builds a list of collections to process from the Collections API list endpoint.
    Returns a list of dicts: [{'pid': 'bdr:123', 'title': 'Foo', 'status': 'pending'}, ...]
    """
    data = get_json(client, COLL_LIST_URL)
    log.debug(f'data: {pprint.pformat(data)}')
    items = data if isinstance(data, list) else data.get('collections') or data.get('items') or []
    queue: list[dict] = []
    for entry in items:
        # be permissive about key shapes
        pid = (entry.get('pid') or entry.get('id') or '').strip() if isinstance(entry, dict) else ''
        name = (entry.get('name') or entry.get('primary_title') or '').strip() if isinstance(entry, dict) else ''
        if pid:
            queue.append({'pid': pid, 'title': name, 'status': 'pending'})
    return queue


## search helpers
def search_parent_pdfs_in_collection(
    client: httpx.Client, coll_pid: str, rows: int, start: int = 0, sample_only: bool = False
) -> dict:
    """
    Uses Search API to fetch parent (non-child) PDFs in a collection.
    When sample_only=True, rows is treated as the sample size and we _attempt_ to sort by last-modified.
    Returns the raw search JSON.
    """
    # Build q with filters; see Search-API examples for rel_is_member_of_collection_ssim and -rel_is_part_of_ssim usage.
    q = f'rel_is_member_of_collection_ssim:"{coll_pid}" AND rel_content_models_ssim:"pdf" AND -rel_is_part_of_ssim:*'
    params: dict[str, str | int] = {'q': q, 'fl': 'pid', 'rows': rows, 'start': start}
    # Try to sort by last-modified if supported; otherwise API will ignore or error (we tolerate by just omitting)
    if sample_only:
        params['sort'] = 'object_last_modified_dsi desc'
    r = client.get(SEARCH_URL, params=params, timeout=30.0)
    time.sleep(REQUEST_PAUSE_SECONDS)
    if r.status_code >= 400:
        # try without sort if we added it
        if sample_only:
            params.pop('sort', None)
            r = client.get(SEARCH_URL, params=params, timeout=30.0)
            time.sleep(REQUEST_PAUSE_SECONDS)
            if r.status_code >= 400:
                return {}
        else:
            return {}
    return r.json()


## item helpers
def get_item_json(client: httpx.Client, pid: str) -> dict:
    """
    Returns the item JSON for `pid` ({} on error).
    """
    return get_json(client, ITEM_URL_TMPL.format(pid=pid))


def has_extracted_text(item_json: dict) -> bool:
    """
    Returns True if `datastreams.EXTRACTED_TEXT` exists (or is linked under `links.content_datastreams`).
    """
    ds = item_json.get('datastreams') or {}
    if 'EXTRACTED_TEXT' in ds:
        return True
    links = item_json.get('links') or {}
    cds = links.get('content_datastreams') or {}
    return 'EXTRACTED_TEXT' in cds


def child_pids(item_json: dict) -> list[str]:
    """
    Returns a list of child item pids from `relations.hasPart`.
    """
    rels = item_json.get('relations') or {}
    parts = rels.get('hasPart') or []
    out: list[str] = []
    for p in parts:
        cpid = (p.get('pid') or p.get('id') or '').strip()
        if cpid:
            out.append(cpid)
    return out


def ensure_collection_title(client: httpx.Client, coll_pid: str, results: dict) -> None:
    """
    Ensures we have a title for the collection in `results[coll_pid]['title']`.
    """
    info = results.setdefault(coll_pid, {'count': 0, 'title': None})
    if info.get('title'):
        return
    data = get_json(client, COLL_URL_TMPL.format(pid=coll_pid))
    title = (data.get('name') or data.get('primary_title') or '').strip()
    info['title'] = title


## sampling logic
def collection_has_any_extracted_text_by_sampling(
    client: httpx.Client, coll_pid: str, sample_size: int
) -> bool:
    """
    Samples up to `sample_size` recent parent PDFs in the collection and
    returns True if any sample parent or its children have EXTRACTED_TEXT.
    """
    search = search_parent_pdfs_in_collection(client, coll_pid, rows=sample_size, start=0, sample_only=True)
    docs = (search.get('response') or {}).get('docs') or []
    for d in docs:
        pid = (d.get('pid') or '').strip()
        if not pid:
            continue
        parent = get_item_json(client, pid)
        if not parent:
            continue
        if has_extracted_text(parent):
            return True
        # check children
        for cpid in child_pids(parent):
            cjson = get_item_json(client, cpid)
            if cjson and has_extracted_text(cjson):
                return True
    return False


## counting logic
def count_collection_extracted_text(
    client: httpx.Client, coll_pid: str, results: dict, state: dict, rows: int
) -> None:
    """
    Counts EXTRACTED_TEXT occurrences for all parent items in the collection,
    attributing child EXTRACTED_TEXT to the parent’s collection.
    Uses paging with resume support in `state['count_paging'][coll_pid]`.
    """
    ensure_collection_title(client, coll_pid, results)

    page_state = state['count_paging'].setdefault(coll_pid, {'start': 0, 'num_found': None})
    start = int(page_state.get('start', 0))
    num_found = page_state.get('num_found')

    # If we don't yet know num_found, fetch first page to learn it
    if num_found is None or start == 0:
        first = search_parent_pdfs_in_collection(client, coll_pid, rows=rows, start=0, sample_only=False)
        if not first:
            # nothing we can do
            state['count_paging'][coll_pid] = {'start': 0, 'num_found': 0}
            return
        num_found = int((first.get('response') or {}).get('numFound') or 0)
        docs = (first.get('response') or {}).get('docs') or []
        _count_docs(client, coll_pid, docs, results)
        start = len(docs)
        state['count_paging'][coll_pid] = {'start': start, 'num_found': num_found}

    # process remaining pages
    while num_found is not None and start < num_found:
        page = search_parent_pdfs_in_collection(client, coll_pid, rows=rows, start=start, sample_only=False)
        docs = (page.get('response') or {}).get('docs') or []
        if not docs:
            break
        _count_docs(client, coll_pid, docs, results)
        start += len(docs)
        state['count_paging'][coll_pid] = {'start': start, 'num_found': num_found}
        show_progress(f'Counting {coll_pid}: {start}/{num_found}')
        # persist after each page
        save_checkpoint(saved_out_path, state)


def _count_docs(client: httpx.Client, coll_pid: str, docs: list[dict], results: dict) -> None:
    """
    For each parent doc, counts EXTRACTED_TEXT on parent and children; increments results[coll_pid]['count'].
    """
    info = results.setdefault(coll_pid, {'count': 0, 'title': None})
    for d in docs:
        pid = (d.get('pid') or '').strip()
        if not pid:
            continue
        parent = get_item_json(client, pid)
        if not parent:
            continue
        if has_extracted_text(parent):
            info['count'] = int(info['count']) + 1
            continue
        for cpid in child_pids(parent):
            cjson = get_item_json(client, cpid)
            if cjson and has_extracted_text(cjson):
                info['count'] = int(info['count']) + 1
                break


## globals for checkpoint path (used inside count paging loop)
saved_out_path = ''


## main manager
def main() -> None:
    """
    Parses CLI, builds (or resumes) the collection queue, samples each collection to avoid
    full scans when possible, counts precisely for "hot" collections, and writes checkpoints.
    """
    parser = argparse.ArgumentParser(description='Find collections with EXTRACTED_TEXT, with minimal API load.')
    parser.add_argument('--json_output_path', required=True, help='Path to write the JSON list.')
    parser.add_argument('--sample_per_collection', type=int, default=DEFAULT_SAMPLE_SIZE,
                        help=f'How many recent parent PDFs to sample per collection (default: {DEFAULT_SAMPLE_SIZE}).')
    parser.add_argument('--rows', type=int, default=DEFAULT_ROWS,
                        help=f'Rows per page when counting (default: {DEFAULT_ROWS}).')
    parser.add_argument('--max_collections', type=int, default=0,
                        help='Optional cap on number of collections to process (0 = all).')
    args = parser.parse_args()

    global saved_out_path
    out_path = Path(args.json_output_path).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    saved_out_path = str(out_path)

    state = load_checkpoint(str(out_path))

    with httpx.Client(headers={'User-Agent': 'bdr-extracted-text-sampler/1.0'}) as client:
        # build queue once
        if not state.get('queue_built'):
            queue = build_collections_queue(client)
            state['collections_queue'] = queue
            state['queue_built'] = True
            save_checkpoint(str(out_path), state)

        processed = 0
        for entry in state['collections_queue']:
            coll_pid: str = entry['pid']
            status: str = entry.get('status') or 'pending'
            title: str | None = entry.get('title') or None

            # optional cap
            if args.max_collections and processed >= args.max_collections:
                break

            # skip if already done
            if status == 'done':
                processed += 1
                continue

            # sampling
            entry['status'] = 'sampling'
            state['current'] = coll_pid
            # persist before API calls (so we can resume mid-collection)
            save_checkpoint(str(out_path), state)
            show_progress(f'Sampling {coll_pid}…')

            has_any = collection_has_any_extracted_text_by_sampling(
                client, coll_pid, sample_size=args.sample_per_collection
            )

            if not has_any:
                # mark done (no ET here); we keep results empty (no JSON entry)
                entry['status'] = 'done'
                state['current'] = None
                save_checkpoint(str(out_path), state)
                processed += 1
                continue

            # hot collection -> count precisely
            entry['status'] = 'counting'
            state['results'].setdefault(coll_pid, {'count': 0, 'title': title})
            save_checkpoint(str(out_path), state)

            count_collection_extracted_text(client, coll_pid, state['results'], state, rows=args.rows)

            # finalize collection
            entry['status'] = 'done'
            state['current'] = None
            # ensure a title was captured
            if not state['results'][coll_pid].get('title'):
                ensure_collection_title(client, coll_pid, state['results'])
            save_checkpoint(str(out_path), state)
            processed += 1

    # final newline after progress messages
    sys.stdout.write('\n')


## dundermain
if __name__ == '__main__':
    main()
