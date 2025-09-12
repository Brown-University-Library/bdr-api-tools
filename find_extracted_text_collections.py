# /// script
# requires-python = "==3.12.*"
# dependencies = [
#   "httpx"
# ]
# ///

"""
Finds all collections that contain items with an EXTRACTED_TEXT datastream.

Saves a JSON list where each entry includes:
- collection_pid
- primary_title
- full_collection_api_url
- full_collection_studio_url
- count_of_extracted_text_files_in_collection

Uses the Search API for matches and resolves collection membership either
directly from the search doc (preferred) or, when missing (child objects),
by fetching the parent item and reading its `relations.isMemberOfCollection`.

All HTTP requests are serial (one-by-one). The script checkpoints work
to `<json_output_path>.checkpoint` so you can safely re-run after a failure.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import httpx

## constants
API_BASE = 'https://repository.library.brown.edu'
SEARCH_URL = f'{API_BASE}/api/search/'
ITEM_URL_TMPL = f'{API_BASE}/api/items/{{pid}}/'
COLL_URL_TMPL = f'{API_BASE}/api/collections/{{pid}}/'
COLL_STUDIO_TMPL = f'{API_BASE}/studio/collections/{{pid}}/'

## loads checkpoint
def load_checkpoint(path: str) -> dict:
    """
    Loads checkpoint state from `<path>.checkpoint` if present; otherwise returns a new state.
    """
    ckpt_path = f'{path}.checkpoint'
    if os.path.exists(ckpt_path):
        with open(ckpt_path, 'r', encoding='utf-8') as fh:
            return json.load(fh)
    return {
        'next_start': 0,
        'num_found': None,
        'collections': {},         # pid -> {'count': int, 'title': str | None}
        'parent_coll_cache': {},   # parent_pid -> [collection_pids]
        'seen_item_pids': set(),   # to avoid double counting if re-run
    }

## saves checkpoint
def save_checkpoint(path: str, state: dict) -> None:
    """
    Saves checkpoint state to `<path>.checkpoint`.
    Also writes the requested JSON list to `<path>` for at-a-glance progress/partial results.
    """
    ckpt_path = f'{path}.checkpoint'
    # write checkpoint
    tmp_ckpt = ckpt_path + '.tmp'
    with open(tmp_ckpt, 'w', encoding='utf-8') as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2, default=list)
    os.replace(tmp_ckpt, ckpt_path)

    # write user-facing list
    entries: list[dict] = []
    for coll_pid, info in sorted(state['collections'].items()):
        title = info.get('title') or ''
        entries.append({
            'collection_pid': coll_pid,
            'primary_title': title,
            'full_collection_api_url': COLL_URL_TMPL.format(pid=coll_pid),
            'full_collection_studio_url': COLL_STUDIO_TMPL.format(pid=coll_pid),
            'count_of_extracted_text_files_in_collection': int(info.get('count', 0)),
        })
    tmp_main = path + '.tmp'
    with open(tmp_main, 'w', encoding='utf-8') as fh:
        json.dump(entries, fh, ensure_ascii=False, indent=2)
    os.replace(tmp_main, path)

## builds search params
def build_search_params(start: int, rows: int) -> dict[str, str | int]:
    """
    Builds Search-API params to find items that include EXTRACTED_TEXT in `zip_filelist_ssim`,
    returning just the fields we need to minimize payload size.
    """
    return {
        'q': 'zip_filelist_ssim:"EXTRACTED_TEXT"',
        'rows': rows,
        'start': start,
        'fl': 'pid,rel_is_member_of_collection_ssim,rel_is_part_of_ssim',
    }

## fetches a search page
def fetch_search_page(client: httpx.Client, start: int, rows: int) -> dict:
    """
    Fetches one page of search results and returns the JSON.
    """
    r = client.get(SEARCH_URL, params=build_search_params(start, rows), timeout=30.0)
    r.raise_for_status()
    return r.json()

## fetches parent collections for a child item
def parent_collections(client: httpx.Client, parent_pid: str, cache: dict[str, list[str]]) -> list[str]:
    """
    Returns a list of collection pids for a parent item by hitting the Item API,
    using an in-memory cache to avoid repeats on re-runs.
    """
    if parent_pid in cache:
        return cache[parent_pid]

    r = client.get(ITEM_URL_TMPL.format(pid=parent_pid), timeout=30.0)
    if r.status_code == 403:
        cache[parent_pid] = []
        return []
    r.raise_for_status()
    data = r.json()
    rels = data.get('relations', {}) or {}
    coll_infos = rels.get('isMemberOfCollection', []) or []
    pids: list[str] = []
    for ci in coll_infos:
        # examples show both 'pid' and 'id' present; try both defensively
        p = (ci.get('pid') or ci.get('id') or '').strip()
        if p:
            pids.append(p)
    cache[parent_pid] = pids
    return pids

## ensures we have a collection title
def ensure_collection_title(client: httpx.Client, coll_pid: str, collections: dict) -> None:
    """
    Fetches the collection record to determine its primary title (stored under 'name' or 'primary_title').
    Caches it in `collections[coll_pid]['title']`.
    """
    info = collections.setdefault(coll_pid, {'count': 0, 'title': None})
    if info.get('title'):
        return
    r = client.get(COLL_URL_TMPL.format(pid=coll_pid), timeout=30.0)
    # collections are public; if 403 or 404, just leave title blank
    if r.status_code >= 400:
        info['title'] = ''
        return
    data = r.json()
    # prefer 'name' (as shown inside Item API relations); fall back to 'primary_title'
    title = (data.get('name') or data.get('primary_title') or '').strip()
    info['title'] = title

## increments counts for a set of collection pids
def add_counts(client: httpx.Client, collections: dict, coll_pids: list[str]) -> None:
    """
    Increments the EXTRACTED_TEXT count for the provided collection pids and ensures each has a title.
    """
    for cp in coll_pids:
        info = collections.setdefault(cp, {'count': 0, 'title': None})
        info['count'] = int(info.get('count', 0)) + 1
        if not info.get('title'):
            ensure_collection_title(client, cp, collections)

## prints a simple progress indicator
def show_progress(done: int, total: int | None) -> None:
    """
    Prints a one-line progress status. Uses total if known; otherwise shows a spinner-like dot.
    """
    if total is not None and total > 0:
        pct = min(100.0, 100.0 * done / total)
        sys.stdout.write(f'\rProcessed {done}/{total} (~{pct:0.1f}%)')
    else:
        sys.stdout.write('.')
    sys.stdout.flush()

## main manager
def main() -> None:
    """
    Parses args, iterates search pages serially, resolves collection membership (parent-aware),
    and writes checkpoint + user JSON on the fly so a failure can safely resume.
    """
    parser = argparse.ArgumentParser(description='Find collections with EXTRACTED_TEXT items.')
    parser.add_argument('--json_output_path', required=True, help='Path to write the JSON list.')
    parser.add_argument('--rows', type=int, default=500, help='Rows per page (default: 500).')
    args = parser.parse_args()

    out_path = Path(args.json_output_path).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    state = load_checkpoint(str(out_path))
    # json can't encode sets; normalize if reloaded
    if isinstance(state.get('seen_item_pids'), list):
        state['seen_item_pids'] = set(state['seen_item_pids'])

    next_start: int = int(state.get('next_start', 0))
    num_found: int | None = state.get('num_found')
    collections: dict = state['collections']
    parent_cache: dict[str, list[str]] = state['parent_coll_cache']
    seen_item_pids: set[str] = state['seen_item_pids']

    with httpx.Client(headers={'User-Agent': 'bdr-extracted-text-audit/1.0'}) as client:
        # get first page if needed to learn numFound
        if num_found is None or next_start == 0:
            first = fetch_search_page(client, start=0, rows=args.rows)
            num_found = int(first.get('response', {}).get('numFound', 0))
            docs: list[dict] = first.get('response', {}).get('docs', [])
            for doc in docs:
                pid = str(doc.get('pid', '')).strip()
                if not pid or pid in seen_item_pids:
                    continue
                coll_pids = [c for c in (doc.get('rel_is_member_of_collection_ssim') or []) if c]
                if not coll_pids:
                    parent_pids = [p for p in (doc.get('rel_is_part_of_ssim') or []) if p]
                    for par in parent_pids:
                        coll_pids.extend(parent_collections(client, par, parent_cache))
                add_counts(client, collections, list(dict.fromkeys(coll_pids)))
                seen_item_pids.add(pid)

            next_start = len(docs)
            state.update({
                'next_start': next_start,
                'num_found': num_found,
                'collections': collections,
                'parent_coll_cache': parent_cache,
                'seen_item_pids': list(seen_item_pids),
            })
            save_checkpoint(str(out_path), state)
            show_progress(next_start, num_found)

        # process remaining pages
        while num_found is not None and next_start < num_found:
            page = fetch_search_page(client, start=next_start, rows=args.rows)
            docs: list[dict] = page.get('response', {}).get('docs', [])
            if not docs:
                break

            for doc in docs:
                pid = str(doc.get('pid', '')).strip()
                if not pid or pid in seen_item_pids:
                    continue
                coll_pids = [c for c in (doc.get('rel_is_member_of_collection_ssim') or []) if c]
                if not coll_pids:
                    parent_pids = [p for p in (doc.get('rel_is_part_of_ssim') or []) if p]
                    for par in parent_pids:
                        coll_pids.extend(parent_collections(client, par, parent_cache))
                add_counts(client, collections, list(dict.fromkeys(coll_pids)))
                seen_item_pids.add(pid)

            next_start += len(docs)
            state.update({
                'next_start': next_start,
                'collections': collections,
                'parent_coll_cache': parent_cache,
                'seen_item_pids': list(seen_item_pids),
            })
            save_checkpoint(str(out_path), state)
            show_progress(next_start, num_found)
            # a tiny pause helps play nice with Cloudflare/bot protection
            time.sleep(0.2)

    # final newline after progress line
    sys.stdout.write('\n')

## dundermain
if __name__ == '__main__':
    main()
