# /// script
# requires-python = "==3.12.*"
# dependencies = [
#   "httpx"
# ]
# ///

"""
Fetches BDR item-api data and gathers zip file data for item and children.
"""

import argparse
import sys
from collections.abc import Callable
from collections import Counter
from typing import Any

import httpx
import functools
from datetime import datetime

BDR_ITEM_API_TEMPLATE = 'https://repository.library.brown.edu/api/items/{pid}/'


def build_item_url(pid: str) -> str:
    """Build item url."""
    return BDR_ITEM_API_TEMPLATE.format(pid=pid)


def fetch_item_json(client: httpx.Client, item_pid: str) -> dict[str, Any]:
    """Fetch item json from bdr api."""
    url = build_item_url(item_pid)
    resp = client.get(url, timeout=httpx.Timeout(15.0))
    resp.raise_for_status()
    return resp.json()


def parse_item_zip_info(
    item_json: dict[str, Any],
    fetcher: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    """
    Parse an item JSON for top-level zip list and each child's zip list.

    - looks for top-level 'zip_filelist_ssim'
    - looks for children under either top-level 'hasPart' or 'relations' -> 'hasPart'
    - for each child pid, fetches the child's JSON and extracts its 'zip_filelist_ssim'
    """
    pid = str(item_json.get('pid', ''))
    item_zip_info: list[str] = list(item_json.get('zip_filelist_ssim', []) or [])

    # support both shapes: top-level 'hasPart' OR nested under 'relations'
    has_part = item_json.get('hasPart')
    if has_part is None:
        has_part = (item_json.get('relations') or {}).get('hasPart')

    has_parts_info: list[dict[str, Any]] = []

    if isinstance(has_part, list):
        for child in has_part:
            child_pid = str((child or {}).get('pid', '')).strip()
            if not child_pid:
                continue
            child_json = fetcher(child_pid)
            child_zip_list = list(child_json.get('zip_filelist_ssim', []) or [])
            if child_zip_list:
                # child summary will be filled in after _ext_from_path is defined
                has_parts_info.append(
                    {
                        'child_pid': child_pid,
                        'child_zip_info': child_zip_list,
                    }
                )

    # build item-level zip summary (by file extension)
    def _ext_from_path(p: str) -> str:
        name = (p or '').rsplit('/', 1)[-1]
        # treat everything after last '.' as extension; lowercase it
        if '.' in name:
            return name.rsplit('.', 1)[-1].lower()
        return 'noext'

    ext_counts = Counter(_ext_from_path(p) for p in item_zip_info)
    item_zip_filetype_summary = {ext: ext_counts[ext] for ext in sorted(ext_counts.keys())}

    # add per-child summaries
    for child in has_parts_info:
        cz_list = child.get('child_zip_info', [])
        c_counts = Counter(_ext_from_path(p) for p in cz_list)
        child['child_zip_filetype_summary'] = {ext: c_counts[ext] for ext in sorted(c_counts.keys())}

    # build overall summary (item + all children)
    overall_counts = Counter(ext_counts)
    for child in has_parts_info:
        c_summary = child.get('child_zip_filetype_summary', {})
        overall_counts.update(c_summary)
    overall_zip_filetype_summary = {ext: overall_counts[ext] for ext in sorted(overall_counts.keys())}

    return {
        '_meta_': {
            'timestamp': datetime.now().astimezone().isoformat(),
            'full_item_api_url': build_item_url(pid),
            'item_pid': pid,
        },
        'item_info': {
            'pid': pid,
            'item_zip_info': item_zip_info,
            'item_zip_filetype_summary': item_zip_filetype_summary,
            'has_parts_zip_info': has_parts_info,
            'overall_zip_filetype_summary': overall_zip_filetype_summary,
        }
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Parse cli args.
    """
    parser = argparse.ArgumentParser(
        description='Fetch BDR item and gather zip file lists for item and children.'
    )
    parser.add_argument(
        '--item_pid',
        required=True,
        help='BDR item PID (e.g., bdr:833705)',
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """
    Main manager.
    """
    args = parse_args(argv)

    # build an httpx client for connection reuse
    headers = {
        # leave open for future auth, custom UA, etc.
        'User-Agent': 'bdr-zip-info/1.0 (+https://repository.library.brown.edu/)',
    }
    transport = httpx.HTTPTransport(retries=2)
    with httpx.Client(headers=headers, transport=transport) as client:
        # fetch parent
        parent_json = fetch_item_json(client, args.item_pid)

        # parse parent + children
        fetcher = functools.partial(fetch_item_json, client)
        result = parse_item_zip_info(parent_json, fetcher)

    # print the final structure as JSON
    try:
        import json

        print(json.dumps(result, indent=2, ensure_ascii=False))
    except Exception:
        # last-resort repr so failures never mask core logic
        print(result)

    return 0


if __name__ == '__main__':
    sys.exit(main())
