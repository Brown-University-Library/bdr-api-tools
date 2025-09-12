# /// script
# requires-python = "==3.12.*"
# dependencies = [
#   "httpx"
# ]
# ///

"""
Fetches BDR item-api data and extracts and summarizes zip file data for item and and any hasParts children.

Usage:
  uv run ./show_zip_info.py --item_pid bdr:833705  # item with hasParts child-items
  uv run ./show_zip_info.py --item_pid bdr:841254  # item with no hasParts child-items
  uv run ./show_zip_info.py --item_pid bdr:417934  # item with no zip data

Output (bdr:833705 excerpt):
{
  "_meta_": {
    "timestamp": "2025-09-11T22:56:48.249463-04:00",
    "full_item_api_url": "https://repository.library.brown.edu/api/items/bdr:833705/",
    "item_pid": "bdr:833705"
  },
  "item_info": {
    "pid": "bdr:833705",
    "primary_title": "(ATOMICS-0) Adjudicated/Annotated Telemetry Signals for Medically Important and Clinically Significant Events-0 Dataset",
    "item_zip_info": [
      "ATOMICS-0/ATOMICS-0-Data-Catalog-Record-2018.pdf",
      "ATOMICS-0/README.md"
    ],
    "item_zip_filetype_summary": {
      "md": 1,
      "pdf": 1
    },
    "has_parts_zip_info": [
      {
        "child_pid": "bdr:841254",
        "primary_title": "ATOMICS SHARED CODE",
        "child_zip_info": [
          "__MACOSX/x1. ATOMICS shared code/._III+ ATOMICS_dataset_window_processor.py",
          "__MACOSX/x1. ATOMICS shared code/._III+ ATOMICS_dataset_window_processorSLC.py",
          "x1. ATOMICS shared code/III+ ATOMICS_dataset_window_processor.py",
          "x1. ATOMICS shared code/III+ ATOMICS_dataset_window_processorSLC.py"
        ],
        "child_zip_filetype_summary": {
          "py": 4
        }
      },
      {
        "child_pid": "bdr:841252",
        "primary_title": "ATOMICS-0 SHARE ALL",
        "child_zip_info": [
          "ATOMICS_0_share_ALL/+ATOMICS dataset descriptions and use instructions v1.0.doc",
          "ATOMICS_0_share_ALL/.DS_Store",
          "ATOMICS_0_share_ALL/PERSEUS+MeTeOR EULA.pdf",
          "ATOMICS_0_share_ALL/select latched controls from ATOMICS-2 dataset.txt",
          "ATOMICS_0_share_ALL/week02_day01_alarmsSLC/x00-86.2015-08-15_alarmsSLC.csv",
          "ATOMICS_0_share_ALL/week02_day01_alarmsSLC/x00-89.2015-08-15_alarmsSLC.csv",
<snip>
        ],
        "child_zip_filetype_summary": {
        "csv": 320,
        "doc": 1,
        "ds_store": 2,
        "pdf": 2,
        "py": 4,
        "txt": 194
        }
      }
    ],
<snip>
    "overall_zip_filetype_summary": {
      "csv": 600,
      "doc": 1,
      "ds_store": 14,
      "md": 1,
      "pdf": 15,
      "py": 8,
      "txt": 363
    }
  }
}
"""

import argparse
import functools
import json
import sys
from collections import Counter
from collections.abc import Callable
from datetime import datetime
from typing import Any

import httpx

BDR_ITEM_API_TEMPLATE = 'https://repository.library.brown.edu/api/items/{pid}/'


def build_item_url(pid: str) -> str:
    """
    Builds item url.
    """
    return BDR_ITEM_API_TEMPLATE.format(pid=pid)


def fetch_item_json(client: httpx.Client, item_pid: str) -> dict[str, Any]:
    """
    Fetches item json from bdr api.
    """
    url = build_item_url(item_pid)
    resp = client.get(url, timeout=httpx.Timeout(15.0))
    resp.raise_for_status()
    return resp.json()


def ext_from_path(p: str) -> str:
    """
    Extracts the lowercase file extension from a path. If none, returns 'noext'.
    """
    name = (p or '').rsplit('/', 1)[-1]
    # treat everything after last '.' as extension; lowercase it
    if '.' in name:
        return name.rsplit('.', 1)[-1].lower()
    return 'noext'


def parse_item_zip_info(
    item_json: dict[str, Any],
    fetcher: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    """
    Parses an item JSON for top-level zip list and each child's zip list.

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
                # child summary will be added below
                has_parts_info.append(
                    {
                        'child_pid': child_pid,
                        'primary_title': str(child_json.get('primary_title', '')),
                        'child_zip_info': child_zip_list,
                    }
                )

    # build item-level zip summary (by file extension)
    ext_counts = Counter(ext_from_path(p) for p in item_zip_info)
    item_zip_filetype_summary = {ext: ext_counts[ext] for ext in sorted(ext_counts.keys())}

    # add per-child summaries
    for child in has_parts_info:
        cz_list = child.get('child_zip_info', [])
        c_counts = Counter(ext_from_path(p) for p in cz_list)
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
            'primary_title': str(item_json.get('primary_title', '')),
            'item_zip_info': item_zip_info,
            'item_zip_filetype_summary': item_zip_filetype_summary,
            'has_parts_zip_info': has_parts_info,
            'overall_zip_filetype_summary': overall_zip_filetype_summary,
        }
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Parses cli args.
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
    Manages main execution.
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
        print(json.dumps(result, indent=2, ensure_ascii=False))
    except Exception:
        # last-resort repr so failures never mask core logic
        print(result)

    return 0


if __name__ == '__main__':
    sys.exit(main())
