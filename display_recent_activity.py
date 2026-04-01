# /// script
# requires-python = "==3.12.*"
# dependencies = [
#   "httpx~=0.28.0"
# ]
# ///


"""
Displays recent BDR repository activity and prints formatted JSON.

Usage:
  uv run ./display_recent_activity.py
  uv run ./display_recent_activity.py --recent-item-count 250
"""

import argparse
import json
import sys
from datetime import datetime
from typing import Any

import httpx

SEARCH_BASE = 'https://repository.library.brown.edu/api/search/'
COLLECTION_API_TEMPLATE = 'https://repository.library.brown.edu/api/collections/{collection_pid}/'
DEFAULT_RECENT_ITEM_COUNT = 100
SEARCH_ROWS_PER_PAGE = 100
SORT_FIELD_CANDIDATES: list[str] = [
    'date_added desc',
    'object_created_dtsi desc',
    'object_created_dsi desc',
    'mods_created_date_sort desc',
    'fed_created_dsi desc',
    'deposit_date desc',
]
DATE_FIELD_CANDIDATES: list[str] = [
    'date_added',
    'object_created_dtsi',
    'object_created_dsi',
    'mods_created_date_sort',
    'fed_created_dsi',
    'deposit_date',
]
COLLECTION_MEMBERSHIP_FIELD = 'rel_is_member_of_collection_ssim'
SEARCH_FIELDS: list[str] = ['pid', 'primary_title', COLLECTION_MEMBERSHIP_FIELD, *DATE_FIELD_CANDIDATES]


def build_search_params(start: int, rows: int, sort_field: str) -> dict[str, str | int]:
    """
    Builds query parameters for a recent-activity search request.

    Called by: fetch_search_page()
    """
    params: dict[str, str | int] = {
        'q': '*:*',
        'rows': rows,
        'start': start,
        'sort': sort_field,
        'fl': ','.join(SEARCH_FIELDS),
    }
    return params


def increment_http_call_count(http_call_count: dict[str, int]) -> None:
    """
    Increments the tracked HTTP call count.

    Called by: fetch_search_page(), fetch_collection_title()
    """
    http_call_count['count'] += 1


def fetch_search_page(
    client: httpx.Client,
    start: int,
    rows: int,
    sort_field: str,
    http_call_count: dict[str, int],
) -> dict[str, Any]:
    """
    Fetches one page of recent-item search results.

    Called by: fetch_recent_docs_for_sort()
    """
    increment_http_call_count(http_call_count)
    response: httpx.Response = client.get(SEARCH_BASE, params=build_search_params(start, rows, sort_field), timeout=30)
    response.raise_for_status()
    page_data: dict[str, Any] = response.json()
    return page_data


def fetch_recent_docs_for_sort(
    client: httpx.Client,
    recent_item_count: int,
    sort_field: str,
    http_call_count: dict[str, int],
) -> list[dict[str, Any]]:
    """
    Fetches the most recent docs using a specific sort field.

    Called by: choose_sort_field_and_docs()
    """
    start: int = 0
    docs: list[dict[str, Any]] = []
    remaining: int = recent_item_count
    while remaining > 0:
        rows: int = min(SEARCH_ROWS_PER_PAGE, remaining)
        page_data: dict[str, Any] = fetch_search_page(client, start, rows, sort_field, http_call_count)
        response_data: dict[str, Any] = page_data.get('response', {})
        page_docs: list[dict[str, Any]] = list(response_data.get('docs', []))
        if not page_docs:
            break
        docs.extend(page_docs)
        remaining = recent_item_count - len(docs)
        start += rows
    return docs[:recent_item_count]


def choose_sort_field_and_docs(
    client: httpx.Client,
    recent_item_count: int,
    http_call_count: dict[str, int],
) -> tuple[str, list[dict[str, Any]]]:
    """
    Chooses the first working sort field and fetches recent docs.

    Called by: main()
    """
    last_error: Exception | None = None
    chosen_sort_field: str = SORT_FIELD_CANDIDATES[-1]
    docs: list[dict[str, Any]] = []
    for sort_field in SORT_FIELD_CANDIDATES:
        try:
            docs = fetch_recent_docs_for_sort(client, recent_item_count, sort_field, http_call_count)
            chosen_sort_field = sort_field
            last_error = None
            break
        except httpx.HTTPStatusError as exc:
            last_error = exc
            if exc.response is None or exc.response.status_code != 400:
                raise
    if last_error is not None:
        raise last_error
    return chosen_sort_field, docs


def normalize_string_list(raw_value: Any) -> list[str]:
    """
    Normalizes an API value into a list of non-empty strings.

    Called by: normalize_collection_pids()
    """
    normalized_values: list[str] = []
    if isinstance(raw_value, list):
        for entry in raw_value:
            if isinstance(entry, str) and entry.strip():
                normalized_values.append(entry.strip())
    elif isinstance(raw_value, str) and raw_value.strip():
        normalized_values = [raw_value.strip()]
    return normalized_values


def normalize_collection_pids(doc: dict[str, Any]) -> list[str]:
    """
    Normalizes collection membership values from a search doc.

    Called by: build_recent_items(), build_updated_collections()
    """
    raw_value: Any = doc.get(COLLECTION_MEMBERSHIP_FIELD)
    candidate_values: list[str] = normalize_string_list(raw_value)
    collection_pids: list[str] = []
    for value in candidate_values:
        if value.startswith('bdr:') and value not in collection_pids:
            collection_pids.append(value)
    return collection_pids


def choose_item_date(doc: dict[str, Any]) -> str | None:
    """
    Chooses the first available recent-date value from a search doc.

    Called by: build_recent_items()
    """
    chosen_value: str | None = None
    for field_name in DATE_FIELD_CANDIDATES:
        raw_value: Any = doc.get(field_name)
        candidate_values: list[str] = normalize_string_list(raw_value)
        if candidate_values:
            chosen_value = candidate_values[0]
            break
    return chosen_value


def build_collection_title(collection_data: dict[str, Any]) -> str | None:
    """
    Builds a display-ready collection title with parent collection provenance.

    Called by: fetch_collection_title()
    """
    base_title: str = collection_data.get('name') or collection_data.get('primary_title') or ''
    parent_title: str = ''
    ancestors: Any = collection_data.get('ancestors')
    derived_title: str | None = None
    if isinstance(ancestors, list) and ancestors:
        last_ancestor: Any = ancestors[-1]
        if isinstance(last_ancestor, dict):
            parent_title = last_ancestor.get('name') or last_ancestor.get('title') or ''
        elif isinstance(last_ancestor, str):
            parent_title = last_ancestor
    if base_title:
        if parent_title:
            derived_title = f'{base_title} -- (from {parent_title})'
        else:
            derived_title = base_title
    return derived_title


def fetch_collection_title(
    client: httpx.Client,
    collection_pid: str,
    http_call_count: dict[str, int],
) -> str | None:
    """
    Fetches the collection title from the collections API.

    Called by: build_collection_title_map()
    """
    url: str = COLLECTION_API_TEMPLATE.format(collection_pid=collection_pid)
    increment_http_call_count(http_call_count)
    response: httpx.Response = client.get(url, timeout=30)
    title: str | None = None
    if response.status_code != 403:
        response.raise_for_status()
        collection_data: dict[str, Any] = response.json()
        title = build_collection_title(collection_data)
    return title


def build_collection_title_map(
    client: httpx.Client,
    docs: list[dict[str, Any]],
    http_call_count: dict[str, int],
) -> dict[str, str]:
    """
    Builds a mapping of collection PID to display title.

    Called by: main()
    """
    collection_title_map: dict[str, str] = {}
    unique_collection_pids: list[str] = []
    for doc in docs:
        for collection_pid in normalize_collection_pids(doc):
            if collection_pid not in unique_collection_pids:
                unique_collection_pids.append(collection_pid)
    for collection_pid in unique_collection_pids:
        title: str | None = fetch_collection_title(client, collection_pid, http_call_count)
        collection_title_map[collection_pid] = title or collection_pid
    return collection_title_map


def build_recent_items(docs: list[dict[str, Any]], collection_title_map: dict[str, str]) -> list[dict[str, Any]]:
    """
    Builds the recent-items output payload.

    Called by: main()
    """
    recent_items: list[dict[str, Any]] = []
    for doc in docs:
        collection_entries: list[dict[str, str]] = []
        for collection_pid in normalize_collection_pids(doc):
            collection_entries.append(
                {
                    'pid': collection_pid,
                    'name': collection_title_map.get(collection_pid, collection_pid),
                }
            )
        recent_items.append(
            {
                'pid': str(doc.get('pid', '')),
                'primary_title': str(doc.get('primary_title', '')),
                'date_added': choose_item_date(doc),
                'collections': collection_entries,
            }
        )
    return recent_items


def build_updated_collections(docs: list[dict[str, Any]], collection_title_map: dict[str, str]) -> list[dict[str, Any]]:
    """
    Builds per-collection recent-item counts from the recent docs.

    Called by: main()
    """
    collection_counts: dict[str, int] = {}
    for doc in docs:
        for collection_pid in normalize_collection_pids(doc):
            collection_counts[collection_pid] = collection_counts.get(collection_pid, 0) + 1
    sorted_collection_pids: list[str] = sorted(
        collection_counts.keys(),
        key=lambda pid: (-collection_counts[pid], collection_title_map.get(pid, pid).lower(), pid),
    )
    updated_collections: list[dict[str, Any]] = []
    for collection_pid in sorted_collection_pids:
        updated_collections.append(
            {
                'collection_pid': collection_pid,
                'collection_title': collection_title_map.get(collection_pid, collection_pid),
                'recent_item_count': collection_counts[collection_pid],
            }
        )
    return updated_collections


def build_output_data(
    recent_item_count_requested: int,
    sort_field_used: str,
    recent_items: list[dict[str, Any]],
    updated_collections: list[dict[str, Any]],
    http_call_count: int,
) -> dict[str, Any]:
    """
    Builds the final pretty-printable JSON payload for stdout output.

    Called by: main()
    """
    output_data: dict[str, Any] = {
        '_meta_': {
            'timestamp': datetime.now().astimezone().isoformat(),
            'recent_item_count_requested': recent_item_count_requested,
            'recent_item_count_returned': len(recent_items),
            'sort_field_used': sort_field_used,
            'http_calls': http_call_count,
        },
        'recent_items': recent_items,
        'updated_collections': updated_collections,
    }
    return output_data


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Parses command-line arguments for the recent-activity script.

    Called by: main()
    """
    parser = argparse.ArgumentParser(description='Print recent BDR repository activity as formatted JSON.')
    parser.add_argument(
        '--recent-item-count',
        type=int,
        default=DEFAULT_RECENT_ITEM_COUNT,
        help='Number of most-recent items to inspect. Defaults to 100.',
    )
    parsed_args: argparse.Namespace = parser.parse_args(argv)
    if parsed_args.recent_item_count < 1:
        parser.error('--recent-item-count must be a positive integer')
    return parsed_args


def main(argv: list[str] | None = None) -> int:
    """
    Orchestrates recent-item retrieval, collection aggregation, and stdout output.

    Called by: dundermain
    """
    args: argparse.Namespace = parse_args(argv)
    headers: dict[str, str] = {'Accept': 'application/json'}
    transport = httpx.HTTPTransport(retries=2)
    http_call_count: dict[str, int] = {'count': 0}
    with httpx.Client(headers=headers, transport=transport) as client:
        sort_field_used, docs = choose_sort_field_and_docs(client, args.recent_item_count, http_call_count)
        collection_title_map: dict[str, str] = build_collection_title_map(client, docs, http_call_count)
    recent_items: list[dict[str, Any]] = build_recent_items(docs, collection_title_map)
    updated_collections: list[dict[str, Any]] = build_updated_collections(docs, collection_title_map)
    output_data: dict[str, Any] = build_output_data(
        recent_item_count_requested=args.recent_item_count,
        sort_field_used=sort_field_used,
        recent_items=recent_items,
        updated_collections=updated_collections,
        http_call_count=http_call_count['count'],
    )
    print(json.dumps(output_data, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
