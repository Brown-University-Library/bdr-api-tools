# /// script
# requires-python = "==3.12.*"
# dependencies = [
#   "httpx~=0.28.0"
# ]
# ///


"""
Displays monthly BDR collection activity counts and writes them to a JSON file.

Usage:
  uv run ./display_collection_activity.py --collection-pid bdr:bwehb8b8 --output-dir '/path/to/output-dir/'
"""

import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

SEARCH_BASE = 'https://repository.library.brown.edu/api/search/'
COLLECTION_API_TEMPLATE = 'https://repository.library.brown.edu/api/collections/{collection_pid}/'
DATE_FIELD = 'deposit_date'
SEARCH_FIELDS: list[str] = ['pid', DATE_FIELD]
MONTH_PATTERN = re.compile(r'^(\d{4})-(\d{2})')


def build_output_path(output_dir: str, collection_pid: str) -> Path:
    """
    Builds the output file path for the collection activity report.

    Called by: main()
    """
    safe_collection_pid: str = collection_pid.replace(':', '_')
    output_path: Path = Path(output_dir).expanduser().resolve() / f'collection_activity__{safe_collection_pid}.json'
    return output_path


def build_search_params(collection_pid: str, start: int, rows: int) -> dict[str, str | int]:
    """
    Builds query parameters for a collection-scoped search request.

    Called by: fetch_search_page()
    """
    params: dict[str, str | int] = {
        'q': f'rel_is_member_of_collection_ssim:"{collection_pid}"',
        'rows': rows,
        'start': start,
        'fl': ','.join(SEARCH_FIELDS),
    }
    return params


def increment_http_call_count(http_call_count: dict[str, int]) -> None:
    """
    Increments the tracked HTTP call count.

    Called by: fetch_search_page()
    """
    http_call_count['count'] += 1


def fetch_search_page(
    client: httpx.Client,
    collection_pid: str,
    start: int,
    rows: int,
    http_call_count: dict[str, int],
) -> dict[str, Any]:
    """
    Fetches one page of search results for the given collection.

    Called by: iter_collection_docs()
    """
    increment_http_call_count(http_call_count)
    response: httpx.Response = client.get(SEARCH_BASE, params=build_search_params(collection_pid, start, rows), timeout=30)
    response.raise_for_status()
    page_data: dict[str, Any] = response.json()
    return page_data


def iter_collection_docs(
    client: httpx.Client,
    collection_pid: str,
    rows: int,
    http_call_count: dict[str, int],
) -> tuple[int, list[dict[str, Any]]]:
    """
    Fetches all collection search documents across paginated results.

    Called by: main()
    """
    start: int = 0
    num_found: int = 0
    all_docs: list[dict[str, Any]] = []

    while True:
        page_data: dict[str, Any] = fetch_search_page(client, collection_pid, start, rows, http_call_count)
        response_data: dict[str, Any] = page_data.get('response', {})
        if start == 0:
            num_found = int(response_data.get('numFound', 0))
        docs: list[dict[str, Any]] = list(response_data.get('docs', []))
        all_docs.extend(docs)
        start += rows
        if not docs or start >= num_found:
            break

    return num_found, all_docs


def fetch_collection_title(client: httpx.Client, collection_pid: str, http_call_count: dict[str, int]) -> str | None:
    """
    Fetches the collection title from the collection API.

    Called by: main()
    """
    url: str = COLLECTION_API_TEMPLATE.format(collection_pid=collection_pid)
    increment_http_call_count(http_call_count)
    response: httpx.Response = client.get(url, timeout=30)
    title: str | None = None
    if response.status_code != 403:
        response.raise_for_status()
        collection_data: dict[str, Any] = response.json()
        title = collection_data.get('name') or collection_data.get('primary_title')
    return title


def normalize_date_value(raw_value: Any) -> str | None:
    """
    Normalizes a raw date value to a YYYY-MM month string when possible.

    Called by: choose_month_from_doc()
    """
    normalized_month: str | None = None
    if isinstance(raw_value, str):
        stripped_value: str = raw_value.strip()
        month_match = MONTH_PATTERN.match(stripped_value)
        if month_match:
            month_number: int = int(month_match.group(2))
            if 1 <= month_number <= 12:
                normalized_month = f'{month_match.group(1)}-{month_match.group(2)}'
    return normalized_month


def iter_candidate_values(raw_value: Any) -> list[Any]:
    """
    Expands a candidate field value into a list of values to inspect.

    Called by: choose_month_from_doc()
    """
    candidate_values: list[Any] = []
    if isinstance(raw_value, list):
        candidate_values = raw_value
    elif raw_value is not None:
        candidate_values = [raw_value]
    return candidate_values


def choose_month_from_doc(doc: dict[str, Any]) -> tuple[str | None, str | None]:
    """
    Chooses a usable month string from the deposit_date field in a search doc.

    Called by: aggregate_monthly_counts()
    """
    chosen_month: str | None = None
    chosen_field: str | None = None
    raw_value: Any = doc.get(DATE_FIELD)

    for candidate_value in iter_candidate_values(raw_value):
        normalized_month: str | None = normalize_date_value(candidate_value)
        if normalized_month is not None:
            chosen_month = normalized_month
            chosen_field = DATE_FIELD
            break

    return chosen_month, chosen_field


def summarize_date_fields(field_counter: Counter[str]) -> tuple[str | None, list[str]]:
    """
    Summarizes which date fields contributed counted items.

    Called by: build_output_data()
    """
    fields_used: list[str] = sorted(field_counter.keys())
    date_field_used: str | None = None
    if len(fields_used) == 1:
        date_field_used = fields_used[0]
    elif len(fields_used) > 1:
        date_field_used = 'mixed'
    return date_field_used, fields_used


def aggregate_monthly_counts(docs: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Aggregates search docs into per-month counts and summary statistics.

    Called by: main()
    """
    month_counter: Counter[str] = Counter()
    field_counter: Counter[str] = Counter()
    items_counted: int = 0
    items_skipped: int = 0

    for doc in docs:
        chosen_month, chosen_field = choose_month_from_doc(doc)
        if chosen_month is None or chosen_field is None:
            items_skipped += 1
        else:
            month_counter[chosen_month] += 1
            field_counter[chosen_field] += 1
            items_counted += 1

    monthly_counts: dict[str, int] = {month: month_counter[month] for month in sorted(month_counter.keys())}
    date_field_used, date_fields_used = summarize_date_fields(field_counter)
    aggregate_data: dict[str, Any] = {
        'monthly_counts': monthly_counts,
        'items_counted': items_counted,
        'items_skipped': items_skipped,
        'date_field_used': date_field_used,
        'date_fields_used': date_fields_used,
    }
    return aggregate_data


def build_output_data(
    collection_pid: str,
    collection_title: str | None,
    num_found: int,
    aggregate_data: dict[str, Any],
    output_path: Path,
) -> dict[str, Any]:
    """
    Builds the final pretty-printable JSON payload for disk output.

    Called by: main()
    """
    output_data: dict[str, Any] = {
        '_meta_': {
            'timestamp': datetime.now().astimezone().isoformat(),
            'collection_pid': collection_pid,
            'collection_title': collection_title,
            'search_url': SEARCH_BASE,
            'date_field_used': aggregate_data['date_field_used'],
            'date_fields_used': aggregate_data['date_fields_used'],
            'num_found': num_found,
            'items_counted': aggregate_data['items_counted'],
            'items_skipped': aggregate_data['items_skipped'],
            'output_file': str(output_path),
        },
        'monthly_counts': aggregate_data['monthly_counts'],
    }
    return output_data


def write_output_file(output_path: Path, output_data: dict[str, Any]) -> None:
    """
    Writes the output JSON to disk with stable pretty-print formatting.

    Called by: main()
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output_data, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Parses command-line arguments for the collection activity script.

    Called by: main()
    """
    parser = argparse.ArgumentParser(description='Write monthly BDR collection activity counts to a JSON file.')
    parser.add_argument('--collection-pid', required=True, help='BDR collection PID, for example bdr:bwehb8b8')
    parser.add_argument('--output-dir', required=True, help='Directory where the JSON report will be written')
    parser.add_argument('--rows', type=int, default=200, help='Search API page size')
    parsed_args: argparse.Namespace = parser.parse_args(argv)
    return parsed_args


def main(argv: list[str] | None = None) -> int:
    """
    Orchestrates collection activity retrieval, aggregation, and file output.

    Called by: dundermain
    """
    args: argparse.Namespace = parse_args(argv)
    output_path: Path = build_output_path(args.output_dir, args.collection_pid)
    headers: dict[str, str] = {'Accept': 'application/json'}
    transport = httpx.HTTPTransport(retries=2)
    http_call_count: dict[str, int] = {'count': 0}

    with httpx.Client(headers=headers, transport=transport) as client:
        collection_title: str | None = fetch_collection_title(client, args.collection_pid, http_call_count)
        num_found, docs = iter_collection_docs(client, args.collection_pid, args.rows, http_call_count)

    aggregate_data: dict[str, Any] = aggregate_monthly_counts(docs)
    output_data: dict[str, Any] = build_output_data(
        collection_pid=args.collection_pid,
        collection_title=collection_title,
        num_found=num_found,
        aggregate_data=aggregate_data,
        output_path=output_path,
    )
    write_output_file(output_path, output_data)
    print(json.dumps(output_data, indent=2, ensure_ascii=False))
    print(f'http_calls: {http_call_count["count"]}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
