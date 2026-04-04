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
  uv run ./display_recent_activity.py --recent-items-count 100
"""

import argparse
import json
import re
import sys
import time
from collections import Counter
from datetime import datetime
from typing import Any

import httpx

SEARCH_BASE = 'https://repository.library.brown.edu/api/search/'
COLLECTION_API_TEMPLATE = 'https://repository.library.brown.edu/api/collections/{collection_pid}/'
DATE_FIELD = 'deposit_date'
COLLECTION_MEMBERSHIP_FIELD = 'rel_is_member_of_collection_ssim'
ROWS_PER_PAGE = 500
DEFAULT_RECENT_ITEMS_COUNT = 100
SEARCH_FIELDS: list[str] = ['pid', 'primary_title', DATE_FIELD, COLLECTION_MEMBERSHIP_FIELD]
PID_PATTERN = re.compile(r'^bdr:[A-Za-z0-9]+$')
PROGRESS_BAR_WIDTH = 24


def format_duration(seconds: float | None) -> str:
    """
    Formats a duration in seconds for compact progress display.

    Called by: ProgressReporter.render_progress()
    """
    if seconds is None or seconds < 0:
        return '?:??'

    rounded_seconds: int = int(round(seconds))
    minutes, seconds_part = divmod(rounded_seconds, 60)
    hours, minutes_part = divmod(minutes, 60)
    formatted_duration: str = f'{minutes_part:02d}:{seconds_part:02d}'
    if hours > 0:
        formatted_duration = f'{hours}:{minutes_part:02d}:{seconds_part:02d}'
    return formatted_duration


def format_elapsed_timetaken(seconds: float) -> str:
    """
    Formats elapsed runtime as hours:minutes:seconds with tenths precision.

    Called by: build_output_data()
    """
    rounded_seconds: float = round(max(seconds, 0.0), 1)
    hours: int = int(rounded_seconds // 3600)
    remaining_seconds: float = rounded_seconds - (hours * 3600)
    minutes: int = int(remaining_seconds // 60)
    seconds_part: float = remaining_seconds - (minutes * 60)
    formatted_timetaken: str = f'{hours}:{minutes:02d}:{seconds_part:04.1f}'
    return formatted_timetaken


def build_progress_bar(completed: int, total: int, width: int = PROGRESS_BAR_WIDTH) -> str:
    """
    Builds a fixed-width ASCII progress bar.

    Called by: ProgressReporter.render_progress()
    """
    if total < 1:
        return '[' + ('-' * width) + ']'

    filled_width: int = min(width, int((completed / total) * width))
    progress_bar: str = '[' + ('#' * filled_width) + ('-' * (width - filled_width)) + ']'
    return progress_bar


class ProgressReporter:
    """
    Displays lightweight progress updates on stderr without affecting JSON stdout.

    Called by: main()
    """

    def __init__(self, enabled: bool, stream: Any = None) -> None:
        """
        Initializes progress-display state for staged script output.

        Called by: main()
        """
        self.enabled: bool = enabled
        self.stream: Any = stream if stream is not None else sys.stderr
        self.is_tty: bool = bool(getattr(self.stream, 'isatty', lambda: False)())
        self.stage_name: str = ''
        self.stage_total: int | None = None
        self.stage_started_at: float | None = None
        self.last_rendered_line_length: int = 0

    def start_stage(self, stage_name: str, total: int | None = None, detail: str = '') -> None:
        """
        Starts a named stage and prints an initial progress line.

        Called by: main()
        """
        if not self.enabled:
            return

        self.stage_name = stage_name
        self.stage_total = total
        self.stage_started_at = time.monotonic()
        self.render_progress(0, total=total, detail=detail)

    def update(self, completed: int, total: int | None = None, detail: str = '') -> None:
        """
        Updates the current stage display with fresh counters and timing.

        Called by: fetch_recent_docs(), enrich_recent_items_with_collections()
        """
        if not self.enabled:
            return

        effective_total: int | None = total if total is not None else self.stage_total
        self.render_progress(completed, total=effective_total, detail=detail)

    def finish(self, completed: int | None = None, total: int | None = None, detail: str = '') -> None:
        """
        Completes the current stage and ends the progress line cleanly.

        Called by: main()
        """
        if not self.enabled:
            return

        if completed is None:
            completed = total if total is not None else self.stage_total
        effective_total: int | None = total if total is not None else self.stage_total
        self.render_progress(completed or 0, total=effective_total, detail=detail)
        self.stream.write('\n')
        self.stream.flush()
        self.last_rendered_line_length = 0

    def render_progress(self, completed: int, total: int | None = None, detail: str = '') -> None:
        """
        Renders the current progress line, including percent and ETA when possible.

        Called by: ProgressReporter.start_stage(), ProgressReporter.update(), ProgressReporter.finish()
        """
        if not self.enabled:
            return

        elapsed_seconds: float | None = None
        if self.stage_started_at is not None:
            elapsed_seconds = time.monotonic() - self.stage_started_at

        line: str = self.stage_name
        if total is not None and total > 0:
            eta_seconds: float | None = None
            if completed > 0 and elapsed_seconds is not None:
                remaining_units: int = max(total - completed, 0)
                eta_seconds = (elapsed_seconds / completed) * remaining_units
            percent_complete: int = min(100, int((completed / total) * 100))
            line = (
                f'{self.stage_name} {build_progress_bar(completed, total)} '
                f'{completed}/{total} {percent_complete:3d}% '
                f'elapsed {format_duration(elapsed_seconds)} eta {format_duration(eta_seconds)}'
            )
        elif elapsed_seconds is not None:
            line = f'{self.stage_name} elapsed {format_duration(elapsed_seconds)}'

        if detail:
            line = f'{line} | {detail}'

        if self.is_tty:
            padded_line: str = line.ljust(self.last_rendered_line_length)
            self.stream.write(f'\r{padded_line}')
            self.last_rendered_line_length = len(line)
        else:
            self.stream.write(f'{line}\n')
        self.stream.flush()


def build_search_params(start: int, rows: int) -> dict[str, str | int]:
    """
    Builds query parameters for a repository-wide recent-items search request.

    Called by: fetch_search_page()
    """
    params: dict[str, str | int] = {
        'q': '*:*',
        'rows': rows,
        'start': start,
        'fl': ','.join(SEARCH_FIELDS),
        'sort': f'{DATE_FIELD} desc',
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
    start: int,
    rows: int,
    http_call_count: dict[str, int],
) -> dict[str, Any]:
    """
    Fetches one page of recent-item search results.

    Called by: fetch_recent_docs()
    """
    increment_http_call_count(http_call_count)
    response: httpx.Response = client.get(SEARCH_BASE, params=build_search_params(start, rows), timeout=30)
    response.raise_for_status()
    page_data: dict[str, Any] = response.json()
    return page_data


def fetch_recent_docs(
    client: httpx.Client,
    requested_count: int,
    http_call_count: dict[str, int],
    progress_reporter: ProgressReporter | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    """
    Fetches the most recent search docs up to the requested count.

    Called by: main()
    """
    start: int = 0
    num_found: int = 0
    docs: list[dict[str, Any]] = []
    pages_fetched: int = 0
    total_pages: int | None = None
    search_progress_started: bool = False

    while len(docs) < requested_count:
        rows: int = min(ROWS_PER_PAGE, requested_count - len(docs))
        page_data: dict[str, Any] = fetch_search_page(client, start, rows, http_call_count)
        pages_fetched += 1
        response_data: dict[str, Any] = page_data.get('response', {})
        if start == 0:
            num_found = int(response_data.get('numFound', 0))
            available_count: int = min(requested_count, num_found)
            total_pages = max(1, (available_count + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE) if available_count else 1
            if progress_reporter is not None and total_pages > 1:
                progress_reporter.start_stage('Processing', total=total_pages, detail='searching recent items')
                search_progress_started = True
        page_docs: list[dict[str, Any]] = list(response_data.get('docs', []))
        if not page_docs:
            break
        docs.extend(page_docs)
        if progress_reporter is not None and search_progress_started:
            progress_reporter.update(
                completed=pages_fetched,
                total=total_pages,
                detail=f'search page {pages_fetched}/{total_pages}',
            )
        start += rows
        if start >= num_found:
            break

    return num_found, docs[:requested_count]


def choose_title(data: dict[str, Any]) -> str:
    """
    Chooses a display-ready title from a BDR JSON payload.

    Called by: summarize_recent_doc()
    """
    title: str = ''
    raw_title: Any = (
        data.get('primary_title') or data.get('mods_title_full_primary_tsi') or data.get('name') or data.get('title')
    )
    if isinstance(raw_title, str):
        title = raw_title.strip()
    elif isinstance(raw_title, list) and raw_title:
        first_entry: Any = raw_title[0]
        if isinstance(first_entry, str):
            title = first_entry.strip()
    return title


def normalize_date_value(raw_value: Any) -> str | None:
    """
    Normalizes a raw date value to a compact display string when possible.

    Called by: choose_deposit_date()
    """
    normalized_value: str | None = None
    if isinstance(raw_value, str):
        stripped_value: str = raw_value.strip()
        if stripped_value:
            normalized_value = stripped_value
    return normalized_value


def iter_candidate_values(raw_value: Any) -> list[Any]:
    """
    Expands a candidate field value into a list of values to inspect.

    Called by: choose_deposit_date()
    """
    candidate_values: list[Any] = []
    if isinstance(raw_value, list):
        candidate_values = raw_value
    elif raw_value is not None:
        candidate_values = [raw_value]
    return candidate_values


def choose_deposit_date(doc: dict[str, Any]) -> str | None:
    """
    Chooses a usable deposit date string from a search doc.

    Called by: summarize_recent_doc()
    """
    chosen_date: str | None = None
    raw_value: Any = doc.get(DATE_FIELD)
    for candidate_value in iter_candidate_values(raw_value):
        normalized_value: str | None = normalize_date_value(candidate_value)
        if normalized_value is not None:
            chosen_date = normalized_value
            break
    return chosen_date


def summarize_recent_doc(doc: dict[str, Any]) -> dict[str, Any]:
    """
    Converts a recent-item search doc into the output item shape.

    Called by: build_recent_items()
    """
    item_summary: dict[str, Any] = {
        'pid': str(doc.get('pid', '')).strip(),
        'primary_title': choose_title(doc),
        'deposit_date': choose_deposit_date(doc),
        '__collection_pids': choose_collection_pids(doc),
        'collections': [],
    }
    return item_summary


def choose_collection_pids(doc: dict[str, Any]) -> list[str]:
    """
    Chooses collection membership PIDs from a search doc.

    Called by: summarize_recent_doc()
    """
    collection_pids: list[str] = []
    seen: set[str] = set()
    raw_value: Any = doc.get(COLLECTION_MEMBERSHIP_FIELD)

    for candidate_value in iter_candidate_values(raw_value):
        if isinstance(candidate_value, str):
            normalized_pid: str = candidate_value.strip()
            if normalized_pid and PID_PATTERN.match(normalized_pid) and normalized_pid not in seen:
                seen.add(normalized_pid)
                collection_pids.append(normalized_pid)

    return collection_pids


def build_collection_url(collection_pid: str) -> str:
    """
    Builds a collection API URL for a PID.

    Called by: fetch_collection_json()
    """
    return COLLECTION_API_TEMPLATE.format(collection_pid=collection_pid)


def fetch_collection_json(
    client: httpx.Client,
    collection_pid: str,
    http_call_count: dict[str, int],
) -> dict[str, Any]:
    """
    Fetches collection JSON from the BDR collections API.

    Called by: fetch_collection_title()
    """
    increment_http_call_count(http_call_count)
    response: httpx.Response = client.get(build_collection_url(collection_pid), timeout=30)
    response.raise_for_status()
    collection_data: dict[str, Any] = response.json()
    return collection_data


def classify_http_status(exc: httpx.HTTPStatusError) -> int | None:
    """
    Classifies an HTTP status error into a numeric status code when available.

    Called by: enrich_recent_items_with_collections(), fetch_collection_title()
    """
    status_code: int | None = None
    if exc.response is not None:
        status_code = exc.response.status_code
    return status_code


def build_collection_title(collection_data: dict[str, Any]) -> str | None:
    """
    Builds a display-ready collection title with parent collection provenance.

    Called by: fetch_collection_title()
    """
    base_title: str = choose_title(collection_data)
    parent_title: str = ''
    ancestors: Any = collection_data.get('ancestors')
    derived_title: str | None = None

    if isinstance(ancestors, list) and ancestors:
        last_ancestor: Any = ancestors[-1]
        if isinstance(last_ancestor, dict):
            parent_title = choose_title(last_ancestor)
        elif isinstance(last_ancestor, str):
            parent_title = last_ancestor.strip()

    if base_title:
        if parent_title:
            derived_title = f'`{base_title}` -- (from parent-collection `{parent_title}`)'
        else:
            derived_title = base_title

    return derived_title


def fetch_collection_title(
    client: httpx.Client,
    collection_pid: str,
    http_call_count: dict[str, int],
    collection_title_cache: dict[str, str | None],
    skipped_collections: list[dict[str, Any]],
) -> str | None:
    """
    Fetches and caches a collection title from the collections API.

    Called by: enrich_recent_items_with_collections()
    """
    if collection_pid not in collection_title_cache:
        try:
            collection_data: dict[str, Any] = fetch_collection_json(client, collection_pid, http_call_count)
            collection_title_cache[collection_pid] = build_collection_title(collection_data)
        except httpx.HTTPStatusError as exc:
            status_code: int | None = classify_http_status(exc)
            if status_code == 403:
                collection_title_cache[collection_pid] = None
                skipped_collections.append(
                    {
                        'collection_pid': collection_pid,
                        'reason': 'forbidden',
                        'status_code': 403,
                    }
                )
            else:
                raise
    return collection_title_cache[collection_pid]


def enrich_recent_items_with_collections(
    client: httpx.Client,
    recent_items: list[dict[str, Any]],
    http_call_count: dict[str, int],
    progress_reporter: ProgressReporter | None = None,
) -> dict[str, Any]:
    """
    Enriches recent items with collection titles derived from collection PIDs.

    Called by: main()
    """
    collection_title_cache: dict[str, str | None] = {}
    skipped_collections: list[dict[str, Any]] = []
    total_items: int = len(recent_items)

    for index, item in enumerate(recent_items, start=1):
        item_pid: str = str(item.get('pid', '')).strip()
        collection_entries: list[dict[str, str | None]] = []
        collection_pids: list[str] = item.pop('__collection_pids', [])
        for collection_pid in collection_pids:
            collection_title: str | None = fetch_collection_title(
                client=client,
                collection_pid=collection_pid,
                http_call_count=http_call_count,
                collection_title_cache=collection_title_cache,
                skipped_collections=skipped_collections,
            )
            collection_entries.append(
                {
                    'pid': collection_pid,
                    'title': collection_title,
                }
            )
        item['collections'] = collection_entries
        if progress_reporter is not None:
            progress_reporter.update(
                completed=index,
                total=total_items,
                detail=f'current {item_pid}; unique collections {len(collection_title_cache)}',
            )

    enrichment_data: dict[str, Any] = {
        'recent_items': recent_items,
        'skipped_collections': deduplicate_skipped_entries(skipped_collections, 'collection_pid'),
    }
    return enrichment_data


def deduplicate_skipped_entries(rows: list[dict[str, Any]], key_name: str) -> list[dict[str, Any]]:
    """
    Deduplicates skipped-entry rows by a chosen identifier key.

    Called by: enrich_recent_items_with_collections()
    """
    deduped_rows: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for row in rows:
        key_value: str = str(row.get(key_name, '')).strip()
        if key_value and key_value not in seen_keys:
            seen_keys.add(key_value)
            deduped_rows.append(row)
    return deduped_rows


def build_recent_items(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Builds recent-item output entries from search docs.

    Called by: main()
    """
    recent_items: list[dict[str, Any]] = []
    for doc in docs:
        recent_items.append(summarize_recent_doc(doc))
    return recent_items


def sort_collection_summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Sorts collection summary rows by count descending, then title and PID.

    Called by: build_collection_summary()
    """
    sorted_rows: list[dict[str, Any]] = sorted(
        rows,
        key=lambda row: (
            -int(row['recent_item_count']),
            str(row.get('collection_title') or ''),
            str(row.get('collection_pid') or ''),
        ),
    )
    return sorted_rows


def build_collection_summary(recent_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Aggregates recent items into per-collection counts.

    Called by: main()
    """
    collection_counter: Counter[str] = Counter()
    collection_titles: dict[str, str | None] = {}

    for item in recent_items:
        for collection in item.get('collections', []):
            collection_pid: str = str(collection.get('pid', '')).strip()
            if not collection_pid:
                continue
            collection_counter[collection_pid] += 1
            if collection_pid not in collection_titles:
                title_value: Any = collection.get('title')
                collection_titles[collection_pid] = title_value if isinstance(title_value, str) else None

    rows: list[dict[str, Any]] = []
    for collection_pid in collection_counter:
        rows.append(
            {
                'collection_pid': collection_pid,
                'collection_title': collection_titles.get(collection_pid),
                'recent_item_count': collection_counter[collection_pid],
            }
        )

    return sort_collection_summary_rows(rows)


def count_unique_collections(recent_items: list[dict[str, Any]]) -> int:
    """
    Counts distinct collection PIDs represented across recent items.

    Called by: main()
    """
    unique_collection_pids: set[str] = set()
    for item in recent_items:
        for collection in item.get('collections', []):
            collection_pid: str = str(collection.get('pid', '')).strip()
            if collection_pid:
                unique_collection_pids.add(collection_pid)
    return len(unique_collection_pids)


def build_output_data(
    requested_count: int,
    num_found: int,
    recent_items: list[dict[str, Any]],
    collection_summary: list[dict[str, Any]],
    http_call_count: int,
    skipped_collections: list[dict[str, Any]],
    elapsed_seconds: float,
) -> dict[str, Any]:
    """
    Builds the final pretty-printable JSON payload for stdout output.

    Called by: main()
    """
    output_data: dict[str, Any] = {
        '_meta_': {
            'timestamp': datetime.now().astimezone().isoformat(),
            'timetaken': format_elapsed_timetaken(elapsed_seconds),
            'requested_recent_items_count': requested_count,
            'items_returned': len(recent_items),
            'repository_items_count': num_found,
            'collections_counted': len(collection_summary),
            'collections_skipped_forbidden': len(skipped_collections),
            'api_search_url': SEARCH_BASE,
            'api_collection_template': COLLECTION_API_TEMPLATE,
            'http_calls': http_call_count,
            'note': 'Collection totals may exceed displayed items because an item may belong to multiple collections.',
        },
        'collection_summary': collection_summary,
        'recent_items': recent_items,
        'skipped_collections': skipped_collections,
    }
    return output_data


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Parses command-line arguments for the recent-activity script.

    Called by: main()
    """
    parser = argparse.ArgumentParser(description='Print recent BDR repository activity as formatted JSON.')
    parser.add_argument(
        '--recent-items-count',
        type=int,
        default=DEFAULT_RECENT_ITEMS_COUNT,
        help='Number of most recent repository items to include; defaults to 100',
    )
    progress_group = parser.add_mutually_exclusive_group()
    progress_group.add_argument(
        '--progress',
        action='store_true',
        help='Show progress updates on stderr, even when stderr is not a TTY',
    )
    progress_group.add_argument(
        '--no-progress',
        action='store_true',
        help='Suppress progress updates on stderr',
    )
    parsed_args: argparse.Namespace = parser.parse_args(argv)
    if parsed_args.recent_items_count < 1:
        parser.error('--recent-items-count must be a positive integer')
    return parsed_args


def main(argv: list[str] | None = None) -> int:
    """
    Orchestrates recent-item retrieval, enrichment, aggregation, and stdout output.

    Called by: __main__
    """
    args: argparse.Namespace = parse_args(argv)
    headers: dict[str, str] = {'Accept': 'application/json'}
    transport = httpx.HTTPTransport(retries=2)
    http_call_count: dict[str, int] = {'count': 0}
    progress_enabled: bool = args.progress or (not args.no_progress and sys.stderr.isatty())
    progress_reporter = ProgressReporter(enabled=progress_enabled)
    started_at: float = time.monotonic()

    with httpx.Client(headers=headers, transport=transport) as client:
        num_found, docs = fetch_recent_docs(
            client,
            args.recent_items_count,
            http_call_count,
            progress_reporter=progress_reporter,
        )
        recent_items: list[dict[str, Any]] = build_recent_items(docs)
        progress_reporter.start_stage('Processing', total=len(recent_items), detail='fetching item and collection details')
        enrichment_data: dict[str, Any] = enrich_recent_items_with_collections(
            client,
            recent_items,
            http_call_count,
            progress_reporter=progress_reporter,
        )
        progress_reporter.finish(
            completed=len(recent_items),
            total=len(recent_items),
            detail=f'unique collections {count_unique_collections(enrichment_data["recent_items"])}',
        )

    recent_items = enrichment_data['recent_items']
    skipped_collections: list[dict[str, Any]] = enrichment_data['skipped_collections']
    collection_summary: list[dict[str, Any]] = build_collection_summary(recent_items)
    output_data: dict[str, Any] = build_output_data(
        requested_count=args.recent_items_count,
        num_found=num_found,
        recent_items=recent_items,
        collection_summary=collection_summary,
        http_call_count=http_call_count['count'],
        skipped_collections=skipped_collections,
        elapsed_seconds=time.monotonic() - started_at,
    )
    print(json.dumps(output_data, indent=2, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
