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
  uv run ./gather_extracted_text.py --collection-pid bdr:bfttpwkj --output-dir "../output_dir" --test-limit 4

Args:
  --collection-pid (required)
  --output-dir (required)
  --test-limit (optional) -- convenient for testing

TODO:
- I just refactored 30 top-level functions into classes.
- I want to add docstrings to the classes -- and lots of comments to the main() function --
  to make the code easier to follow.
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


class CollectionMetadata:
    """
    Parses collection metadata values from collection JSON.
    """

    @staticmethod
    def title_from_json(coll_json: dict[str, object]) -> str:
        """
        Prepares a customized collection-title from a collection's item-api JSON.

        Uses the collection's `name` and, when available, the last ancestor's name/title
        to append a source in the form " -- (from {parent})".

        The reason is because lots of collections make be named "Theses and Dissertations",
        and I want users to be able to see at a glance which collection they're a part of.
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
            coll_title = f'{coll_name} -- (from {parent_name})'
        else:
            coll_title = coll_name or ''
        return coll_title


class UrlBuilder:
    """
    Builds canonical URLs used throughout the tool.
    """

    def __init__(self, base: str = BASE) -> None:
        self.base: str = base

    def item_api_url(self, pid: str) -> str:
        return ITEM_URL_TPL.format(pid=pid)

    def studio_url(self, pid: str) -> str:
        return f'{self.base}/studio/item/{pid}/'

    def storage_text_url(self, pid: str) -> str:
        return STORAGE_URL_TPL.format(pid=pid)


class ItemTextResolver:
    """
    Finds EXTRACTED_TEXT links and sizes from item JSON, including child handling helpers.
    """

    def __init__(self, storage_url_tpl: str = STORAGE_URL_TPL) -> None:
        self.storage_url_tpl: str = storage_url_tpl

    def extract_child_pids(self, item_json: dict[str, object]) -> list[str]:
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

    def extract_size_from_datastreams(self, item_json: dict[str, object]) -> int | None:
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

    def find_link_and_size(self, item_json: dict[str, object], pid: str) -> tuple[str, int | None] | None:
        """
        Locates EXTRACTED_TEXT download URL and size if available.
        Checks links.content_datastreams, then links.datastreams, then datastreams size + constructs URL.
        """
        links: dict[str, object] = item_json.get('links', {})  # type: ignore[assignment]
        content_ds: dict[str, object] = links.get('content_datastreams', {}) or {}  # type: ignore[assignment]
        if 'EXTRACTED_TEXT' in content_ds and isinstance(content_ds['EXTRACTED_TEXT'], str):
            url: str = content_ds['EXTRACTED_TEXT']  # type: ignore[index]
            size: int | None = self.extract_size_from_datastreams(item_json)
            return (url, size)

        alt_ds: dict[str, object] = links.get('datastreams', {}) or {}  # type: ignore[assignment]
        if 'EXTRACTED_TEXT' in alt_ds and isinstance(alt_ds['EXTRACTED_TEXT'], str):
            url = alt_ds['EXTRACTED_TEXT']  # type: ignore[index]
            size = self.extract_size_from_datastreams(item_json)
            return (url, size)

        ds_block: dict[str, object] = item_json.get('datastreams', {}) or {}  # type: ignore[assignment]
        if 'EXTRACTED_TEXT' in ds_block and isinstance(ds_block['EXTRACTED_TEXT'], dict):
            url = self.storage_url_tpl.format(pid=pid)
            size_obj: object = ds_block['EXTRACTED_TEXT'].get('size')  # type: ignore[index]
            size_int: int | None = (
                int(size_obj) if isinstance(size_obj, int) or (isinstance(size_obj, str) and size_obj.isdigit()) else None
            )
            return (url, size_int)
        return None


class ApiClient:
    """
    Encapsulates httpx interactions and retry/backoff policies.
    """

    def __init__(self, client: httpx.Client) -> None:
        self.client: httpx.Client = client

    def get_with_retries(self, url: str, *, max_tries: int = 4, timeout_s: float = 30.0) -> httpx.Response:
        last_exc: Exception | None = None
        for attempt in range(1, max_tries + 1):
            try:
                _sleep(0.2)
                resp: httpx.Response = self.client.get(url, timeout=timeout_s, follow_redirects=True)
                if resp.status_code >= 500:
                    raise httpx.HTTPStatusError(f'server error {resp.status_code}', request=resp.request, response=resp)
                return resp
            except (httpx.HTTPError, httpx.TransportError) as exc:
                last_exc = exc
                _sleep(min(2**attempt, 15))
        assert last_exc is not None
        raise last_exc

    def stream_text_with_retries(self, url: str, *, max_tries: int = 4, timeout_s: float = 60.0) -> str:
        last_exc: Exception | None = None
        for attempt in range(1, max_tries + 1):
            try:
                _sleep(0.2)
                with self.client.stream('GET', url, timeout=timeout_s, follow_redirects=True) as resp:
                    resp.raise_for_status()
                    chunks: list[str] = []
                    for chunk in resp.iter_text():
                        if chunk:
                            chunks.append(chunk)
                    return ''.join(chunks)
            except (httpx.HTTPError, httpx.TransportError) as exc:
                last_exc = exc
                _sleep(min(2**attempt, 15))
        assert last_exc is not None
        raise last_exc

    def search_collection_pids(self, collection_pid: str) -> list[dict[str, object]]:
        rows: int = 500
        start: int = 0
        docs: list[dict[str, object]] = []
        fq: str = f'rel_is_member_of_collection_ssim:"{collection_pid}"'
        fl: str = 'pid,primary_title'
        while True:
            url: str = f'{SEARCH_URL}?q=*:*&fq={httpx.QueryParams({"fq": fq})["fq"]}&fl={fl}&rows={rows}&start={start}'
            log.debug(f' trying search url, ``{url}``')
            resp: httpx.Response = self.get_with_retries(url)
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

    def fetch_item_json(self, pid: str) -> dict[str, object]:
        url: str = ITEM_URL_TPL.format(pid=pid)
        log.debug(f'trying item url, ``{url}``')
        resp: httpx.Response = self.get_with_retries(url)
        resp.raise_for_status()
        return resp.json()

    def fetch_collection_json(self, pid: str) -> dict[str, object]:
        url: str = COLLECTION_URL_TPL.format(pid=pid)
        resp: httpx.Response = self.get_with_retries(url)
        resp.raise_for_status()
        return resp.json()


class RunDirectoryManager:
    """
    Manages run directory naming, prior-run detection, path creation, and copy-forward.
    """

    def __init__(self, out_dir: Path, safe_collection_pid: str) -> None:
        self.out_dir: Path = out_dir
        self.safe_collection_pid: str = safe_collection_pid
        self.run_dir: Path | None = None

    def run_dir_name_for(self) -> str:
        return f'run-{_now_compact_local()}-{self.safe_collection_pid}'

    def create_run_dir(self) -> Path:
        name: str = self.run_dir_name_for()
        p: Path = self.out_dir / name
        p.mkdir(parents=True, exist_ok=True)
        self.run_dir = p
        return p

    def _is_run_dir_for(self, path: Path) -> bool:
        name: str = path.name
        return name.startswith('run-') and name.endswith(f'-{self.safe_collection_pid}') and path.is_dir()

    def find_latest_prior_run_dir(self) -> Path | None:
        candidates: list[Path] = [p for p in self.out_dir.iterdir() if self._is_run_dir_for(p)]
        if not candidates:
            return None
        candidates.sort(key=lambda p: p.name, reverse=True)
        latest: Path = candidates[0]
        ck: Path = latest / f'checkpoint_for_collection_pid-{self.safe_collection_pid}.json'
        listing_p: Path = latest / f'listing_for_collection_pid-{self.safe_collection_pid}.json'
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

    def copy_prior_outputs(self, prior_dir: Path) -> None:
        assert self.run_dir is not None
        prior_combined: Path = prior_dir / f'extracted_text_for_collection_pid-{self.safe_collection_pid}.txt'
        prior_listing: Path = prior_dir / f'listing_for_collection_pid-{self.safe_collection_pid}.json'
        new_combined: Path = self.run_dir / prior_combined.name
        new_listing: Path = self.run_dir / prior_listing.name
        if prior_combined.exists():
            shutil.copy2(prior_combined, new_combined)
        if prior_listing.exists():
            shutil.copy2(prior_listing, new_listing)

    def combined_text_path(self) -> Path:
        assert self.run_dir is not None
        return self.run_dir / f'extracted_text_for_collection_pid-{self.safe_collection_pid}.txt'

    def listing_path(self) -> Path:
        assert self.run_dir is not None
        return self.run_dir / f'listing_for_collection_pid-{self.safe_collection_pid}.json'

    def checkpoint_path(self) -> Path:
        assert self.run_dir is not None
        return self.run_dir / f'checkpoint_for_collection_pid-{self.safe_collection_pid}.json'


class ListingStore:
    """
    Owns listing JSON data and I/O; provides helper methods.
    """

    def __init__(self, path: Path) -> None:
        self.path: Path = path
        self.data: dict[str, object] = {}

    def load_or_init(self) -> None:
        if self.path.exists():
            with self.path.open('r', encoding='utf-8') as fh:
                self.data = json.load(fh)
            return
        self.data = {
            'summary': {
                'timestamp': _now_iso(),
                'all_extracted_text_file_size': '0 Bytes',
                'count_of_all_extracted_text_files': 0,
                'combined_text_path': '',
                'listing_path': '',
                'collection_pid': '',
                'collection_primary_title': '',
            },
            'items': [],
        }

    def save(self) -> None:
        self.data['summary']['timestamp'] = _now_iso()
        with self.path.open('w', encoding='utf-8') as fh:
            json.dump(self.data, fh, ensure_ascii=False, indent=2)

    def add_entry(self, *, item_pid: str, primary_title: str, item_api_url: str, studio_url: str, size: int | None) -> None:
        items: list[dict[str, object]] = self.data.setdefault('items', [])  # type: ignore[assignment]
        idx: int | None = next((i for i, d in enumerate(items) if d.get('item_pid') == item_pid), None)
        human_size: str | None = humanize.naturalsize(size) if isinstance(size, int) else None
        entry: dict[str, object] = {
            'item_pid': item_pid,
            'primary_title': primary_title,
            'full_item_api_url': item_api_url,
            'full_studio_url': studio_url,
            'extracted_text_file_size': human_size,
        }
        if idx is None:
            items.append(entry)
        else:
            items[idx] = entry

    def processed_set(self) -> set[str]:
        done: set[str] = set()
        for item in self.data.get('items', []):
            pid: object = item.get('item_pid')  # type: ignore[index]
            if isinstance(pid, str):
                done.add(pid)
        return done

    def update_summary(self, combined_path: Path) -> None:
        count: int = sum(1 for d in self.data.get('items', []) if d.get('extracted_text_file_size'))
        size: int = combined_path.stat().st_size if combined_path.exists() else 0
        self.data['summary'].pop('all_extracted_text_file_size_bytes', None)
        self.data['summary'].pop('all_extracted_text_file_size_human', None)
        self.data['summary']['count_of_all_extracted_text_files'] = count
        self.data['summary']['all_extracted_text_file_size'] = humanize.naturalsize(size)
        self.data['summary']['timestamp'] = _now_iso()
        self.data['summary']['combined_text_path'] = f'{combined_path.parent.name}/{combined_path.name}'
        self.data['summary']['listing_path'] = f'{self.path.parent.name}/{self.path.name}'

    def counts(self, total_docs: int) -> dict[str, int]:
        items: list[dict[str, object]] = self.data.get('items', [])  # type: ignore[assignment]
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

    def set_collection_info(self, collection_pid: str, collection_title: str) -> None:
        self.data['summary']['collection_pid'] = collection_pid
        self.data['summary']['collection_primary_title'] = collection_title


class CheckpointStore:
    """
    Manages checkpoint JSON lifecycle and counts.
    """

    def __init__(self, path: Path) -> None:
        self.path: Path = path
        self.data: dict[str, object] = {}

    def load_or_init(
        self,
        collection_pid: str,
        safe_collection_pid: str,
        run_directory_name: str,
        listing: ListingStore,
        combined_path: Path,
        listing_path: Path,
    ) -> None:
        existing: dict[str, object] | None = None
        if self.path.exists():
            try:
                with self.path.open('r', encoding='utf-8') as fh:
                    existing = json.load(fh)
            except Exception:
                existing = None
        created_at: str = (
            existing.get('created_at')
            if isinstance(existing, dict) and isinstance(existing.get('created_at'), str)
            else _now_iso()
        )
        counts: dict[str, int] = listing.counts(total_docs=0)
        self.data = {
            'collection_pid': collection_pid,
            'safe_collection_pid': safe_collection_pid,
            'created_at': created_at,
            'updated_at': _now_iso(),
            'run_directory_name': run_directory_name,
            'completed': False,
            'counts': counts,
            'paths': {
                'combined_text': f'{combined_path.parent.name}/{combined_path.name}',
                'listing_json': f'{listing_path.parent.name}/{listing_path.name}',
            },
        }
        with self.path.open('w', encoding='utf-8') as fh:
            json.dump(self.data, fh, ensure_ascii=False, indent=2)

    def save(
        self,
        collection_pid: str,
        safe_collection_pid: str,
        run_directory_name: str,
        listing: ListingStore,
        combined_path: Path,
        listing_path: Path,
        *,
        total_docs: int,
        completed: bool,
    ) -> None:
        created_at: str = self.data.get('created_at') if isinstance(self.data.get('created_at'), str) else _now_iso()  # type: ignore[assignment]
        self.data['collection_pid'] = collection_pid
        self.data['safe_collection_pid'] = safe_collection_pid
        self.data['created_at'] = created_at
        self.data['updated_at'] = _now_iso()
        self.data['run_directory_name'] = run_directory_name
        self.data['completed'] = completed
        self.data['counts'] = listing.counts(total_docs=total_docs)
        self.data['paths'] = {
            'combined_text': f'{combined_path.parent.name}/{combined_path.name}',
            'listing_json': f'{listing_path.parent.name}/{listing_path.name}',
        }
        with self.path.open('w', encoding='utf-8') as fh:
            json.dump(self.data, fh, ensure_ascii=False, indent=2)

    def mark_completed(self) -> None:
        self.data['completed'] = True
        self.data['updated_at'] = _now_iso()
        with self.path.open('w', encoding='utf-8') as fh:
            json.dump(self.data, fh, ensure_ascii=False, indent=2)


class CombinedTextWriter:
    """
    Owns the combined text file and append operations.
    """

    def __init__(self, path: Path) -> None:
        self.path: Path = path

    def ensure_file(self) -> None:
        self.path.touch(exist_ok=True)

    def append(self, pid: str, text: str) -> None:
        prefix = f'---|||start-of-pid:{pid}|||---\n'
        with self.path.open('a', encoding='utf-8') as fh:
            fh.write(prefix)
            fh.write(text.rstrip('\n'))
            fh.write('\n')


class ExtractionProcessor:
    """
    Handles per-pid processing using collaborators.
    """

    def __init__(
        self, api: ApiClient, resolver: ItemTextResolver, urls: UrlBuilder, writer: CombinedTextWriter, listing: ListingStore
    ) -> None:
        self.api = api
        self.resolver = resolver
        self.urls = urls
        self.writer = writer
        self.listing = listing

    def process_pid(self, pid: str) -> bool:
        item_json: dict[str, object] = self.api.fetch_item_json(pid)
        primary_title: str = item_json.get('primary_title') or item_json.get('mods_title_full_primary_tsi') or ''  # type: ignore[assignment]
        studio_url: str = item_json.get('uri') or self.urls.studio_url(pid)  # type: ignore[assignment]
        item_api_url: str = self.urls.item_api_url(pid)

        found: tuple[str, int | None] | None = self.resolver.find_link_and_size(item_json, pid)
        if found:
            url, size = found
            try:
                text: str = self.api.stream_text_with_retries(url)
            except httpx.HTTPStatusError as exc:
                if exc.response is not None and exc.response.status_code == 403:
                    self.listing.add_entry(
                        item_pid=pid,
                        primary_title=primary_title,
                        item_api_url=item_api_url,
                        studio_url=studio_url,
                        size=None,
                    )
                    # mark forbidden for counts via optional status
                    self.listing.data['items'][-1]['status'] = 'forbidden'  # type: ignore[index]
                    return False
                raise
            self.writer.append(pid, text)
            self.listing.add_entry(
                item_pid=pid,
                primary_title=primary_title,
                item_api_url=item_api_url,
                studio_url=studio_url,
                size=size,
            )
            return True

        # try children via hasPart
        child_pids: list[str] = self.resolver.extract_child_pids(item_json)
        for child_pid in child_pids:
            child_json: dict[str, object] = self.api.fetch_item_json(child_pid)
            child_title: str = child_json.get('primary_title') or child_json.get('mods_title_full_primary_tsi') or ''  # type: ignore[assignment]
            child_studio_url: str = child_json.get('uri') or self.urls.studio_url(child_pid)  # type: ignore[assignment]
            child_api_url: str = self.urls.item_api_url(child_pid)
            child_found: tuple[str, int | None] | None = self.resolver.find_link_and_size(child_json, child_pid)
            if child_found:
                url, size = child_found
                try:
                    text = self.api.stream_text_with_retries(url)
                except httpx.HTTPStatusError as exc:
                    if exc.response is not None and exc.response.status_code == 403:
                        self.listing.add_entry(
                            item_pid=child_pid,
                            primary_title=child_title,
                            item_api_url=child_api_url,
                            studio_url=child_studio_url,
                            size=None,
                        )
                        self.listing.data['items'][-1]['status'] = 'forbidden'  # type: ignore[index]
                        self.listing.add_entry(
                            item_pid=pid,
                            primary_title=primary_title,
                            item_api_url=item_api_url,
                            studio_url=studio_url,
                            size=None,
                        )
                        self.listing.data['items'][-1]['status'] = 'forbidden_via_child'  # type: ignore[index]
                        return False
                    raise
                self.writer.append(child_pid, text)
                self.listing.add_entry(
                    item_pid=child_pid,
                    primary_title=child_title,
                    item_api_url=child_api_url,
                    studio_url=child_studio_url,
                    size=size,
                )
                self.listing.add_entry(
                    item_pid=pid,
                    primary_title=primary_title,
                    item_api_url=item_api_url,
                    studio_url=studio_url,
                    size=None,
                )
                self.listing.data['items'][-1]['status'] = 'handled_via_child'  # type: ignore[index]
                return True

        # no extracted text found
        self.listing.add_entry(
            item_pid=pid,
            primary_title=primary_title,
            item_api_url=item_api_url,
            studio_url=studio_url,
            size=None,
        )
        return False


class CLI:
    """
    Encapsulates CLI building and parsing.
    """

    @staticmethod
    def build_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description='Collect EXTRACTED_TEXT for a collection.')
        parser.add_argument('--collection-pid', required=True, help='Collection PID like bdr:c9fzffs9')
        parser.add_argument('--output-dir', required=True, help='Directory to write outputs')
        parser.add_argument(
            '--test-limit',
            type=int,
            default=None,
            metavar='INTEGER',
            help='Optional. Stop after this many extracted_texts have been successfully appended (useful for testing).',
        )
        return parser

    @staticmethod
    def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
        return CLI.build_parser().parse_args(argv)


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


def main() -> int:
    """
    Fetches collection members, finds EXTRACTED_TEXT, writes combined text and JSON listing with resume support.
    """
    args: argparse.Namespace = CLI.parse_args()

    collection_pid: str = args.collection_pid.strip()
    safe_collection_pid: str = collection_pid.replace(':', '_')
    out_dir: Path = Path(args.output_dir).expanduser().resolve()

    run_mgr = RunDirectoryManager(out_dir, safe_collection_pid)
    prior_dir: Path | None = run_mgr.find_latest_prior_run_dir()
    ts_dir: Path = run_mgr.create_run_dir()
    if prior_dir is not None:
        run_mgr.copy_prior_outputs(prior_dir)

    combined_txt_path: Path = run_mgr.combined_text_path()
    listing_json_path: Path = run_mgr.listing_path()
    checkpoint_json_path: Path = run_mgr.checkpoint_path()

    listing_store = ListingStore(listing_json_path)
    listing_store.load_or_init()

    writer = CombinedTextWriter(combined_txt_path)
    writer.ensure_file()

    effective_limit: int | None = None
    if args.test_limit is not None:
        prior_appended: int = listing_store.counts(total_docs=0)['appended_count']
        effective_limit = max(0, args.test_limit - prior_appended)

    checkpoint = CheckpointStore(checkpoint_json_path)
    checkpoint.load_or_init(
        collection_pid, safe_collection_pid, ts_dir.name, listing_store, combined_txt_path, listing_json_path
    )

    headers: dict[str, str] = {'user-agent': 'bdr-extracted-text-collector/1.0 (+https://repository.library.brown.edu/)'}
    timeout: httpx.Timeout = httpx.Timeout(connect=30.0, read=60.0, write=60.0, pool=30.0)
    limits: httpx.Limits = httpx.Limits(max_keepalive_connections=10, max_connections=10)
    with httpx.Client(headers=headers, timeout=timeout, limits=limits) as client:
        api = ApiClient(client)
        resolver = ItemTextResolver()
        urls = UrlBuilder(BASE)

        try:
            coll_json: dict[str, object] = api.fetch_collection_json(collection_pid)
            coll_title: str = CollectionMetadata.title_from_json(coll_json)
        except Exception:
            coll_title = ''
        listing_store.set_collection_info(collection_pid, coll_title)

        docs: list[dict[str, object]] = api.search_collection_pids(collection_pid)
        checkpoint.save(
            collection_pid,
            safe_collection_pid,
            ts_dir.name,
            listing_store,
            combined_txt_path,
            listing_json_path,
            total_docs=len(docs),
            completed=False,
        )
        if not docs:
            print(f'No items found for collection {collection_pid}', file=sys.stderr)

        if effective_limit == 0:
            listing_store.update_summary(combined_txt_path)
            listing_store.save()
            checkpoint.save(
                collection_pid,
                safe_collection_pid,
                ts_dir.name,
                listing_store,
                combined_txt_path,
                listing_json_path,
                total_docs=len(docs),
                completed=False,
            )
            print('Done. Appended text for 0 item(s). (Effective limit reached from prior run.)')
            print(f'Combined text: {combined_txt_path}')
            print(f'Listing JSON:  {listing_json_path}')
            return 0

        appended_count: int = 0
        processed: set[str] = listing_store.processed_set()
        processor = ExtractionProcessor(api, resolver, urls, writer, listing_store)
        for doc in tqdm(docs, total=len(docs), desc='Processing items'):
            pid: object = doc.get('pid')
            if not isinstance(pid, str) or pid in processed:
                continue
            try:
                if processor.process_pid(pid):
                    appended_count += 1
                    if effective_limit is not None and appended_count >= effective_limit:
                        listing_store.update_summary(combined_txt_path)
                        listing_store.save()
                        checkpoint.save(
                            collection_pid,
                            safe_collection_pid,
                            ts_dir.name,
                            listing_store,
                            combined_txt_path,
                            listing_json_path,
                            total_docs=len(docs),
                            completed=False,
                        )
                        break
            except Exception as exc:
                listing_store.add_entry(
                    item_pid=pid,
                    primary_title=doc.get('primary_title') or '',  # type: ignore[index]
                    item_api_url=urls.item_api_url(pid),
                    studio_url=urls.studio_url(pid),
                    size=None,
                )
                print(f'Error processing {pid}: {exc}', file=sys.stderr)

            listing_store.update_summary(combined_txt_path)
            listing_store.save()
            checkpoint.save(
                collection_pid,
                safe_collection_pid,
                ts_dir.name,
                listing_store,
                combined_txt_path,
                listing_json_path,
                total_docs=len(docs),
                completed=False,
            )

        listing_store.update_summary(combined_txt_path)
        listing_store.save()
        checkpoint.save(
            collection_pid,
            safe_collection_pid,
            ts_dir.name,
            listing_store,
            combined_txt_path,
            listing_json_path,
            total_docs=len(docs),
            completed=True,
        )

    print(f'Done. Appended text for {appended_count} item(s).')
    print(f'Combined text: {combined_txt_path}')
    print(f'Listing JSON:  {listing_json_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
