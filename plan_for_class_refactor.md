# Refactor Plan: Class-based organization for `gather_extracted_text.py`

This plan proposes a class-based reorganization of `bdr-api-tools/gather_extracted_text.py` to improve structure and maintainability without changing user-facing CLI behavior or on-disk outputs.

It preserves current CLI behavior and file outputs, and follows the project coding guidelines. Functions referenced by tests may be moved into classes; tests will be updated to call the new class methods:

- Use Python 3.12 type hints everywhere; avoid unnecessary `typing` imports.
- Structure scripts with `if __name__ == '__main__': main()` and keep `main()` simple.
- Use `httpx` for all HTTP calls.
- Do not use nested functions; prefer single-return functions.
- Use present-tense triple-quoted function docstrings.

All design choices below are made to align with these guidelines.


## 0) Maintainer preferences for this refactor

- Keep `main()` as an independent manager function. Do not bundle it into another class. It is OK to change existing calls inside `main()` from standalone functions to class methods.
- Only introduce a class if it will have two or more methods. If a concept would result in fewer than two methods, keep the logic as standalone functions for now.
- Functions referenced by tests may be moved into classes. Update tests to call the new class methods instead of module-level functions.
- Do not create a separate run-level "runner" class; per-run orchestration remains in `main()`.
- Do not provide thin wrappers around the new methods. Remove old top-level functions where they are absorbed into classes.


## 1) Current system overview

`gather_extracted_text.py` currently contains 30 functions that together:
- Fetch collection metadata and item lists from BDR APIs.
- For each item (or its children), discover an `EXTRACTED_TEXT` asset.
- Stream the text, append it to a combined output file, and update a JSON listing.
- Maintain a checkpoint to support resume and interruption handling.
- Optionally copy forward outputs from a prior (incomplete) run.

Endpoints/constants used:
- `BASE = https://repository.library.brown.edu`
- `SEARCH_URL = f"{BASE}/api/search/"`
- `ITEM_URL_TPL = f"{BASE}/api/items/{pid}/"`
- `STORAGE_URL_TPL = f"{BASE}/storage/{pid}/EXTRACTED_TEXT/"`
- `COLLECTION_URL_TPL = f"{BASE}/api/collections/{pid}/"`


## 2) Current code-flow (high-level steps)

1. Parse CLI args (`--collection-pid`, `--output-dir`, optional `--test-limit`).
2. Normalize inputs (compute `safe_collection_pid`, resolve `out_dir`, ensure it exists).
3. Determine resume context:
   - Search for the latest prior run directory matching the `safe_collection_pid`.
   - If found, later copy forward combined text and listing.
4. Create a new timestamped run directory within `out_dir`.
5. Establish file paths for this run:
   - Combined text path `extracted_text_for_collection_pid-<safe>.txt`.
   - Listing JSON path `listing_for_collection_pid-<safe>.json`.
   - Checkpoint JSON path `checkpoint_for_collection_pid-<safe>.json`.
6. If a prior run exists, copy forward prior outputs (combined text and listing) into the new run dir.
7. Load the listing (creating a default structure if not present) and ensure the combined file exists.
8. Compute an effective `test_limit` that subtracts any already-appended items from prior runs.
9. Initialize a minimal checkpoint (with `total_docs = 0`).
10. Open an `httpx.Client`.
11. Fetch collection metadata (`collection_title_from_json`) and record it in listing summary.
12. Fetch all collection item docs via search API (`search_collection_pids`).
13. Update the checkpoint with `total_docs`.
14. If `effective_limit == 0`, immediately persist summary/listing/checkpoint and exit.
15. Iterate all docs with a progress bar:
    - Skip docs lacking a string `pid`.
    - Skip pids already present in the listing (resume support).
    - For each new pid, `process_pid_for_extracted_text`:
      - Fetch item JSON; try to find `EXTRACTED_TEXT` for the item.
      - If not found, try all children (`relations.hasPart`).
      - If found, stream text and append, update listing entry with size.
      - Handle HTTP 403 by adding a listing entry with `status` markers without appending.
    - After each pid, persist: update summary, save listing, save checkpoint.
16. After the loop, persist a final summary, listing, and a checkpoint with `completed = True`.
17. Print final paths and counts; exit with code 0.


## 3) Function inventory grouped by purpose

Utilities (time/sleep):
- `_now_iso()`
- `_now_compact_local()`
- `_sleep()`

HTTP/Network:
- `_retrying_get(client, url, ...)`
- `_retrying_stream_text(client, url, ...)`
- `search_collection_pids(client, collection_pid)`
- `fetch_item_json(client, pid)`
- `fetch_collection_json(client, pid)`

Item parsing / title / link discovery:
- `collection_title_from_json(coll_json)`
- `_extract_child_pids(item_json)`
- `_find_extracted_text_link_and_size(item_json, pid)`
- `_extract_size_from_datastreams(item_json)`

Filesystem and run-dir management:
- `ensure_dir(path)`
- `_parent_dir_and_name(p)`
- `_run_dir_name_for(safe_collection_pid)`
- `_is_run_dir_for(path, safe_collection_pid)`
- `find_latest_prior_run_dir(out_dir, safe_collection_pid)`
- `copy_prior_outputs(prior_dir, new_dir, safe_collection_pid)`

Listing JSON management:
- `load_listing(path)`
- `save_listing(path, listing)`
- `add_listing_entry(listing, ...)`
- `already_processed(listing)`
- `processed_set_from_listing(listing)`
- `counts_from_listing(listing, total_docs=0)`
- `update_summary(listing, combined_path, listing_path)`

Checkpointing:
- `save_checkpoint(checkpoint_path, ..., listing, combined_path, listing_path, total_docs, completed)`

Per-pid processing:
- `append_text(out_txt_path, pid, text)`
- `process_pid_for_extracted_text(client, pid, out_txt_path, listing)`

CLI:
- `parse_args()`
- `main()`


## 4) Proposed class structure

The following classes group related functions and responsibilities. Only classes with two or more methods are introduced; otherwise, logic remains as standalone functions. The goal is to absorb as many top-level functions as is sensible into cohesive classes.

- ApiClient
  - Purpose: Encapsulates all `httpx` interactions and retry/backoff policies.
  - Key methods:
    - `get_with_retries(url: str) -> httpx.Response` (from `_retrying_get`)
    - `stream_text_with_retries(url: str) -> str` (from `_retrying_stream_text`)
    - `search_collection_pids(collection_pid: str) -> list[dict[str, object]]`
    - `fetch_item_json(pid: str) -> dict[str, object]`
    - `fetch_collection_json(pid: str) -> dict[str, object]`
  - Notes:
    - Holds a reference to an externally-managed `httpx.Client`.
    - Accepts base URLs/templates via constructor or uses module constants.

- ItemTextResolver
  - Purpose: Encapsulates logic to find `EXTRACTED_TEXT` links and sizes from item JSON, including fallbacks to children.
  - Key methods:
    - `find_link_and_size(item_json: dict, pid: str) -> tuple[str, int | None] | None`
    - `extract_child_pids(item_json: dict) -> list[str]`
    - `extract_size_from_datastreams(item_json: dict) -> int | None`
  - Notes:
    - Stateless; may accept `STORAGE_URL_TPL` via constructor for testability.

- CollectionMetadata
  - Purpose: Parse and format collection-centric metadata used in summaries and tests.
  - Key methods:
    - `title_from_json(coll_json: dict[str, object]) -> str` (moves `collection_title_from_json` into a class)
    - `pid_from_json(coll_json: dict[str, object]) -> str | None` (utility to satisfy 2+ methods and aid summary fields)

- UrlBuilder
  - Purpose: Build canonical URLs used throughout the tool for API, Studio, and storage.
  - Key methods:
    - `item_api_url(pid: str) -> str`
    - `studio_url(pid: str) -> str`
    - `storage_text_url(pid: str) -> str`

- RunDirectoryManager
  - Purpose: Manage run directory naming, detection of prior runs, and file path creation/copying.
  - Fields:
    - `out_dir: Path`, `safe_collection_pid: str`, `run_dir: Path` (set on create).
  - Key methods:
    - `run_dir_name_for() -> str` (from `_run_dir_name_for`)
    - `create_run_dir() -> Path` (ensures and sets `run_dir`)
    - `find_latest_prior_run_dir() -> Path | None`
    - `copy_prior_outputs(prior_dir: Path) -> None`
    - `combined_text_path() -> Path`, `listing_path() -> Path`, `checkpoint_path() -> Path`
  - Notes:
    - Performs necessary directory creation internally (no separate `ensure_dir` function).

- ListingStore
  - Purpose: Own in-memory listing dict and I/O to listing JSON; provide helper queries/updates.
  - Fields:
    - `path: Path`, `data: dict[str, object]`
  - Key methods:
    - `load_or_init() -> None` (from `load_listing`)
    - `save() -> None` (from `save_listing`)
    - `add_entry(item_pid: str, primary_title: str, item_api_url: str, studio_url: str, size: int | None, status: str | None = None) -> None` (from `add_listing_entry` + status tagging)
    - `processed_set() -> set[str]` (from `already_processed`/`processed_set_from_listing`)
    - `update_summary(combined_path: Path) -> None` (from `update_summary`; folds the `_parent_dir_and_name` formatting internally)
    - `counts(total_docs: int) -> dict[str, int]` (from `counts_from_listing`)
    - `set_collection_info(collection_pid: str, collection_title: str) -> None`

- CheckpointStore
  - Purpose: Manage checkpoint JSON lifecycle and counters.
  - Fields:
    - `path: Path`, `data: dict[str, object]`
  - Key methods:
    - `load_or_init(...) -> None` (initializes structure with created_at/updated_at)
    - `save(...) -> None` (replaces `save_checkpoint` function)
    - `mark_completed() -> None`

- CombinedTextWriter
  - Purpose: Own the combined text file and append operations.
  - Fields:
    - `path: Path`
  - Key methods:
    - `ensure_file() -> None` (create if missing)
    - `append(pid: str, text: str) -> None` (from `append_text`)

- ExtractionProcessor
  - Purpose: Handle per-pid processing (but not the whole run). Uses `ApiClient`, `ItemTextResolver`, `UrlBuilder`, `CombinedTextWriter`, `ListingStore`, and `CheckpointStore` to perform one unit of work.
  - Key methods:
    - `process_pid(pid: str) -> None` (from `process_pid_for_extracted_text`)

- CLI
  - Purpose: Encapsulate CLI assembly.
  - Key methods:
    - `build_parser() -> argparse.ArgumentParser`
    - `parse_args(argv: list[str] | None) -> argparse.Namespace`

- Clock
  - Purpose: Provide deterministic, central time handling.
  - Key methods:
    - `now_iso() -> str` (from `_now_iso`)
    - `now_compact_local() -> str` (from `_now_compact_local`)
    - `sleep(seconds: float) -> None` (from `_sleep`)

What remains top-level
- `main()` — remains the independent manager function that orchestrates a run by instantiating and using the classes above.
- Module constants (e.g., base URLs) — remain at the module level for clarity and single-source-of-truth.


## 5) Backwards compatibility guarantees

- Tests are updated alongside the refactor:
  - `tests/test_collection_title.py` is changed to import `CollectionMetadata` and call `CollectionMetadata.title_from_json(...)` instead of importing `collection_title_from_json` from the module.
  - Any other tests calling former top-level functions are updated to call the corresponding class methods.
- No thin wrappers are retained. Top-level functions absorbed into classes are removed to reduce the module’s public surface.
- Preserve CLI behavior precisely, including:
  - Command-line options and help text.
  - Output directory naming convention and file names.
  - Checkpoint JSON schema and listing JSON schema.
  - Logging format/noise suppression for `httpx`.


## 6) Old-to-new mapping (proposed)

Utilities:
- `_now_iso`, `_now_compact_local`, `_sleep` -> methods of `Clock`.

HTTP/Network:
- `_retrying_get` -> `ApiClient.get_with_retries()`
- `_retrying_stream_text` -> `ApiClient.stream_text_with_retries()`
- `search_collection_pids` -> `ApiClient.search_collection_pids()`
- `fetch_item_json` -> `ApiClient.fetch_item_json()`
- `fetch_collection_json` -> `ApiClient.fetch_collection_json()`

Item parsing / title / link discovery:
- `collection_title_from_json` -> `CollectionMetadata.title_from_json()`
- `_extract_child_pids` -> `ItemTextResolver.extract_child_pids()`
- `_find_extracted_text_link_and_size` -> `ItemTextResolver.find_link_and_size()`
- `_extract_size_from_datastreams` -> `ItemTextResolver.extract_size_from_datastreams()`

Filesystem / run-dir:
- `ensure_dir` -> folded into `RunDirectoryManager.create_run_dir()` and related methods.
- `_parent_dir_and_name` -> folded into `ListingStore` and `CheckpointStore` internal formatting helpers.
- `_run_dir_name_for` -> `RunDirectoryManager.run_dir_name_for()`
- `_is_run_dir_for` -> internal helper in `RunDirectoryManager`
- `find_latest_prior_run_dir` -> `RunDirectoryManager.find_latest_prior_run_dir()`
- `copy_prior_outputs` -> `RunDirectoryManager.copy_prior_outputs()`

Listing management:
- `load_listing` -> `ListingStore.load_or_init()`
- `save_listing` -> `ListingStore.save()`
- `add_listing_entry` -> `ListingStore.add_entry()`
- `already_processed`/`processed_set_from_listing` -> `ListingStore.processed_set()`
- `counts_from_listing` -> `ListingStore.counts()`
- `update_summary` -> `ListingStore.update_summary()` (internal path formatting)

Checkpoint:
- `save_checkpoint` -> `CheckpointStore.save()`; related init/update helpers -> `CheckpointStore.load_or_init()`, `CheckpointStore.mark_completed()`

Per-pid processing:
- `append_text` -> `CombinedTextWriter.append()`
- `process_pid_for_extracted_text` -> `ExtractionProcessor.process_pid()`

CLI:
- `parse_args` -> `CLI.parse_args()` (and `CLI.build_parser()`)
- `main` -> remains the independent manager function orchestrating the run using the classes above


## 7) Data model snapshots (for reference)

Listing JSON (current):
- `summary: { timestamp, all_extracted_text_file_size, count_of_all_extracted_text_files, combined_text_path, listing_path, collection_pid, collection_primary_title }`
- `items: [ { item_pid, primary_title, full_item_api_url, full_studio_url, extracted_text_file_size, status? } ]`

Checkpoint JSON (current):
- `{ collection_pid, safe_collection_pid, created_at, updated_at, run_directory_name, completed, counts: { total_docs, processed_count, appended_count, no_text_count, forbidden_count }, paths: { combined_text, listing_json } }`

Statuses used in listing items:
- Absent or `None` when text appended.
- `forbidden` (403 when fetching text for the item)
- `forbidden_via_child` (403 when fetching text for a child used to satisfy the parent)
- `handled_via_child` (text appended for child; parent marked as handled)


## 8) Implementation plan (step-by-step)

1. Introduce class blocks inside `gather_extracted_text.py` (no module splits yet):
   - Add `ApiClient`, `ItemTextResolver`, `CollectionMetadata`, `UrlBuilder`, `RunDirectoryManager`, `ListingStore`, `CheckpointStore`, `CombinedTextWriter`, `ExtractionProcessor`, `CLI`, and `Clock`.
   - Keep constants and logging at the top of `gather_extracted_text.py`.

2. Port logic into classes and delete absorbed top-level functions:
   - Move HTTP, parsing, filesystem, listing, checkpoint, append, and per-pid logic into the classes listed above.
   - Remove thin wrappers entirely.

3. Update `main()` to use class instances and methods:
   - Instantiate `RunDirectoryManager` to create the run and compute paths.
   - Instantiate `ListingStore`, `CheckpointStore`, `CombinedTextWriter`, `ApiClient`, `ItemTextResolver`, `UrlBuilder`, and `ExtractionProcessor`.
   - Replace calls to standalone functions with calls to the appropriate methods.

4. Update tests:
   - Change `tests/test_collection_title.py` to import `CollectionMetadata` and call `CollectionMetadata.title_from_json(...)`.
   - Adjust or add tests for moved methods where appropriate.

5. Preserve behavior:
   - Ensure identical CLI options and help text via `CLI`.
   - Keep JSON schema for listing and checkpoint unchanged.
   - Preserve retry/throttling and progress bar behavior.

6. Manual sanity checks against a small collection PID with and without `--test-limit`.

7. Optional later step: Extract mature classes to separate modules (e.g., `api_client.py`, `run_dir.py`) once stable. `main()` remains in `gather_extracted_text.py`.


## 9) Risks and mitigations

- Behavior drift in file naming or summary fields
  - Mitigation: Keep path templates and formatting identical; consolidate `_parent_dir_and_name` logic inside `ListingStore`/`CheckpointStore` and test it.

- Performance differences due to added abstraction
  - Mitigation: Methods remain thin; I/O frequency unchanged; `tqdm` usage preserved.

- Test churn due to method moves
  - Mitigation: Update tests in lockstep; add focused unit tests for new classes where helpful.


## 10) Acceptance criteria

- CLI usage and outputs are unchanged for end users.
- Tests are updated and pass (e.g., `tests/test_collection_title.py` now calls `CollectionMetadata.title_from_json`).
- Checkpoint and listing JSON schemas remain unchanged.
- The code is reorganized into cohesive classes where appropriate (2+ methods), and `main()` remains the independent manager function orchestrating the run using those classes.
- The number of top-level functions is dramatically reduced (target: `main()` plus a small number of module constants only).
- All new/ported functions and methods use 3.12 type hints and present-tense docstrings.


 ## 11) Notes for the implementer

- Prefer constructor injection for objects that hold state (paths, client). Avoid global state.
- Keep retry and throttling behavior exactly the same to avoid changing server load patterns.
- Avoid nested functions (project guideline). Keep methods small and single-purpose with single returns.
  - Logging: preserve the existing `logging.basicConfig(...)` format and the suppression of `httpx/httpcore` noise when `LOG_LEVEL<=INFO`.
  - When in doubt, favor preserving current behavior over additional refactoring.


## 12) Implementation cheat-sheet (for a fresh session)

This section captures the concrete constants, invariants, and pseudo-steps so the refactor can be implemented without re-reading the source.

### A) Constants and endpoints (keep values identical)
- `BASE = 'https://repository.library.brown.edu'`
- `SEARCH_URL = f'{BASE}/api/search/'`
- `ITEM_URL_TPL = f'{BASE}/api/items/{pid}/'`
- `STORAGE_URL_TPL = f'{BASE}/storage/{pid}/EXTRACTED_TEXT/'`
- `COLLECTION_URL_TPL = f'{BASE}/api/collections/{pid}/'`

### B) HTTP client settings and retry/backoff (preserve semantics)
- Headers: `{'user-agent': 'bdr-extracted-text-collector/1.0 (+https://repository.library.brown.edu/)'}`
- Timeout: `httpx.Timeout(connect=30.0, read=60.0, write=60.0, pool=30.0)`
- Limits: `httpx.Limits(max_keepalive_connections=10, max_connections=10)`
- Follow redirects: `follow_redirects=True` for GET and stream.
- Throttle before each request: `_sleep(0.2)` seconds.
- Retry/backoff parameters (apply to GET and streaming):
  - `max_tries=4`
  - Backoff between retries: `_sleep(min(2 ** attempt, 15))` seconds.
  - For `_retrying_get`: treat `resp.status_code >= 500` as retryable by raising `httpx.HTTPStatusError`.

### C) Logging behavior (preserve output characteristics)
- Configure root logging via env var `LOG_LEVEL` (default `INFO`).
- Format: `'[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s'` with date format `'%d/%b/%Y %H:%M:%S'`.
- Suppress noisy libs when `LOG_LEVEL <= INFO`: set `httpx` and `httpcore` to `WARNING` and `propagate = False`.

### D) CLI (args and help)
- `--collection-pid` (required), string like `bdr:c9fzffs9`.
- `--output-dir` (required): directory to write run subdir and outputs.
- `--test-limit` (optional int): stop after this many successful appends in THIS run.

### E) Files, directories, and naming conventions
- Safe collection pid: replace `:` with `_`, e.g., `bdr:bfttpwkj` → `bdr_bfttpwkj`.
- Run directory name: `run-<YYYYmmddTHHMMSS±ZZZZ>-<safe_collection_pid>` (local tz offset; see `_now_compact_local()`).
- Output files inside the run directory:
  - Combined text: `extracted_text_for_collection_pid-<safe>.txt`
  - Listing JSON: `listing_for_collection_pid-<safe>.json`
  - Checkpoint JSON: `checkpoint_for_collection_pid-<safe>.json`
- Path formatting inside JSON (summary and checkpoint): store as `parent-dir/filename` (not absolute paths), where `parent-dir` is the immediate dir name only.

### F) Listing JSON schema (unchanged)
```
summary: {
  timestamp,
  all_extracted_text_file_size,             # humanized string (via humanize.naturalsize)
  count_of_all_extracted_text_files,        # count of items with text appended
  combined_text_path,                       # parent-dir/filename
  listing_path,                             # parent-dir/filename
  collection_pid,
  collection_primary_title,
}
items: [
  {
    item_pid,
    primary_title,
    full_item_api_url,
    full_studio_url,
    extracted_text_file_size,               # humanized string or null
    status?,                                # optional: 'forbidden' | 'forbidden_via_child' | 'handled_via_child'
  }
]
```

### G) Checkpoint JSON schema (unchanged)
```
{
  collection_pid,
  safe_collection_pid,
  created_at,
  updated_at,
  run_directory_name,
  completed,                                # bool
  counts: {
    total_docs,
    processed_count,
    appended_count,
    no_text_count,
    forbidden_count,
  },
  paths: {
    combined_text,                          # parent-dir/filename
    listing_json,                           # parent-dir/filename
  }
}
```

### H) Search API behavior
- Use `rows=500`, paginate with `start`, stop when `start >= numFound`.
- Query: `q=*:*` with filter `fq=rel_is_member_of_collection_ssim:"<collection_pid>"`.
- Build URL safely: the current code computes `httpx.QueryParams({"fq": fq})["fq"]` and inlines it.
- Request `fl=pid,primary_title`.

### I) Per-item processing semantics (must match behavior)
- Preferred EXTRACTED_TEXT link discovery order:
  1) `links.content_datastreams.EXTRACTED_TEXT` (string URL)
  2) `links.datastreams.EXTRACTED_TEXT` (string URL)
  3) `datastreams.EXTRACTED_TEXT` (dict with `size`); construct URL via `STORAGE_URL_TPL`.
- Append combined text with a prefix line: `---|||start-of-pid:{pid}|||---\n` followed by the text (rstrip final newline) and then a trailing `\n`.
- On HTTP 403 when streaming text for the parent: add a listing entry for the parent with `status='forbidden'`; do not append text.
- If parent has no text, try each child pid from `relations.hasPart`:
  - On child 403: add an entry for the child with `status='forbidden'` AND add a parent entry with `status='forbidden_via_child'`.
  - On child success: append child text (with child pid in prefix), add entry for child, and add parent entry with `status='handled_via_child'`.
- For any other exception during a pid in the main loop, add a listing entry for the pid with `extracted_text_file_size = None` and print an error to stderr.

### J) Resume and prior-run handling
- Prior-run detection: find latest subdir (descending by name) matching `run-...-<safe_pid>` that contains a checkpoint file with `completed == False` AND a listing file.
- Resume copy-forward: copy prior combined text and listing JSON into the new run directory before proceeding.
- Effective test limit: if `--test-limit` is provided, compute `effective_limit = max(0, test_limit - prior_appended_count)` where `prior_appended_count` is the count of items with a truthy `extracted_text_file_size` already in the loaded listing. If `effective_limit == 0`, persist summary/listing/checkpoint and exit early.

### K) Progress and persistence cadence
- Use tqdm progress bar: `tqdm(docs, total=len(docs), desc="Processing items")`.
- After each pid (even on error), update summary, save listing, and save checkpoint.
- At the end, update summary, save listing, and save a checkpoint with `completed=True`.

### L) Printing (CLI UX)
- On early exit due to `effective_limit == 0`, print:
  - `Done. Appended text for 0 item(s). (Effective limit reached from prior run.)`
  - Then the combined text and listing JSON paths.
- On normal completion, print:
  - `Done. Appended text for {appended_count} item(s).`
  - Then the combined text and listing JSON paths.

### M) Class wiring pseudocode (sketch)
```
args = CLI.parse_args(argv=None)
collection_pid = args.collection_pid.strip()
safe = collection_pid.replace(':', '_')
out_dir = Path(args.output_dir).expanduser().resolve()

run_mgr = RunDirectoryManager(out_dir, safe)
ts_dir = run_mgr.create_run_dir()
prior_dir = run_mgr.find_latest_prior_run_dir()
if prior_dir:
    run_mgr.copy_prior_outputs(prior_dir)

combined_path = run_mgr.combined_text_path()
listing_path = run_mgr.listing_path()
checkpoint_path = run_mgr.checkpoint_path()

listing = ListingStore(listing_path)
listing.load_or_init()

writer = CombinedTextWriter(combined_path)
writer.ensure_file()

effective_limit = None
if args.test_limit is not None:
    prior_appended = listing.counts(total_docs=0)['appended_count']
    effective_limit = max(0, args.test_limit - prior_appended)

checkpoint = CheckpointStore(checkpoint_path)
checkpoint.load_or_init(collection_pid, safe, ts_dir.name, listing, combined_path, listing_path)

timeout, limits, headers = <as above>
with httpx.Client(headers=headers, timeout=timeout, limits=limits) as client:
    api = ApiClient(client)
    resolver = ItemTextResolver()
    urls = UrlBuilder(BASE)

    try:
        coll_json = api.fetch_collection_json(collection_pid)
        coll_title = CollectionMetadata.title_from_json(coll_json)
    except Exception:
        coll_title = ''
    listing.set_collection_info(collection_pid, coll_title)

    docs = api.search_collection_pids(collection_pid)
    checkpoint.save(collection_pid, safe, ts_dir.name, listing, combined_path, listing_path, total_docs=len(docs), completed=False)

    if effective_limit == 0:
        listing.update_summary(combined_path)
        listing.save()
        checkpoint.save(..., total_docs=len(docs), completed=False)
        print(... early-exit message ...)
        return 0

    processor = ExtractionProcessor(api, resolver, urls, writer, listing, checkpoint)
    appended = 0
    processed = listing.processed_set()
    for doc in tqdm(docs, total=len(docs), desc='Processing items'):
        pid = doc.get('pid')
        if not isinstance(pid, str) or pid in processed:
            continue
        try:
            if processor.process_pid(pid):
                appended += 1
                if effective_limit is not None and appended >= effective_limit:
                    listing.update_summary(combined_path)
                    listing.save()
                    checkpoint.save(... completed=False)
                    break
        except Exception as exc:
            listing.add_entry(item_pid=pid, primary_title=doc.get('primary_title') or '', item_api_url=urls.item_api_url(pid), studio_url=urls.studio_url(pid), size=None)
            print(f'Error processing {pid}: {exc}', file=sys.stderr)

        listing.update_summary(combined_path)
        listing.save()
        checkpoint.save(... completed=False)

    listing.update_summary(combined_path)
    listing.save()
    checkpoint.save(... completed=True)
    print(... normal completion message ...)
    return 0
```

### N) Test updates (example)
- Update `bdr-api-tools/tests/test_collection_title.py`:
```
from gather_extracted_text import CollectionMetadata

computed: str = CollectionMetadata.title_from_json(coll_json)
```

### O) Dependencies (as of current script header)
- Python: `==3.12.*`
- Packages: `httpx`, `tqdm`, `humanize`

### P) Refactor order of operations (suggested)
1. Create class shells and move constants/fields; add type hints and docstrings.
2. Port HTTP and parsing logic (`ApiClient`, `ItemTextResolver`, `CollectionMetadata`).
3. Port filesystem, listing, and checkpoint (`RunDirectoryManager`, `ListingStore`, `CombinedTextWriter`, `CheckpointStore`).
4. Port per-pid logic into `ExtractionProcessor`.
5. Replace calls in `main()` with class method calls.
6. Remove now-unused top-level functions; run linter and tests.
7. Update tests; run and validate on a small collection with and without `--test-limit`.

### Q) Behavioral invariants and edge cases
- Processed skipping invariant:
  - The main loop skips any pid already present in the listing (not just those with text). This means items that previously errored or were forbidden are not retried automatically on later runs.
- Counts semantics (match `counts_from_listing`):
  - `processed_count`: number of unique `item_pid` entries present in listing.
  - `appended_count`: number of items with truthy `extracted_text_file_size`.
  - `no_text_count`: number of items with `extracted_text_file_size` in `(None, '')`.
  - `forbidden_count`: number of items with `status == 'forbidden'` only (does not include `forbidden_via_child`).
- Summary maintenance:
  - `update_summary()` sets `all_extracted_text_file_size` to the humanized size of the combined text file on disk.
  - It removes deprecated keys `all_extracted_text_file_size_bytes` and `all_extracted_text_file_size_human` if present.
  - It stores `combined_text_path` and `listing_path` in `parent-dir/filename` form.
- Checkpoint `created_at` preservation:
  - When saving checkpoints, preserve the original `created_at` if a checkpoint exists; only `updated_at` advances.
- Collection title algorithm (maintain exact behavior):
  - Use `coll_json['name']` as the base title if present; otherwise empty string.
  - If `ancestors` is a non-empty list, take the last element; if it is a dict, prefer `name` then `title`; if it is a string, use it directly. If both base and parent exist, format as `"{base} -- (from {parent})"`; else just `base`.
- No-items message:
  - If search returns no docs, print `"No items found for collection {collection_pid}"` to stderr but continue to persist state.
- Entry point invariant:
  - Keep `if __name__ == '__main__': raise SystemExit(main())` and have `main()` return an `int` exit code.

### R) Proposed method signatures (sketch)
```
class ApiClient:
    def __init__(self, client: httpx.Client) -> None: ...
    def get_with_retries(self, url: str, *, max_tries: int = 4, timeout_s: float = 30.0) -> httpx.Response: ...
    def stream_text_with_retries(self, url: str, *, max_tries: int = 4, timeout_s: float = 60.0) -> str: ...
    def search_collection_pids(self, collection_pid: str) -> list[dict[str, object]]: ...
    def fetch_item_json(self, pid: str) -> dict[str, object]: ...
    def fetch_collection_json(self, pid: str) -> dict[str, object]: ...

class ItemTextResolver:
    def __init__(self, storage_url_tpl: str = STORAGE_URL_TPL) -> None: ...
    def extract_child_pids(self, item_json: dict[str, object]) -> list[str]: ...
    def extract_size_from_datastreams(self, item_json: dict[str, object]) -> int | None: ...
    def find_link_and_size(self, item_json: dict[str, object], pid: str) -> tuple[str, int | None] | None: ...

class CollectionMetadata:
    @staticmethod
    def title_from_json(coll_json: dict[str, object]) -> str: ...
    @staticmethod
    def pid_from_json(coll_json: dict[str, object]) -> str | None: ...

class UrlBuilder:
    def __init__(self, base: str = BASE) -> None: ...
    def item_api_url(self, pid: str) -> str: ...
    def studio_url(self, pid: str) -> str: ...
    def storage_text_url(self, pid: str) -> str: ...

class RunDirectoryManager:
    def __init__(self, out_dir: Path, safe_collection_pid: str) -> None: ...
    def run_dir_name_for(self) -> str: ...
    def create_run_dir(self) -> Path: ...
    def find_latest_prior_run_dir(self) -> Path | None: ...
    def copy_prior_outputs(self, prior_dir: Path) -> None: ...
    def combined_text_path(self) -> Path: ...
    def listing_path(self) -> Path: ...
    def checkpoint_path(self) -> Path: ...

class ListingStore:
    def __init__(self, path: Path) -> None: ...
    def load_or_init(self) -> None: ...
    def save(self) -> None: ...
    def add_entry(self, *, item_pid: str, primary_title: str, item_api_url: str, studio_url: str, size: int | None) -> None: ...
    def processed_set(self) -> set[str]: ...
    def update_summary(self, combined_path: Path) -> None: ...
    def counts(self, total_docs: int) -> dict[str, int]: ...
    def set_collection_info(self, collection_pid: str, collection_title: str) -> None: ...

class CheckpointStore:
    def __init__(self, path: Path) -> None: ...
    def load_or_init(self, collection_pid: str, safe_collection_pid: str, run_directory_name: str, listing: dict[str, object], combined_path: Path, listing_path: Path) -> None: ...
    def save(self, collection_pid: str, safe_collection_pid: str, run_directory_name: str, listing: dict[str, object], combined_path: Path, listing_path: Path, *, total_docs: int, completed: bool) -> None: ...
    def mark_completed(self) -> None: ...

class CombinedTextWriter:
    def __init__(self, path: Path) -> None: ...
    def ensure_file(self) -> None: ...
    def append(self, pid: str, text: str) -> None: ...

class ExtractionProcessor:
    def __init__(self, api: ApiClient, resolver: ItemTextResolver, urls: UrlBuilder, writer: CombinedTextWriter, listing: ListingStore, checkpoint: CheckpointStore) -> None: ...
    def process_pid(self, pid: str) -> bool: ...

class CLI:
    @staticmethod
    def build_parser() -> argparse.ArgumentParser: ...
    @staticmethod
    def parse_args(argv: list[str] | None = None) -> argparse.Namespace: ...

class Clock:
    @staticmethod
    def now_iso() -> str: ...
    @staticmethod
    def now_compact_local() -> str: ...
    @staticmethod
    def sleep(seconds: float) -> None: ...
```
