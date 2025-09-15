# Refactor Plan: Class-based organization for `gather_extracted_text.py`

This plan proposes a class-based reorganization of `bdr-api-tools/gather_extracted_text.py` to improve structure and maintainability without changing external behavior.

It preserves current CLI behavior and public functions used by tests (notably `collection_title_from_json()`), and follows the project coding guidelines:

- Use Python 3.12 type hints everywhere; avoid unnecessary `typing` imports.
- Structure scripts with `if __name__ == '__main__': main()` and keep `main()` simple.
- Use `httpx` for all HTTP calls.
- Do not use nested functions; prefer single-return functions.
- Use present-tense triple-quoted function docstrings.

All design choices below are made to align with these guidelines.


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

The following classes group related functions and responsibilities. Most are light wrappers rather than heavy state holders. Where state is useful (paths, listing store, checkpoint “created_at”), it is maintained explicitly.

- ApiClient
  - Purpose: Encapsulates all `httpx` interactions and retry policies.
  - Key methods:
    - `get_with_retries(url: str) -> httpx.Response` (from `_retrying_get`)
    - `stream_text_with_retries(url: str) -> str` (from `_retrying_stream_text`)
    - `search_collection_pids(collection_pid: str) -> list[dict[str, object]]`
    - `fetch_item_json(pid: str) -> dict[str, object]`
    - `fetch_collection_json(pid: str) -> dict[str, object]`
  - Notes:
    - Holds a reference to an externally-managed `httpx.Client`.
    - Accepts base URLs/templates via constructor or uses module constants.

- TextLinkResolver
  - Purpose: Encapsulates logic to find `EXTRACTED_TEXT` links and sizes from item JSON, including fallbacks.
  - Key methods:
    - `find_link_and_size(item_json: dict, pid: str) -> tuple[str, int | None] | None`
    - `extract_child_pids(item_json: dict) -> list[str]`
    - `extract_size_from_datastreams(item_json: dict) -> int | None`
  - Notes:
    - Stateless; may accept `STORAGE_URL_TPL` via constructor for testability.

- RunDirectoryManager
  - Purpose: Manage run directory naming, detection of prior runs, and file path creation/copying.
  - Fields:
    - `out_dir: Path`, `safe_collection_pid: str`, `run_dir: Path` (set on create).
  - Key methods:
    - `run_dir_name_for() -> str` (from `_run_dir_name_for`)
    - `create_run_dir() -> Path` (ensures and sets `run_dir`)
    - `find_latest_prior_run_dir() -> Path | None`
    - `copy_prior_outputs(prior_dir: Path) -> None`
    - `paths() -> tuple[Path, Path, Path]` to return `(combined_txt, listing_json, checkpoint_json)` for the run.
    - `parent_dir_and_name(p: Path) -> str`
  - Notes:
    - Wraps `ensure_dir()`.

- ListingStore
  - Purpose: Own in-memory listing dict and I/O to listing JSON; provide helper queries/updates.
  - Fields:
    - `path: Path`, `data: dict[str, object]`
  - Key methods:
    - `load_or_init() -> None` (from `load_listing`)
    - `save() -> None` (from `save_listing`)
    - `add_entry(item_pid: str, primary_title: str, item_api_url: str, studio_url: str, size: int | None, status: str | None = None) -> None` (from `add_listing_entry` + status tagging)
    - `processed_set() -> set[str]` (from `already_processed`/`processed_set_from_listing`)
    - `update_summary(combined_path: Path) -> None` (from `update_summary`; uses `parent_dir_and_name` for both combined and self.path)
    - `counts(total_docs: int) -> dict[str, int]` (from `counts_from_listing`)
    - Getters for summary fields (`collection_pid`, `collection_primary_title`) with in-place set operations.

- CheckpointStore
  - Purpose: Persist checkpoints with stable `created_at` and fresh counts.
  - Fields:
    - `path: Path`, `created_at: str | None`
  - Key methods:
    - `save(listing: ListingStore, combined_path: Path, total_docs: int, completed: bool) -> None` (from `save_checkpoint`; `listing_path` derived from `ListingStore.path`)
    - `load_created_at_if_present() -> None` (one-time read to preserve original `created_at`)

- CombinedTextWriter
  - Purpose: Append combined text with PID markers.
  - Fields:
    - `path: Path`
  - Key methods:
    - `append(pid: str, text: str) -> None` (from `append_text`)

- ExtractedTextProcessor
  - Purpose: Orchestrate per-pid processing.
  - Fields:
    - `api: ApiClient`, `resolver: TextLinkResolver`, `writer: CombinedTextWriter`, `listing: ListingStore`
  - Key methods:
    - `process_pid(pid: str) -> bool` (from `process_pid_for_extracted_text`)
  - Notes:
    - Updates listing entries (including `status: forbidden`, `status: forbidden_via_child`, `status: handled_via_child`).
    - Returns whether any text was appended.

- GatherExtractedTextRunner
  - Purpose: End-to-end orchestration for one run.
  - Fields:
    - `collection_pid: str`, `safe_collection_pid: str`, `out_dir: Path`, `test_limit: int | None`
  - Key steps in `run() -> int`:
    1) Prepare run directory and paths; copy forward prior outputs if present.
    2) Initialize `ListingStore` and `CombinedTextWriter` (touch combined file).
    3) Initialize `CheckpointStore` (write initial checkpoint with `total_docs=0`).
    4) Create `httpx.Client`, wrap with `ApiClient`.
    5) Fetch collection metadata; store in listing summary.
    6) Fetch docs via search; update checkpoint with `total_docs`.
    7) Compute `effective_limit` (respecting prior appended items).
    8) If `effective_limit == 0`, persist and exit.
    9) Build `ExtractedTextProcessor` and iterate docs:
       - Skip malformed/processed pids.
       - Process pid; if appended, increment `appended_count`; honor `effective_limit`.
       - After each pid: `listing.update_summary()`, `listing.save()`, `checkpoint.save()`.
    10) After loop, final persist with `completed = True`.


## 5) Backwards compatibility guarantees

- Keep `collection_title_from_json()` import path stable. The function will remain at module level or be re-exported from `gather_extracted_text.py` even if its implementation lives in a class (e.g., `CollectionTitle` or `TextLinkResolver`).
- For other existing top-level functions, provide thin wrappers for one release cycle:
  - Each wrapper delegates to the corresponding class method to avoid duplication while preserving the public API for callers (if any exist beyond this repo).
- Preserve CLI behavior precisely, including:
  - Command-line options and help text.
  - Output directory naming convention and file names.
  - Checkpoint JSON schema and listing JSON schema.
  - Logging format/noise suppression for `httpx`.


## 6) Old-to-new mapping (proposed)

Utilities:
- `_now_iso` -> `SystemClock.now_iso()` (or remain a small module-level util if preferred)
- `_now_compact_local` -> `SystemClock.now_compact_local()`
- `_sleep` -> `SystemClock.sleep()` (or local helper inside `ApiClient` retry loop)

HTTP/Network:
- `_retrying_get` -> `ApiClient.get_with_retries()`
- `_retrying_stream_text` -> `ApiClient.stream_text_with_retries()`
- `search_collection_pids` -> `ApiClient.search_collection_pids()`
- `fetch_item_json` -> `ApiClient.fetch_item_json()`
- `fetch_collection_json` -> `ApiClient.fetch_collection_json()`

Item parsing / title / link discovery:
- `collection_title_from_json` -> keep as-is or `CollectionTitle.from_json()` with top-level re-export
- `_extract_child_pids` -> `TextLinkResolver.extract_child_pids()`
- `_find_extracted_text_link_and_size` -> `TextLinkResolver.find_link_and_size()`
- `_extract_size_from_datastreams` -> `TextLinkResolver.extract_size_from_datastreams()`

Filesystem / run-dir:
- `ensure_dir` -> `RunDirectoryManager.ensure_dir()` (or static `Filesystem.ensure_dir()`)
- `_parent_dir_and_name` -> `RunDirectoryManager.parent_dir_and_name()`
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
- `update_summary` -> `ListingStore.update_summary()`

Checkpoint:
- `save_checkpoint` -> `CheckpointStore.save()`

Per-pid processing:
- `append_text` -> `CombinedTextWriter.append()`
- `process_pid_for_extracted_text` -> `ExtractedTextProcessor.process_pid()`

CLI:
- `parse_args` -> keep as-is
- `main` -> call `GatherExtractedTextRunner.run()`


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

1. Introduce class files or class blocks:
   - Minimal change path: define classes within `gather_extracted_text.py` first, then consider extracting to modules later if desired (`api_client.py`, `run_dir.py`, etc.).
   - Keep constants and logging at the top of `gather_extracted_text.py`.

2. Implement `RunDirectoryManager`, `ListingStore`, `CheckpointStore`, `CombinedTextWriter`.
   - Wire in existing functions and move logic as-is.
   - Ensure `ListingStore.update_summary()` and `CheckpointStore.save()` both use `parent_dir_and_name()` to keep relative path semantics identical.

3. Implement `ApiClient` and `TextLinkResolver`.
   - Move retry logic; reuse `httpx.Client` provided by `main`.
   - Ensure request throttling and backoff behavior remains identical.

4. Implement `ExtractedTextProcessor`.
   - Port `process_pid_for_extracted_text()` logic with careful handling of 403 cases and child walks.

5. Implement `GatherExtractedTextRunner.run()` and refactor `main()` to delegate to it.
   - Keep CLI behavior unchanged.

6. Provide thin wrappers for key old functions:
   - E.g., `def fetch_item_json(client, pid): return ApiClient(client).fetch_item_json(pid)`.
   - For `collection_title_from_json`, either keep as-is or call `CollectionTitle.from_json()` internally.

7. Validate against existing tests.
   - `tests/test_collection_title.py` imports `collection_title_from_json` from `gather_extracted_text`. Ensure this remains true.

8. Manual sanity checks against a small collection PID with and without `--test-limit`.

9. Optional: Extract classes to separate modules for long-term organization.
   - Update `gather_extracted_text.py` to import and re-export as needed.


## 9) Risks and mitigations

- Behavior drift in file naming or summary fields
  - Mitigation: Keep path templates and `parent_dir_and_name()` logic identical; reuse `humanize` for sizes.

- Performance differences due to added abstraction
  - Mitigation: Methods remain thin; I/O frequency unchanged; `tqdm` usage preserved.

- Backwards compatibility for external callers
  - Mitigation: Provide wrappers for one cycle; document deprecations in docstrings.


## 10) Acceptance criteria

- CLI usage and outputs are unchanged for end users.
- `tests/test_collection_title.py` continues to pass without modifications.
- Checkpoint and listing JSON schemas remain unchanged.
- The code is reorganized into cohesive classes, and `main()` delegates orchestration to a runner object.
- All new/ported functions and methods use 3.12 type hints and present-tense docstrings.


## 11) Notes for the implementer

- Prefer constructor injection for objects that hold state (paths, client). Avoid global state.
- Keep retry and throttling behavior exactly the same to avoid changing server load patterns.
- Avoid nested functions (project guideline). Keep methods small and single-purpose with single returns.
- Logging: preserve the existing `logging.basicConfig(...)` format and the suppression of `httpx/httpcore` noise when `LOG_LEVEL<=INFO`.
- When in doubt, favor preserving current behavior over additional refactoring.
