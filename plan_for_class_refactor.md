# Refactor Plan: Class-based organization for `gather_extracted_text.py`

This plan proposes a class-based reorganization of `bdr-api-tools/gather_extracted_text.py` to improve structure and maintainability without changing external behavior.

It preserves current CLI behavior and public functions used by tests (notably `collection_title_from_json()`), and follows the project coding guidelines:

- Use Python 3.12 type hints everywhere; avoid unnecessary `typing` imports.
- Structure scripts with `if __name__ == '__main__': main()` and keep `main()` simple.
- Use `httpx` for all HTTP calls.
- Do not use nested functions; prefer single-return functions.
- Use present-tense triple-quoted function docstrings.

All design choices below are made to align with these guidelines.


## 0) Maintainer preferences for this refactor

- Keep `main()` as an independent manager function. Do not bundle it into another class.
- Only introduce a class if it will have three or more methods. If a concept would result in fewer than three methods, keep the logic as standalone functions for now.


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

The following classes group related functions and responsibilities. Only classes with three or more methods are introduced; otherwise, logic remains as standalone functions.

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
  - Notes:
    - Uses the standalone `ensure_dir()` helper and the standalone `_parent_dir_and_name()` helper where needed.

- ListingStore
  - Purpose: Own in-memory listing dict and I/O to listing JSON; provide helper queries/updates.
  - Fields:
    - `path: Path`, `data: dict[str, object]`
  - Key methods:
    - `load_or_init() -> None` (from `load_listing`)
    - `save() -> None` (from `save_listing`)
    - `add_entry(item_pid: str, primary_title: str, item_api_url: str, studio_url: str, size: int | None, status: str | None = None) -> None` (from `add_listing_entry` + status tagging)
    - `processed_set() -> set[str]` (from `already_processed`/`processed_set_from_listing`)
    - `update_summary(combined_path: Path) -> None` (from `update_summary`; uses the standalone `_parent_dir_and_name()` for both combined and self.path)
    - `counts(total_docs: int) -> dict[str, int]` (from `counts_from_listing`)
    - Getters for summary fields (`collection_pid`, `collection_primary_title`) with in-place set operations.

Intentionally not classes (kept as functions for now):
- `_now_iso()`, `_now_compact_local()`, `_sleep()` — simple time/clock helpers.
- `_parent_dir_and_name(p: Path)` — string helper for displaying relative paths.
- `append_text(out_txt_path, pid, text)` — single-purpose I/O helper.
- `process_pid_for_extracted_text(client, pid, out_txt_path, listing)` — orchestrates per-pid processing.
- `save_checkpoint(checkpoint_path, ..., listing, combined_path, listing_path, total_docs, completed)` — persists checkpoint with stable `created_at`.
- `ensure_dir(path)` — simple directory creation helper used by `RunDirectoryManager`.
- `main()` — remains the independent manager function that orchestrates the run using the classes above.


## 5) Backwards compatibility guarantees

- Keep `collection_title_from_json()` import path stable. The function remains at module level (standalone) and continues to be importable from `gather_extracted_text.py`.
- For other existing top-level functions, provide thin wrappers for one release cycle:
  - Each wrapper delegates to the corresponding class method to avoid duplication while preserving the public API for callers (if any exist beyond this repo).
- Preserve CLI behavior precisely, including:
  - Command-line options and help text.
  - Output directory naming convention and file names.
  - Checkpoint JSON schema and listing JSON schema.
  - Logging format/noise suppression for `httpx`.


## 6) Old-to-new mapping (proposed)

Utilities:
- Keep `_now_iso`, `_now_compact_local`, and `_sleep` as standalone helpers. They are simple and widely reusable.

HTTP/Network:
- `_retrying_get` -> `ApiClient.get_with_retries()`
- `_retrying_stream_text` -> `ApiClient.stream_text_with_retries()`
- `search_collection_pids` -> `ApiClient.search_collection_pids()`
- `fetch_item_json` -> `ApiClient.fetch_item_json()`
- `fetch_collection_json` -> `ApiClient.fetch_collection_json()`

Item parsing / title / link discovery:
- `collection_title_from_json` -> keep at module level (optionally internally call `TextLinkResolver` helpers if desired)
- `_extract_child_pids` -> `TextLinkResolver.extract_child_pids()`
- `_find_extracted_text_link_and_size` -> `TextLinkResolver.find_link_and_size()`
- `_extract_size_from_datastreams` -> `TextLinkResolver.extract_size_from_datastreams()`

Filesystem / run-dir:
- `ensure_dir` -> keep as standalone helper used by `RunDirectoryManager`
- `_parent_dir_and_name` -> keep as standalone helper (used by `ListingStore.update_summary()` and `save_checkpoint()`)
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
- `update_summary` -> `ListingStore.update_summary()` (uses `_parent_dir_and_name` helper)

Checkpoint:
- `save_checkpoint` -> keep as a standalone function (uses `_parent_dir_and_name` and `ListingStore.counts()`)

Per-pid processing:
- `append_text` -> keep as a standalone function
- `process_pid_for_extracted_text` -> keep as a standalone function (internally use `ApiClient` and `TextLinkResolver` where appropriate)

CLI:
- `parse_args` -> keep as-is
- `main` -> remains the independent manager function orchestrating the run using the classes and helpers above


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

2. Implement `RunDirectoryManager` and `ListingStore`.
   - Wire in existing functions and move logic as-is.
   - Ensure `ListingStore.update_summary()` and `save_checkpoint()` both use the standalone `_parent_dir_and_name()` helper to keep relative path semantics identical.

3. Implement `ApiClient` and `TextLinkResolver`.
   - Move retry logic; reuse `httpx.Client` provided by `main`.
   - Ensure request throttling and backoff behavior remains identical.

4. Keep `process_pid_for_extracted_text()` as a standalone function.
   - Internally use `ApiClient` and `TextLinkResolver` to reduce parameter passing and centralize HTTP/JSON parsing, while preserving behavior (403 handling, child traversal).

5. Keep `main()` as the independent manager function.
   - Refactor its internals to instantiate and use `RunDirectoryManager`, `ListingStore`, `ApiClient`, and `TextLinkResolver` as above.
   - Preserve CLI behavior unchanged.

6. Provide thin wrappers for key old functions:
   - E.g., `def fetch_item_json(client, pid): return ApiClient(client).fetch_item_json(pid)`.
   - For `collection_title_from_json`, keep it as-is (standalone function).

7. Validate against existing tests.
   - `tests/test_collection_title.py` imports `collection_title_from_json` from `gather_extracted_text`. Ensure this remains true.

8. Manual sanity checks against a small collection PID with and without `--test-limit`.

9. Optional: Extract classes to separate modules for long-term organization once stable.
   - Update `gather_extracted_text.py` to import and re-export as needed while keeping `main()`.


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
- The code is reorganized into cohesive classes where appropriate (3+ methods), and `main()` remains the independent manager function orchestrating the run using those classes and helper functions.
- All new/ported functions and methods use 3.12 type hints and present-tense docstrings.


## 11) Notes for the implementer

- Prefer constructor injection for objects that hold state (paths, client). Avoid global state.
- Keep retry and throttling behavior exactly the same to avoid changing server load patterns.
- Avoid nested functions (project guideline). Keep methods small and single-purpose with single returns.
- Logging: preserve the existing `logging.basicConfig(...)` format and the suppression of `httpx/httpcore` noise when `LOG_LEVEL<=INFO`.
- When in doubt, favor preserving current behavior over additional refactoring.
