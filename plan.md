# Checkpoint-and-Resume Plan for gather_extracted_text.py

This document describes how to add robust checkpoint-and-resume behavior to `bdr-api-tools/gather_extracted_text.py` so long-running, multi-request runs can be stopped and resumed later with minimal server load.

It is self-contained and can be followed in a fresh session without prior context.


## Goals

- Ensure that if processing is interrupted, a subsequent run can pick up where it left off without re-downloading or re-processing items already handled.
- Minimize server load by reusing prior outputs (combined text and listing) rather than re-fetching data.
- Keep the existing timestamped output-directory convention and start each run in a new timestamped subdirectory.
- Treat HTTP 403 Forbidden responses for EXTRACTED_TEXT as final (do not retry in later runs).
- Save state frequently enough (after each PID) to make interruption safe.


## Current Behavior Summary (as of the code today)

Script: `bdr-api-tools/gather_extracted_text.py`

- Inputs:
  - `--collection-pid` (e.g., `bdr:c9fzffs9`)
  - `--output-dir` (directory path)
  - `--test-limit` (optional, stop after N successful appends)

- On each run, the script creates a new timestamped run directory under `--output-dir` in the form:
  - `run-YYYYmmddTHHMMSS-0500-bdr_xxxxxxxx/` where `bdr:xxxx` becomes `bdr_xxxx`.

- The script creates two files inside the run directory:
  1) Combined text file: `extracted_text_for_collection_pid-{safe_collection_pid}.txt`
  2) Listing JSON: `listing_for_collection_pid-{safe_collection_pid}.json`

- It queries the search API to get all member PIDs for the collection, then iterates them:
  - For each PID, it tries to find `EXTRACTED_TEXT` (directly or via child pids) and, if found, appends the text to the combined file and records metadata in the listing JSON.
  - If no extracted text is found, it records a listing entry with `extracted_text_file_size: null`.
  - If an exception occurs (including 403), it currently records a listing entry with `extracted_text_file_size: null` and prints an error.
  - After each PID, it writes the listing to disk and updates a summary block including counts and human-readable sizes.

- The script’s current resume behavior is limited to resuming within the same run directory if it already has a partially populated listing file. However, because each run creates a new timestamped directory, later runs do not automatically reuse output from prior runs.


## Desired Behavior

- On a new run for the same `--collection-pid`, automatically detect the most recent prior run directory for that collection that was not completed and reuse its outputs by copying them into the new timestamped run directory.
- Resume processing from the next unprocessed PID (skipping anything already present in the listing from the previous run).
- Consider PIDs with prior `Forbidden` results as final and do not retry them in later runs.
- Periodically save a `checkpoint.json` file that explicitly summarizes progress and status.


## Output/State Files

Each run directory will contain these files:

- Combined text (existing):
  - `extracted_text_for_collection_pid-{safe_collection_pid}.txt`

- Listing JSON (existing, augmented in future changes but backward compatible):
  - `listing_for_collection_pid-{safe_collection_pid}.json`

- Checkpoint JSON (new):
  - `checkpoint_for_collection_pid-{safe_collection_pid}.json`


## Checkpoint File Design

A small, explicit state file that allows a new run to:
- Confirm collection identity and associated run directory.
- Know whether the run completed fully.
- Know how many items were processed/appended so far.
- Know which PIDs are already finalized.

Proposed schema (fields may be extended in the future):

```json
{
  "collection_pid": "bdr:c9fzffs9",
  "safe_collection_pid": "bdr_c9fzffs9",
  "created_at": "2025-09-13T11:45:00-04:00",
  "updated_at": "2025-09-13T11:52:13-04:00",
  "run_directory_name": "run-20250913T115200-0400-bdr_c9fzffs9",
  "completed": false,
  "counts": {
    "total_docs": 1234,
    "processed_count": 278,
    "appended_count": 251
  },
  "paths": {
    "combined_text": "run-20250913T115200-0400-bdr_c9fzffs9/extracted_text_for_collection_pid-bdr_c9fzffs9.txt",
    "listing_json": "run-20250913T115200-0400-bdr_c9fzffs9/listing_for_collection_pid-bdr_c9fzffs9.json"
  },
  "processed_pids": [
    "bdr:abc123", "bdr:def456"  
  ],
  "finalized_pids": [
    "bdr:abc123", "bdr:def456"  
  ],
  "appended_pids": [
    "bdr:abc123"
  ],
  "no_text_pids": [
    "bdr:def456"
  ],
  "forbidden_pids": [
    
  ],
  "retryable_error_pids": [
    
  ]
}
```

Notes:
- "finalized" means: do not attempt again on resume. This includes `appended_pids`, `no_text_pids`, and `forbidden_pids`.
- `retryable_error_pids` is reserved for future use. Given the primary goal to minimize load, the default behavior will not retry these automatically unless the user opts in.
- We will keep listing JSON as the canonical record of item-level metadata. The checkpoint is a small helper to drive resume logic and avoid reprocessing.


## How to Determine What to Skip vs. Retry

- Skip (do not retry):
  - Any PID already present in `listing.items` (current code’s behavior): we will continue this behavior to minimize load.
  - Any PID explicitly recorded as `forbidden` in the checkpoint (after enhancing error handling to detect HTTP 403), or when inferring from an older run that recorded a `null`/None size.
    - User clarification: prior entries with `null` size often reflect `Forbidden`; those do not need to be reprocessed.
  - Any PID where no `EXTRACTED_TEXT` was found (recorded with `null` size) should be considered final.

- Retry (optional, opt-in only):
  - PIDs that failed due to transient/network errors (not 403). This requires differentiating errors by status code in the future. By default, to reduce load, we will not retry these automatically across runs; users can add a flag later (e.g., `--retry-errors`) if desired.


## Resume Algorithm

1) On startup, compute `safe_collection_pid` and create a new timestamped run directory (current behavior stays).

2) Search the `--output-dir/` for prior run directories matching `run-*-{safe_collection_pid}`. Sort by timestamp descending.

3) For the most recent prior run:
   - If a `checkpoint_for_collection_pid-{safe_collection_pid}.json` exists and `completed: false`, use it as the authoritative state.
   - Else, if no checkpoint exists but a `listing_for_collection_pid-{safe_collection_pid}.json` exists, infer state by:
     - `processed_pids = set(item["item_pid"])` for all items in listing
     - `appended_pids = subset where item["extracted_text_file_size"] is not null`
     - `no_text_pids = subset where item["extracted_text_file_size"] is null`
     - Consider all of the above as finalized/do-not-retry

4) Copy the prior run’s `combined text` and `listing.json` into the new run directory.
   - After copying, immediately update the listing’s `summary.combined_text_path` and `summary.listing_path` to point to the new run directory.
   - Save a new `checkpoint.json` in the new directory, derived from the inferred or prior checkpoint data, with `run_directory_name` set to the new directory.

5) Build the set `processed = processed_pids` (from checkpoint or inferred). When iterating `docs` for the current run, skip any PID in `processed`.

6) After processing each PID:
   - Append to combined text if applicable, update listing entry, update summary, and write the listing JSON (current behavior).
   - Update `checkpoint.json` with counts, `processed_pids`, and any status-specific lists, then write it to disk.

7) On completion:
   - Mark `completed: true` in `checkpoint.json` and write it.


## Handling HTTP 403 Forbidden

- Improve error handling in `process_pid_for_extracted_text(...)` to catch 403 specifically when streaming EXTRACTED_TEXT and mark an item as `forbidden`.
- In the listing entry, continue to store `extracted_text_file_size: null` for forbidden cases (to retain backward compatibility), and optionally add a new field `status: "forbidden"` (non-breaking addition) to distinguish from other `null` cases.
- In the checkpoint, add the PID to `forbidden_pids` and include it in `finalized_pids`.
- On resume, do not reattempt forbidden items.


## Persistence Frequency

- After each PID:
  - Update `listing.json` (already done today).
  - Update `checkpoint.json` (new).

- On early exit due to `--test-limit`: write both listing and checkpoint before exiting.


## Backward Compatibility

- If a prior run has no `checkpoint.json`, we infer from `listing.json` as described and proceed. This allows immediate benefit from resume without requiring a prior checkpoint file.
- The listing JSON may gain a backward-compatible field (e.g., `status`) in each item. Older tools that ignore unknown fields will continue to work.


## CLI Considerations (optional)

- No new flags are strictly required for basic resume. The script will auto-detect and attempt to resume the latest incomplete run for the same collection.
- Possible future flags:
  - `--no-resume` to force a fresh run without copying previous outputs.
  - `--retry-errors` to reprocess items that previously failed with non-403 errors.


## Implementation Steps

1) Utilities for run directories:
   - Implement function to list and sort prior run directories for a given `safe_collection_pid`.
   - Implement function to copy prior run’s files (combined text, listing) and rewrite listing summary paths to the new run directory.

2) Checkpoint helpers:
   - Implement `load_checkpoint(path) -> dict` and `save_checkpoint(path, data) -> None` with strict 3.12 type hints and present-tense docstrings.
   - Implement `infer_checkpoint_from_listing(listing: dict) -> dict` that populates `processed_pids`, `appended_pids`, `no_text_pids` from listing items.

3) Startup resume logic:
   - In `main()`, after creating the new run directory, look for a prior run to resume.
   - If found and incomplete, copy its files, build or load checkpoint, and initialize `processed` set accordingly.

4) Loop-time persistence:
   - After each PID, call `update_summary(...)`, `save_listing(...)`, and `save_checkpoint(...)`.
   - On `--test-limit` exit path, persist both before `break`.

5) Error differentiation:
   - In `process_pid_for_extracted_text(...)`, detect HTTP 403 from the streaming call and tag the listing item as `status: "forbidden"` (optional, backward-compatible) and update checkpoint lists accordingly.

6) Completion:
   - After the loop, mark checkpoint as `completed: true` and save it.

7) Type hints and style:
   - Use Python 3.12 type hints throughout, present-tense docstrings, single-return functions where feasible, `httpx` for HTTP calls, and no nested functions.


## Testing and Validation

- Use a small collection or `--test-limit` to simulate partial runs.
- Scenario A: Fresh run, interrupt after several items. Verify that a subsequent run for the same collection copies prior outputs and resumes from the next PID.
- Scenario B: Prior run with `Forbidden` for some items. Verify those PIDs are not retried and the run completes without reattempting them.
- Scenario C: No checkpoint present but listing exists. Verify we can infer state from listing and resume successfully.
- Scenario D: Complete run. Verify subsequent run does not attempt to resume from a completed checkpoint.


## Edge Cases and Safeguards

- If prior `combined text` is missing but listing is present: proceed with resume but warn that combined text cannot be reconstructed without re-fetching; continue writing new appends into the new combined file.
- If `output_dir` contains multiple prior runs, always choose the most recent with `completed: false`. If none exists, start fresh.
- Ensure that listing summary paths (`combined_text_path`, `listing_path`) are rewritten to the new directory after copying.
- Ensure that resumed `appended_count` honors `--test-limit` semantics by counting previously appended items.


## Future Enhancements (optional)

- Add `status` to listing items: `ok|no_text|forbidden|error` for clearer semantics.
- Add a `docs_snapshot` (pid + title list) into the checkpoint for stable ordering and to allow resume without re-hitting the search API.
- Add metrics like elapsed time and average throughput.


## Summary

This plan introduces a small `checkpoint.json` per run and a startup routine that discovers the latest incomplete prior run for the same collection, copies its outputs, and resumes work without reprocessing. It distinguishes `Forbidden` as final to reduce load. Persistence occurs after each PID, ensuring safe interruption at any time. All changes align with the existing output structure and project style guidelines.
