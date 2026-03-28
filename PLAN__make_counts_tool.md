# Implementation Plan

## Prompt

Goal:

- create a bdr-api-tool that can display collection-activity over time.

- it should be able to be run via: vu run ```uv run https://brown-university-library.github.io/bdr-api-tools/display_collection_activity.py --collection-pid bdr:bwehb8b8 --output-dir '/path/to/output-dir/'```

- the result of that will be a json-file showing monthly counts of new-items added to the collection.

Context:

- Review the bdr-tools-website to get an overview of the project: <https://brown-university-library.github.io/bdr-api-tools/>

- Review the bdr-tools-repository to understand how an individual tool needs to be structured: <https://github.com/Brown-University-Library/bdr-api-tools> -- including the uv-compatible inline-script-metadata at the top of each file.

- Review the bdr-api documentation at <https://github.com/Brown-University-Library/bdr_api_documentation/wiki> to understand collection-api and item-api search-api capabilities.

Tasks:

- review `bdr-api-tools/AGENTS.md` for code-directives to follow.

- create a plan for implementation, saved to `bdr-api-tools/PLAN__.md`

- the plan should include a small, pretty-printed `example.json` file

- _after_ the plan is saved to disk, incorporate this prompt at the top of the plan.


## Objective

Create a new `uv`-runnable script named `display_collection_activity.py` that accepts `--collection-pid` and `--output-dir`, queries BDR APIs, and writes a JSON file containing monthly counts of items added to the collection.

## Assumptions

- The script will live at the repository root alongside the existing tools.
- The script will follow the repo's inline `uv` script metadata pattern and use `httpx`.
- The Search API is the best primary source for collection membership and date-based item metadata because it already supports collection-scoped queries and pagination.
- The output should be written to a file in the requested directory rather than printed only to stdout.
- The JSON should be stable and human-readable via pretty-printing.
- To remain server-friendly in light of the documentation's Cloudflare note, requests should be synchronous and moderate in page size.

## API Approach

### Primary collection membership lookup

- Use the Search API with a query constrained by collection membership:
  - `q=rel_is_member_of_collection_ssim:"{collection_pid}"`
- Page through all results with `rows` and `start` parameters.
- Request only the fields needed for monthly aggregation and output metadata.

### Candidate date fields to inspect

- Start with search-result fields likely to reflect ingest or object creation chronology.
- Prefer a single authoritative field if it is consistently present across result docs.
- If more than one plausible field exists, centralize field selection logic so the script can:
  - prefer the highest-confidence field
  - count skipped items when no usable date exists
  - report which field was chosen in output metadata

### Collection title lookup

- Use the Collection API to fetch collection display metadata for the output file's `_meta_` block.

## Output Design

The script should write one JSON file into `--output-dir`, with a predictable filename such as:

- `collection_activity__bdr_bwehb8b8.json`

Suggested top-level shape:

```json
{
  "_meta_": {
    "timestamp": "2026-03-13T07:37:00-04:00",
    "collection_pid": "bdr:bwehb8b8",
    "collection_title": "Brown University Open Data Collection",
    "search_url": "https://repository.library.brown.edu/api/search/",
    "date_field_used": "dateCreated",
    "num_found": 908,
    "items_counted": 901,
    "items_skipped": 7,
    "output_file": "/path/to/output-dir/collection_activity__bdr_bwehb8b8.json"
  },
  "monthly_counts": {
    "2021-11": 3,
    "2021-12": 8,
    "2022-01": 12
  }
}
```

## example.json

```json
{
  "_meta_": {
    "timestamp": "2026-03-13T07:37:00-04:00",
    "collection_pid": "bdr:bwehb8b8",
    "collection_title": "Brown University Open Data Collection",
    "search_url": "https://repository.library.brown.edu/api/search/",
    "date_field_used": "dateCreated",
    "num_found": 6,
    "items_counted": 5,
    "items_skipped": 1,
    "output_file": "/tmp/output/collection_activity__bdr_bwehb8b8.json"
  },
  "monthly_counts": {
    "2024-10": 1,
    "2024-11": 2,
    "2024-12": 2
  }
}
```

## Implementation Milestones

1. Confirm the date-bearing Search API field to use for monthly aggregation by inspecting representative collection search results.
2. Implement `display_collection_activity.py` with:
   - inline `uv` metadata
   - CLI parsing for `--collection-pid`, `--output-dir`, and optionally `--rows`
   - collection title retrieval
   - paginated Search API retrieval
   - month bucketing in `YYYY-MM` format
   - JSON file writing to the requested output directory
3. Add focused `unittest` coverage for:
   - month extraction from a valid date value
   - fallback/skip behavior when dates are missing or malformed
   - aggregation correctness across multiple docs
   - output filename/path generation
4. Run `uv run ./run_tests.py` and, if helpful, a direct script smoke test against a real collection.

## Cross-Session Context / Handoff Notes

- This repository expects runnable standalone scripts at the repo root, with `uv` inline script metadata at the very top of each script file.
- Existing examples most relevant to this task are:
  - `calc_collection_size.py` for collection-scoped Search API pagination and optional Collection API title lookup
  - `show_zip_info.py` for pretty-printed JSON output structure
- The intended invocation style for the new tool is remote-friendly:
  - `uv run https://brown-university-library.github.io/bdr-api-tools/display_collection_activity.py --collection-pid bdr:bwehb8b8 --output-dir '/path/to/output-dir/'`
- The implementation should therefore avoid local-package assumptions and keep dependencies minimal; `httpx` is the expected HTTP library.
- `AGENTS.md` is the source of truth for coding directives in this repo. Key requirements for implementation:
  - Python 3.12 type hints throughout
  - simple `main()` that only orchestrates
  - no nested functions
  - triple-quoted present-tense docstrings
  - docstrings should end with a `Called by:` line
- `ruff.toml` confirms:
  - max line length of `125`
  - single-quote formatting style
- A likely first-pass implementation strategy is:
  - fetch collection title from Collection API
  - fetch paginated collection members from Search API
  - inspect one or more date fields on each doc
  - normalize usable dates to `YYYY-MM`
  - count per month
  - write a single pretty-printed JSON file into `--output-dir`
- Important unresolved implementation detail:
  - the exact Search API field that best represents when an item was added to a collection has not yet been confirmed
  - if no reliable search-result field exists, a fallback to Item API may be required, but that should be treated as a second-choice approach because it increases request volume
- The BDR API wiki mentions Cloudflare bot protection, so implementation should stay conservative:
  - use synchronous requests
  - reuse an `httpx.Client`
  - avoid excessive page sizes or unnecessary follow-up requests
- The output format currently planned is:
  - top-level `_meta_` block with run metadata and counts
  - top-level `monthly_counts` mapping keyed by `YYYY-MM`
- The plan currently assumes one output file per run, written to `--output-dir`, with a predictable filename such as `collection_activity__bdr_bwehb8b8.json`.
- If implementation resumes later, a good first validation step is to inspect live Search API responses for one representative collection and identify which date field is populated consistently enough to support month bucketing.

## Code Structure Notes

Note: be sure to review `bdr-api-tools/AGENTS.md` for coding-directives to follow.

- Keep `main()` limited to argument parsing and orchestration.
- Put HTTP operations in top-level helpers or a small class if that makes date-field evaluation cleaner.
- Use Python 3.12 style type hints and single quotes.
- Use present-tense triple-quoted docstrings that end with the required `Called by:` line.
- Avoid nested functions.

## Open Questions To Resolve During Implementation

- Which exact Search API field is the most reliable source of item-added chronology for this repository's collections?
- Whether the API exposes the relevant field directly in search results for all items, or whether a fallback Item API lookup is needed for some records.
- Whether output should be strictly one file per run or also echoed to stdout after writing.

## Initial Recommendation

Implement the tool using Search API pagination only if the date field is available there, because that will keep the script fast, simple, and aligned with the existing `calc_collection_size.py` pattern. Fall back to Item API lookups only if search results prove insufficient for consistent monthly aggregation.
