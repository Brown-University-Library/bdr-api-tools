# Feature Request: add a `display_recent_activity.py` tool for the most recent N items

## Summary

Add a new CLI tool to display recent BDR activity based on the most recently added items.

A likely script name would be `display_recent_activity.py`.

## Proposed behavior

The tool should:

- accept an optional integer argument such as `--recent-item-count`
- default that argument to `100`
- query the BDR APIs for the most recent `N` items added
- output structured JSON
- list the recent items returned
- summarize which collections those items belong to, with counts per collection

## Example use case

If the 100 most recent items include 40 new theses, and those theses belong to 4 different collections, the output should show:

- the individual recent items
- the collections represented in that recent set
- the count of recent items associated with each collection

## Suggested CLI

```bash
uv run ./display_recent_activity.py
uv run ./display_recent_activity.py --recent-item-count 100
uv run ./display_recent_activity.py --recent-item-count 250
```

## Suggested output shape

```json
{
  "_meta_": {
    "timestamp": "2026-04-01T12:00:00-04:00",
    "recent_item_count_requested": 100,
    "recent_item_count_returned": 100,
    "sort_field": "date_added_or_equivalent",
    "http_calls": 3
  },
  "recent_items": [
    {
      "pid": "bdr:123456",
      "primary_title": "Example title",
      "date_added": "2026-03-31T14:10:00Z",
      "collections": [
        {
          "pid": "bdr:aaaaaa",
          "name": "Theses and Dissertations -- (from Computer Science)"
        }
      ]
    }
  ],
  "updated_collections": [
    {
      "collection_pid": "bdr:aaaaaa",
      "collection_title": "Theses and Dissertations -- (from Computer Science)",
      "recent_item_count": 40
    }
  ]
}
```

## Implementation notes

- Reuse the repo's current patterns:
  - `uv` runnable standalone script
  - `httpx` for HTTP calls
  - formatted JSON output
  - focused `unittest` coverage
- The script should determine the best available BDR field for "most recent" ordering.
- If items can belong to multiple collections, each collection should be counted appropriately and explicitly.
- It would be helpful to include `_meta_` information similar to the existing tools.

## Why this would help

The existing tools summarize collection size and collection activity, but there is not currently a quick way to answer:

- what are the most recent items added?
- which collections have been updated most recently?
- how much of that recent activity is concentrated in specific collection groups such as theses?

This tool would provide a compact operational view of recent ingest activity.
