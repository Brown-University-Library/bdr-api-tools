# Feature Request: display recent repository activity by item and collection

## Summary

Add a new utility, tentatively named `display_recent_activity.py`, to show the most recently added BDR items and summarize which collections those items belong to.

## Proposed CLI

- Required behavior: display recent activity as JSON
- Optional argument: `--recent-items-count`
- Argument type: integer
- Default value: `100`

Example:

```bash
uv run https://brown-university-library.github.io/bdr-api-tools/display_recent_activity.py --recent-items-count 100
```

## Requested behavior

Use the BDR APIs to query the most recent items that have been added to the repository, defaulting to the most recent 100 items.

The JSON output should include:

- A list of the recent items returned by the query
- Basic item metadata for each item, such as PID, title, deposit date, and collection membership when available
- A summary of the collections represented in those recent items
- Per-collection counts showing how many of the recent items belong to each collection

## Example use case

If 40 recently added theses appear in the result set, and those theses belong to four different collections, the output should:

- List the 40 thesis items individually
- Also show the four affected collections with counts indicating how many recent items came from each one

## Suggested output shape

```json
{
  "_meta_": {
    "timestamp": "...",
    "recent_items_count": 100,
    "search_url": "...",
    "http_calls": 0
  },
  "recent_items": [
    {
      "pid": "bdr:...",
      "primary_title": "...",
      "deposit_date": "...",
      "collections": [
        {
          "pid": "bdr:...",
          "title": "..."
        }
      ]
    }
  ],
  "collection_summary": [
    {
      "collection_pid": "bdr:...",
      "collection_title": "...",
      "recent_item_count": 40
    }
  ]
}
```

## Notes

- The exact script name can be adjusted, but `display_recent_activity.py` seems consistent with the existing naming style.
- The feature should be collection-aware even when multiple recent items belong to the same collection.
- The summary should make it easy to see which collections have had the most recent ingestion activity.
