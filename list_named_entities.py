# /// script
# requires-python = "==3.12.*"
# dependencies = [
#   "httpx~=0.28.0",
#   "spacy~=3.8.0"
# ]
# ///

"""
Runs spaCy's named entity recognition on an item's extracted-text, if it exists.

Usage:
  uv run ./list_named_entities.py --item-pid bdr:bfttpwkj

Args:
  --item-pid (required)
"""

BASE = 'https://repository.library.brown.edu'
ITEM_URL_TPL = f'{BASE}/api/items/{{pid}}/'
STORAGE_URL_TPL = f'{BASE}/storage/{{pid}}/EXTRACTED_TEXT/'


def main():
    pass


if __name__ == '__main__':
    """
    Runs main() and exits with its return value.
    - If all goes well, returns 0.
    - Otherwise, returns the value returned by main().
    """
    raise SystemExit(main())
