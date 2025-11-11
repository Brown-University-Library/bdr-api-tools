# /// script
# requires-python = "==3.12.*"
# dependencies = [
#   "httpx~=0.28.0",
#   "spacy~=3.8.0"
# ]
# ///

import argparse

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


def call_item_api(item_pid) -> dict:
    """
    Calls the item-api to determine how to access the extracted-text datastream.
    """
    item_api_response = httpx.get(ITEM_URL_TPL.format(pid=item_pid))
    return item_api_response.json()


def manage_ner_processing(item_pid) -> None:
    """
    Manages the named entity recognition (NER) processing for a single item.
    """
    ## call item-api to determine how to access extracted-text ------
    item_api_response = call_item_api(item_pid)
    ## process item-api response ------------------------------------
    ## grab extracted-text datastream -------------------------------
    ## process extracted-text with spaCy ----------------------------
    ## process spaCy results ----------------------------------------
    ## return response ----------------------------------------------
    return


if __name__ == '__main__':
    """
    Runs main() and exits with its return value.
    - If all goes well, returns 0.
    - Otherwise, returns the value returned by main().
    """
    parser = argparse.ArgumentParser(
        description="Runs spaCy's named entity recognition on an item's extracted-text, if it exists."
    )
    parser.add_argument('--item-pid', required=True)
    args = parser.parse_args()
    try:
        manage_ner_processing(args.item_pid)
    except Exception as e:
        raise SystemExit(e)
