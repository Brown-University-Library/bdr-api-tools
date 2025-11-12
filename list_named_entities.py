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

import argparse
import json
import logging
import os
from datetime import datetime, timedelta

import httpx
import spacy

## setup logging ----------------------------------------------------
log_level_name: str = os.getenv('LOG_LEVEL', 'INFO').upper()
log_level = getattr(
    logging, log_level_name, logging.INFO
)  # maps the string name to the corresponding logging level constant; defaults to INFO
logging.basicConfig(
    level=log_level,
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S',
)
if log_level <= logging.INFO:
    for noisy in ('httpx', 'httpcore'):  # prevent httpx from logging
        lg = logging.getLogger(noisy)
        lg.setLevel(logging.WARNING)  # or logging.ERROR if you prefer only errors
        lg.propagate = False  # don't bubble up to root
log = logging.getLogger(__name__)

## constants --------------------------------------------------------
BASE_URL: str = 'https://repository.library.brown.edu'
ITEM_URL_TPL: str = f'{BASE_URL}/api/items/THE_PID/'
STORAGE_URL_TPL: str = f'{BASE_URL}/storage/THE_PID/EXTRACTED_TEXT/'


def call_item_api(item_pid) -> dict:
    """
    Calls the item-api to determine how to access the extracted-text datastream.
    Called by: manage_ner_processing()
    """
    item_api_url: str = ITEM_URL_TPL.replace('THE_PID', item_pid)
    item_api_response: httpx.Response = httpx.get(item_api_url)
    item_api_response_jdict: dict = item_api_response.json()
    return item_api_response_jdict


def evaluate_item_api_response(item_api_response_jdict) -> tuple[str, str]:
    """
    Evaluates the item-api response to determine how to access the extracted-text datastream.
    Called by: manage_ner_processing()
    """
    extracted_text_url: str = ''
    err: str = ''
    try:
        extracted_text_url: str = item_api_response_jdict['links']['EXTRACTED_TEXT']
    except KeyError:
        message: str = 'extracted-text not at `links.EXTRACTED_TEXT`'
        log.warning(message)
        err = message
    return extracted_text_url, err


def get_extracted_text_datastream(extracted_text_url) -> str:
    """
    Gets the extracted-text datastream.
    Called by: manage_ner_processing()
    """
    extracted_text: str = httpx.get(extracted_text_url).text
    return extracted_text


def build_err_response(item_pid: str, err: str, start_time: datetime) -> str:
    """
    Builds an error response string.
    Called by: manage_ner_processing()
    """
    elapsed: timedelta = datetime.now() - start_time
    meta: dict = {
        'item_pid': item_pid,
        'tool': 'list_named_entities',
        'timestamp': start_time.isoformat(),
        'elapsed': str(elapsed),
    }
    rsp_dct: dict = {
        'meta': meta,
        'error': err,
    }
    jsn: str = json.dumps(rsp_dct)
    return jsn


def get_extracted_datastream(extracted_text_url: str) -> str:
    """
    Gets the extracted-text datastream.
    Called by: manage_ner_processing()
    """
    extracted_text: str = httpx.get(extracted_text_url).text
    return extracted_text


def process_extracted_text_with_spacy(extracted_text: str) -> list:
    """
    Processes the extracted-text with spaCy.
    Called by: manage_ner_processing()
    """
    doc: spacy.tokens.Doc = nlp(extracted_text)
    return doc


def manage_ner_processing(item_pid) -> None:
    """
    Manages the named entity recognition (NER) processing for a single item.
    Called by: dundermain
    """
    start_time: datetime = datetime.now()
    ## call item-api to grab item data -----------------------------
    item_api_response_jdict: dict = call_item_api(item_pid)
    ## get extracted-text url --------------------------------------
    extracted_text_url, err = evaluate_item_api_response(item_api_response_jdict)
    assert type(extracted_text_url) is str
    assert type(err) is str
    if not extracted_text_url:
        rsp: str = build_err_response(item_pid, err, start_time)
        return rsp
    ## grab extracted-text datastream -------------------------------
    extracted_text: str = get_extracted_text_datastream(extracted_text_url)
    ## process extracted-text with spaCy ----------------------------
    ner_results: list = process_extracted_text_with_spacy(extracted_text)
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
