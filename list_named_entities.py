# /// script
# requires-python = "==3.12.*"
# dependencies = [
#   "httpx~=0.28.0",
#   "spacy~=3.8.0",
#   "en_core_web_sm @ https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.8.0/en_core_web_sm-3.8.0-py3-none-any.whl"
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
import pprint
from collections import Counter
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
ITEM_URL_TPL: str = f'{BASE_URL}/studio/item/THE_PID/'
ITEM_API_URL_TPL: str = f'{BASE_URL}/api/items/THE_PID/'
STORAGE_URL_TPL: str = f'{BASE_URL}/storage/THE_PID/EXTRACTED_TEXT/'


def call_item_api(item_pid) -> dict:
    """
    Calls the item-api to determine how to access the extracted-text datastream.
    Called by: manage_ner_processing()
    """
    item_api_url: str = ITEM_API_URL_TPL.replace('THE_PID', item_pid)
    item_api_response: httpx.Response = httpx.get(item_api_url)
    item_api_response_jdict: dict = item_api_response.json()
    # log.debug(f'item_api_response_jdict, ``{pprint.pformat(item_api_response_jdict)}``')
    return item_api_response_jdict


def evaluate_item_api_response(item_api_response_jdict) -> tuple[str, str]:
    """
    Evaluates the item-api response to determine how to access the extracted-text datastream.
    Called by: manage_ner_processing()
    """
    extracted_text_url: str = ''
    err: str = ''
    try:
        extracted_text_url: str = item_api_response_jdict['links']['content_datastreams']['EXTRACTED_TEXT']
    except KeyError:
        message: str = 'extracted-text not at `links.EXTRACTED_TEXT`'
        log.warning(message)
        err = message
    log.debug(f'extracted_text_url: {extracted_text_url}')
    log.debug(f'err: {err}')
    return extracted_text_url, err


def get_extracted_text_datastream(extracted_text_url) -> str:
    """
    Gets the extracted-text datastream.
    Called by: manage_ner_processing()
    """
    extracted_text: str = httpx.get(extracted_text_url).text
    # log.debug(f'extracted_text, ``{extracted_text}``')
    return extracted_text


def build_err_response(item_pid: str, err: str, start_time: datetime) -> str:
    """
    Builds an error response string.
    Called by: manage_ner_processing()
    """
    log.debug(f'item_pid: {item_pid}')
    log.debug(f'err: {err}')
    log.debug(f'start_time: {start_time}')
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


def extract_entities(extracted_text: str) -> list:
    """
    Processes the extracted-text with spaCy.
    Called by: manage_ner_processing()
    """
    nlp = spacy.load('en_core_web_sm')
    doc: spacy.tokens.Doc = nlp(extracted_text)
    # log.debug(f'doc, ``{doc}``')
    spacy_named_entities = []
    for ent in doc.ents:
        token = ent.text
        label = ent.label_
        tuple_ = (token, label)
        spacy_named_entities.append(tuple_)
    # log.debug(f'spacy_named_entities, ``{pprint.pformat(spacy_named_entities)}``')
    return spacy_named_entities


class Processor:
    def __init__(self, original_entities: list | None = None):
        """
        Original entities are the named entities returned by spaCy.
        They may look like this:
        [
          ("Egypt", "GPE"),
          ("Barca", "PRODUCT"),
          ("Egypt\n", "GPE"),
          -- etc --
        ]
        """
        self.original_entities: list = original_entities if original_entities is not None else []
        self.cleaned_entities: list = []
        self.sorted_unique_entries: list = []
        self.by_entity_display: dict = {}
        self.by_top_x_display: dict = {}

    def manage_processing(self, cut_off: int = 3) -> None:
        """
        Manages the processing of named entities.
        Called by: manage_ner_processing()
        """
        self.clean_entities()
        self.make_uniques()
        self.group_by_entity()
        self.determine_top_x(cut_off)
        return

    def clean_entities(self) -> None:
        """
        Cleans up entity text by stripping whitespace andnewlines.
        Called by: manage_processing()
        """
        self.cleaned_entities: list = []
        for value, label in self.original_entities:
            cleaned_value = value.strip()
            cleaned_value = cleaned_value.replace('\n', '')
            self.cleaned_entities.append((cleaned_value, label))
        return

    def make_uniques(self) -> None:
        """
        Creates a list of alphabetical unique entities, with counts.
        Called by: manage_processing()

        Input (from self.cleaned_entities):
        [
            ('Egypt', 'GPE'),
            ('Barca', 'PRODUCT'),
            ('Africa From', 'LOC'),
            ('Cyrene', 'PERSON'),
            ('Egypt', 'GPE'),
        ]

        Output (to self.sorted_unique_entries):
        [
            (('Africa From', 'LOC'), 1),
            (('Barca', 'PRODUCT'), 1),
            (('Cyrene', 'PERSON'), 1),
            (('Egypt', 'GPE'), 2),
        ]

        The process:

        First creates a Counter object from self.cleaned_entities, like this:
        Counter(
            {('Egypt', 'GPE'): 2,
             ('Barca', 'PRODUCT'): 1,
             ('Africa From', 'LOC'): 1,
             ('Cyrene', 'PERSON'): 1}
            )

        Then builds sortable tuples: (value_lower, ne_label, value_original, count)
        Then sorts.
        Then reconstructs the desired shape: [ ((value, label), count), ... ]

        This is an experiment to avoid the much more direct, but dense:
        ```self.sorted_unique_entries = sorted(named_entity_counts.items(), key=lambda kv: (kv[0][0].lower(), kv[0][1]))```
        Here's how much faster the one-liner is:
        n=  1000 current_impl: 0.0210s  one_liner: 0.0016s  ratio=12.96x
        n=  5000 current_impl: 0.1059s  one_liner: 0.0107s  ratio=9.89x
        n= 20000 current_impl: 0.4328s  one_liner: 0.0486s  ratio=8.90x
        ...based on running `temp_benchmark_make_uniques.py` multiple times and averaging the results.

        Despite the significant ratios, I'll leave the more explicit code for now, because the total time increase
        isn't burdensome.
        """
        named_entity_counts: Counter[tuple[str, str]] = Counter(self.cleaned_entities)
        ## build sortable tuples ------------------------------------
        sortable: list[tuple[str, str, str, int]] = []
        for (value, label), count in named_entity_counts.items():
            sortable.append((value.lower(), label, value, count))
        ## sort via default tuple ordering --------------------------
        sortable.sort()
        ## reconstruct desired shape ---------------------------------
        self.sorted_unique_entries = []
        for _value_lower, label, value, count in sortable:
            self.sorted_unique_entries.append(((value, label), count))
        log.debug(f'sorted_unique_entries, ``{pprint.pformat(self.sorted_unique_entries)}``')
        return

        ## end def make_uniques()

    def group_by_entity(self) -> None:
        """
        Makes dict of entities and their value-counts, like:

        Input (from self.sorted_unique_entries):
        [
            (('Africa From', 'LOC'), 1),
            (('Barca', 'PRODUCT'), 1),
            (('Cyrene', 'PERSON'), 1),
            (('Egypt', 'GPE'), 2),
            (('Tunisia', 'GPE'), 1),
        ]

        Output (to self.by_type_counts):
        {
            'GPE': {'Egypt': 2, 'Tunisia': 1},
            'LOC': {'Africa From': 1},
            'PERSON': {'Cyrene': 1},
            'PRODUCT': {'Barca': 1},
        }

        Called by: Processor.manage_processing()
        """
        self.by_entity_display = {}
        for (value, label), count in self.sorted_unique_entries:
            bucket = self.by_entity_display.setdefault(label, {})  # creates bucket if it doesn't exist
            bucket[value] = bucket.get(value, 0) + count  # updating bucket auto-updates `by_entity_display`
        log.debug(f'by_type_counts, ``{pprint.pformat(self.by_entity_display)}``')
        return

    def determine_top_x(self, cut_off: int) -> None:
        """
        Determines the top X entities for each entity type.
        Called by: Processor.manage_processing()
        """
        self.by_top_x_display = {}

        for entity_label, value_counts in self.by_entity_display.items():
            # simple-case, preserve simple data ---------------------
            if len(value_counts) == 1:
                only_value: str = next(iter(value_counts.keys()))
                only_count: int = value_counts[only_value]
                self.by_top_x_display[entity_label] = [(str(only_count), only_value)]
                continue

            # build counts-to-values dict ---------------------------
            """
            example counts-to-values dict built:
            counts_to_values = {
                2: ['Egypt', 'Tunisia'],
                1: ['Africa From', 'Barca', 'Cyrene']
            }
            """
            counts_to_values: dict[int, list[str]] = {}
            for value, count in value_counts.items():
                bucket: list[str] | None = counts_to_values.get(count)
                if bucket is None:
                    counts_to_values[count] = [value]
                else:
                    bucket.append(value)

            # sort --------------------------------------------------
            sorted_counts: list[int] = sorted(
                counts_to_values.keys(), reverse=True
            )  # counts descending, and values within each count ascending

            # build top-list ----------------------------------------
            top_counts: list[int] = sorted_counts[:cut_off]
            top_list: list[tuple[str, list[str]]] = []
            for cnt in top_counts:
                values_for_cnt: list[str] = counts_to_values[cnt]
                values_for_cnt.sort()
                top_list.append((str(cnt), values_for_cnt))

            self.by_top_x_display[entity_label] = top_list
        return

    ## end class Processor


def build_response(item_pid: str, processor: Processor, start_time: datetime) -> str:
    """
    Builds a response for the named entity recognition (NER) processing for a single item.
    Called by: manage_ner_processing()
    """
    time_stamp: str = start_time.isoformat()
    time_taken: float = (datetime.now() - start_time).total_seconds()
    time_taken_str: str = f'{time_taken:.1f} seconds'
    meta = {
        'time_stamp': time_stamp,
        'time_taken': time_taken_str,
        'item_pid': item_pid,
        'item_url': ITEM_URL_TPL.replace('THE_PID', item_pid),
    }
    rsp_dct = {
        'meta': meta,
        'data_all': processor.by_entity_display,
        'data_top_x': processor.by_top_x_display,
    }
    jsn: str = json.dumps(rsp_dct, sort_keys=True, indent=2)
    return jsn


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
    ## run spaCy ----------------------------------------------------
    original_entities: list = extract_entities(extracted_text)
    ## process entities ---------------------------------------------
    processor: Processor = Processor(original_entities)
    processor.manage_processing()
    ## return response ----------------------------------------------
    jsn: str = build_response(item_pid, processor, start_time)
    # jsn: str = json.dumps(processed_entities, sort_keys=True, indent=2)
    print(jsn)
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
