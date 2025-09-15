# About bdr-api-tools

This is collection of small utilities for interacting with the Brown Digital Repository (BDR) [APIs](https://github.com/Brown-University-Library/bdr_api_documentation/wiki). The tools are designed to be easily run from anywhere via the wonderful python package-manager, `uv` ([link](https://docs.astral.sh/uv/)) -- which allows referencing these tools via a URL. No special installation of python or virtual-environments or dependency-packages is required. Just [installing](https://docs.astral.sh/uv/getting-started/installation/) `uv` is enough to let you run these tools like this:

```bash
uv run https://brown-university-library.github.io/bdr-api-tools/calc_collection_size.py --collection-pid bdr:bwehb8b8
```

For Brown community members, a recommendation: if you're off-campus, enable [Brown-VPN](https://it.brown.edu/services/virtual-private-network-vpn) before using these tools. The reason: We've had to enable certain protections to prevent problems due to excessive bot-traffic. If you're on VPN, you'll minimize the chance of being blocked when the BDR-APIs are accessed.

---


## tools listing

- calc_collection_size.py ([brief](#calc_collection_sizepy-brief)), ([detailed](#calc_collection_sizepy-detailed))
- gather_extracted_text.py ([brief](#gather_extracted_textpy-brief)), ([detailed](#gather_extracted_textpy-detailed))
- show_zip_info.py ([brief](#show_zip_infopy-brief)), ([detailed](#show_zip_infopy-detailed))

---
---


## brief info

### calc_collection_size.py (brief)

Calculates the total storage size of all items in the given BDR collection. It queries the Search API with pagination to retrieve and process the necessary data, and prints a summary including a human-readable size. It separately fetches the collection title via the Collections API. 

---


### gather_extracted_text.py (brief)

Collects EXTRACTED_TEXT across all items (parent & child) in a collection, writing a single combined text file and a detailed listing JSON. It saves progress to support interruptions and resuming without overloading the server, and offers an optional --test-limit flag for testing convenience. 

---

### show_zip_info.py (brief)

Fetches data from the item-api and summarizes zip-file contents for the givenitem -- AND zip-file contents for all child-items. It computes per-item and aggregate-summary filetype counts based on extension, and outputs a structured JSON with _meta_ and item_info.

---
---


## detailed info


### calc_collection_size.py (detailed)

Calculates the total storage size of all items in the given BDR collection. It queries the Search API with pagination to retrieve and process the necessary data, and prints a summary including a human-readable size. It separately fetches the collection title via the Collections API. 

Args: --collection-pid (required)

Example usage:
```
uv run https://brown-university-library.github.io/bdr-api-tools/calc_collection_size.py --collection-pid bdr:bwehb8b8
```

Output:
```
Collection: bdr:bwehb8b8
Title: Brown University Open Data Collection
Items found: 908
Items with size counted: 908
Total bytes: 257151383232
Human: 239.49 GB
```

[Code](https://github.com/Brown-University-Library/bdr-api-tools/blob/main/calc_collection_size.py)

---


### gather_extracted_text.py (detailed)

Collects EXTRACTED_TEXT BDR data-streams across all items in the given collection, writing a single combined text file (prefixed by PID delimiters) and a detailed listing JSON. It enumerates members via the Search API, locates EXTRACTED_TEXT links (parent or hasPart child), streams content with retries/throttling, and saves progress after every item to support interruptions and resuming without overloading the server. It shows a progress display, and offers an optional --test-limit flag for testing convenience, and human-readable size summaries.

Args: --collection-pid (required), --output-dir (required), --test-limit (optional).


Example usage:
```
uv run gather_extracted_text.py --collection-pid bdr:bfttpwkj --output-dir "../output_dir" --test-limit 2
```

Output:
```
% cd ../output_dir 

% cd ./run-20250913T133756-0400-bdr_bfttpwkj 

% ls                                        
total 1368
 ./
 ../
 checkpoint_for_collection_pid-bdr_bfttpwkj.json
 extracted_text_for_collection_pid-bdr_bfttpwkj.txt
 listing_for_collection_pid-bdr_bfttpwkj.json

% cat ./checkpoint_for_collection_pid-bdr_bfttpwkj.json 
{
  "collection_pid": "bdr:bfttpwkj",
  "safe_collection_pid": "bdr_bfttpwkj",
  "created_at": "2025-09-13T13:37:56.331650-04:00",
  "updated_at": "2025-09-13T13:37:59.869694-04:00",
  "run_directory_name": "run-20250913T133756-0400-bdr_bfttpwkj",
  "completed": true,
  "counts": {
    "total_docs": 209,
    "processed_count": 2,
    "appended_count": 2,
    "no_text_count": 0,
    "forbidden_count": 0
  },
  "paths": {
    "combined_text": "run-20250913T133756-0400-bdr_bfttpwkj/extracted_text_for_collection_pid-bdr_bfttpwkj.txt",
    "listing_json": "run-20250913T133756-0400-bdr_bfttpwkj/listing_for_collection_pid-bdr_bfttpwkj.json"
  }
}

% cat listing_for_collection_pid-bdr_bfttpwkj.json 
{
  "summary": {
    "timestamp": "2025-09-13T13:37:59.869516-04:00",
    "all_extracted_text_file_size": "691.8 kB",
    "count_of_all_extracted_text_files": 2,
    "combined_text_path": "run-20250913T133756-0400-bdr_bfttpwkj/extracted_text_for_collection_pid-bdr_bfttpwkj.txt",
    "listing_path": "run-20250913T133756-0400-bdr_bfttpwkj/listing_for_collection_pid-bdr_bfttpwkj.json",
    "collection_pid": "bdr:bfttpwkj",
    "collection_primary_title": "Theses and Dissertations -- (from Computer Science)"
  },
  "items": [
    {
      "item_pid": "bdr:386312",
      "primary_title": "Policy Delegation and Migration for Software-Defined Networks",
      "full_item_api_url": "https://repository.library.brown.edu/api/items/bdr:386312/",
      "full_studio_url": "https://repository.library.brown.edu/studio/item/bdr:386312/",
      "extracted_text_file_size": "233.1 kB"
    },
    {
      "item_pid": "bdr:386305",
      "primary_title": "Efficient Cryptography for Information Privacy",
      "full_item_api_url": "https://repository.library.brown.edu/api/items/bdr:386305/",
      "full_studio_url": "https://repository.library.brown.edu/studio/item/bdr:386305/",
      "extracted_text_file_size": "458.6 kB"
    }
  ]
}
```

Not `cat`-ing the `extracted_text_for_collection_pid-bdr_bfttpwkj.txt` -- it contains all the extracted text for each item. Each individual item is prefixed like: 

`---|||start-of-pid:bdr:386312|||---`

[Code](https://github.com/Brown-University-Library/bdr-api-tools/blob/main/gather_extracted_text.py)

---


### show_zip_info.py (detailed)

Fetches data from the item-api and summarizes (based on extension) zip-file contents for the given item. It also lists, and summarizes, zip-file contents for all child-items. In addition to the per-item summary, it also summarizes the filetype counts across both parent and all child-items.

Args: --item_pid (required).

Example usage:
```
uv run https://brown-university-library.github.io/bdr-api-tools/show_zip_info.py --item_pid bdr:833705
```

Output (excerpt):
```
{
  "_meta_": {
    "timestamp": "2025-09-11T22:56:48.249463-04:00",
    "full_item_api_url": "https://repository.library.brown.edu/api/items/bdr:833705/",
    "item_pid": "bdr:833705"
  },
  "item_info": {
    "pid": "bdr:833705",
    "primary_title": "(ATOMICS-0) Adjudicated/Annotated Telemetry Signals for Medically Important and Clinically Significant Events-0 Dataset",
    "item_zip_info": [
      "ATOMICS-0/ATOMICS-0-Data-Catalog-Record-2018.pdf",
      "ATOMICS-0/README.md"
    ],
    "item_zip_filetype_summary": {
      "md": 1,
      "pdf": 1
    },
    "has_parts_zip_info": [
      {
        "child_pid": "bdr:841254",
        "primary_title": "ATOMICS SHARED CODE",
        "child_zip_info": [
          "__MACOSX/x1. ATOMICS shared code/._III+ ATOMICS_dataset_window_processor.py",
          "__MACOSX/x1. ATOMICS shared code/._III+ ATOMICS_dataset_window_processorSLC.py",
          "x1. ATOMICS shared code/III+ ATOMICS_dataset_window_processor.py",
          "x1. ATOMICS shared code/III+ ATOMICS_dataset_window_processorSLC.py"
        ],
        "child_zip_filetype_summary": {
          "py": 4
        }
      },
      {
        "child_pid": "bdr:841252",
        "primary_title": "ATOMICS-0 SHARE ALL",
        "child_zip_info": [
          "ATOMICS_0_share_ALL/+ATOMICS dataset descriptions and use instructions v1.0.doc",
          "ATOMICS_0_share_ALL/.DS_Store",
          "ATOMICS_0_share_ALL/PERSEUS+MeTeOR EULA.pdf",
          "ATOMICS_0_share_ALL/select latched controls from ATOMICS-2 dataset.txt",
          "ATOMICS_0_share_ALL/week02_day01_alarmsSLC/x00-86.2015-08-15_alarmsSLC.csv",
          "ATOMICS_0_share_ALL/week02_day01_alarmsSLC/x00-89.2015-08-15_alarmsSLC.csv",
<snip>
        ],
        "child_zip_filetype_summary": {
        "csv": 320,
        "doc": 1,
        "ds_store": 2,
        "pdf": 2,
        "py": 4,
        "txt": 194
        }
      }
    ],
<snip>
    "overall_zip_filetype_summary": {
      "csv": 600,
      "doc": 1,
      "ds_store": 14,
      "md": 1,
      "pdf": 15,
      "py": 8,
      "txt": 363
    }
  }
}
```

[Code](https://github.com/Brown-University-Library/bdr-api-tools/blob/main/show_zip_info.py)

---
---
