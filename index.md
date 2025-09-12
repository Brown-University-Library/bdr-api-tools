Collection of small utilities, easily run from anywhere via the wonderful python package-manager, `uv` ([link](https://docs.astral.sh/uv/)).

---


## tools listing

- [calc_collection_size.py](#calc_collection_sizepy)
- [show_zip_info.py](#show_zip_infopy)

---


## calc_collection_size.py

Calculates the size of a collection in the Brown Digital Repository.

Example:
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

## show_zip_info.py

Fetches BDR item-api data and extracts and summarizes zip file data for item and and any `hasParts` child-items.

Example:
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
