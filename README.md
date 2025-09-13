# About bdr-api-tools

This collection of small utilities is designed to be easily run from anywhere via the wonderful python package-manager, `uv` ([link](https://docs.astral.sh/uv/)) -- which allows referencing these tools via a URL. No special installation of python or virtual-environments or dependency-packages is required. (Just installing `uv` is enough.)

---


## calc_collection_size.py 

Calculates the total storage size of all items in the given BDR collection. It queries the Search API with pagination to retrieve and process the necessary data, and prints a summary including a human-readable size. It separately fetches the collection title via the Collections API. 

Args: --collection-pid (required)

---


## gather_extracted_text.py 

Collects EXTRACTED_TEXT BDR data-streams across all items in the given collection, writing a single combined text file (prefixed by PID delimiters) and a detailed listing JSON. It enumerates members via the Search API, locates EXTRACTED_TEXT links (parent or hasPart child), streams content with retries/throttling, and saves progress after every item to support interruptions and resuming without overloading the server. It shows a progress display, and offers an optional --test-limit flag for testing convenience, and human-readable size summaries. 

Args: --collection-pid (required), --output-dir (required), --test-limit (optional).

---

## show_zip_info.py 

Fetches data from the item-api and summarizes zip-file contents for the givenitem -- AND zip-file contents for all child-items. It computes per-item and aggregate-summary filetype counts based on extension, and outputs a structured JSON with _meta_ and item_info.

Args: --item_pid (required).

---
