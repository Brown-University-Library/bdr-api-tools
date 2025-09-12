Collection of small utilities, easily run from anywhere via the wonderful package, `uv` ([link](https://docs.astral.sh/uv/)).

_(In usage examples below, where a line ends in a backslash, that backslash is used to break the line into multiple lines for readability. The backslash is not part of the command. It can be removed and the command can be run as a single line.)_

---


## calc_collection_size

Calculates the size of a collection in the Brown Digital Repository.

Example:
```
uv run https://brown-university-library.github.io/bdr-api-tools/calc_collection_size.py --collection-pid bdr:bwehb8b8

Collection: bdr:bwehb8b8
Title: Brown University Open Data Collection
Items found: 908
Items with size counted: 908
Total bytes: 257151383232
Human: 239.49 GB
```


[Code](https://github.com/Brown-University-Library/bdr-api-tools/blob/main/calc_collection_size.py)

---
