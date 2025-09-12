Collection of small utilities, easily run from anywhere via the wonderful package, `uv` ([link](https://docs.astral.sh/uv/)).

_(In usage examples below, where a line ends in a backslash, that backslash is used to break the line into multiple lines for readability. The backslash is not part of the command. It can be removed and the command can be run as a single line.)_

---


## calc_collection_size

Calculates the size of a collection in the Brown Digital Repository.

Example:
```
uv run https://birkin.github.io/utilities-project/calc_collection_size.py --collection-pid bdr:bwehb8b8
```

Tweak page-size if desired (API typically caps at <= 500):
```
  uv run ./calc_collection_size.py --collection-pid bdr:bwehb8b8 --rows 500

[Code](https://github.com/birkin/utilities-project/blob/main/random_id_maker.py)

---
