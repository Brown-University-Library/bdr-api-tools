See [index.md](index.md) for more info; that's used for the auto-generated github.io [landing page](https://brown-university-library.github.io/bdr-api-tools/).

Additional notes:
- The inline-metadata at the top of each script allows `uv` to run the script without a local installation of python or virtual-environments or dependency-packages.

- Dependencies are _also_ listed in the `pyproject.toml` file to run tests easily.

- Current standalone tools include `calc_collection_size.py`, `display_collection_activity.py`, `display_recent_activity.py`, `gather_extracted_text.py`, `list_named_entities.py`, and `show_zip_info.py`.

- The `pyproject.toml` file contains some project guidelines for LLM-helpers to follow when generating code. I wanted to put this somewhere unobtrusive. Windsurf's `GPT-5 (low-reasoning)` does a good job following these.

---
