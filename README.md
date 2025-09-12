# bdr_api_tools


## Purpose

To assemble a collection of little utilities that can be run from anywhere, referencing:

<https://brown-university-library.github.io/bdr-api-tools/>

...powered by the wonderful python package-manager, [uv](https://docs.astral.sh/uv/)


## Usage – directly

Example for calculating a collection size:
```bash
$ uv run ./calc_collection_size.py --collection-pid bdr:XXXXXXX
```

## Usage — via git.io url
```bash
$ uv run https://brown-university-library.github.io/bdr-api-tools/calc_collection_size.py --collection-pid bdr:XXXXXXX
```


## Developer/IDE-agent Notes

- Do not use `python3 ...` directly. Always run commands using `uv run`.

- Similarly, run tests like:
    ```bash
    $ uv run -m unittest discover -v
    ```
