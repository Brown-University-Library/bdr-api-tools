# To run tests...

## All tests...
```bash
cd ./bdr-api-tools
uv run -m unittest discover -v
```

## Single test-file...
```bash
cd ./bdr-api-tools
uv run -m unittest -v tests/test_calc_collection_size.py
```

## Single test-class...
```bash
cd ./bdr-api-tools
uv run -m unittest -v tests.test_calc_collection_size.TestHumanBytes
```

##  Single test-method...
```bash
cd ./bdr-api-tools
uv run -m unittest -v tests.test_calc_collection_size.TestHumanBytes.test_bytes
```
