import json
import unittest
from pathlib import Path

# Import from the project root (bdr-api-tools). Pytest/unittest usually adds this to sys.path when run from the repo root.
from gather_extracted_text import collection_title_from_json


class TestCollectionTitleFromJson(unittest.TestCase):
    """
    Tests the collection_title_from_json function using a real fixture.
    """

    def test_builds_expected_title_from_fixture(self) -> None:
        """
        Loads the fixture JSON and checks the computed title.
        """
        fixture_path: Path = Path(__file__).parent / 'test_data' / 'collection_data_bdr-bfttpwkj.json'
        with fixture_path.open('r', encoding='utf-8') as fh:
            coll_json: dict[str, object] = json.load(fh)

        computed: str = collection_title_from_json(coll_json)
        expected: str = 'Theses and Dissertations -- (from Computer Science)'
        self.assertEqual(computed, expected)


if __name__ == '__main__':
    unittest.main()
