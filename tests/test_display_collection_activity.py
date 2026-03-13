import tempfile
import unittest
from pathlib import Path

from display_collection_activity import aggregate_monthly_counts
from display_collection_activity import build_output_path
from display_collection_activity import normalize_date_value
from display_collection_activity import write_output_file


class TestNormalizeDateValue(unittest.TestCase):
    """
    Tests date normalization behavior.
    """

    def test_extracts_month_from_iso_date(self):
        """
        Checks extraction of a YYYY-MM value from an ISO-like date string.
        """
        self.assertEqual(normalize_date_value('2024-11-15T10:30:00Z'), '2024-11')

    def test_returns_none_for_missing_or_malformed_date(self):
        """
        Checks fallback behavior for invalid date values.
        """
        self.assertIsNone(normalize_date_value(None))
        self.assertIsNone(normalize_date_value('not-a-date'))
        self.assertIsNone(normalize_date_value('2024-13-01'))


class TestAggregateMonthlyCounts(unittest.TestCase):
    """
    Tests monthly aggregation behavior.
    """

    def test_aggregates_multiple_docs_by_month(self):
        """
        Checks aggregation correctness across multiple documents.
        """
        docs = [
            {'pid': 'bdr:1', 'dateCreated': '2024-11-03T12:00:00Z'},
            {'pid': 'bdr:2', 'dateCreated': '2024-11-20'},
            {'pid': 'bdr:3', 'dateCreated': '2024-12-01'},
        ]

        result = aggregate_monthly_counts(docs)

        self.assertEqual(result['monthly_counts'], {'2024-11': 2, '2024-12': 1})
        self.assertEqual(result['items_counted'], 3)
        self.assertEqual(result['items_skipped'], 0)
        self.assertEqual(result['date_field_used'], 'dateCreated')
        self.assertEqual(result['date_fields_used'], ['dateCreated'])

    def test_skips_docs_without_usable_dates_and_uses_fallback_field(self):
        """
        Checks skip behavior when some docs have no usable date and another candidate field is used.
        """
        docs = [
            {'pid': 'bdr:1', 'dateCreated': None, 'dateIssued': '2023-01-10'},
            {'pid': 'bdr:2', 'dateCreated': 'bad-value', 'dateIssued': ['2023-01-15', '2023-02-01']},
            {'pid': 'bdr:3', 'dateCreated': None, 'dateIssued': None},
        ]

        result = aggregate_monthly_counts(docs)

        self.assertEqual(result['monthly_counts'], {'2023-01': 2})
        self.assertEqual(result['items_counted'], 2)
        self.assertEqual(result['items_skipped'], 1)
        self.assertEqual(result['date_field_used'], 'dateIssued')
        self.assertEqual(result['date_fields_used'], ['dateIssued'])


class TestOutputPathAndWriting(unittest.TestCase):
    """
    Tests output path creation and file writing.
    """

    def test_build_output_path_generates_expected_filename(self):
        """
        Checks output filename generation.
        """
        output_path = build_output_path('/tmp/example-output', 'bdr:bwehb8b8')
        self.assertEqual(output_path.name, 'collection_activity__bdr_bwehb8b8.json')

    def test_write_output_file_writes_pretty_printed_json(self):
        """
        Checks JSON file writing behavior.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / 'nested' / 'report.json'
            output_data = {
                '_meta_': {'collection_pid': 'bdr:test'},
                'monthly_counts': {'2024-11': 2},
            }

            write_output_file(output_path, output_data)

            file_text = output_path.read_text(encoding='utf-8')
            self.assertTrue(file_text.endswith('\n'))
            self.assertIn('\n  "_meta_": {\n', file_text)
            self.assertIn('\n  "monthly_counts": {\n', file_text)


if __name__ == '__main__':
    unittest.main()
