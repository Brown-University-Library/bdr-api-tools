import unittest

from display_collection_activity import aggregate_monthly_counts, build_collection_title, normalize_date_value


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
            {'pid': 'bdr:1', 'deposit_date': '2024-11-03T12:00:00Z'},
            {'pid': 'bdr:2', 'deposit_date': '2024-11-20'},
            {'pid': 'bdr:3', 'deposit_date': '2024-12-01'},
        ]

        result = aggregate_monthly_counts(docs)

        self.assertEqual(result['monthly_counts'], {'2024-11': 2, '2024-12': 1})
        self.assertEqual(result['items_counted'], 3)
        self.assertEqual(result['items_skipped'], 0)
        self.assertEqual(result['date_field_used'], 'deposit_date')
        self.assertEqual(result['date_fields_used'], ['deposit_date'])

    def test_skips_docs_without_usable_deposit_dates(self):
        """
        Checks skip behavior when some docs have no usable deposit date.
        """
        docs = [
            {'pid': 'bdr:1', 'deposit_date': '2023-01-10'},
            {'pid': 'bdr:2', 'deposit_date': ['2023-01-15', '2023-02-01']},
            {'pid': 'bdr:3', 'deposit_date': None, 'dateCreated': '2023-01-20'},
            {'pid': 'bdr:4', 'deposit_date': 'bad-value'},
        ]

        result = aggregate_monthly_counts(docs)

        self.assertEqual(result['monthly_counts'], {'2023-01': 2})
        self.assertEqual(result['items_counted'], 2)
        self.assertEqual(result['items_skipped'], 2)
        self.assertEqual(result['date_field_used'], 'deposit_date')
        self.assertEqual(result['date_fields_used'], ['deposit_date'])


class TestBuildCollectionTitle(unittest.TestCase):
    """
    Tests collection title formatting behavior.
    """

    def test_builds_title_with_parent_collection_name(self):
        """
        Checks parent-aware title formatting when the collection JSON includes an ancestor name.
        """
        collection_data = {
            'name': 'Theses and Dissertations',
            'ancestors': [
                {'name': 'Library Collections'},
                {'name': 'Computer Science'},
            ],
        }

        result = build_collection_title(collection_data)

        self.assertEqual(result, '`Theses and Dissertations` -- (from parent-collection `Computer Science`)')

    def test_builds_title_with_undetermined_parent_suffix_when_missing(self):
        """
        Checks fallback title formatting when no parent collection title can be derived.
        """
        collection_data = {
            'name': 'Theses and Dissertations',
            'ancestors': [],
        }

        result = build_collection_title(collection_data)

        self.assertEqual(result, 'Theses and Dissertations')


if __name__ == '__main__':
    unittest.main()
