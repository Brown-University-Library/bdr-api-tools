import unittest

import httpx

from display_recent_activity import (
    build_collection_summary,
    build_collection_title,
    build_progress_bar,
    choose_collection_pids,
    choose_deposit_date,
    count_unique_collections,
    enrich_recent_items_with_collections,
    format_duration,
    format_elapsed_timetaken,
    format_integer_with_underscores,
    parse_args,
)


class TestChooseDepositDate(unittest.TestCase):
    """
    Tests deposit-date selection behavior.
    """

    def test_chooses_first_usable_string_date(self):
        """
        Checks that the first usable deposit-date value is selected.
        """
        doc = {'deposit_date': ['2026-03-31T16:00:00Z', '2026-03-30T16:00:00Z']}

        result = choose_deposit_date(doc)

        self.assertEqual(result, '2026-03-31T16:00:00Z')

    def test_returns_none_when_no_usable_date_exists(self):
        """
        Checks fallback behavior when no usable deposit-date value exists.
        """
        doc = {'deposit_date': ['', None]}

        result = choose_deposit_date(doc)

        self.assertIsNone(result)


class TestChooseCollectionPids(unittest.TestCase):
    """
    Tests search-doc collection-membership extraction behavior.
    """

    def test_extracts_unique_collection_pids_from_search_doc(self):
        """
        Checks extraction from the search response collection-membership field.
        """
        doc = {
            'rel_is_member_of_collection_ssim': [
                'bdr:alpha1',
                'bdr:beta2',
                'bdr:alpha1',
                '',
            ]
        }

        result = choose_collection_pids(doc)

        self.assertEqual(result, ['bdr:alpha1', 'bdr:beta2'])


class TestBuildCollectionSummary(unittest.TestCase):
    """
    Tests collection-summary aggregation behavior.
    """

    def test_builds_counts_across_recent_items(self):
        """
        Checks aggregation correctness when multiple recent items share collections.
        """
        recent_items = [
            {
                'pid': 'bdr:1',
                'collections': [
                    {'pid': 'bdr:col1', 'title': 'Collection One'},
                    {'pid': 'bdr:col2', 'title': 'Collection Two'},
                ],
            },
            {
                'pid': 'bdr:2',
                'collections': [
                    {'pid': 'bdr:col1', 'title': 'Collection One'},
                ],
            },
            {
                'pid': 'bdr:3',
                'collections': [
                    {'pid': 'bdr:col3', 'title': 'Collection Three'},
                ],
            },
        ]

        result = build_collection_summary(recent_items)

        self.assertEqual(
            result,
            [
                {
                    'collection_pid': 'bdr:col1',
                    'collection_title': 'Collection One',
                    'recent_item_count': 2,
                },
                {
                    'collection_pid': 'bdr:col3',
                    'collection_title': 'Collection Three',
                    'recent_item_count': 1,
                },
                {
                    'collection_pid': 'bdr:col2',
                    'collection_title': 'Collection Two',
                    'recent_item_count': 1,
                },
            ],
        )

    def test_counts_unique_collections_across_recent_items(self):
        """
        Checks distinct-collection counting across repeated memberships.
        """
        recent_items = [
            {
                'pid': 'bdr:1',
                'collections': [
                    {'pid': 'bdr:col1', 'title': 'Collection One'},
                    {'pid': 'bdr:col2', 'title': 'Collection Two'},
                ],
            },
            {
                'pid': 'bdr:2',
                'collections': [
                    {'pid': 'bdr:col1', 'title': 'Collection One'},
                ],
            },
        ]

        result = count_unique_collections(recent_items)

        self.assertEqual(result, 2)


class TestEnrichRecentItemsWithCollections(unittest.TestCase):
    """
    Tests recent-item enrichment behavior.
    """

    def test_uses_cached_collection_lookups_for_search_derived_membership(self):
        """
        Checks collection-title lookup reuse when membership comes from search docs.
        """

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == '/api/collections/bdr:alpha1/':
                return httpx.Response(
                    status_code=200,
                    request=request,
                    json={'name': 'Alpha Collection', 'ancestors': [{'name': 'Parent One'}]},
                )
            if request.url.path == '/api/collections/bdr:beta2/':
                return httpx.Response(
                    status_code=200,
                    request=request,
                    json={'name': 'Beta Collection', 'ancestors': []},
                )
            return httpx.Response(status_code=404, request=request)

        transport = httpx.MockTransport(handler)
        client = httpx.Client(transport=transport)
        http_call_count = {'count': 0}
        recent_items = [
            {
                'pid': 'bdr:item1',
                'primary_title': 'Item One',
                'deposit_date': '2026-03-31T00:00:00Z',
                '__collection_pids': ['bdr:alpha1', 'bdr:beta2'],
                'collections': [],
            },
            {
                'pid': 'bdr:item2',
                'primary_title': 'Item Two',
                'deposit_date': '2026-03-30T00:00:00Z',
                '__collection_pids': ['bdr:alpha1'],
                'collections': [],
            }
        ]

        result = enrich_recent_items_with_collections(client, recent_items, http_call_count)

        self.assertEqual(
            result['recent_items'][0]['collections'],
            [
                {'pid': 'bdr:alpha1', 'title': '`Alpha Collection` -- (from parent-collection `Parent One`)'},
                {'pid': 'bdr:beta2', 'title': 'Beta Collection'},
            ],
        )
        self.assertEqual(
            result['recent_items'][1]['collections'],
            [{'pid': 'bdr:alpha1', 'title': '`Alpha Collection` -- (from parent-collection `Parent One`)'}],
        )
        self.assertNotIn('skipped_items', result)
        self.assertEqual(result['skipped_collections'], [])
        self.assertNotIn('__collection_pids', result['recent_items'][0])
        self.assertNotIn('collection_lookup_status', result['recent_items'][0])
        self.assertEqual(http_call_count['count'], 2)
        client.close()


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

    def test_builds_plain_title_when_no_parent_exists(self):
        """
        Checks fallback title formatting when no parent collection title can be derived.
        """
        collection_data = {
            'name': 'Datasets',
            'ancestors': [],
        }

        result = build_collection_title(collection_data)

        self.assertEqual(result, 'Datasets')


class TestProgressHelpers(unittest.TestCase):
    """
    Tests progress-display helper behavior.
    """

    def test_formats_short_duration(self):
        """
        Checks minute-second formatting for short elapsed times.
        """
        result = format_duration(65)

        self.assertEqual(result, '01:05')

    def test_formats_long_duration(self):
        """
        Checks hour-aware formatting for longer elapsed times.
        """
        result = format_duration(3665)

        self.assertEqual(result, '1:01:05')

    def test_builds_progress_bar(self):
        """
        Checks ASCII progress-bar rendering at partial completion.
        """
        result = build_progress_bar(completed=3, total=4, width=8)

        self.assertEqual(result, '[######--]')

    def test_formats_elapsed_timetaken(self):
        """
        Checks runtime formatting with tenths-of-a-second precision.
        """
        result = format_elapsed_timetaken(3661.26)

        self.assertEqual(result, '1:01:01.3')

    def test_formats_integer_with_underscores(self):
        """
        Checks integer display formatting with underscore separators.
        """
        result = format_integer_with_underscores(1234567)

        self.assertEqual(result, '1_234_567')


class TestParseArgs(unittest.TestCase):
    """
    Tests command-line argument parsing behavior.
    """

    def test_parses_progress_flag(self):
        """
        Checks parsing of the explicit progress-display flag.
        """
        parsed_args = parse_args(['--progress'])

        self.assertTrue(parsed_args.progress)
        self.assertFalse(parsed_args.no_progress)


if __name__ == '__main__':
    unittest.main()
