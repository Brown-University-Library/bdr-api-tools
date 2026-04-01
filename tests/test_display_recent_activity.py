import unittest

import httpx

from display_recent_activity import (
    build_collection_title,
    build_recent_items,
    build_updated_collections,
    choose_item_date,
    choose_sort_field_and_docs,
    normalize_collection_pids,
)


class TestNormalizeCollectionPids(unittest.TestCase):
    """
    Tests collection-membership normalization behavior.
    """

    def test_returns_unique_bdr_collection_pids(self) -> None:
        """
        Checks that the membership field is normalized to unique BDR collection PIDs.
        """
        doc: dict[str, object] = {
            'rel_is_member_of_collection_ssim': ['bdr:one', 'bdr:two', 'bdr:one', 'not-a-bdr-value', ''],
        }

        result: list[str] = normalize_collection_pids(doc)

        self.assertEqual(result, ['bdr:one', 'bdr:two'])


class TestChooseItemDate(unittest.TestCase):
    """
    Tests recent-date selection behavior.
    """

    def test_prefers_earlier_candidate_fields(self) -> None:
        """
        Checks that the date chooser returns the first populated candidate field.
        """
        doc: dict[str, object] = {
            'deposit_date': '2024-01-01',
            'object_created_dsi': '2024-02-02',
        }

        result: str | None = choose_item_date(doc)

        self.assertEqual(result, '2024-02-02')


class TestRecentActivityAggregation(unittest.TestCase):
    """
    Tests recent-item and collection-summary aggregation behavior.
    """

    def test_builds_recent_items_and_collection_counts(self) -> None:
        """
        Checks that items and updated collections are summarized into the expected output shape.
        """
        docs: list[dict[str, object]] = [
            {
                'pid': 'bdr:1',
                'primary_title': 'Item One',
                'object_created_dsi': '2026-03-31T14:10:00Z',
                'rel_is_member_of_collection_ssim': ['bdr:theses', 'bdr:comp-sci'],
            },
            {
                'pid': 'bdr:2',
                'primary_title': 'Item Two',
                'deposit_date': '2026-03-30',
                'rel_is_member_of_collection_ssim': ['bdr:theses'],
            },
            {
                'pid': 'bdr:3',
                'primary_title': 'Item Three',
                'rel_is_member_of_collection_ssim': ['bdr:archives'],
            },
        ]
        collection_title_map: dict[str, str] = {
            'bdr:theses': 'Theses and Dissertations -- (from Computer Science)',
            'bdr:comp-sci': 'Computer Science Department',
            'bdr:archives': 'University Archives',
        }

        recent_items: list[dict[str, object]] = build_recent_items(docs, collection_title_map)
        updated_collections: list[dict[str, object]] = build_updated_collections(docs, collection_title_map)

        self.assertEqual(
            recent_items,
            [
                {
                    'pid': 'bdr:1',
                    'primary_title': 'Item One',
                    'date_added': '2026-03-31T14:10:00Z',
                    'collections': [
                        {'pid': 'bdr:theses', 'name': 'Theses and Dissertations -- (from Computer Science)'},
                        {'pid': 'bdr:comp-sci', 'name': 'Computer Science Department'},
                    ],
                },
                {
                    'pid': 'bdr:2',
                    'primary_title': 'Item Two',
                    'date_added': '2026-03-30',
                    'collections': [
                        {'pid': 'bdr:theses', 'name': 'Theses and Dissertations -- (from Computer Science)'},
                    ],
                },
                {
                    'pid': 'bdr:3',
                    'primary_title': 'Item Three',
                    'date_added': None,
                    'collections': [
                        {'pid': 'bdr:archives', 'name': 'University Archives'},
                    ],
                },
            ],
        )
        self.assertEqual(
            updated_collections,
            [
                {
                    'collection_pid': 'bdr:theses',
                    'collection_title': 'Theses and Dissertations -- (from Computer Science)',
                    'recent_item_count': 2,
                },
                {
                    'collection_pid': 'bdr:comp-sci',
                    'collection_title': 'Computer Science Department',
                    'recent_item_count': 1,
                },
                {
                    'collection_pid': 'bdr:archives',
                    'collection_title': 'University Archives',
                    'recent_item_count': 1,
                },
            ],
        )


class TestBuildCollectionTitle(unittest.TestCase):
    """
    Tests collection title formatting behavior.
    """

    def test_builds_title_with_parent_collection_name(self) -> None:
        """
        Checks parent-aware title formatting when the collection JSON includes an ancestor name.
        """
        collection_data: dict[str, object] = {
            'name': 'Theses and Dissertations',
            'ancestors': [
                {'name': 'Library Collections'},
                {'name': 'Computer Science'},
            ],
        }

        result: str | None = build_collection_title(collection_data)

        self.assertEqual(result, 'Theses and Dissertations -- (from Computer Science)')


class TestChooseSortFieldAndDocs(unittest.TestCase):
    """
    Tests sort-field fallback behavior.
    """

    def test_falls_back_after_400_for_invalid_sort_field(self) -> None:
        """
        Checks that the first working sort field is selected after a 400 response.
        """

        class DummyClient:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def get(self, url: str, params: dict[str, object], timeout: int) -> httpx.Response:
                sort_field: str = str(params['sort'])
                self.calls.append(sort_field)
                request: httpx.Request = httpx.Request('GET', url, params=params)
                if sort_field == 'date_added desc':
                    return httpx.Response(400, request=request, json={'message': 'bad sort field'})
                return httpx.Response(
                    200,
                    request=request,
                    json={
                        'response': {
                            'docs': [
                                {
                                    'pid': 'bdr:123',
                                    'primary_title': 'Example',
                                    'object_created_dsi': '2026-03-31T14:10:00Z',
                                    'rel_is_member_of_collection_ssim': ['bdr:theses'],
                                }
                            ]
                        }
                    },
                )

        client = DummyClient()
        http_call_count: dict[str, int] = {'count': 0}

        sort_field_used, docs = choose_sort_field_and_docs(client, 1, http_call_count)  # type: ignore[arg-type]

        self.assertEqual(sort_field_used, 'object_created_dtsi desc')
        self.assertEqual(len(docs), 1)
        self.assertEqual(client.calls[:2], ['date_added desc', 'object_created_dtsi desc'])
        self.assertEqual(http_call_count['count'], 2)


if __name__ == '__main__':
    unittest.main()
