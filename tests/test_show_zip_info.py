import unittest

from show_zip_info import ext_from_path, parse_item_zip_info


class TestExtFromPath(unittest.TestCase):
    """
    Tests file extension extraction behavior.
    """

    def test_returns_lowercase_extension_or_noext(self) -> None:
        """
        Checks extension normalization for regular paths and paths without an extension.
        """
        self.assertEqual(ext_from_path('folder/FILE.PDF'), 'pdf')
        self.assertEqual(ext_from_path('__MACOSX/.DS_Store'), 'ds_store')
        self.assertEqual(ext_from_path('folder/README'), 'noext')


class TestParseItemZipInfo(unittest.TestCase):
    """
    Tests item zip parsing behavior.
    """

    def test_builds_expected_structure_for_item_with_children(self) -> None:
        """
        Checks that parent and child zip data are summarized into the expected output structure.
        """

        def fetcher(child_pid: str) -> dict[str, object]:
            child_data: dict[str, object] = {
                'bdr:child-1': {
                    'pid': 'bdr:child-1',
                    'primary_title': 'Child One',
                    'zip_filelist_ssim': ['child-folder/file.TXT', 'child-folder/image.png'],
                },
                'bdr:child-2': {
                    'pid': 'bdr:child-2',
                    'primary_title': 'Child Two',
                    'zip_filelist_ssim': ['nested/archive.tar.gz'],
                },
            }
            return child_data[child_pid]

        item_json: dict[str, object] = {
            'pid': 'bdr:parent-1',
            'primary_title': 'Parent Title',
            'zip_filelist_ssim': ['top-level/README.md', 'top-level/scan.PDF'],
            'hasPart': [
                {'pid': 'bdr:child-1'},
                {'pid': 'bdr:child-2'},
            ],
        }

        result: dict[str, object] = parse_item_zip_info(item_json, fetcher)

        item_info: dict[str, object] = result['item_info']
        self.assertEqual(item_info['pid'], 'bdr:parent-1')
        self.assertEqual(item_info['primary_title'], 'Parent Title')
        self.assertEqual(item_info['item_zip_info'], ['top-level/README.md', 'top-level/scan.PDF'])
        self.assertEqual(item_info['item_zip_filetype_summary'], {'md': 1, 'pdf': 1})
        self.assertEqual(
            item_info['has_parts_zip_info'],
            [
                {
                    'child_pid': 'bdr:child-1',
                    'primary_title': 'Child One',
                    'child_zip_info': ['child-folder/file.TXT', 'child-folder/image.png'],
                    'child_zip_filetype_summary': {'png': 1, 'txt': 1},
                },
                {
                    'child_pid': 'bdr:child-2',
                    'primary_title': 'Child Two',
                    'child_zip_info': ['nested/archive.tar.gz'],
                    'child_zip_filetype_summary': {'gz': 1},
                },
            ],
        )
        self.assertEqual(item_info['overall_zip_filetype_summary'], {'gz': 1, 'md': 1, 'pdf': 1, 'png': 1, 'txt': 1})

        meta: dict[str, object] = result['_meta_']
        self.assertEqual(meta['full_item_api_url'], 'https://repository.library.brown.edu/api/items/bdr:parent-1/')
        self.assertEqual(meta['item_pid'], 'bdr:parent-1')
        self.assertIsInstance(meta['timestamp'], str)

    def test_returns_empty_child_and_summary_lists_when_no_zip_data_exists(self) -> None:
        """
        Checks that an item without zip data produces empty zip collections and summaries.
        """

        def fetcher(child_pid: str) -> dict[str, object]:
            raise AssertionError(f'fetcher should not be called for {child_pid}')

        item_json: dict[str, object] = {
            'pid': 'bdr:empty',
            'primary_title': 'No Zip Data',
        }

        result: dict[str, object] = parse_item_zip_info(item_json, fetcher)

        item_info: dict[str, object] = result['item_info']
        self.assertEqual(item_info['item_zip_info'], [])
        self.assertEqual(item_info['item_zip_filetype_summary'], {})
        self.assertEqual(item_info['has_parts_zip_info'], [])
        self.assertEqual(item_info['overall_zip_filetype_summary'], {})


if __name__ == '__main__':
    unittest.main()
