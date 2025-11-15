import unittest

from list_named_entities import Processor


class TestProcessor(unittest.TestCase):
    """
    Tests the Processor class.
    """

    def test_clean_entities(self) -> None:
        """
        Checks that newlines and whitespace are stripped from entity text.
        """
        original_entities: list = [
            ('Egypt', 'GPE'),
            ('Barca ', 'PRODUCT'),
            ('Africa\n From', 'LOC'),
            ('Cyrene', 'PERSON'),
            ('Egypt', 'GPE'),
        ]
        processor: Processor = Processor(original_entities)
        processor.clean_entities()
        computed: list = processor.cleaned_entities
        expected: list = [
            ('Egypt', 'GPE'),
            ('Barca', 'PRODUCT'),
            ('Africa From', 'LOC'),
            ('Cyrene', 'PERSON'),
            ('Egypt', 'GPE'),
        ]
        self.assertEqual(computed, expected)

    def test_make_uniques(self) -> None:
        """
        Checks that the make_uniques() returns an alphabetized list of unique entities, with counts.
        """
        cleaned_entities_lst: list = [
            ('Egypt', 'GPE'),
            ('Barca', 'PRODUCT'),
            ('Africa From', 'LOC'),
            ('Cyrene', 'PERSON'),
            ('Egypt', 'GPE'),
        ]
        processor: Processor = Processor()
        processor.cleaned_entities = cleaned_entities_lst
        processor.make_uniques()
        computed: list = processor.sorted_unique_entries
        expected: list = [
            (('Africa From', 'LOC'), 1),
            (('Barca', 'PRODUCT'), 1),
            (('Cyrene', 'PERSON'), 1),
            (('Egypt', 'GPE'), 2),
        ]
        self.assertEqual(computed, expected)

    def test_group_by_entity(self) -> None:
        """
        Checks that the group_by_entity() returns results grouped by entity type.
        """
        sorted_unique_entries_input: list = [
            (('Africa From', 'LOC'), 1),
            (('Barca', 'PRODUCT'), 1),
            (('Cyrene', 'PERSON'), 1),
            (('Egypt', 'GPE'), 2),
            (('Tunisia', 'GPE'), 1),
        ]
        processor: Processor = Processor()
        processor.sorted_unique_entries = sorted_unique_entries_input
        processor.group_by_entity()
        computed: dict = processor.by_entity_display
        expected: dict = {
            'GPE': {'Egypt': 2, 'Tunisia': 1},
            'LOC': {'Africa From': 1},
            'PERSON': {'Cyrene': 1},
            'PRODUCT': {'Barca': 1},
        }
        self.assertEqual(computed, expected)

    def test_determine_top_x(self) -> None:
        """
        Checks that the determine_top_x() returns results grouped by entity type -- only for the top-x results.
        """
        by_entity_display_input: dict = {
            'GPE': {'Algeria': 1, 'Egypt': 2, 'Tunisia': 1},
            'LOC': {
                'Opt-1': 1,
                'Opt-2': 9,
                'Opt-3': 2,
                'Opt-4': 10,
                'Opt-5': 1,
                'Opt-6': 2,
            },
            'PERSON': {'Cyrene': 1},
            'PRODUCT': {'Barca': 1},
        }
        processor: Processor = Processor()
        processor.by_entity_display = by_entity_display_input
        cut_off: int = 3
        processor.determine_top_x(cut_off)
        computed: dict = processor.by_top_x_display
        expected: dict = {
            'GPE': [
                ('2', ['Egypt']),
                ('1', ['Algeria', 'Tunisia']),
            ],
            'LOC': [
                ('10', ['Opt-4']),
                ('9', ['Opt-2']),
                ('2', ['Opt-3', 'Opt-6']),
            ],
            'PERSON': [
                ('1', 'Cyrene'),
            ],
            'PRODUCT': [
                ('1', 'Barca'),
            ],
        }
        self.assertEqual(computed, expected)


if __name__ == '__main__':
    unittest.main()


#  ('Egypt', 'GPE'),
#  ('Barca', 'PRODUCT'),
#  ('Africa\n From', 'LOC'),
#  ('Cyrene', 'PERSON'),
