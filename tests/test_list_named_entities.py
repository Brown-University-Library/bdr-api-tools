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
        cleaned_entities: list = [
            ('Egypt', 'GPE'),
            ('Barca', 'PRODUCT'),
            ('Africa From', 'LOC'),
            ('Cyrene', 'PERSON'),
            ('Egypt', 'GPE'),
        ]
        processor: Processor = Processor()
        processor.cleaned_entities = cleaned_entities
        processor.make_uniques()
        computed: list = processor.sorted_unique_entries
        expected: list = [
            (('Africa From', 'LOC'), 1),
            (('Barca', 'PRODUCT'), 1),
            (('Cyrene', 'PERSON'), 1),
            (('Egypt', 'GPE'), 2),
        ]
        self.assertEqual(computed, expected)


if __name__ == '__main__':
    unittest.main()


#  ('Egypt', 'GPE'),
#  ('Barca', 'PRODUCT'),
#  ('Africa\n From', 'LOC'),
#  ('Cyrene', 'PERSON'),
