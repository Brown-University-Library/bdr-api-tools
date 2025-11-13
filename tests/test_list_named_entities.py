import unittest

from list_named_entities import Processor


class TestProcessor(unittest.TestCase):
    """
    Tests the Processor class.
    """

    def test_clean_entitiesR(self) -> None:
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


if __name__ == '__main__':
    unittest.main()


#  ('Egypt', 'GPE'),
#  ('Barca', 'PRODUCT'),
#  ('Africa\n From', 'LOC'),
#  ('Cyrene', 'PERSON'),
