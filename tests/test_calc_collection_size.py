import unittest

from calc_collection_size import human_bytes


class TestHumanBytes(unittest.TestCase):
    """
    Tests the human_bytes function.
    """
    def test_bytes(self):
        """
        Checks the human_bytes function with byte values.
        """
        self.assertEqual(human_bytes(0), '0 B')
        self.assertEqual(human_bytes(1023), '1023 B')

    def test_kilobytes(self):
        """
        Checks the human_bytes function with kilobyte values.
        """
        self.assertEqual(human_bytes(1024), '1.00 KB')
        # Just under 1 MB still shows KB (rounded)
        self.assertEqual(human_bytes(1024 * 1024 - 1), '1024.00 KB')
        # Arbitrary
        self.assertEqual(human_bytes(1500), '1.46 KB')

    def test_megabytes(self):
        """
        Checks the human_bytes function with megabyte values.
        """
        self.assertEqual(human_bytes(1024**2), '1.00 MB')
        # Just under 1 GB still shows MB (rounded)
        self.assertEqual(human_bytes(1024**3 - 1), '1024.00 MB')
        # Arbitrary
        self.assertEqual(human_bytes(10_000_000), '9.54 MB')

    def test_gigabytes(self):
        """
        Checks the human_bytes function with gigabyte values.
        """
        self.assertEqual(human_bytes(1024**3), '1.00 GB')
        # Just under 1 TB still shows GB (rounded)
        self.assertEqual(human_bytes(1024**4 - 1), '1024.00 GB')
        # Arbitrary
        self.assertEqual(human_bytes(2_000_000_000), '1.86 GB')

    def test_terabytes_and_above(self):
        """
        Checks the human_bytes function with terabyte values.
        """
        self.assertEqual(human_bytes(1024**4), '1.00 TB')
        # Just under 1 PB still shows TB (rounded)
        self.assertEqual(human_bytes(1024**5 - 1), '1024.00 TB')
        # Arbitrary
        self.assertEqual(human_bytes(5_000_000_000_000), '4.55 TB')


if __name__ == '__main__':
    unittest.main()
