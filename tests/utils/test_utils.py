from db_test import DatabaseTestCase

from _utils import utils


class TestUtils(DatabaseTestCase):
    """Tests (at least some) utils."""

    def test_strcoord_basic(self):
        string = "foo\nbarr\nbaz\r\rspam\neggs"
        for index, char in enumerate(string):
            row, col = utils.strcoord(string, index)
            self.assertGreater(row, 0)
            self.assertLessEqual(row, 6)
            self.assertGreater(col, 0)
            self.assertLessEqual(col, 5)
            self.assertEqual(
                char,
                string.splitlines(keepends=True)[row - 1][col - 1],
                (index, char)
            )

    def test_strcoord_edge_cases(self):
        with self.assertRaises(IndexError):
            utils.strcoord("", 0)
        with self.assertRaises(IndexError):
            utils.strcoord("foo", -1)
        with self.assertRaises(IndexError):
            utils.strcoord("foo", 4)
