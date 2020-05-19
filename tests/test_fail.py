import unittest

from db_test import DatabaseTestCase


class TestFail(DatabaseTestCase):

    def test_fail(self):
        self.fail("Failing for test")

    def test_error(self):
        raise Exception("Error for test")

    def test_pass(self):
        pass

    @unittest.expectedFailure
    def test_pass_unexpected(self):
        pass
