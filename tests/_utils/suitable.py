"""
suitable is a generic extension of unittest. It provides hooks to specify a
custom TestSuite which implements testUpSuite() and tearDownSuite().
"""
from contextlib import contextmanager
import unittest
import sys


class PluggableTestProgram(unittest.TestProgram):
    """
    A command-line TestProgram that can be configured with a custom TestSuite
    class and hooks for handling the test result. All existing tests will be
    wrapped into an instance of self.testSuiteClass.
    """

    def __init__(self, **kwargs):
        self.testSuiteClass = kwargs.pop('testSuiteClass', self.testSuiteClass)
        super().__init__(**kwargs)

    testSuiteClass = unittest.TestSuite

    def handleUnsuccessfulResult(self, result):
        """
        Hook method for handling an unsuccessful test result.
        """

    def handleResult(self, result):
        if not result.wasSuccessful():
            self.handleUnsuccessfulResult(result)

    def runTests(self):
        self.test = self.testSuiteClass([self.test])
        with self._basicRunTests():
            return_value = super().runTests()
            self.handleResult(self.result)
        return return_value

    @contextmanager
    def _basicRunTests(self):
        """
        Run tests like super does. Meanwhile, disable exit to ensure further
        operations can be executed after the tests have failed.
        """
        _exit = self.exit
        self.exit = False

        yield

        self.exit = _exit
        if self.exit:
            sys.exit(not self.result.wasSuccessful())


class FixtureTestSuite(unittest.TestSuite):
    """
    A TestSuite that can be configured with setUpSuite() and tearDownSuite()
    methods in order to manage suite-wide fixtures.
    """

    def __init__(self, tests):
        super().__init__(tests)
        self._cleanups = []

    def addCleanup(self, function, *args, **kwargs):
        """
        Add a function, with arguments, to be called when the test suite is
        completed. Functions added are called on a LIFO basis and are called
        after tearDownSuite on test failure or success.

        Cleanup items are called even if setUp fails (unlike tearDown).
        """
        self._cleanups.append((function, args, kwargs))

    def doCleanups(self):
        while self._cleanups:
            function, args, kwargs = self._cleanups.pop(-1)
            function(*args, **kwargs)

    def run(self, result, debug=False):
        try:
            self.setUpSuite()
            try:
                return super().run(result, debug)
            finally:
                self.tearDownSuite()
        finally:
            self.doCleanups()

    def setUpSuite(self) -> None:
        """
        Hook method for setting up suite fixture before running tests in the
        suite.
        """
        pass

    def tearDownSuite(self) -> None:
        """
        Hook method for deconstructing the suite fixture after running all
        tests in the suite.
        """
        pass


def _main(
        testSuiteClass=FixtureTestSuite,
        testProgramClass=PluggableTestProgram):
    """
    Main entry point of the suitable module to run the tests.
    """
    unittest.__unittest = True
    testProgramClass(module=None, testSuiteClass=testSuiteClass)
