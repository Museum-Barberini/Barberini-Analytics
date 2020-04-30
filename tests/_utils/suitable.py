import unittest


class DatabaseTestProgram(unittest.TestProgram):
    """
    A command-line TestProgram that can be configured with a custom TestSuite
    class. All existing tests will be wrapped into an instance of
    self.testSuiteClass.
    """

    def __init__(self, **kwargs):
        self.testSuiteClass = kwargs.pop('testSuiteClass', self.testSuiteClass)
        super().__init__(**kwargs)

    testSuiteClass = unittest.TestSuite

    def runTests(self):
        self.test = self.testSuiteClass([self.test])
        return super().runTests()


class FixtureTestSuite(unittest.TestSuite):
    """
    A TestSuite that can be configured with setUpSuite() and tearDownSuite()
    methods in order to manage suite-wide fixtures.
    """

    def run(self, result, debug=False):
        self.setUpSuite()
        try:
            return super().run(result, debug)
        finally:
            self.tearDownSuite()

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
