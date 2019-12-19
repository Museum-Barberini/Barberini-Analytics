import unittest

# DOCUMENTATION: This is kind of 💩💩💩
# Don't like it, don't keep it, if you can.
# This should be very easy by using unittest.mock the right way, but I failed ...
# I also failed at manually modifying the object's dicts.
# So the current solution just creates output files instead of mocking anything.

class TaskTest(unittest.TestCase):
	def isolate(self, task):
		task.complete = lambda: True
		return task
