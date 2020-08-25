"""
Provides analysis tasks to make predict the number of museum visitors.

6 prediction sets are made, predictions for the next 1/7/30 days and sample
predictions of the last 1/7/30 days. All sets are stored in the same relation.
"""

from .predict import PredictionsToDb


__all__ = [PredictionsToDb]
