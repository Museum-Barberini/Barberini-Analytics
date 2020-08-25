"""
Provides analysis tasks to make predict the number of museum visitors.

6 prediction sets are made, predictions for the next 1/7/30 days and sample
predictions of the last 1/7/30 days. All sets are stored in the same relation.

Developed by Georg Tennigkeit as part of his Bachelor Thesis in 2020.
See https://gitlab.hpi.de/georg.tennigkeit/ba-visitor-prediction.
"""

from .predict import PredictionsToDb


__all__ = [PredictionsToDb]
