import luigi
from _utils.data_preparation import DataPreparationTask
from _utils.csv_to_db import CsvToDb
from _utils.query_db import QueryDb
import datetime as dt
import pandas as pd

from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import MinMaxScaler

from visitor_prediction.exhibition_popularity import ExhibitionPopularity
from visitor_prediction.preprocessing import preprocess_entries
from gomus.daily_entries import DailyEntriesToDb


N_NEIGHBORS = 5
SEQUENCE_LENGTH = 1
# These parameters as well as conventions
# in training models were optimized as part of
# https://gitlab.hpi.de/georg.tennigkeit/ba-visitor-prediction


class PredictNext1Day(CsvToDb):
    table = 'next_1_day_prediction'

    def requires(self):
        return PredictVisitors(days_to_predict=1)


class PredictVisitors(DataPreparationTask):
    days_to_predict = luigi.parameter.IntParameter(default=7)
    sample_prediction = luigi.parameter.BoolParameter(
        default=False,
        description="if sample_prediction, predict the last"
        "days_to_predict days instead")

    # def _requires(self):
    #     return luigi.task.flatten([
    #         DailyEntriesToDb(),
    #         super()._requires()
    #     ])

    def requires(self):
        yield QueryDb(  # daily entries
            query='''
                SELECT DATE(datetime), SUM(unique_count) AS entries
                FROM gomus_daily_entry
                WHERE datetime > date('2017-01-01')
                GROUP BY DATE(datetime)
                ORDER BY DATE(datetime)
            '''
        )
        yield ExhibitionPopularity()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/visitor_prediction/prediction'
            f'{"_sample" if self.sample_prediction else ""}'
            f'{self.days_to_predict}_days_ahead.csv',
            format=luigi.format.UTF8)

    def run(self):
        with self.input()[0].open('r') as entries_file:
            all_entries = pd.read_csv(
                entries_file,
                parse_dates=['date'],
                index_col='date'
            )
        all_entries.sort_index(inplace=True, ascending=True)
        with self.input()[1].open('r') as exhibitions_file:
            exhibitions = pd.read_csv(
                exhibitions_file,
                parse_dates=['start_date', 'end_date'],
                keep_default_na=False
            )

        if self.sample_prediction:
            all_entries = all_entries.iloc[:-self.days_to_predict].copy()

        last_date = all_entries.index.max()

        # --- append dates to be predicted ---
        # --- so everything is preprocessed together ---
        to_be_predicted_entries = pd.DataFrame(
            index=pd.date_range(
                start=last_date + dt.timedelta(days=1),
                periods=self.days_to_predict),
            columns=['entries'])

        all_entries = all_entries.append(to_be_predicted_entries)

        # --- preprocess ---
        all_entries = preprocess_entries(
            all_entries,
            exhibitions)

        to_be_rescaled = [
            'entries',
            'exhibition_popularity',
            'exhibition_progress']

        scaler_dict = dict()
        for col in to_be_rescaled:
            scaler = MinMaxScaler()
            all_entries[[col]] = scaler.fit_transform(all_entries[[col]])
            scaler_dict[col] = scaler

        # --- separate into training set and to_be_predicted ---

        train_entries = all_entries[:-self.days_to_predict].copy()
        to_be_predicted_entries = all_entries[-self.days_to_predict:].copy()

        # add last SEQUENCE_LENGTH entries to each row
        # done after scaling to not rescale some e-X columns differently
        for i in range(1, SEQUENCE_LENGTH):
            train_entries[f'e-{i}'] = train_entries['entries'].shift(periods=i)

        # --- train ---
        feature_columns = [col for col in train_entries.columns
                           if col not in ['entries']]

        model = KNeighborsRegressor(n_neighbors=N_NEIGHBORS)
        model = model.fit(
            train_entries.filter(feature_columns),
            train_entries['entries'])

        previous_entries = list(train_entries['entries'].values)
        predictions = []
        for i in range(len(to_be_predicted_entries)):
            new_row = to_be_predicted_entries.iloc[[i]].copy()
            if new_row['is_closed'][0] == 1.0 or\
               new_row['weekday_1'][0] == 1.0:
                new_prediction = 0.0  # not predicting Tuesdays and closed days
            else:
                for j in range(1, SEQUENCE_LENGTH + 1):
                    new_row[f'e-{j}'] = previous_entries[-j]
                new_prediction = model.predict(
                    new_row.filter(feature_columns))[0]

            previous_entries.append(new_prediction)
            predictions.append(new_prediction)
        import numpy as np
        predicted_entries = pd.DataFrame(
            data=np.swapaxes([
                to_be_predicted_entries.index,
                predictions],
                0, 1),
            columns=['date', 'entries'])
        with self.output().open('w') as output_file:
            predicted_entries.to_csv(output_file, index=False, header=True)
