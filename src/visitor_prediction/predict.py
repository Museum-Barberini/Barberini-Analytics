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


N_NEIGHBORS = 5
SEQUENCE_LENGTH = 1


class PredictNext1Day(CsvToDb):
    table = 'next_1_day_prediction'

    def requires(self):
        return PredictVisitors(days_to_predict=1)


class PredictVisitors(DataPreparationTask):

    days_to_predict = luigi.parameter.IntParameter(default=7)
    sample_prediction = luigi.parameter.BoolParameter(default=False)

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
            f'{"_sample" if sample_prediction else ""}'
            f'{days_to_predict}_days_ahead.csv',
            format=luigi.format.UTF8)

    def run(self):
        with self.input()[0].path.open('r') as entries_file:
            train_entries = pd.read_csv(
                entries_file,
                parse_dates=['date'],
                index_col='date'
            )
        train_entries.sort_index(inplace=True, ascending=True)
        with self.input()[1].path.open('r') as exhibitions_file:
            exhibitions = pd.read_csv(
                exhibitions_file,
                parse_dates=['start_date', 'end_date']
            )


        train_entries = train_entries.iloc[:days_to_predict].copy()
        last_date = train_entries.index.max()

        # --- preprocess ---
        train_entries = preprocess_entries(
            train_entries,
            exhibitions)

        to_be_rescaled = [
            'entries',
            'exhibition_popularity',
            'exhibition_progress']

        scaler_dict = dict()
        for col in to_be_rescaled:
            scaler = MinMaxScaler()
            train_entries[[col]] = scaler.fit_transform(train_entries[[col]])
            scaler_dict[col] = scaler

        # add last SEQUENCE_LENGTH entries to each row
        for i in range(1, SEQUENCE_LENGTH):
            train_entries[f'e-{i}'] = train_entries['entries'].shift(periods=i)

        # --- train ---
        feature_columns = [col for col in train_entries.columns
                           if col not in ['entries']]

        model = KNeighborsRegressor(n_neighbors=N_NEIGHBORS)
        model = model.fit(
            train_entries.filter(feature_columns),
            train_entries['entries'])

        # --- prepare prediction ---
        predicted_entries = pd.DataFrame(
            index=pd.date_range(
                start=last_date + dt.timedelta(days=1),
                periods=days_to_predict))

        predicted_entries = preprocess_entries(predicted_entries, exhibitions)
        #TODO: scale together with everything else