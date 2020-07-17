from data_preparation import DataPreparationTask
from query_db import QueryDb
import datetime as dt
import pandas as pd
import luigi

from gomus.exhibitions import ExhibitionsToDb
from facebook import FbPostsToDb, FbPostPerformanceToDb


class ExhibitionPopularity(DataPreparationTask):

    def _requires(self):
        return luigi.task.flatten([
            ExhibitionsToDb(),
            FbPostsToDb(),
            FbPostPerformanceToDb(),
            super()._requires()
        ])

    def requires(self):
        yield QueryDb(  # exhibitions
            query='''
                SELECT *
                FROM exhibition NATURAL JOIN exhibition_time
            '''
        )
        yield QueryDb(  # facebook posts
            query='''
                SELECT *
                FROM fb_post_rich
            '''
        )

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/visitor_prediction/exhib_with_popularity.csv',
            format=luigi.format.UTF8)

    def run(self):
        # load data
        with self.input()[0].open('r') as exhibitions_file:
            exhibitions = pd.read_csv(
                exhibitions_file,
                parse_dates=['start_date', 'end_date'],
                keep_default_na=False
            )
        with self.input()[1].open('r') as posts_file:
            posts = pd.read_csv(
                posts_file,
                parse_dates=['post_date']
            )

        # match posts with announced exhibitions
        def find_related_exhib(post):
            mentioned_exhibitions = []
            for exhib in exhibitions.itertuples():
                if exhib.special:
                    continue

                first_title_half = exhib.title.split('.')[0]
                simple_text = simplify_text(str(post['text']))
                simple_title = simplify_text(first_title_half)
                if simple_title in simple_text and \
                    exhib.start_date - dt.timedelta(days=360) \
                        < post['post_date'] < exhib.start_date:
                    mentioned_exhibitions.append(exhib.title)
            if len(mentioned_exhibitions) == 1:  # avoid ambiguity
                return mentioned_exhibitions[0]
            else:
                return ''
        posts['announces'] = posts.apply(find_related_exhib, axis=1)

        announcing_posts = posts[posts['announces'] != '']

        # calculate popularity per exhibition
        popul_per_exhib = announcing_posts.filter(
            ['announces', 'likes']).groupby(['announces']).max()
        average_max_likes = popul_per_exhib['likes'].mean()

        # assign to exhibitions
        for exhibition in exhibitions.itertuples():
            try:
                popularity = popul_per_exhib.loc[exhibition.title]['likes']
            except KeyError:
                popularity = average_max_likes
            exhibitions.loc[
                exhibitions['title'] == exhibition.title, 'popularity'
                ] = popularity

        with self.output().open('w') as output_file:
            exhibitions.to_csv(output_file, index=False, header=True)


def simplify_text(text):
    return ''.join(s for s in text if s.isalnum()).lower()
