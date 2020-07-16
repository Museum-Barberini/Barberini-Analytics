from _utils.data_preparation import DataPreparationTask
from _utils.query_db import QueryDb
import datetime as dt
import pandas as pd
import luigi


class ExhibitionPopularity(DataPreparationTask):

    def requires(self): # TODO: _require related ToDb
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
        with open('raw_exhibitions.csv', encoding='utf-8') as exhibitions_file:
            exhibitions = pd.read_csv(
                exhibitions_file,
                parse_dates=['start_date', 'end_date']
            )
        with open('fb_post.csv', encoding='utf-8') as posts_file:
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

                if '.' in exhib.title:
                    first_title_half = exhib.title.split('.')[0]
                else:
                    first_title_half = exhib.title
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
