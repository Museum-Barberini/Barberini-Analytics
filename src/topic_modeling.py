"""
Provides tasks for assigning semantic topics to each posts in the database.

Flow of control
--------------

            TopicModeling()
            |             |
            |             |
TopicModelingTextsToDb   TopicModelingTopicsToDb
            |             |
            |             |
TopicModelingTextsDf     TopicModelingTopicsDf
            |             |
            |             |
        TopicModelingFindTopics()
                  |
                  |
      TopicModelingPreprocessCorpus()
                  |
                  |
       TopicModelingCreateCorpus()

Minimal Mode
------------
The topic modeling does not need to be adapted for the
minimal mode. Other tasks only fetch few posts in minimal
runs. Therefore the topic modeling is also very fast in
the minimal runs.
This only applies if the minimal run uses a fresh test database.
"""

from collections import defaultdict
import logging
import pickle

from gsdmm import MovieGroupProcess
import langdetect
import luigi
from luigi.format import UTF8
from nltk.tokenize import word_tokenize
import pandas as pd
from stop_words import get_stop_words

from _utils import CsvToDb, DataPreparationTask, StreamToLogger, logger
from _posts import PostsToDb


class TopicModeling(luigi.WrapperTask):
    """Run all topic modeling tasks."""

    def requires(self):
        yield TopicModelingTextsToDb()
        yield TopicModelingTopicsToDb()


class TopicModelingTextsToDb(CsvToDb):
    """
    Store all text comments associated to a topic into the database.

    The table topic_modeling.topic_text assigns one topic to each text comment
    for each model (if the text comment is withing the timespan the model was
    constructed for).
    """

    table = 'topic_modeling.topic_text'

    replace_content = True

    def requires(self):

        return TopicModelingTextDf()


class TopicModelingTopicsToDb(CsvToDb):
    """
    Store all identified topics into the database.

    The table topic_modeling.topic contains the most frequent terms for each
    topic and each model.
    """

    table = 'topic_modeling.topic'

    replace_content = True

    def requires(self):

        return TopicModelingTopicsDf()


class TopicModelingTopicsDf(DataPreparationTask):
    """
    Helper task to provide a single output target for the downstream task.

    TODO: Suspicious. Do we need it?
    """

    def requires(self):
        return TopicModelingFindTopics()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/topic_modeling/topics_2.csv',
            format=UTF8
        )

    def run(self):

        with self.input().open('r') as topics_file:
            data = pd.read_csv(topics_file)
        with self.output().open('w') as output_file:
            data.to_csv(output_file, index=True)


class TopicModelingTextDf(DataPreparationTask):
    """
    Helper task to provide a single output target for the downstream task.

    TODO: Suspicious. Do we need it?
    """

    def requires(self):
        return TopicModelingFindTopics()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/topic_modeling/text_2.csv',
            format=UTF8
        )

    def run(self):

        with self.input().open('r') as text_file:
            data = pd.read_csv(text_file)
        with self.output().open('w') as output_file:
            data.to_csv(output_file, index=True)


class TopicModelingFindTopics(DataPreparationTask):
    """
    Train a series of topic models and generate predictions.

    For each year we train one model. We use the model to
    generate predictions only for the posts in the year
    the model was trained for. There is also one model that
    takes into account all posts.

    The algorithm we use is the 'Gibbs Sampling algorithm for the
    Dirichlet Multinomial Mixture model' (GSDMM). This algorithms
    is designed specifically for short text topic modeling. Link to the paper:
    http://dbgroup.cs.tsinghua.edu.cn/wangjy/papers/KDD14-GSDMM.pdf
    """

    def requires(self):
        return TopicModelingPreprocessCorpus()

    def output(self):
        yield luigi.LocalTarget(
            f'{self.output_dir}/topic_modeling/topics.csv',
            format=UTF8
        )
        yield luigi.LocalTarget(
            f'{self.output_dir}/topic_modeling/texts.csv',
            format=UTF8
        )

    def run(self):

        with self.input().open('rb') as corpus_file:
            corpus = pickle.load(corpus_file)

        terms_df, texts_df = self.find_topics(corpus)

        output_files = self.output()
        with next(output_files).open('w') as terms_file:
            terms_df.to_csv(terms_file, index=False)
        with next(output_files).open('w') as texts_file:
            texts_df.to_csv(texts_file, index=False)

    def find_topics(self, docs):
        # one model per year
        models = {'all'}
        for doc in docs:
            models.add(str(doc.post_date.year))

        # accumulate topic predictions and important terms
        # for the different models in these variables.
        text_dfs = []
        topic_dfs = []

        for model_name in models:
            docs_in_timespan = [
                doc for doc in docs if doc.in_year(model_name)
            ]

            # allow for more topics if all posts are used
            model = self.train_mgp(
                docs_in_timespan,
                K=12 if model_name else 10
            )

            for doc in docs_in_timespan:
                doc.predict(model, model_name)

            # cols: text,source,post_date,topic,model_name
            text_df = pd.DataFrame([doc.to_dict() for doc in docs_in_timespan])

            # cols: topic,term,count,model
            out = []
            for i, topic_terms in enumerate(self.top_terms(model)):
                for term in topic_terms:
                    out.append({
                        'topic': i,
                        'term': term[0],
                        'count': term[1],
                        'model': model_name
                    })
            topic_df = pd.DataFrame(out)

            # name topics
            topic_df['topic'] = topic_df['topic'].apply(
                lambda x: self.top_terms(model)[x][0][0])
            text_df['topic'] = text_df['topic'].apply(
                lambda x: self.top_terms(model)[x][0][0])

            text_dfs.append(text_df)
            topic_dfs.append(topic_df)

        return pd.concat(topic_dfs), pd.concat(text_dfs)

    def train_mgp(
            self,
            docs,
            K=10,  # noqa: N803
            alpha=0.1, beta=0.1,
            n_iters=30
            ):
        vocab = set(x for doc in docs for x in doc.tokens)
        n_terms = len(vocab)

        mgp = MovieGroupProcess(K=K, alpha=alpha, beta=beta, n_iters=n_iters)
        with StreamToLogger(log_level=logging.DEBUG).activate():
            mgp.fit([doc.tokens for doc in docs], n_terms)
        return mgp

    def top_terms(self, model, n=20):
        top_terms = []
        for doc_distribution in model.cluster_word_distribution:
            terms = list(doc_distribution.items())
            terms = sorted(terms, key=lambda x: x[1], reverse=True)
            terms = terms[:n]
            top_terms.append(terms)
        return top_terms


class TopicModelingPreprocessCorpus(DataPreparationTask):
    """
    Preprocess a corpus of Doc instances including several steps.

    - lowercasing
    - tokenization
    - removing single-character tokens and non-alphabetic tokens
    - stop word removal
    - removing non-german Docs
    - discarding Docs with less than three tokens
    - removing tokens that appear only once in the entire corpus
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_words = [
            *get_stop_words('german'),
            *get_stop_words('english'),
            *['http', 'https', 'www', 'com', 'de', 'google', 'translated',
              'twitter', 'fur', 'uber', 'html', 'barberini',
              'museumbarberini', 'museum', 'ausstellung', 'ausstellungen',
              'potsdam', 'mal']
        ]

    def requires(self):
        return TopicModelingCreateCorpus()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/topic_modeling/corpus_preprocessed.pkl',
            format=luigi.format.Nop  # output is binary file
        )

    def run(self):

        with self.input().open('rb') as input_corpus:
            corpus = pickle.load(input_corpus)

        corpus = self.preprocess(corpus)

        with self.output().open('w') as output_corpus:
            pickle.dump(corpus, output_corpus)

    def preprocess(self, docs):

        # remove leading 'None' (introduced by DB export)
        for doc in docs:
            doc.text = doc.text.replace('None ', '', 1)

        # consider only german docs
        docs = [doc for doc in docs if doc.guess_language() == 'de']

        for doc in docs:
            doc.tokens = doc.text.lower()
            doc.tokens = word_tokenize(doc.tokens)
            # remove stop words
            doc.tokens = [token for token in doc.tokens
                          if token not in self.stop_words]
            # keep only alphabetical tokens
            doc.tokens = [token for token in doc.tokens
                          if token.isalpha()]
            # remove single-digit tokens
            doc.tokens = [token for token in doc.tokens
                          if len(token) > 1]

        # remove tokens that appear only once
        tokens = defaultdict(lambda: 0)
        for doc in docs:
            for token in doc.tokens:
                tokens[token] += 1
        for doc in docs:
            for token in doc.tokens:
                if tokens[token] == 1:
                    doc.tokens.remove(token)

        # remove very short docs
        docs = [doc for doc in docs if not doc.too_short()]

        return docs


class TopicModelingCreateCorpus(DataPreparationTask):
    """
    Create a corpus of posts, i.e. a collection of Docs.

    The corpus consists of all non-empty posts from the post view that were
    not authored by the museum.
    """

    def requires(self):
        yield PostsToDb()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/topic_modeling/corpus.pkl',
            format=luigi.format.Nop  # output is binary file
        )

    def run(self):

        texts = self.db_connector.query('''
            SELECT text, source, post_date, post_id
            FROM post
            WHERE NOT is_from_museum AND text IS NOT NULL
        ''')
        corpus = [
            Doc(row[0], row[1], row[2], row[3])
            for row in texts if row[0] is not None
        ]

        with self.output().open('w') as output_file:
            pickle.dump(corpus, output_file)


class Doc:
    """
    Represents an individual text document.

    In our case, a text document is a user post from a public platform.
    """

    def __init__(self, text, source=None, post_date=None,
                 post_id=None, tokens=None):
        self.text = text
        self.source = source
        self.post_date = post_date
        self.post_id = post_id
        self.tokens = tokens
        self.topic = None
        self.model_name = None
        self.language = None

    def in_year(self, year):
        if year == 'all':
            return True
        return str(self.post_date.year) == str(year)

    def too_short(self):
        return len(self.tokens) <= 2

    def predict(self, model, model_name):
        self.topic = model.choose_best_label(self.tokens)[0]
        self.model_name = model_name

    def guess_language(self):
        try:
            return langdetect.detect(self.text)
        except langdetect.lang_detect_exception.LangDetectException as e:
            # langdetect can not handle emoji-only and link-only texts
            logger.debug(f'langdetect failed for one Doc. Error: {e}')
            logger.debug(f'Failure happened for Doc {self.to_dict()}')

    def to_dict(self):
        return {
            'post_id': self.post_id,
            'text': self.text,
            'source': self.source,
            'post_date': self.post_date,
            'topic': self.topic,
            'model_name': self.model_name
        }
