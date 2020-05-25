import luigi
from stop_words import get_stop_words
import pickle
import pandas as pd
import numpy as np
from gsdmm import MovieGroupProcess
from nltk.tokenize import word_tokenize, MWETokenizer
import nltk
from collections import defaultdict
import langdetect
import logging
from luigi.format import UTF8

from apple_appstore import AppstoreReviewsToDB
from csv_to_db import CsvToDb
from data_preparation import DataPreparationTask
from db_connector import db_connector
from google_maps import GoogleMapsReviewsToDB
from gplay.gplay_reviews import GooglePlaystoreReviewsToDB
from twitter import TweetsToDB

logger = logging.getLogger('luigi-interface')

# TODO: add migration script

"""
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
"""


class TopicModeling(luigi.WrapperTask):

    def requires(self):
        yield TopicModelingTextsToDb()
        yield TopicModelingTopicsToDb()


class TopicModelingTextsToDb(CsvToDb):

    table = "topic_modeling_texts"

    def requires(self):
        return TopicModelingTextDf()


class TopicModelingTopicsToDb(CsvToDb):

    table = "topic_modeling_topics"

    def requires(self):
        return TopicModelingTopicsDf()


class TopicModelingTopicsDf(DataPreparationTask):
    """
    This task's main purpose is to provide a
    single output target that can be used a
    CsvToDb task.
    The the task also deletes existing data to 
    ensure consistency.
    """

    def requires(self):
        return TopicModelingFindTopics()

    def output(self):
        return luigi.LocalTarget(
            f"{self.output_dir}/topic_modeling/topics_2.csv",
            format=UTF8
        )

    def run(self):

        with list(self.input())[0].open("r") as topics_file:
            data = pd.read_csv(topics_file)
        with self.output().open("w") as output_file:
            data.to_csv(output_file, index=True)

        # delete existing topics data to make sure
        # there are no inconsistencies after the new
        # topics data is inserted.
        db_connector().execute(
            "TRUNCATE topic_modeling_topics"
        )

class TopicModelingTextDf(DataPreparationTask):
    """
    This task's only purpose is to provide a
    single output target that can be used a
    CsvToDb task.
    The the task also deletes existing data to 
    ensure consistency.
    """

    def requires(self):
        return TopicModelingFindTopics()

    def output(self):
        return luigi.LocalTarget(
            f"{self.output_dir}/topic_modeling/text_2.csv",
            format=UTF8
        )

    def run(self):
        
        with list(self.input())[1].open("r") as text_file:
            data = pd.read_csv(text_file)
        with self.output().open("w") as output_file:
            data.to_csv(output_file, index=True)

        # delete existing topics data to make sure
        # there are no inconsistencies after the new
        # topics data is inserted.
        db_connector().execute(
            "TRUNCATE topic_modeling_texts"
        )


class TopicModelingFindTopics(DataPreparationTask):
   
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_words = [
            *get_stop_words("german"),
            *get_stop_words("english"),
            *["http", "https", "www", "com", "de", "google", "translated", 
              "twitter", "fur", "uber", "html", "barberini", 
              "museumbarberini", "museum", "ausstellung", "ausstellungen",
              "potsdam", "mal"]
        ]

    def requires(self):
        return TopicModelingPreprocessCorpus()

    def output(self):
        yield luigi.LocalTarget(
            f"{self.output_dir}/topic_modeling/topics.csv",
            format=UTF8
        )
        yield luigi.LocalTarget(
            f"{self.output_dir}/topic_modeling/texts.csv",
            format=UTF8
        )

    def run(self):

        with self.input().open("rb") as corpus_file:
            corpus = pickle.load(corpus_file)

        terms_df, texts_df = self.find_topics(corpus)

        output_files = self.output()
        with next(output_files).open("w") as terms_file:
            terms_df.to_csv(terms_file, index=False)
        with next(output_files).open("w") as texts_file:
            texts_df.to_csv(texts_file, index=False)


    def find_topics(self, docs):
        # TODO: don't hardcode the years
        # Maybe one task per model
        models = ["all", "2020", "2019", "2018", "2017", "2016"]

        text_dfs = []
        topic_dfs = []

        for model_name in models:
            docs_in_timespan = [
                doc for doc in docs if doc.in_year(model_name)]

            if model_name == "all":
                model = self.train_mgp(docs_in_timespan, K=12)
            else:
                model = self.train_mgp(docs_in_timespan, K=10)

            for doc in docs_in_timespan:
                doc.predict(model, model_name)

            # cols: text,source,post_date,topic,model_name
            text_df = pd.DataFrame([doc.to_dict() for doc in docs_in_timespan])

            # topic,term,count,model
            out = []
            for i, topic_terms in enumerate(self.top_terms(model)):
                for term in topic_terms:
                    out.append({
                        "topic": i,
                        "term": term[0],
                        "count": term[1],
                        "model": model_name
                    })
            topic_df = pd.DataFrame(out)

            # name topics
            get_title = lambda x: self.top_terms(model)[x][0][0]
            topic_df["topic"] = topic_df["topic"].apply(get_title)
            text_df["topic"] = text_df["topic"].apply(get_title)

            # Small topics are bundled in topic "other".
            # A topic is considered small if it contains less than 2 percent 
            # of all text comments.
            total_size = len(text_df)
            topic_sizes = text_df["topic"].value_counts().to_dict()
            validate_topic = lambda x: x if topic_sizes[x] >= 0.02 * total_size else "other"
            topic_df["topic"] = topic_df["topic"].apply(validate_topic)
            text_df["topic"] = text_df["topic"].apply(validate_topic)

            text_dfs.append(text_df)
            topic_dfs.append(topic_df)

        return pd.concat(topic_dfs), pd.concat(text_dfs)


    def train_mgp(self, docs, K=10, alpha=0.1, beta=0.1, n_iters=30):
        vocab = set(x for doc in docs for x in doc.tokens)
        n_terms = len(vocab)

        mgp = MovieGroupProcess(K=K, alpha=alpha, beta=beta, n_iters=n_iters)
        mgp.fit([doc.tokens for doc in docs], n_terms)
        return mgp

    def top_terms(self, model, n=20):
        top_terms = [
            sorted(list(doc_dist.items()), key = lambda x: x[1], reverse=True)[:n]
            for doc_dist in model.cluster_word_distribution
        ]
        return top_terms


class TopicModelingPreprocessCorpus(DataPreparationTask):
   
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_words = [
            *get_stop_words("german"),
            *get_stop_words("english"),
            *["http", "https", "www", "com", "de", "google", "translated", 
              "twitter", "fur", "uber", "html", "barberini", 
              "museumbarberini", "museum", "ausstellung", "ausstellungen",
              "potsdam", "mal"]
        ]

    def requires(self):
        return TopicModelingCreateCorpus()

    def output(self):
        return luigi.LocalTarget(
            f"{self.output_dir}/topic_modeling/corpus_preprocessed.pkl",
            format=luigi.format.Nop  # output is binary file
        )

    def run(self):

        with self.input().open("rb") as corpus_file:
            corpus = pickle.load(corpus_file)

        corpus = self.preprocess(corpus)

        with self.output().open("w") as output_file:
            pickle.dump(corpus, output_file)

    def preprocess(self, docs):
        
        for doc in docs:
            # remove leading "None" (introduced by DB export)
            doc.text = doc.text.replace("None ", "", 1)
            doc.tokens = doc.text.lower()
            doc.guess_language()
            doc.tokens = word_tokenize(doc.tokens)
            # remove stop words
            doc.tokens = [token for token in doc.tokens if token not in self.stop_words]
            # keep only alphabetical tokens
            doc.tokens = [token for token in doc.tokens if token.isalpha()]
            # remove single-digit tokens
            doc.tokens = [token for token in doc.tokens if len(token) > 1]

        # consider only german docs
        docs = [doc for doc in docs if doc.language == "de"]

        # stemming
        #stemmer = nltk.stem.cistem.Cistem()
        #for doc in docs:
        #    doc.tokens = [stemmer.stem(token) for token in doc.tokens]
        
        # remove tokens that appear only once
        tokens = defaultdict(lambda: 0)
        for doc in docs:
            for token in doc.tokens:
                tokens[token] += 1
        for doc in docs:
            for token in doc.tokens:
                if tokens[token] == 1:
                    doc.tokens.remove(token)
                    
        # remove docs with less than three tokens
        docs = [doc for doc in docs if not doc.too_short()]
                
        return docs


class TopicModelingCreateCorpus(DataPreparationTask):

    def requires(self):
        yield AppstoreReviewsToDB()
        yield GoogleMapsReviewsToDB()
        yield GooglePlaystoreReviewsToDB()
        yield TweetsToDB()

    def output(self):
        return luigi.LocalTarget(
            f"{self.output_dir}/topic_modeling/corpus.pkl",
            format=luigi.format.Nop  # output is binary file
        )

    def run(self):

        texts = db_connector().query("""
            SELECT text, source, post_date, post_id 
            FROM post
            WHERE NOT is_from_museum AND text IS NOT NULL
        """)
        corpus = [
            Doc(row[0], row[1], row[2], row[3])
            for row in texts if row[0] is not None
        ]

        with self.output().open("w") as output_file:
            pickle.dump(corpus, output_file)


class Doc:
    def __init__(self, text, source, post_date, post_id):
        self.text = text
        self.source = source
        self.post_date = post_date
        self.post_id = post_id
        self.tokens = None
        self.topic = None
        self.model_name = None
        self.language = None

    def in_year(self, year):
        if year == "all":
            return True
        return str(self.post_date.year) == str(year)

    def too_short(self):
        return len(self.tokens) <= 2

    def predict(self, model, model_name):
        self.topic = model.choose_best_label(self.tokens)[0]
        self.model_name = model_name

    def guess_language(self):
        try:
            self.language = langdetect.detect(self.text)
        except langdetect.lang_detect_exception.LangDetectException as e:
            # langdetect can not handle emoji-only and link-only texts
            logger.warning(f"langdetect failed for one Doc. Error: {e}")
            logger.info(f"Failure happened for Doc {self.to_dict()}")

    def to_dict(self):
        return {
            "post_id": self.post_id,
            "text": self.text,
            "source": self.source,
            "post_date": self.post_date,
            "topic": self.topic,
            "model_name": self.model_name
        }
