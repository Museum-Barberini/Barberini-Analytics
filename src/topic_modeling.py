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
from data_preparation_task import DataPreparationTask
from google_maps import GoogleMapsReviewsToDB
from gplay.gplay_reviews import GooglePlaystoreReviewsToDB
from twitter import TweetsToDB

logger = logging.getLogger('luigi-interface')


class Doc:
    def __init__(self, text, source, post_date):
        self.text = text
        self.source = source
        self.post_date = post_date
        self.tokens = None
        self.topic = None
        self.model_name = None
        self.language = None

    def in_year(self, year):
        if year == "all":
            return True
        return year in self.post_date

    def too_short(self):
        return len(self.tokens) <= 2

    def predict(self, model, model_name):
        self.topic = model.choose_best_label(self.tokens)[0]
        self.model_name = model_name
    
    def guess_language(self):
        try:
            self.language = langdetect.detect(self.text)
        except langdetect.LangDetectException as e:
            # langdetect does not handle emojis well
            logger.warning(e)
            logger.info(f"Warning was raised for doc {self.to_dict()}")

    def to_dict(self):
        return {
            "text": self.text,
            "source": self.source,
            "post_date": self.post_date,
            "topic": self.topic,
            "model_name": self.model_name
        }


class TopicModeling(DataPreparationTask):
   
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_words = [
            *get_stop_words("german"),
            *get_stop_words("english"),
            *["http", "https", "www", "com", "de", "google", "translated", 
              "twitter", "fur", "uber", "html", "barberini", 
              "museumbarberini", "museum", "ausstellung", "ausstellungen",
              "potsdam"]
        ]

    def requires(self):
        yield AppstoreReviewsToDB()
        yield GoogleMapsReviewsToDB()
        yield GooglePlaystoreReviewsToDB()
        yield TweetsToDB()

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

        docs = self.build_corpus()
        docs = self.preprocess(docs)
        topic_df, text_df = self.find_topics(docs)

        with next(self.output()).open("w") as topic_file:
            topic_df.to_csv(topic_file, index=False)
        with next(self.output()).open("w") as text_file:
            text_df.to_csv(text_file, index=False)

    def preprocess(self, docs):
        
        for doc in docs:
            # remove leading "None" (introduced by DB export)
            doc.text = doc.text.replace("None ", "", 1)
            doc.tokens = doc.text.lower()
            doc.guess_language()
            doc.tokens = word_tokenize(doc.tokens)
            doc.tokens = [token for token in doc.tokens if token not in stop_words]
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
    
    def find_topics(self, docs):
        # TODO: don't hardcode the years
        models = ["all", "2020", "2019", "2018", "2017", "2016"]

        text_dfs = []
        topic_dfs = []

        for model_name in models:
            data_in_timespan = [
                doc for doc in relevant_docs if doc.in_year(model_name)]
            model = self.train_mgp(data_in_timespan)

            for doc in docs:
                doc.predict(model, model_name)

            # cols: source,post_date,topic,model
            text_df = pd.DataFrame([doc.to_dict() for doc in docs])

            # topic,term,count,model
            out = []
            for i, topic_terms in enumerate(top_terms(model)):
                for term in topic_terms:
                    out.append({
                        "topic": i,
                        "term": term[0],
                        "count": term[1],
                        "model": model_name
                    })
            topic_df = pd.DataFrame(out)

            text_dfs.append(text_df)
            topic_dfs.append(topic_df)

            # TODO: name topics

        return pd.concat(topic_dfs), pd.concat(text_dfs)


    def train_mgp(docs, K=10, alpha=0.1, beta=0.1, n_iters=10):
        vocab = set(x for doc in docs for x in doc.tokens)
        n_terms = len(vocab)

        mgp = MovieGroupProcess(K=K, alpha=alpha, beta=beta, n_iters=n_iters)
        mgp.fit([doc.tokens for doc in docs], n_terms)
        return mgp

    def top_terms(model, n=20, print_it=False):
        top_terms = [
            sorted(list(doc_dist.items()), key = lambda x: x[1], reverse=True)[:n]
            for doc_dist in model.cluster_word_distribution
        ]
        return top_terms
