import pandas as pd
import spacy
from spacy.tokens import Doc, Token

from data_preparation import DataPreparationTask
from posts import PostsToDb


"""
NEXT STEPS:
- read pattern definitions (see opinion_patterns.jsonc)
- one task that matches patterns and outputs aspect-sentiment-pairs
- one task that reads aspect-sentiment-pairs, matches sentiment weights and groups them by aspect
	- where to put dbscan?
- Download model (python3 -m spacy download de_core_news_sm)

- rename aspect_sentiment to opinion globally?
"""

class CollectPostOpinions(DataPreparationTask):

	def requires(self):

		return PostsToDb()

	def run(self):

		with self.input().open() as input:
			df = pd.read_csv(input)

		nlp = spacy.load('de_core_news_sm')

		df['doc'] = df['text'].apply(nlp)
		df['pos_tags'] = df['doc'].apply(lambda doc: [token.pos_ for token in doc])

