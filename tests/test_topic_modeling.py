from unittest import TestCase
from unittest.mock import MagicMock, patch
from datetime import datetime
import pickle
import luigi

from luigi.mock import MockTarget

from db_connector import db_connector
from db_test import DatabaseTestCase
from topic_modeling import *


class TestDoc(TestCase):

    def test_guess_language(self):

        doc = Doc('english text goes here')
        doc.guess_language()
        self.assertEqual(doc.language, 'en')

        doc = Doc('hier ist ein deutscher Text')
        doc.guess_language()
        self.assertEqual(doc.language, 'de')

        doc = Doc('https://blablabla.de')
        doc.guess_language()
        self.assertIsNone(doc.language)

    def test_too_short(self):

        doc = Doc('Text with many tokens')
        doc.tokens = ['Text', 'with', 'many', 'tokens']
        self.assertFalse(doc.too_short())

        doc = Doc('short')
        doc.tokens = ['short']
        self.assertTrue(doc.too_short())

    def test_in_year(self):

        doc = Doc('some text', post_date=datetime(2019, 8, 30, 0, 0))
        self.assertTrue(doc.in_year("2019"))
        self.assertFalse(doc.in_year("2020"))
        self.assertFalse(doc.in_year("2018"))
        self.assertTrue(doc.in_year("all"))


class TestCreateCorpus(DatabaseTestCase):

    @patch.object(TopicModelingCreateCorpus, 'output')
    def test_create_corpus(self, output_mock):
        output_target = MockTarget('corpus_out', format=luigi.format.Nop)
        output_mock.return_value = output_target

        db_connector().execute('''
            INSERT INTO tweet(user_id,tweet_id,text,response_to,post_date)
            VALUES (\'user_id\', \'tweet_id\', \'tweet text\', NULL, \'2020-05-24 10:56:21\')
        ''')
        db_connector().execute('''
            INSERT INTO fb_post_comment(post_id,comment_id,post_date,
                text,is_from_museum,response_to)
            VALUES ('post1','comment1','2020-05-24 10:56:21','text1',false,NULL),
                   ('post2','comment2','2018-05-24 10:56:21','text2',true,NULL)
        ''')

        task = TopicModelingCreateCorpus()
        task.requires = MagicMock(return_value=None)

        task.run()

        with output_target.open("r") as fp:
            corpus = pickle.load(fp)

        self.assertEqual(len(corpus), 2)
        self.assertIsInstance(corpus[0], Doc)
        self.assertIsInstance(corpus[1], Doc)


class TestPreprocessing(DatabaseTestCase):
    pass
