from unittest import TestCase
from unittest.mock import MagicMock, patch
from datetime import datetime
import pickle
import pandas as pd
import luigi
import sys, os

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

        # -------- SET UP MOCK DATA ------------
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

        # ------- RUN TASK UNDER TEST --------
        task = TopicModelingCreateCorpus()
        task.requires = MagicMock(return_value=None) # don't run any other tasks
        task.run()

        # ------- INSPECT OUTPUT -------
        with output_target.open("r") as fp:
            corpus = pickle.load(fp)

        self.assertEqual(len(corpus), 2)
        self.assertIsInstance(corpus[0], Doc)
        self.assertIsInstance(corpus[1], Doc)


class TestPreprocessing(TestCase):

    @patch.object(TopicModelingPreprocessCorpus, 'input')
    @patch.object(TopicModelingPreprocessCorpus, 'output')
    def test_preprocessing(self, output_mock, input_mock):

        # -------- SET UP MOCK DATA ------------
        output_target = MockTarget('corpus_out', format=luigi.format.Nop)
        input_target = MockTarget('corpus_in', format=luigi.format.Nop)
        output_mock.return_value = output_target
        input_mock.return_value = input_target
        with input_target.open('w') as fp:
            pickle.dump(
                [
                    Doc('Ich bin der erste Post über ein Kulturinstitut in der Landeshauptstadt'),
                    Doc('Ich bin der 2 Post über mit Bezug zur Landeshauptstadt. toll '),
                    Doc('Trallala noch ein Post 2 zum Museum'),
                    Doc('noch weitere Posts zum weitere testen. Barberini toll'),
                    Doc('this document is in english')
                ],
                fp
            )

        # ------- RUN TASK UNDER TEST --------
        task = TopicModelingPreprocessCorpus()
        task.run()

        # ------- INSPECT OUTPUT -------
        with output_target.open("r") as fp:
            output = pickle.load(fp)
        self.assertEqual(len(output), 2)
        self.assertEqual(output[0].tokens, ['post', 'landeshauptstadt', 'toll'])
        self.assertEqual(output[1].tokens, ['weitere', 'weitere', 'toll'])


class TestFindTopics(TestCase):

    @patch.object(TopicModelingFindTopics, 'output')
    @patch.object(TopicModelingPreprocessCorpus, 'output')
    def test_find_topics(self, input_mock, output_mock):

        # -------- SET UP MOCK DATA ------------
        input_target = MockTarget('corpus_in', format=luigi.format.Nop)
        output_target_topics = MockTarget('topics_out', format=luigi.format.UTF8)
        output_target_texts = MockTarget('texts_out', format=luigi.format.UTF8)
        input_mock.return_value = input_target
        output_mock.return_value = iter([output_target_topics, output_target_texts]) 

        with input_target.open('w') as fp:
            pickle.dump(
                [
                    Doc('text1', 'source1', datetime(2019, 8, 30, 0, 0), 'id1', ['A', 'A']),
                    Doc('text2', 'source1', datetime(2019, 8, 30, 0, 0), 'id1', ['B', 'B']),
                    Doc('text3', 'source1', datetime(2018, 8, 30, 0, 0), 'id1', ['A', 'A']),
                    Doc('text4', 'source1', datetime(2018, 8, 30, 0, 0), 'id1', ['C', 'C']),
                    Doc('text5', 'source2', datetime(2019, 8, 30, 0, 0), 'id1', ['A', 'A']),
                    Doc('text6', 'source2', datetime(2018, 8, 30, 0, 0), 'id1', ['B', 'B']),
                    Doc('text7', 'source2', datetime(2019, 8, 30, 0, 0), 'id1', ['B', 'B']),
                    Doc('text8', 'source3', datetime(2019, 8, 30, 0, 0), 'id1', ['C', 'C']),
                    Doc('text9', 'source3', datetime(2021, 8, 30, 0, 0), 'id1', ['A', 'A']),
                    Doc('text10', 'source3',datetime(2021, 8, 30, 0, 0), 'id1', ['B', 'B'])
                ],
                fp
            )

        # ------- RUN TASK UNDER TEST --------
        # suppress messages printed during training
        with open(os.devnull, 'w') as null_file:
            sys.stdout = null_file

            task = TopicModelingFindTopics()
            task.run()
        # stop suppressing messages
        sys.stdout = sys.__stdout__

        # ------- INSPECT OUTPUT -------
        # validate dataframe with topic predictions
        with output_target_texts.open('r') as fp:
            texts = pd.read_csv(fp)
        self.assertEqual(len(texts), 20)
        self.assertEqual(list(texts.columns),
            ['post_id', 'text', 'source', 'post_date', 'topic', 'model_name'])
        for model in ['all', '2018', '2019', '2021']:
            self.assertIn(model, list(texts['model_name']))
        
        # validate dataframe with term counts
        with output_target_topics.open('r') as fp:
            topics = pd.read_csv(fp)
        self.assertEqual(sum(topics['count']), 40)
        self.assertEqual(list(topics.columns),
            ['topic', 'term', 'count', 'model'])
        for model in ['all', '2018', '2019', '2021']:
            self.assertIn(model, list(topics['model']))

