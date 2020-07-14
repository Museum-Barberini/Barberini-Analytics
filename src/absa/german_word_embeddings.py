import logging
from urllib.request import urlopen

import luigi
from luigi.format import Nop

from data_preparation import DataPreparationTask

logger = logging.getLogger('luigi-interface')


class FetchGermanWordEmbeddings(DataPreparationTask):

    url = 'http://cloud.devmount.de/d2bc5672c523b086/german.model'

    def output(self):

        # This file is about about 600 MB large, thus cache it in secret_files
        return luigi.LocalTarget(
            'secret_files/absa/german_word_embeddings.model',
            format=Nop
        )

    def run(self):

        logger.info("Downloading german_word_embeddings")
        response = urlopen(self.url)

        logger.info("Writing german_word_embeddings to file")
        with self.output().open('wb') as output:
            while True:
                data = response.read(4096)
                if data:
                    output.write(data)
                else:
                    break

        logger.info("Fetching of german_word_embeddings completed.")
