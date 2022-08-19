import io
import logging
import os
from packaging import version as versions
from shutil import rmtree
import sys
import tarfile
from urllib.request import urlopen

import spacy
from spacy.cli.download import run_command
import luigi
from luigi.format import UTF8

from _utils import DataPreparationTask

logger = logging.getLogger('luigi-interface')


class FetchSpacyModel(DataPreparationTask):

    name = luigi.Parameter()

    # TODO: Introduce an official place for cross-container caches
    cache_dir = 'secret_files/absa/spacy_models'

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/absa/spacy_models/{self.name}.file',
            format=UTF8
        )

    def run(self):

        dirpath = os.path.join(self.cache_dir, f'{self.name}')
        latest_version = self.get_latest_version()

        model_path = os.path.join(
            dirpath,
            self.name,
            f'{self.name}-{latest_version}'
        )
        cached_version = self.get_cached_version(model_path)
        logger.info(f"Cached version: {self.name}-{cached_version}")
        logger.info(f"Latest version: {self.name}-{latest_version}")

        if not cached_version < latest_version:
            logger.info("Latest version is already cached.")
        else:
            logger.info('Downloading newer version ...')

            try:
                rmtree(dirpath)
            except FileNotFoundError:
                pass

            url = (f'https://github.com/explosion/spacy-models/releases/'
                   f'download/{self.name}-{latest_version}/'
                   f'{self.name}-{latest_version}.tar.gz')

            logger.info(f"Downloading and extracting {url} ...")
            response = urlopen(url)
            with tarfile.open(
                name=None,
                fileobj=io.BytesIO(response.read())
            ) as tar:
                parent = f'{self.name}-{latest_version}/'
                members = [
                    tarinfo for tarinfo in tar.getmembers()
                    if tarinfo.name.startswith(parent)
                ]
                for member in members:
                    member.path = member.path[len(parent):]
                tar.extractall(path=dirpath, members=members)

        logger.info(f"Installing {self.name}-{latest_version} ...")
        run_command(['bash', '-c', ' && '.join(map(' '.join, [
            ['cd', dirpath],
            [sys.executable, 'setup.py', 'egg_info'],
            [sys.executable, '-m',
                'pip', 'install', '-r', '*.egg-info/requires.txt']
        ]))])

        with self.output().open('w') as output:
            output.write(model_path)

    def get_latest_version(self):

        from spacy.cli.download import get_compatibility, get_version
        version = get_version(self.name, get_compatibility())
        return versions.parse(version)

    def get_cached_version(self, model_path):

        try:
            model = spacy.load(model_path, disable=['tagger', 'parser', 'ner'])
            version = model.meta['version']
        except OSError:
            return versions.NegativeInfinity
        return versions.parse(version)
