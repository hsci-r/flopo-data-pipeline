#!/usr/bin/env python3
"""flopo-data-pipeline 'Makefile'
"""

import glob
import logging
import re
import shutil

import luigi
from luigi_support.forceable_task import ForceableTask
from plumbum import FG, local

logging.basicConfig(level=logging.INFO)


def log_and_execute(cmd):
    logging.info("Executing %s.", cmd)
    cmd & FG


class PrepareForTurkuNLP(ForceableTask):
    dataset = luigi.Parameter()
    split = luigi.IntParameter()

    def output(self):
        return luigi.LocalTarget(f'data/processed/for-turkunlp/{self.dataset}')

    def run_internal(self):
        shutil.rmtree(self.output().path, ignore_errors=True)
        log_and_execute(
            local[f"./code/prepare-{self.dataset}-for-turkunlp.py"]['-i', f'data/input/{self.dataset}', '-o', self.output().path, '-s', self.split])


class TurkuNLPChunk(ForceableTask):
    dataset = luigi.Parameter()
    chunk = luigi.Parameter()
    container_system = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f'data/processed/conll/{self.dataset}/chunk-{self.chunk}.conll')

    def run_internal(self):
        self.output().makedirs()
        if self.container_system == 'singularity':
            container_command = local['singularity']['run',
                                                     'docker://hsci/turku-neural-parser-openshift:latest']
        else:
            container_command = local['docker']['run', '-i',
                                                'hsci/turku-neural-parser-openshift:latest']
        log_and_execute(
            (container_command < f'data/processed/for-turkunlp/{self.dataset}/chunk-{self.chunk}.txt') > self.output().path)


class TurkuNLP(ForceableTask):
    dataset = luigi.Parameter()
    container_system = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def complete(self):
        return self.done

    def run_internal(self):
        tasks = []
        for source in glob.glob(f'data/processed/for-turkunlp/{self.dataset}/chunk-*.txt'):
            chunk = re.match(r".*chunk-(.+)\.txt", source).group(1)
            tasks.append(TurkuNLPChunk(dataset=self.dataset,
                         container_system=self.container_system, chunk=chunk))
        yield tasks
        self.done = True


class CONLLToCSV(ForceableTask):
    dataset = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f'data/processed/conll-csv/{self.dataset}/{self.dataset}-conll.csv')

    def run_internal(self):
        self.output().makedirs()
        log_and_execute(local['flopo-convert']['-f', 'conll', '-t', 'csv', '-r',
                                               '-i', f'data/processed/conll/{self.dataset}', '-o', self.output().path])


class Pipeline(ForceableTask):
    dataset = luigi.Parameter(description='Dataset to process')
    split = luigi.IntParameter(
        default=200000, description='Number of articles to put in a single file')
    container_system = luigi.Parameter(default='docker', description='Container system to use')
    done = False

    def complete(self):
        return self.done

    def run_internal(self):
        yield PrepareForTurkuNLP(dataset=self.dataset, split=self.split)
        yield TurkuNLP(dataset=self.dataset, container_system=self.container_system)
        yield CONLLToCSV(dataset=self.dataset)
        self.done = True


if __name__ == '__main__':
    luigi.run()
