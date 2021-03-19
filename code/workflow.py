#!/usr/bin/env python3

import luigi
from plumbum import local, FG
from plumbum.cmd import docker
import glob
import re
import os
import logging
import shutil
from luigi_support.forceable_task import ForceableTask

logging.basicConfig(level=logging.INFO)

def logAndExecute(cmd):
    logging.info(f"Executing {cmd}.")
    cmd & FG

class PrepareForTurkuNLP(ForceableTask):
    dataset = luigi.Parameter()
    split = luigi.IntParameter()
    def output(self):
        return luigi.LocalTarget(f'data/processed/for-turkunlp/{self.dataset}')
    def run_internal(self):
        shutil.rmtree(self.output().path)
        logAndExecute(local[f"./code/prepare-{self.dataset}-for-turkunlp.py"]['-i',f'data/input/{self.dataset}','-o',self.output().path,'-s',self.split])

class TurkuNLPChunk(ForceableTask):
    dataset = luigi.Parameter()
    chunk = luigi.Parameter()
    def output(self):
        return luigi.LocalTarget(f'data/processed/conll/{self.dataset}/chunk-{self.chunk}.conll')
    def run_internal(self):
        self.output().makedirs()
        logAndExecute((docker['run','-i','hsci/turku-neural-parser-openshift:latest'] < f'data/processed/for-turkunlp/{self.dataset}/chunk-{self.chunk}.txt') > self.output().path)

class TurkuNLP(ForceableTask):
    dataset = luigi.Parameter()
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False
    def complete(self):
        return self.done
    def run_internal(self):
        tasks = []
        for source in glob.glob(f'data/processed/for-turkunlp/{self.dataset}/chunk-*.txt'):
            chunk = re.match(".*chunk-(\d+).txt",source).group(1)
            tasks.append(TurkuNLPChunk(dataset=self.dataset,chunk=chunk))
        yield tasks
        self.done = True

class CONLLToCSV(ForceableTask):
    dataset = luigi.Parameter()
    def output(self):
        return luigi.LocalTarget(f'data/processed/csv/{self.dataset}-conll.csv')
    def run_internal(self):
        logAndExecute(local['flopo-convert']['-f','conll','-t','csv','-r','-i',f'data/processed/conll/{self.dataset}','-o',self.output().path])

class Pipeline(ForceableTask):
    dataset = luigi.Parameter(description='Dataset to process')
    split = luigi.IntParameter(default=200000,description='Number of articles to put in a single file')
    done = False
    def complete(self):
        return self.done
    def run_internal(self):
        yield PrepareForTurkuNLP(dataset=self.dataset,split=self.split)
        yield TurkuNLP(dataset=self.dataset)
        yield CONLLToCSV(dataset=self.dataset)
        self.done = True

if __name__ == '__main__':
    luigi.run()