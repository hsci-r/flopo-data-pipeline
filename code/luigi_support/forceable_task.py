import inspect
import logging
import os
import shutil
from abc import ABCMeta, abstractmethod

import luigi


class ForceableTask(luigi.Task):
    __metaclass__ = ABCMeta
    force = luigi.BoolParameter(significant=False, default=False,
                                description="Force running of the task by removing its outputs")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.force is True:
            self.remove_outputs()

    def remove_outputs(self):
        for out in self.output():
            if out.exists():
                if out.isdir():
                    shutil.rmtree(out.path)
                else:
                    os.remove(out.path)

    @abstractmethod
    def run_internal(self):
        raise NotImplementedError()

    def run(self):
        try:
            ret = self.run_internal()
            if inspect.isgenerator(ret):
                yield from ret
        except GeneratorExit:
            raise
        except:
            logging.exception("Encountered an exception while running task.")
            self.remove_outputs()
            raise
