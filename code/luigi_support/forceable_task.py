import luigi
import os
import logging
import shutil
import inspect
import logging

class ForceableTask(luigi.Task):
    force = luigi.BoolParameter(significant=False, default=False,description="Force running of the task by removing its outputs")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.force is True:
            self.remove_outputs()
            
    def remove_outputs(self):
        if hasattr(self,'outputs'):
            for out in self.outputs:
                if out.exists():
                    if out.isdir():
                        shutil.rmtree(out.path)
                    else:
                        os.remove(out.path)

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