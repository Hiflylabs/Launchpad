import pandas as pd
import abc
from nkmfraud.core import log
import time
from sqlalchemy import create_engine
from pdb import set_trace


class Pipe(metaclass=abc.ABCMeta):
    """Superclass to create Pipes.

    A Pipeline consists of one or more Pipes, but the Pipeline is a subclass of Pipe itself.

    Every Pipe subclass has a self.logger attribute, which should be used for logging to the
    log file defined by "logname", with a Pipe name, defined by "name".

    Args:
        name (str): The name of the Pipe, will be used in the log files, so make it unique.
        logname(str): The name of the logfile, where logs will be saved to.

    Examples:
        class SamplePipe(Pipe):
            def run(self, df, col, constant):
                df[col] = df[col]+constant
                return df
    """

    def __init__(self, name, logname):

        # This logger must be the same as the pipeline logger
        self.logger = log.create_logger(name=name, logname=logname)
        self.logname = logname

    @staticmethod
    def timeit(method):
        """Decorator to measure execution time [s]. Use @Pipe.timeit on run(self) for implementation"""

        def timed(*args, **kwargs):
            ts = time.time()  # Start timer
            result = method(*args, **kwargs)  # Execute method
            te = time.time()  # Stop timer
            args[0].logger.debug('Execution time: {0:.3f} s'.format((te-ts)))
            return result
        return timed

    @abc.abstractmethod
    def run(self):
        """The method to be implemented by Pipe subclasses

        Currently, the run method has to configure a Pipe and run it too, so everything goes into one method.

        Examples:
            class SamplePipe(Pipe):
                def run(self, df, col, constant):
                    df[col] = df[col]+constant
                    return df

        """
