import sys
import time

import contextlib
import pandas
import random
from loguru import logger
from overrides import overrides
from typing import Iterator, Union

from core import Tuple
from core.data_processor import DataProcessor
from core.models.tuple import InputExhausted
from core.udf import UDFOperator
from edu.uci.ics.amber.engine.common import LinkIdentity

new_level = logger.level("PRINT", no=38)


class StreamToLogger(object):
    """
    This class is used to redirect `print` to loguru's logger, instead of stdout.
    """

    def __init__(self, level=new_level):
        self._level = level

    def write(self, buffer):
        for line in buffer.rstrip().splitlines():
            logger.opt(depth=1).log("PRINT", line.rstrip())

    def flush(self):
        pass


class EchoOperator(UDFOperator):
    @overrides
    def process_texera_tuple(self, tuple_: Union[Tuple, InputExhausted], input_: LinkIdentity) -> Iterator[Tuple]:
        if isinstance(tuple_, Tuple):
            # time.sleep(0.1)
            yield tuple_


class TrainOperator(UDFOperator):
    def __init__(self):
        super(TrainOperator, self).__init__()
        self.records = list()

    def train(self) -> "model":
        return {"predict": True, "f1-score": random.random()}

    @overrides
    def process_texera_tuple(self, tuple_: Union[Tuple, InputExhausted], input_: LinkIdentity) -> Iterator[Tuple]:
        if isinstance(tuple_, Tuple):
            self.records.append(tuple_)
            time.sleep(0.01)
        elif isinstance(tuple_, InputExhausted):
            model = self.train()
            yield pandas.concat([self.records[-1], pandas.Series(model)])


if __name__ == '__main__':
    # redirect user's print into logger
    with contextlib.redirect_stdout(StreamToLogger()):
        data_processor = DataProcessor(host="localhost", input_port=int(sys.argv[1]), output_port=int(sys.argv[2]),
                                       udf_operator=EchoOperator())
        data_processor.start()
        data_processor.join()
