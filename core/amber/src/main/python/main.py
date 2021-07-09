import sys
import time

import pandas
import random
from loguru import logger
from typing import Iterable, Union

from core import Tuple
from core.data_processor import DataProcessor
from core.models.tuple import InputExhausted
from core.udf import UDFOperator
from edu.uci.ics.amber.engine.common import LinkIdentity


class EchoOperator(UDFOperator):
    def process_texera_tuple(self, tuple_: Union[Tuple, InputExhausted], link: LinkIdentity) -> Iterable[Tuple]:
        if isinstance(tuple_, Tuple):
            # time.sleep(0.01)
            yield tuple_


class TrainOperator(UDFOperator):
    def __init__(self):
        super(TrainOperator, self).__init__()
        self.records = list()

    def train(self) -> "model":
        return {"predict": True, "f1-score":  random.random()}

    def process_texera_tuple(self, tuple_: Union[Tuple, InputExhausted], link: LinkIdentity) -> Iterable[Tuple]:
        if isinstance(tuple_, Tuple):
            self.records.append(tuple_)
            time.sleep(0.01)
        elif isinstance(tuple_, InputExhausted):
            model = self.train()
            yield Tuple.from_series(pandas.concat([self.records[-1].as_series(), pandas.Series(model)]))


if __name__ == '__main__':
    data_processor = DataProcessor(host="localhost", input_port=int(sys.argv[1]), output_port=int(sys.argv[2]),
                                   udf_operator=EchoOperator())
    data_processor.start()
    data_processor.join()
