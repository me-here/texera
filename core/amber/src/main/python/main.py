import sys
from typing import Iterable, Union

from loguru import logger

from worker import Tuple
from worker.data_processor import DataProcessor
from worker.models.tuple import InputExhausted
from worker.udf import UDFOperator


class EchoOperator(UDFOperator):
    def process_texera_tuple(self, row: Union[Tuple, InputExhausted], nth_child: int = 0) -> Iterable[Tuple]:
        logger.debug("processing one row")
        return [row]


if __name__ == '__main__':
    data_processor = DataProcessor(host="localhost", input_port=int(sys.argv[1]), output_port=int(sys.argv[2]),
                                   udf_operator=EchoOperator())

    data_processor.start()

    data_processor.join()

    logger.info("main finished")
