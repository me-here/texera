from core.udf import UDFOperator

code = """
import sys
import time

import pandas
import random
from overrides import overrides
from typing import Iterator, Union

from core import Tuple
from core.data_processor import DataProcessor
from core.models.tuple import InputExhausted
from core.udf import UDFOperator
from edu.uci.ics.amber.engine.common import LinkIdentity


class EchoOperator(UDFOperator):
    @overrides
    def process_texera_tuple(self, tuple_: Union[Tuple, InputExhausted], input_: LinkIdentity) -> Iterator[Tuple]:
        if isinstance(tuple_, Tuple):
            # time.sleep(0.1)
            yield tuple_
            yield tuple_
        
"""

import importlib.util
import inspect

spec = importlib.util.spec_from_loader('helper', loader=None)
helper = importlib.util.module_from_spec(spec)
exec(code, helper.__dict__)

if __name__ == '__main__':
    print(inspect.isabstract(UDFOperator))
    operators = list(filter(lambda v: inspect.isclass(v)
                                      and issubclass(v, UDFOperator)
                                      and not inspect.isabstract(v),
                            helper.__dict__.values()))
    print(operators)
    assert len(operators) == 1, "There should be only one UDFOperator defined"
    t = operators[0]()
    print(t)
