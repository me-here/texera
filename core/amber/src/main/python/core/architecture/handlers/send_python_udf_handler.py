from importlib import util

import inspect

from edu.uci.ics.amber.engine.architecture.worker import SendPythonUdf
from .handler_base import Handler
from ..managers.context import Context
from ...udf import UDFOperator


class SendPythonUdfHandler(Handler):
    cmd = SendPythonUdf

    def __call__(self, context: Context, command: SendPythonUdf, *args, **kwargs):
        spec = util.spec_from_loader('udf_module', loader=None)
        udf_module = util.module_from_spec(spec)
        exec(command.udf, udf_module.__dict__)

        operators = list(filter(lambda v: inspect.isclass(v)
                                          and issubclass(v, UDFOperator)
                                          and not inspect.isabstract(v),
                                udf_module.__dict__.values()))

        assert len(operators) == 1, "There should be only one UDFOperator defined"
        context.dp._udf_is_source = command.is_source
        context.dp._udf_operator = operators[0]()
        context.dp._udf_operator.open()
        return None
