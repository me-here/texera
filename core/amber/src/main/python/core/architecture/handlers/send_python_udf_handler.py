from loguru import logger

from edu.uci.ics.amber.engine.architecture.worker import SendPythonUdf
from .handler_base import Handler
from ..managers.context import Context


class SendPythonUdfHandler(Handler):
    cmd = SendPythonUdf

    def __call__(self, context: Context, command: SendPythonUdf, *args, **kwargs):
        # context.udf = command.udf
        # logger.info(" python received udf: \n" + command.udf)
        return None
