from core.architecture.handlers.handler import Handler
from core.architecture.manager.context import Context
from edu.uci.ics.amber.engine.architecture.worker import UpdateInputLinking


class UpdateInputLinkingHandler(Handler):
    def __call__(self, context: Context, command: UpdateInputLinking, *args, **kwargs):
        context.batch_to_tuple_converter.register_input(command.identifier, command.input_link)
        return None
