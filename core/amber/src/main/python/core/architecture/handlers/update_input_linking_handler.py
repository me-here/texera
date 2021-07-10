from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context
from edu.uci.ics.amber.engine.architecture.worker import UpdateInputLinking


class UpdateInputLinkingHandler(Handler):
    cmd = UpdateInputLinking

    def __call__(self, context: Context, command: UpdateInputLinking, *args, **kwargs):
        context.batch_to_tuple_converter.register_input(command.identifier, command.input_link)
        return None
