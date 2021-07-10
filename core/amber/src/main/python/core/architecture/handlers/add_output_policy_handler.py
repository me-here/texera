from edu.uci.ics.amber.engine.architecture.worker import AddOutputPolicy
from .handler_base import Handler
from ..manager.context import Context


class AddOutputPolicyHandler(Handler):
    cmd = AddOutputPolicy

    def __call__(self, context: Context, command: AddOutputPolicy, *args, **kwargs):
        context.tuple_to_batch_converter.add_policy(command.policy)
        return None
