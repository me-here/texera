from edu.uci.ics.amber.engine.architecture.worker import AddOutputPolicy
from .handler import Handler
from ..manager.context import Context


class AddOutputPolicyHandler(Handler):
    def __call__(self, context: Context, command: AddOutputPolicy, *args, **kwargs):
        context.tuple_to_batch_converter.add_policy(command.policy)
        return None


"""
command = get_oneof(payload.command)
            if isinstance(command, AddOutputPolicy):
                logger.info("it's AddOutputPolicy")
                self._tuple_to_batch_converter.add_policy(command.policy)
            elif isinstance(command, UpdateInputLinking):
                logger.info("it's UpdateInputLinking")
                # self.stateManager.assertState(Ready)
                self._batch_to_tuple_converter.register_input(command.identifier, command.input_link)
            else:
                logger.info(f"it's other control command: {command}")

"""
