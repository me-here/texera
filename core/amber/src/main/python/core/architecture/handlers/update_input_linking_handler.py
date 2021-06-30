from core.architecture.handlers.handler import Handler


class UpdateInputLinkingHandler(Handler):
    def __call__(self, *args, **kwargs):
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
