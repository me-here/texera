from queue import Queue
from typing import Iterable, Union

from core.architecture.manager.context import Context
from core.architecture.messaginglayer.batch_to_tuple_converter import EndMarker, EndOfAllMarker
from core.architecture.sync_rpc.sync_rpc_server import SyncRPCServer
from core.models.internal_queue import ControlElement, InputDataElement, OutputDataElement, InternalQueue
from core.models.tuple import ITuple, InputExhausted, Tuple
from core.udf.udf_operator import UDFOperator
from core.util.proto.proto_helper import get_oneof, set_oneof
from core.util.thread.stoppable_queue_blocking_thread import StoppableQueueBlockingThread
from edu.uci.ics.amber.engine.architecture.worker import WorkerExecutionCompleted, ControlCommand
from edu.uci.ics.amber.engine.common import ControlInvocation, ControlPayload, Uninitialized, Ready, Running, Completed, \
    ControllerVirtualIdentity
from edu.uci.ics.amber.engine.common import LinkIdentity, ActorVirtualIdentity


class DPThread(StoppableQueueBlockingThread):
    def __init__(self, input_queue: InternalQueue, output_queue: InternalQueue, udf_operator: UDFOperator):
        super().__init__(self.__class__.__name__, queue=input_queue)

        self._input_queue: InternalQueue = input_queue
        self._output_queue: InternalQueue = output_queue
        self._udf_operator = udf_operator
        self._current_input_tuple: Union[ITuple, InputExhausted] = None
        self._current_input_link: LinkIdentity = None

        self.context = Context(self)
        self._rpc_server = SyncRPCServer(output_queue, context=self.context)

        self._data_cache_queue = Queue()

    def before_loop(self) -> None:
        self._udf_operator.open()
        self.context.state_manager.assert_state(Uninitialized())
        self.context.state_manager.transit_to(Ready())

    def after_loop(self) -> None:
        pass

    def main_loop(self) -> None:

        next_entry = self.interruptible_get()
        # logger.info(f"PYTHON DP receive an entry from queue: {next_entry}")
        if isinstance(next_entry, InputDataElement):

            if self.context.state_manager.confirm_state(Ready()):
                self.context.state_manager.transit_to(Running())
            # logger.info(f"PYTHON DP receive a DATA: {next_entry}")
            for element in self.context.batch_to_tuple_converter.process_data_payload(next_entry.from_,
                                                                                      next_entry.payload):
                if isinstance(element, Tuple):
                    self._current_input_tuple = element
                    self.handle_input_tuple()
                    self.check_and_handle_control()
                    # TODO: add self.check_and_handle_control()

                elif isinstance(element, EndMarker):
                    self._current_input_tuple = InputExhausted()
                    self.handle_input_tuple()
                    self.check_and_handle_control()
                elif isinstance(element, EndOfAllMarker):
                    for to, batch in self.context.tuple_to_batch_converter.emit_end_of_upstream():
                        self._output_queue.put(OutputDataElement(payload=batch, to=to))
                        self.check_and_handle_control()
                    self.complete()
                # TODO: handle else

        elif isinstance(next_entry, ControlElement):
            # logger.info(f"PYTHON DP receive a CONTROL: {next_entry}")
            self.process_control_command(next_entry.cmd, next_entry.from_)
        else:
            raise TypeError(f"unknown InternalQueueElement {next_entry}")

    def process_control_command(self, cmd: ControlPayload, from_: ActorVirtualIdentity):
        # logger.info(f"PYTHON DP processing one CONTROL: {cmd} from {from_}")
        payload = get_oneof(cmd)
        if isinstance(payload, ControlInvocation):
            # logger.info(f"PYTHON DP processing one CONTROL INVOCATION: {payload} from {from_}")
            self._rpc_server.receive(control_invocation=payload, from_=from_)

        # TODO: handle ReturnPayload

        # TODO: handle else

    def handle_input_tuple(self):

        if isinstance(self._current_input_tuple, ITuple):
            self.context.statistics_manager.input_tuple_count += 1

        for result in self.process_tuple(self._current_input_tuple, self._current_input_link):
            self.context.statistics_manager.output_tuple_count += 1
            self.pass_tuple_to_downstream(result)

    def process_tuple(self, tuple_: Union[ITuple, InputExhausted], link: LinkIdentity) -> Iterable[ITuple]:
        return self._udf_operator.process_texera_tuple(tuple_, link)

    def pass_tuple_to_downstream(self, tuple_: ITuple):
        for to, batch in self.context.tuple_to_batch_converter.tuple_to_batch(tuple_):
            self._output_queue.put(OutputDataElement(payload=batch, to=to))

    def complete(self):
        self._udf_operator.close()
        self.context.state_manager.transit_to(Completed())
        control_command = set_oneof(ControlCommand, WorkerExecutionCompleted())
        payload = set_oneof(ControlPayload, ControlInvocation(1, command=control_command))
        from_ = set_oneof(ActorVirtualIdentity, ControllerVirtualIdentity())
        self._output_queue.put(ControlElement(from_=from_, cmd=payload))

    def check_and_handle_control(self):

        while not self._input_queue.master_empty():
            next_entry = self.interruptible_get()

            if isinstance(next_entry, ControlElement):
                # logger.info(f"PYTHON DP receive a CONTROL: {next_entry}")
                self.process_control_command(next_entry.cmd, next_entry.from_)
            else:
                raise TypeError("wrong type of message being handled")
