import typing
from overrides import overrides
from pampy import match
from typing import Iterator, Union

from core.architecture.managers.context import Context
from core.architecture.packaging.batch_to_tuple_converter import EndMarker, EndOfAllMarker
from core.architecture.sync_rpc.sync_rpc_server import SyncRPCServer
from core.models.internal_queue import ControlElement, InputDataElement, InternalQueue, OutputDataElement
from core.models.marker import SenderChangeMarker
from core.models.tuple import InputExhausted, Tuple
from core.udf.udf_operator import UDFOperator
from core.util.proto.proto_helper import get_one_of, set_one_of
from core.util.queue.queue_base import IQueue
from core.util.stoppable.stoppable_queue_blocking_thread import StoppableQueueBlockingThread
from edu.uci.ics.amber.engine.architecture.worker import ControlCommand, WorkerExecutionCompleted
from edu.uci.ics.amber.engine.common import ActorVirtualIdentity, Completed, ControlInvocation, ControlPayload, \
    ControllerVirtualIdentity, LinkIdentity, Ready, Running, Uninitialized


class DPThread(StoppableQueueBlockingThread):
    def __init__(self, input_queue: InternalQueue, output_queue: InternalQueue, udf_operator: UDFOperator):
        super().__init__(self.__class__.__name__, queue=input_queue)

        self._input_queue: InternalQueue = input_queue
        self._output_queue: InternalQueue = output_queue
        self._udf_operator = udf_operator
        self._current_input_tuple: Union[Tuple, InputExhausted]
        self._current_input_link: LinkIdentity

        self.context = Context(self)
        self._rpc_server = SyncRPCServer(output_queue, context=self.context)

    @overrides
    def pre_start(self) -> None:
        self._udf_operator.open()
        self.context.state_manager.assert_state(Uninitialized())
        self.context.state_manager.transit_to(Ready())

    @overrides
    def receive(self, next_entry: IQueue.QueueElement) -> None:

        # logger.info(f"PYTHON DP receive an entry from queue: {next_entry}")
        match(
            next_entry,
            InputDataElement, self._process_input_data_element,
            ControlElement, self._process_control_element,
        )

    def process_control_command(self, cmd: ControlPayload, from_: ActorVirtualIdentity):
        # logger.info(f"PYTHON DP processing one CONTROL: {cmd} from {from_}")

        match(
            (get_one_of(cmd), from_),
            typing.Tuple[ControlInvocation, ActorVirtualIdentity], self._process_control_invocation
            # TODO: handle ReturnPayload
        )

    def process_input_tuple(self):

        if isinstance(self._current_input_tuple, Tuple):
            self.context.statistics_manager.input_tuple_count += 1

        for result in self.process_tuple(self._current_input_tuple, self._current_input_link):
            self.context.statistics_manager.output_tuple_count += 1
            self.pass_tuple_to_downstream(result)

    def process_tuple(self, tuple_: Union[Tuple, InputExhausted], link: LinkIdentity) -> Iterator[Tuple]:
        return self._udf_operator.process_texera_tuple(tuple_, link)

    def pass_tuple_to_downstream(self, tuple_: Tuple) -> None:
        for to, batch in self.context.tuple_to_batch_converter.tuple_to_batch(tuple_):
            self._output_queue.put(OutputDataElement(payload=batch, to=to))

    def complete(self) -> None:
        self._udf_operator.close()
        self.context.state_manager.transit_to(Completed())
        control_command = set_one_of(ControlCommand, WorkerExecutionCompleted())
        payload = set_one_of(ControlPayload, ControlInvocation(1, command=control_command))
        from_ = set_one_of(ActorVirtualIdentity, ControllerVirtualIdentity())
        self._output_queue.put(ControlElement(from_=from_, cmd=payload))

    def check_and_process_control(self) -> None:

        while not self._input_queue.main_empty() or self.context.pause_manager.is_paused():
            next_entry = self.interruptible_get()

            match(
                next_entry,
                ControlElement, self._process_control_element
            )

    def _process_control_element(self, control_element: ControlElement) -> None:
        # logger.info(f"PYTHON DP receive a CONTROL: {next_entry}")
        self.process_control_command(control_element.cmd, control_element.from_)

    def _process_tuple(self, tuple_: Tuple) -> None:
        self._current_input_tuple = tuple_
        self.process_input_tuple()
        self.check_and_process_control()

    def _process_sender_change_marker(self, sender_change_marker: SenderChangeMarker) -> None:
        self._current_input_link = sender_change_marker.link

    def _process_end_marker(self, _: EndMarker) -> None:
        self._current_input_tuple = InputExhausted()
        self.process_input_tuple()
        self.check_and_process_control()

    def _process_end_of_all_marker(self, _: EndOfAllMarker) -> None:
        for to, batch in self.context.tuple_to_batch_converter.emit_end_of_upstream():
            self._output_queue.put(OutputDataElement(payload=batch, to=to))
            self.check_and_process_control()
        self.complete()

    def _process_input_data_element(self, input_data_element: InputDataElement) -> None:
        if self.context.state_manager.confirm_state(Ready()):
            self.context.state_manager.transit_to(Running())
        for element in self.context.batch_to_tuple_converter.process_data_payload(input_data_element.from_,
                                                                                  input_data_element.payload):
            match(
                element,
                Tuple, self._process_tuple,
                SenderChangeMarker, self._process_sender_change_marker,
                EndMarker, self._process_end_marker,
                EndOfAllMarker, self._process_end_of_all_marker
            )

    def _process_control_invocation(self, control_invocation: ControlInvocation, from_: ActorVirtualIdentity) -> None:
        self._rpc_server.receive(control_invocation=control_invocation, from_=from_)
