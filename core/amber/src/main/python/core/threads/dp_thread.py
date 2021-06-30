import typing
from core.architecture.messaginglayer.batch_to_tuple_converter import BatchToTupleConverter, EndMarker, EndOfAllMarker
from core.architecture.messaginglayer.tuple_to_batch_converter import TupleToBatchConverter
from core.architecture.sync_rpc.sync_rpc_server import SyncRPCServer
from core.models.internal_queue import ControlElement, InputDataElement, OutputDataElement, InternalQueue
from core.models.tuple import ITuple, InputExhausted, Tuple
from core.udf.udf_operator import UDFOperator
from core.util.proto_helper import get_oneof
from core.util.stoppable_queue_blocking_thread import StoppableQueueBlockingThread
from loguru import logger
from typing import Iterable, Union

from edu.uci.ics.amber.engine.common import ControlInvocation, ControlPayload
from edu.uci.ics.amber.engine.common import LinkIdentity, ActorVirtualIdentity


class BatchProducer:
    pass


class DPThread(StoppableQueueBlockingThread):
    def __init__(self, input_queue: InternalQueue, output_queue: InternalQueue, udf_operator: UDFOperator,
                 batch_to_tuple_converter: BatchToTupleConverter):
        super().__init__(self.__class__.__name__, queue=input_queue)

        self._input_queue = input_queue
        self._output_queue = output_queue
        self._udf_operator = udf_operator
        self._current_input_tuple: Union[ITuple, InputExhausted] = None
        self._current_input_link: LinkIdentity = None
        self.output_tuple_count: int = 0
        self.input_tuple_count: int = 0
        self._batch_to_tuple_converter = batch_to_tuple_converter
        self._tuple_to_batch_converter = TupleToBatchConverter()
        self._rpc_server = SyncRPCServer(output_queue)

    def before_loop(self) -> None:
        self._udf_operator.open()

    def after_loop(self) -> None:
        self._udf_operator.close()

    def main_loop(self) -> None:
        next_entry = self.interruptible_get()
        logger.info(f"PYTHON DP receive an entry from queue: {next_entry}")
        if isinstance(next_entry, InputDataElement):
            logger.info(f"PYTHON DP receive a DATA: {next_entry}")
            for element in self._batch_to_tuple_converter.process_data_payload(next_entry.from_, next_entry.batch):
                if isinstance(element, Tuple):
                    self._current_input_tuple = element
                    self.handle_input_tuple()
                    self.check_and_handle_control()

                elif isinstance(element, EndMarker):
                    self._current_input_tuple = InputExhausted()
                    self.handle_input_tuple()
                elif isinstance(element, EndOfAllMarker):
                    # TODO: in original design, when receiving EndOfAllMarker,
                    #  it needs to send InputExhausted() to all downstream actors.
                    #  Here we do not have such information.
                    #  we should instead send a control message back to Java side,
                    #  to invoke `batchProducer.emitEndOfUpstream()`
                    self.stop()
        elif isinstance(next_entry, ControlElement):
            logger.info(f"PYTHON DP receive a CONTROL: {next_entry}")
            self.process_control_command(next_entry.cmd, next_entry.from_)
        else:
            raise TypeError(f"unknown InternalQueueElement {next_entry}")

    def process_control_command(self, cmd: ControlPayload, from_: ActorVirtualIdentity):
        logger.info(f"PYTHON DP processing one CONTROL: {cmd} from {from_}")

        payload = get_oneof(cmd)
        if isinstance(payload, ControlInvocation):
            logger.info(f"PYTHON DP processing one CONTROL INVOCATION: {payload} from {from_}")
            self._rpc_server.receive(control_invocation=payload, from_=from_)

    #      cmd match {
    #       case invocation: ControlInvocation =>
    #         asyncRPCServer.logControlInvocation(invocation, from)
    #         asyncRPCServer.receive(invocation, from)
    #       case ret: ReturnPayload =>
    #         asyncRPCClient.logControlReply(ret, from)
    #         asyncRPCClient.fulfillPromise(ret)
    #     }

    def handle_input_tuple(self):
        results: Iterable[ITuple] = self.process_tuple(self._current_input_tuple, self._current_input_link)
        if isinstance(self._current_input_tuple, ITuple):
            self.input_tuple_count += 1
        for result in results:
            self.pass_tuple_to_downstream(result)

    def process_tuple(self, tuple_: Union[ITuple, InputExhausted], link: LinkIdentity) -> Iterable[ITuple]:
        typing.cast(tuple_, Union[Tuple, InputExhausted])
        logger.debug(f"processing a tuple {tuple_}")
        return self._udf_operator.process_texera_tuple(tuple_, link)

    def pass_tuple_to_downstream(self, tuple_: ITuple):
        for to, batch in self._tuple_to_batch_converter.tuple_to_batch(tuple_):
            self._output_queue.put(OutputDataElement(batch, to))
