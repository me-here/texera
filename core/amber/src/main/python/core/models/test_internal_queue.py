# import pytest
#
# from core import Tuple
# from core.models.internal_queue import InternalQueue, ControlElement, InputDataElement, OutputDataElement
# from core.models.payload import DataFrame
# from core.models.tuple import InputExhausted
# from edu.uci.ics.amber.engine.common import ControlPayload, ActorVirtualIdentity
#
#
# class TestInternalQueue:
#     @pytest.fixture
#     def internal_queue(self):
#         return InternalQueue()
#
#     def test_internal_queue_cannot_put_non_internal_queue_element(self, internal_queue):
#         with pytest.raises(TypeError):
#             internal_queue.put(1)
#
#         with pytest.raises(TypeError):
#             internal_queue.put(PrioritizedItem((1, 1), InputDataElement(payload=DataFrame(frame=[Tuple()]),
#                                                                         from_=ActorVirtualIdentity())))
#
#         with pytest.raises(TypeError):
#             internal_queue.put(Tuple())
#
#         with pytest.raises(TypeError):
#             internal_queue.put(None)
#
#         with pytest.raises(TypeError):
#             internal_queue.put(InputExhausted())
#
#     def test_internal_queue_can_put_internal_queue_element(self, internal_queue):
#         internal_queue.put(InputDataElement(payload=DataFrame(frame=[Tuple()]),
#                                             from_=ActorVirtualIdentity()))
#         internal_queue.put(ControlElement(ControlPayload(), ActorVirtualIdentity()))
#         internal_queue.put(OutputDataElement(payload=DataFrame(frame=[Tuple()]),
#                                              to=ActorVirtualIdentity()))
#
#     def test_internal_queue_should_emit_control_first(self, internal_queue):
#
#         elements = [InputDataElement(payload=DataFrame(frame=[Tuple()]),
#                                      from_=ActorVirtualIdentity()),
#                     OutputDataElement(payload=DataFrame(frame=[Tuple()]),
#                                       to=ActorVirtualIdentity())]
#         for element in elements:
#             internal_queue.put(element)
#
#         # enqueue a ControlElement:
#         control = ControlElement(ControlPayload(), ActorVirtualIdentity())
#         internal_queue.put(control)
#
#         assert internal_queue.get() == control
#
#     def test_internal_queue_should_emit_stable_result(self, internal_queue):
#         elements1 = [InputDataElement(payload=DataFrame(frame=[Tuple()]),
#                                       from_=ActorVirtualIdentity()),
#                      OutputDataElement(payload=DataFrame(frame=[Tuple()]),
#                                        to=ActorVirtualIdentity())]
#         for element in elements1:
#             internal_queue.put(element)
#
#         # enqueue a ControlElement:
#         control1 = ControlElement(ControlPayload(), ActorVirtualIdentity())
#         internal_queue.put(control1)
#
#         elements2 = [InputDataElement(payload=DataFrame(frame=[Tuple()]),
#                                       from_=ActorVirtualIdentity()),
#                      InputDataElement(payload=DataFrame(frame=[Tuple()]),
#                                       from_=ActorVirtualIdentity()),
#                      OutputDataElement(payload=DataFrame(frame=[Tuple()]),
#                                        to=ActorVirtualIdentity()),
#                      InputDataElement(payload=DataFrame(frame=[Tuple()]),
#                                       from_=ActorVirtualIdentity())
#                      ]
#         for element in elements2:
#             internal_queue.put(element)
#
#         # enqueue a ControlElement:
#         control2 = ControlElement(ControlPayload(), ActorVirtualIdentity())
#         internal_queue.put(control2)
#
#         assert internal_queue.get() == control1
#         assert internal_queue.get() == control2
#
#         for element in elements1 + elements2:
#             assert internal_queue.get() == element
