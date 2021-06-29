# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: edu/uci/ics/amber/engine/architecture/worker/promisehandler2.proto
# plugin: python-betterproto
import betterproto
from dataclasses import dataclass


@dataclass(eq=False, repr=False)
class StartWorker(betterproto.Message):
    pass


@dataclass(eq=False, repr=False)
class UpdateInputLinking(betterproto.Message):
    identifier: "__common__.ActorVirtualIdentity" = betterproto.message_field(1)
    input_link: "__common__.LinkIdentity" = betterproto.message_field(2)


@dataclass(eq=False, repr=False)
class AddOutputPolicy(betterproto.Message):
    policy: "_sendsemantics__.DataSendingPolicy" = betterproto.message_field(1)


@dataclass(eq=False, repr=False)
class ControlCommand(betterproto.Message):
    add_output_policy: "AddOutputPolicy" = betterproto.message_field(
        3, group="sealed_value"
    )
    start_worker: "StartWorker" = betterproto.message_field(4, group="sealed_value")
    update_input_linking: "UpdateInputLinking" = betterproto.message_field(
        5, group="sealed_value"
    )


from .. import sendsemantics as _sendsemantics__
from ... import common as __common__