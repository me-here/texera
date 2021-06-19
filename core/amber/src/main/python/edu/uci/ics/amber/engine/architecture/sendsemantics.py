# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: edu/uci/ics/amber/engine/architecture/sendsemantics/datatransferpolicy2.proto
# plugin: python-betterproto
import betterproto
from dataclasses import dataclass
from typing import List

from .edu.uci.ics.amber.engine import common


@dataclass
class Pause(betterproto.Message):
    pass


@dataclass
class Resume(betterproto.Message):
    pass


@dataclass
class DataSendingPolicy(betterproto.Message):
    policy_tag: common.LinkIdentity = betterproto.message_field(1)
    batch_size: int = betterproto.int32_field(2)
    receivers: List[common.ActorVirtualIdentity] = betterproto.message_field(3)