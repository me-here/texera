from betterproto import which_one_of
from google.protobuf.any_pb2 import Any


def get_oneof(base: Any) -> Any:
    _, value = which_one_of(base, "sealed_value")
    return value
