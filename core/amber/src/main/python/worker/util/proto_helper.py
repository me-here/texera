from google.protobuf.any_pb2 import Any


def get_oneof(base: Any, instance_type: type) -> Any:
    name: str = instance_type.__name__
    k = name[0].lower() + name[1:]
    if base.WhichOneof('sealed_value') == k:
        return getattr(base, k)
