import time
from collections import defaultdict

from betterproto import which_one_of, Message
from google.protobuf.any_pb2 import Any

from edu.uci.ics.amber.engine.common import LayerIdentity, LinkIdentity


def get_oneof(base: Any) -> Any:
    _, value = which_one_of(base, "sealed_value")
    return value


class ProtoDict(defaultdict):

    def __setitem__(self, key: Message, value):
        # assert isinstance(key, Message)
        super(ProtoDict, self).__setitem__(key.__repr__(), value)

    def __getitem__(self, key: Message):
        # assert isinstance(key, Message)

        return super(ProtoDict, self).__getitem__(key.__repr__())


Message.__hash__ = lambda x: hash(x.__repr__())

# class ProtoSet(set):
#     def __init__(self):
#         super().__init__()
#         self._dict = ProtoDict()
#
#     def add(self, element: _T) -> None:
#         self._dict[element] = element
#
#     def pop(self) -> _T:
#         super(ProtoSet, self).pop()

if __name__ == '__main__':
    proto_dict = ProtoDict()
    start = time.time()
    n = 100000
    for i in range(n):
        a = LinkIdentity(LayerIdentity(str(i), str(i), str(i)), LayerIdentity(str(i), str(i), str(i)))
        proto_dict[a] = 1
    end = time.time()
    print((end - start) / n)
    print(len(proto_dict))
    print(proto_dict)
