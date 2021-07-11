import queue
from overrides import overrides
from typing import T

from core.util.queue.queue_base import IQueue


class DoubleBlockingQueue(IQueue):

    def __init__(self, *sub_types: type):
        super().__init__()
        self._main_queue = queue.Queue()
        self._sub_queue = queue.Queue()
        self._sub_types = sub_types
        self._sub_enabled = True

    def disable_sub(self) -> None:
        self._sub_enabled = False

    @overrides
    def empty(self) -> bool:
        if self._sub_enabled:
            return self.main_empty() and self.sub_empty()
        else:
            return self.main_empty()

    def enable_sub(self) -> None:
        self._sub_enabled = True

    @overrides
    def get(self):
        if self._sub_enabled and self._main_queue.empty() and not self._sub_queue.empty():
            return self._sub_queue.get()
        else:
            return self._main_queue.get()

    @overrides
    def put(self, item: T) -> None:
        if isinstance(item, self._sub_types):
            self._sub_queue.put(item)
        else:
            self._main_queue.put(item)

    def main_empty(self) -> bool:
        return self._main_queue.empty()

    def sub_empty(self) -> bool:
        return self._main_queue.empty()


if __name__ == '__main__':
    q = DoubleBlockingQueue(int, float)
    q.put(1)
    q.put("control")
    print(q.get())
    q.disable_sub()
    print(q.empty())
    # q.disable_slave()
    print(q.get())
