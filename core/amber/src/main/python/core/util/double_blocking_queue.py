from queue import Queue
from typing import Optional
from typing_extensions import T


class DoubleBlockingQueue(Queue):

    def __init__(self, *slave_types: type):
        super().__init__()
        self._slave_queue = Queue()
        self._slave_types = slave_types
        self._slave_enabled = True

    def get(self, **kwargs):
        if self._slave_enabled and super(DoubleBlockingQueue, self).empty() and not self._slave_queue.empty():
            return self._slave_queue.get()
        else:
            return super(DoubleBlockingQueue, self).get()

    def put(self, item: T, block: bool = ..., timeout: Optional[float] = ...) -> None:
        if isinstance(item, self._slave_types):
            self._slave_queue.put(item)
        else:
            super(DoubleBlockingQueue, self).put(item)

    def disable_slave(self) -> None:
        self._slave_enabled = False

    def enable_slave(self) -> None:
        self._slave_enabled = True

    def empty(self) -> bool:
        return super(DoubleBlockingQueue, self).empty() and (self._slave_queue.empty() if self._slave_enabled else True)


if __name__ == '__main__':
    q = DoubleBlockingQueue(int, float)
    q.put(1)
    q.put("control")
    print(q.get())
    q.disable_slave()
    print(q.empty())
    # q.disable_slave()
    print(q.get())
