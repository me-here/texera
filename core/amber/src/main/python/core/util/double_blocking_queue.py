import queue
from loguru import logger

from typing_extensions import T, Protocol


class Queue(Protocol):
    def get(self) -> T:
        pass

    def put(self, item: T) -> None:
        pass

    def empty(self) -> bool:
        pass


class DoubleBlockingQueue:

    def __init__(self, *slave_types: type):
        super().__init__()
        self._master_queue = queue.Queue()
        self._slave_queue = queue.Queue()
        self._slave_types = slave_types
        self._slave_enabled = True

    def get(self):

        if self._slave_enabled and self._master_queue.empty() and not self._slave_queue.empty():
            return self._slave_queue.get()
        else:
            return self._master_queue.get()

    def put(self, item: T) -> None:

        if isinstance(item, self._slave_types):
            self._slave_queue.put(item)
        else:
            self._master_queue.put(item)

    def disable_slave(self) -> None:
        self._slave_enabled = False

    def enable_slave(self) -> None:
        self._slave_enabled = True

    def empty(self) -> bool:
        logger.debug(f" checking empty")
        if self._slave_enabled:
            logger.debug(f" with slave")
            return self._slave_queue.empty() and self._master_queue.empty()
        else:
            logger.debug(f" without slave")
            return self._master_queue.empty()

    def master_empty(self) -> bool:
        return self._master_queue.empty()


if __name__ == '__main__':
    q = DoubleBlockingQueue(int, float)
    q.put(1)
    q.put("control")
    print(q.get())
    q.disable_slave()
    print(q.empty())
    # q.disable_slave()
    print(q.get())
