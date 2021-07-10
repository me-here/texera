import queue

from typing_extensions import T


class DoubleBlockingQueue:

    def __init__(self, *slave_types: type):
        super().__init__()
        self._main_queue = queue.Queue()
        self._sub_queue = queue.Queue()
        self._sub_types = slave_types
        self._sub_enabled = True

    def get(self):
        if self._sub_enabled and self._main_queue.empty() and not self._sub_queue.empty():
            return self._sub_queue.get()
        else:
            return self._main_queue.get()

    def put(self, item: T) -> None:
        if isinstance(item, self._sub_types):
            self._sub_queue.put(item)
        else:
            self._main_queue.put(item)

    def disable_slave(self) -> None:
        self._sub_enabled = False

    def enable_slave(self) -> None:
        self._sub_enabled = True

    def empty(self) -> bool:
        if self._sub_enabled:
            return self._sub_queue.empty() and self._main_queue.empty()
        else:
            return self._main_queue.empty()

    def master_empty(self) -> bool:
        return self._main_queue.empty()


if __name__ == '__main__':
    q = DoubleBlockingQueue(int, float)
    q.put(1)
    q.put("control")
    print(q.get())
    q.disable_slave()
    print(q.empty())
    # q.disable_slave()
    print(q.get())
