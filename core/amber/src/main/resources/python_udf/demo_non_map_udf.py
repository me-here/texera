import logging

import texera_udf_operator_base


class DemoOperator(texera_udf_operator_base.TexeraUDFOperator):
    logger = logging.getLogger("PythonUDF.DemoOperator")

    def __init__(self):
        super().__init__()
        self._result_tuples = []

    @texera_udf_operator_base.exception(logger)
    def accept(self, row, nth_child=0):
        self._result_tuples.append(row)  # must take args
        self._result_tuples.append(row)

    @texera_udf_operator_base.exception(logger)
    def has_next(self):
        return len(self._result_tuples) != 0

    @texera_udf_operator_base.exception(logger)
    def next(self):
        return self._result_tuples.pop()

    @texera_udf_operator_base.exception(logger)
    def close(self):
        pass


operator_instance = DemoOperator()
