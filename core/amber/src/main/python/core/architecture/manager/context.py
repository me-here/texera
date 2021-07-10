from edu.uci.ics.amber.engine.common import Uninitialized, Ready, Recovering, Paused, Running, Completed
from .pause_manager import PauseManager
from .state_manager import StateManager
from .statistics_manager import StatisticsManager
from ..messaginglayer.batch_to_tuple_converter import BatchToTupleConverter
from ..messaginglayer.tuple_to_batch_converter import TupleToBatchConverter


class Context:
    def __init__(self, dp):
        self.dp = dp
        self.state_manager = StateManager({
            Uninitialized(): {Ready(), Recovering()},
            Ready(): {Paused(), Running(), Recovering()},
            Running(): {Paused(), Completed(), Recovering()},
            Paused(): {Running(), Recovering()},
            Completed(): {Recovering()},
            Recovering(): {Uninitialized(),
                           Ready(),
                           Running(),
                           Paused(),
                           Completed()}

        }, Uninitialized())

        self.statistics_manager = StatisticsManager()
        self.pause_manager = PauseManager()
        self.tuple_to_batch_converter = TupleToBatchConverter()
        self.batch_to_tuple_converter = BatchToTupleConverter()
