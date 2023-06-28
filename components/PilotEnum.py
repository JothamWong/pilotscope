from enum import Enum


class PilotEnum(Enum):
    def __eq__(self, *args, **kwargs):
        return self.name == args[0].name

    def __hash__(self, *args, **kwargs):
        return hash(self.name)


class DatabaseEnum(PilotEnum):
    POSTGRESQL = 0,
    SPARK = 1


class HintEnum(PilotEnum):
    ENABLE_HASH_JOIN = 0


class EventEnum(PilotEnum):
    PERIOD_TRAIN_EVENT = 0
    PERIODIC_TRAINING_EVENT = 0,
    PERIODIC_COLLECTION_EVENT = 0,
    PRETRAINING_EVENT = 1,


class DataFetchMethodEnum(PilotEnum):
    HTTP = 0


class AllowedFetchDataEnum(PilotEnum):
    PHYSICAL_PLAN = "physical_plan",
    LOGICAL_PLAN = "logical_plan",
    EXECUTION_TIME = "execution_time",
    REAL_COST_SUBPLAN = "real_cost_subplan",
    REAL_CARD_SUBQUERY = "real_card_subquery",
    SUBQUERY_2_CARDS = "subquery_2_cards",
    HINTS = "hints",


class FetchMethod(PilotEnum):
    INNER = 0,
    OUTER = 1,


class TrainSwitchMode(PilotEnum):
    WAIT = 0,
    DB = 1