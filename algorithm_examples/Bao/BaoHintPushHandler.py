from concurrent.futures.thread import ThreadPoolExecutor

from pilotscope.Anchor.BaseAnchor.BasePushHandler import HintPushHandler
from pilotscope.Common.TimeStatistic import TimeStatistic
from pilotscope.Common.Util import (
    wait_futures_results,
)
from pilotscope.DBInteractor.PilotDataInteractor import PilotDataInteractor
from pilotscope.Factory.DBControllerFectory import DBControllerFactory
from pilotscope.PilotConfig import PilotConfig
from pilotscope.PilotEnum import DatabaseEnum
from pilotscope.PilotModel import PilotModel
from pilotscope.PilotTransData import PilotTransData


class BaoHintPushHandler(HintPushHandler):
    class HintForBao:
        def __init__(self, db_type: DatabaseEnum) -> None:  # Hint Chores Factory
            if db_type == DatabaseEnum.POSTGRESQL:
                self.ALL_OPTIONS = [
                    "enable_nestloop",
                    "enable_hashjoin",
                    "enable_mergejoin",
                    "enable_seqscan",
                    "enable_indexscan",
                    "enable_indexonlyscan",
                ]
                self.ARMS_OPTION = [
                    63,
                    62,
                    43,
                    42,
                    59,
                ]  # each arm's option in binary format
                self.arms_hint2val = [
                    self.arm_idx_to_hint2val(
                        i, self.ARMS_OPTION, self.ALL_OPTIONS, ["off", "on"]
                    )
                    for i in range(len(self.ARMS_OPTION))
                ]
                pass
            else:
                raise NotImplementedError

        def arm_idx_to_hint2val(
            self, arm_idx, arms_option, all_options, value_names: list
        ):
            hint2val = dict()
            for i in range(len(all_options)):
                hint2val[all_options[i]] = value_names[1 & (arms_option[arm_idx] >> i)]
            return hint2val

    def __init__(self, model: PilotModel, config: PilotConfig) -> None:
        super().__init__(config)
        self.model = model
        self.config = config
        self.db_controller = DBControllerFactory.get_db_controller(config)
        self.bao_hint = self.HintForBao(config.db_type)

    def predict(self, plans):
        return self.model.model.predict(plans)

    def acquire_injected_data(self, sql):
        try:
            TimeStatistic.start("AI")
            # with ThreadPoolExecutor(max_workers=len(self.bao_hint.arms_hint2val)) as pool:
            with ThreadPoolExecutor(max_workers=1) as pool:
                futures = []
                for hint2val in self.bao_hint.arms_hint2val:
                    future = pool.submit(self._get_plan, sql, hint2val)
                    futures.append(future)
                plans = wait_futures_results(futures)
                pass

            origin_plans = plans
            plans = []

            TimeStatistic.start("Predict")
            est_exe_time = self.model.model.predict(plans)
            TimeStatistic.end("Predict")
            print("BAO: ", est_exe_time, flush=True)
            TimeStatistic.end("AI")
            idx = est_exe_time.argmin()
            pass
        except Exception as e:
            idx = 0
        return self.bao_hint.arms_hint2val[idx]

    def _get_plan(self, sql, hint2val):
        pilot_data_interactor = PilotDataInteractor(self.config)
        # print(hint2val)
        pilot_data_interactor.push_hint(hint2val)
        pilot_data_interactor.pull_physical_plan()
        if self.model.have_cache_data:
            pilot_data_interactor.pull_buffercache()

        data: PilotTransData = pilot_data_interactor.execute(sql)
        plan = data.physical_plan
        if self.model.have_cache_data:
            plan["Buffers"] = data.buffercache
        return plan
