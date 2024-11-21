from concurrent.futures.thread import ThreadPoolExecutor
from pilotscope.Common.Util import pilotscope_exit
from pilotscope.Common.TimeStatistic import TimeStatistic
from pandas import DataFrame
from algorithm_examples.ExampleConfig import (
    get_time_statistic_img_path,
    get_time_statistic_xlsx_file_path,
)
import unittest
from pilotscope.Common.Drawer import Drawer
from algorithm_examples.utils import (
    load_training_sql,
    load_test_sql,
    to_tree_json,
    json_str_to_json_obj,
)

from pilotscope.Factory.SchedulerFactory import SchedulerFactory
from pilotscope.Common.Util import (
    wait_futures_results,
)
from pilotscope.PilotScheduler import PilotScheduler
from pilotscope.PilotConfig import PostgreSQLConfig
from pilotscope.DataManager.DataManager import DataManager
from pilotscope.DBController.BaseDBController import BaseDBController
from pilotscope.PilotEvent import PretrainingModelEvent
from pilotscope.DBInteractor.PilotDataInteractor import PilotDataInteractor
from pilotscope.PilotTransData import PilotTransData
from pilotscope.PilotEnum import DatabaseEnum
from pilotscope.Factory.DBControllerFectory import DBControllerFactory
from pilotscope.PilotConfig import PilotConfig
from pilotscope.PilotModel import PilotModel
from pilotscope.Anchor.BaseAnchor.BasePushHandler import HintPushHandler


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
            elif db_type == DatabaseEnum.SPARK:
                self.ALL_OPTIONS = [
                    "spark.sql.cbo.enabled",
                    "spark.sql.join.preferSortMergeJoin",
                    "spark.sql.adaptive.skewJoin.enabled",
                    "spark.sql.codegen.wholeStag",
                    "spark.sql.cbo.joinReorder.enabled  ",
                    "spark.sql.sources.bucketing.autoBucketedScan.enabled",
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
                        i, self.ARMS_OPTION, self.ALL_OPTIONS, ["false", "true"]
                    )
                    for i in range(len(self.ARMS_OPTION))
                ]
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
        sql = modify_sql_for_spark(self.config, sql)
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
            if self.config.db_type == DatabaseEnum.SPARK:
                for plan in origin_plans:
                    plan = to_tree_json(plan)
                    compress = SparkPlanCompress()
                    plan["Plan"] = compress.compress(plan["Plan"])
                    plans.append(plan)

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


class BaoPretrainingModelEvent(PretrainingModelEvent):
    def __init__(
        self,
        config: PilotConfig,
        bind_model: PilotModel,
        data_saving_table,
        enable_collection=True,
        enable_training=True,
    ):
        super().__init__(
            config, bind_model, data_saving_table, enable_collection, enable_training
        )
        self.pilot_data_interactor = PilotDataInteractor(self.config)
        self.bao_hint = BaoHintPushHandler.HintForBao(config.db_type)
        self.sqls = self.load_sql()
        self.cur_sql_idx = 0

    def load_sql(self):
        return load_training_sql(self.config.db)[0:10]  # only for development test

    def iterative_data_collection(
        self, db_controller: BaseDBController, train_data_manager: DataManager
    ):
        # self.load_sql()
        column_2_value_list = []

        sql = self.sqls[self.cur_sql_idx]
        sql = modify_sql_for_spark(self.config, sql)

        print(
            "current  is {}-th sql, and total sqls is {}".format(
                self.cur_sql_idx, len(self.sqls)
            )
        )
        for hint2val in self.bao_hint.arms_hint2val:
            column_2_value = {}
            self.pilot_data_interactor.push_hint(hint2val)
            self.pilot_data_interactor.pull_physical_plan()
            self.pilot_data_interactor.pull_execution_time()
            if self._model.have_cache_data:
                self.pilot_data_interactor.pull_buffercache()
            data: PilotTransData = self.pilot_data_interactor.execute(sql)
            if data is not None and data.execution_time is not None:
                column_2_value["plan"] = data.physical_plan
                column_2_value["sql"] = sql
                if self._model.have_cache_data:
                    column_2_value["plan"]["Buffers"] = data.buffercache
                column_2_value["time"] = data.execution_time
                column_2_value["sql_idx"] = self.cur_sql_idx
                column_2_value_list.append(column_2_value)
        self.cur_sql_idx += 1
        return column_2_value_list, True if self.cur_sql_idx >= len(
            self.sqls
        ) else False

    def custom_model_training(
        self, bind_model, db_controller: BaseDBController, data_manager: DataManager
    ):
        data: DataFrame = data_manager.read_all(self.data_saving_table)
        bao_model = BaoRegression(
            verbose=True,
            have_cache_data=self._model.have_cache_data,
            is_spark=self.config.db_type == DatabaseEnum.SPARK,
        )
        new_plans, new_times = self.filter(data["plan"].values, data["time"].values)
        bao_model.fit(new_plans, new_times)
        return bao_model

    def filter(self, plans, times):
        new_plans = []
        new_times = []

        for i, plan in enumerate(plans):
            if (
                self.config.db_type == DatabaseEnum.POSTGRESQL
                and not self.contain_outlier_plan(plan)
            ):
                new_plans.append(plan)
                new_times.append(times[i])
            elif self.config.db_type == DatabaseEnum.SPARK:
                plan = to_tree_json(plan)
                compress = SparkPlanCompress()
                plan["Plan"] = compress.compress(plan["Plan"])
                new_plans.append(plan)
                new_times.append(times[i])
        return new_plans, new_times

    def contain_outlier_plan(self, plan):
        if isinstance(plan, str):
            plan = json_str_to_json_obj(plan)["Plan"]
        children = plan["Plans"] if "Plans" in plan else []
        for child in children:
            flag = self.contain_outlier_plan(child)
            if flag:
                return True

        if plan["Node Type"] == "BitmapAnd":
            return True
        return False


class BaoTest(unittest.TestCase):
    def setUp(self):
        self.config: PostgreSQLConfig = PostgreSQLConfig(
            db_host="localhost",
            db_port="5432",
            db_user="postgres",
            db_user_pwd="postgres",
        )
        self.config.db = "stats_tiny"

        self.used_cache = False
        if self.used_cache:
            self.model_name = "bao_model_wc"
        else:
            self.model_name = "bao_model"

        self.test_data_table = "{}_{}_test_data_table".format(
            self.model_name, self.config.db
        )
        self.pg_test_data_table = "{}_{}_test_data_table".format("pg", self.config.db)
        self.pretraining_data_table = (
            "bao_{}_pretraining_collect_data".format(self.config.db)
            if not self.used_cache
            else "bao_{}_pretraining_collect_data_wc".format(self.config.db)
        )
        self.algo = "bao"

    def test_bao(self):
        try:
            config = self.config
            config.once_request_timeout = config.sql_execution_timeout = 500
            config.print()

            bao_pilot_model: BaoPilotModel = BaoPilotModel(
                self.model_name, have_cache_data=self.used_cache
            )
            bao_pilot_model.load()
            bao_handler = BaoHintPushHandler(bao_pilot_model, config)

            # core
            scheduler: PilotScheduler = SchedulerFactory.create_scheduler(config)
            scheduler.register_custom_handlers([bao_handler])
            scheduler.register_required_data(
                self.test_data_table,
                pull_physical_plan=True,
                pull_execution_time=True,
                pull_buffer_cache=self.used_cache,
            )

            pretraining_event = BaoPretrainingModelEvent(
                config,
                bao_pilot_model,
                self.pretraining_data_table,
                enable_collection=True,
                enable_training=True,
            )
            scheduler.register_events([pretraining_event])

            # start
            scheduler.init()
            print("start to test sql")
            sqls = load_test_sql(config.db)
            for i, sql in enumerate(sqls):
                print("current is the {}-th sql, total is {}".format(i, len(sqls)))
                TimeStatistic.start("Bao")
                scheduler.execute(sql)
                TimeStatistic.end("Bao")
            TimeStatistic.save_xlsx(
                get_time_statistic_xlsx_file_path(self.algo, config.db)
            )
            name_2_value = TimeStatistic.get_average_data()
            Drawer.draw_bar(
                name_2_value,
                get_time_statistic_img_path(self.algo, self.config.db),
                is_rotation=True,
            )
            print("run ok")
        finally:
            pilotscope_exit()
