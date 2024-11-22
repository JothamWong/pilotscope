from algorithm_examples.Bao.BaoHintPushHandler import BaoHintPushHandler
from algorithm_examples.Bao.source.model import BaoRegression
from algorithm_examples.utils import (
    json_str_to_json_obj,
    load_training_sql,
)
from pandas import DataFrame
from pilotscope.DataManager.DataManager import DataManager
from pilotscope.DBController.BaseDBController import BaseDBController
from pilotscope.DBInteractor.PilotDataInteractor import PilotDataInteractor
from pilotscope.PilotConfig import PilotConfig
from pilotscope.PilotEnum import DatabaseEnum
from pilotscope.PilotEvent import PretrainingModelEvent
from pilotscope.PilotModel import PilotModel
from pilotscope.PilotTransData import PilotTransData


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
