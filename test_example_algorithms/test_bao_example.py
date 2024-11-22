import unittest

from algorithm_examples.Bao.BaoHintPushHandler import BaoHintPushHandler
from algorithm_examples.Bao.EventImplement import BaoPretrainingModelEvent
from algorithm_examples.Bao.BaoPilotModel import BaoPilotModel
from algorithm_examples.ExampleConfig import (
    get_time_statistic_img_path,
    get_time_statistic_xlsx_file_path,
)
from algorithm_examples.utils import (
    load_test_sql,
)
from pilotscope.Common.Drawer import Drawer
from pilotscope.Common.TimeStatistic import TimeStatistic
from pilotscope.Common.Util import (
    pilotscope_exit,
)
from pilotscope.Factory.SchedulerFactory import SchedulerFactory
from pilotscope.PilotConfig import PostgreSQLConfig
from pilotscope.PilotScheduler import PilotScheduler


class BaoTest(unittest.TestCase):
    def setUp(self):
        self.config = PostgreSQLConfig()
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
            bao_pilot_model.load_model()
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


if __name__ == "__main__":
    unittest.main()
