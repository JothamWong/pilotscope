import os

from algorithm_examples.Bao.source.model import BaoRegression
from pilotscope.PilotModel import PilotModel

from .source.main import BaoModel


# self.load()
# self.model.predict(plans)
# self.model.have_cache_data
class BaoPilotModel(PilotModel):
    def __init__(self, model_name, have_cache_data):
        super().__init__(model_name)
        self.have_cache_data = have_cache_data
        self.bao_model_save_dir = "../algorithm_examples/ExampleData/Bao/Model"
        self.model_path = os.path.join(self.bao_model_save_dir, self.model_name)

    def save_model(self):
        self.model.save(self.model_path)

    def load_model(self):
        bao_model = BaoRegression()
        try:
            bao_model.load(self.model_path)
        except FileNotFoundError:
            print("Can not load model. Bao model file not find, so init by random.")
        self.model = bao_model
