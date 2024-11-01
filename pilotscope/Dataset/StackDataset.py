from pilotscope.Dataset.BaseDataset import BaseDataset
import tarfile
import os

from pilotscope.PilotEnum import DatabaseEnum

class StackDataset(BaseDataset):
  """
  Stack dataset.
  Ryan Marcus, Parimarjan Negi, Hongzi Mao, Nesime Tatbul, Mohammad Alizadeh, Tim Kraska. 2021. Bao: Making Learned Query Optimization Practical. SIGMOD 2021
  The original Stack dataset can be found in https://rmarcus.info/stack.html
  The data, indexes and queries used for AI for DB are from 
  """
  data_location_dict = {DatabaseEnum.POSTGRESQL: ["https://www.dropbox.com/s/55bxfhilcu19i33/so_pg13?dl=1"], DatabaseEnum.SPARK: None}
  sub_dir = "Stack"
  now_path = os.path.join(os.path.dirname(__file__), sub_dir)
  file_db_type = DatabaseEnum.POSTGRESQL
  
  def __init__(self, use_db_type: DatabaseEnum, created_db_name="stack", data_dir=None) -> None:
    super().__init__(use_db_type, created_db_name, data_dir)
    self.download_urls = self.data_location_dict[use_db_type]
  
  def test_sql_fast(self):
    return self._get_sql(os.path.join(self.now_path, "stack_fast_sql.txt"))
