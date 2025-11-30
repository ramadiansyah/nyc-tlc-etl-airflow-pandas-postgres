from airflow.models import BaseOperator
from airflow.utils.context import Context

from datetime import datetime, timezone

from services.capstone2.p2.extractor import extract_data
from utils.logger import setup_logger

logger = setup_logger()

class ExtractGreenTaxiOperator(BaseOperator):
   
    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config

    def execute(self, context: Context):
       
        data_dir = "data/nyc/raw"
        ti = context["ti"]

        # ✅ Pull the serialized string
        start_date_str = ti.xcom_pull(task_ids="decide_date", key="start_date")
        end_date_str = ti.xcom_pull(task_ids="decide_date", key="end_date")

        # ✅ Parse back into datetime if needed
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        logger.info(f"Start extracting green taxi data between {start_date} and {end_date}") 
                
        extract_data(data_dir, start_date, end_date)

        