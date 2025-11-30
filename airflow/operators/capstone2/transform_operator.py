"""
extract_operator.py
"""

from airflow.models import BaseOperator
from airflow.utils.context import Context

from datetime import datetime
from utils.logger import setup_logger

from services.capstone2.p2.transformer import transform_data

logger = setup_logger()

class TransformGreenTaxiOperator(BaseOperator):
   
    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config

    def execute(self, context: Context):
       
        data_dir = "data/nyc/processed"
        ti = context["ti"]

        # ✅ Pull the serialized string
        start_date_str = ti.xcom_pull(task_ids="decide_date", key="start_date")
        end_date_str = ti.xcom_pull(task_ids="decide_date", key="end_date")

        # ✅ Parse back into datetime if needed
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        # ✅ Parse back into datetime if needed
        filename = f"green_tripdata_{start_date.strftime('%Y-%m')}_extracted.csv"
       
        logger.info(f"Start Transforming green taxi data {filename}, start_date: {start_date}, end_date: {end_date}") 
                
        transform_data(data_dir, filename, start_date, end_date)





        