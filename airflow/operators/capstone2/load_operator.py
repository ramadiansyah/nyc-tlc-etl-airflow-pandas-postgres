import os
import pandas as pd

from airflow.models import BaseOperator
from airflow.utils.context import Context

from datetime import datetime

from services.capstone2.p2.loader import TaxiDataLoader

from db.postgres import PostgresDB
from utils.logger import setup_logger

logger = setup_logger()

class LoadGreenTaxiOperator(BaseOperator):

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

        # ✅ Build filename correctly
        filename = f"green_tripdata_{start_date.strftime('%Y-%m')}_transformed.csv"
        file_path = os.path.join(data_dir, filename)

        # ✅ Read CSV into DataFrame
        transformed_df = pd.read_csv(file_path)

        logger.info(f"✅ Loaded DataFrame from {file_path} with shape {transformed_df.shape}")
        logger.info(f"Start loading green taxi data {filename}, start_date: {start_date}, end_date: {end_date}")

        # Initialize database connection
        db = PostgresDB()

        # Initialize Loader
        loader = TaxiDataLoader(db_connector=db, table_name="green_taxi_trip")
        loader.insert_data(transformed_df)

        logger.info(f"✅ Finished loading {filename}")

        