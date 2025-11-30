import pandas as pd
from psycopg2.extras import execute_values

from db.postgres import PostgresDB
from utils.logger import setup_logger

logger = setup_logger(log_name="loader")

class TaxiDataLoader:
    def __init__(self, db_connector: PostgresDB, table_name: str):
        self.db = db_connector
        self.table_name = table_name

    def get_existing_records(self, start_date, end_date):
        """Optional – bisa tetap dipakai kalau mau analisa data existing"""
        try:
            query = f"""
                SELECT *
                FROM {self.table_name}
                WHERE pickup_datetime BETWEEN %s AND %s;
            """
            params = (start_date, end_date)

            logger.info(f"Running query: {query} with params {params}")

            conn = self.db.engine.raw_connection()
            try:
                existing_df = pd.read_sql(query, conn, params=params)
            finally:
                conn.close()

            logger.info(f"Fetched {existing_df.shape[0]} existing records from {self.table_name}")
            return existing_df

        except Exception as e:
            logger.error(f"Failed to fetch existing records: {e} | Query: {query} | Params: {params}")
            raise

    def insert_data(self, insert_df: pd.DataFrame):
        """Insert data with ON CONFLICT DO NOTHING to skip duplicates"""
        if insert_df.empty:
            logger.info("No new data to insert. Skipping insertion.")
            return

        conn, cur = None, None
        try:
            conn = self.db.engine.raw_connection()
            cur = conn.cursor()

            # Convert DataFrame to list of tuples
            data_tuples = [tuple(x) for x in insert_df.to_numpy()]
            columns = ', '.join(insert_df.columns)

            # Build query with ON CONFLICT DO NOTHING
            query = f"""
                INSERT INTO {self.table_name} ({columns})
                VALUES %s
                ON CONFLICT ON CONSTRAINT green_taxi_unique DO NOTHING;
            """

            # Batch insert
            execute_values(cur, query, data_tuples)

            conn.commit()
            logger.info(f"Inserted {len(data_tuples)} rows (duplicates skipped automatically).")

        except Exception as e:
            logger.error(f"Failed to insert data into {self.table_name}: {e}")
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

