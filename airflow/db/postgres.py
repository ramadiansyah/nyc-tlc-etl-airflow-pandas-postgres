# db/postgres.py

from psycopg2 import sql, extras
from sqlalchemy import create_engine

from utils.env_reader import load_db_credentials
from utils.logger import setup_logger

logger = setup_logger()

class PostgresDB:
    def __init__(self):
        self.credentials = load_db_credentials()

        # Initialize the connection and engine
        self.conn = None
        self.engine = None
        self.connect()

    def connect(self):
        try:
            # Assuming the credentials returned from load_db_credentials() is a dictionary
            db_url = f"postgresql+psycopg2://{self.credentials['user']}:{self.credentials['password']}@{self.credentials['host']}:{self.credentials['port']}/{self.credentials['dbname']}"
            logger.debug(f"db_url: {db_url}")

            # Initialize the SQLAlchemy engine using psycopg2 driver
            self.engine = create_engine(db_url)

            # Establish the connection using SQLAlchemy's engine
            self.conn = self.engine.connect()
        
            logger.info("Connected to the database using SQLAlchemy engine.")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def insert_many(self, table_name, columns, data_rows):
        if not data_rows:
            logger.warning("No data to insert.")
            return

        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = sql.SQL(
            f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        )

        try:
            with self.conn.cursor() as cursor:
                extras.execute_batch(cursor, insert_query, data_rows)
            self.conn.commit()
            logger.info(f"Inserted {len(data_rows)} rows into {table_name}.")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting batch into {table_name}: {e}")
            raise

    def execute_query(self, query, params=None):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                if cursor.description:
                    return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed.")
