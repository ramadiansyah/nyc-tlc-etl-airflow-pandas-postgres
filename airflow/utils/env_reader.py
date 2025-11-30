import os
from dotenv import load_dotenv

def load_db_credentials():
    load_dotenv()

    return {
        "host": os.getenv("NYC_POSTGRES_DB_HOST"),
        "port": int(os.getenv("NYC_POSTGRES_DB_PORT")),
        "dbname": os.getenv("NYC_POSTGRES_DB_NAME"),
        "user": os.getenv("NYC_POSTGRES_DB_USER"),
        "password": os.getenv("NYC_POSTGRES_DB_PASS"),
    }
