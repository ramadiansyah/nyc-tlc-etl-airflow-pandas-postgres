import pandas as pd
import numpy as np

from utils.logger import get_logger

logger = get_logger()

def fail_task():
    """
    Simulates a task failure by logging an error and raising a ValueError.

    This function is commonly used in Airflow DAGs to intentionally fail
    a task for testing failure callbacks, alerting systems, or retry behavior.

    Logs an error message before raising the exception.

    Raises:
        ValueError: Always raised to simulate a task failure.
    """
    logger.error("❌ fail_task triggered: Simulating failure...")
    raise ValueError("Simulasi error")

def max_column_lengths(csv_file: str):
    # Read CSV
    df = pd.read_csv(csv_file)

    # Dictionary to hold max length per column
    max_lengths = {}

    for col in df.columns:
        # Convert all values to string and find max length
        max_len = df[col].astype(str).map(len).max()
        max_lengths[col] = max_len

    # Show result
    for col, length in max_lengths.items():
        print(f"Column '{col}' -> Max length: {length}")

    return max_lengths


def max_column_lengths(csv_file: str):
    # Read CSV
    df = pd.read_csv(csv_file)

    # Dictionary to hold max length per column
    max_lengths = {}

    for col in df.columns:
        # Convert all values to string and find max length
        max_len = df[col].astype(str).map(len).max()
        max_lengths[col] = max_len

    # Show result
    for col, length in max_lengths.items():
        print(f"Column '{col}' -> Max length: {length}")

    return max_lengths



def suggest_postgres_ddl(csv_file: str, table_name: str = "my_table"):
    # Load CSV
    df = pd.read_csv(csv_file)

    ddl = f"CREATE TABLE {table_name} (\n"
    col_defs = []

    for col in df.columns:
        series = df[col].dropna()

        if series.empty:
            # Default to TEXT if no values
            col_type = "TEXT"
        elif pd.api.types.is_integer_dtype(series):
            # Use BIGINT for integers
            col_type = "BIGINT"
        elif pd.api.types.is_float_dtype(series):
            # Suggest NUMERIC with precision and scale
            # Precision = total digits, Scale = digits after decimal
            str_vals = series.astype(str)
            int_part = str_vals.str.split(".").str[0].str.len().max()
            frac_part = str_vals.str.split(".").str[1].fillna("").str.len().max()
            precision = int_part + frac_part
            scale = frac_part
            col_type = f"NUMERIC({precision},{scale})" if precision > 0 else "NUMERIC"
        elif pd.api.types.is_datetime64_any_dtype(series):
            col_type = "TIMESTAMP"
        else:
            # Treat as string -> VARCHAR(n)
            max_len = series.astype(str).map(len).max()
            col_type = f"VARCHAR({max_len})" if max_len > 0 else "TEXT"

        col_defs.append(f'    "{col}" {col_type}')

    ddl += ",\n".join(col_defs) + "\n);"

    print(ddl)
    return ddl
