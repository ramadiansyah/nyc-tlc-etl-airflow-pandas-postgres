import os
import pandas as pd
from datetime import datetime, timezone

from utils.logger import setup_logger
logger = setup_logger()

def extract_data(data_dir, start_date, end_date): 
    try:    
        # Convert to string using strftime
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        logger.info(f"start_date_str: {start_date_str}, end_date_str: {end_date_str}, data_dir: {data_dir}")

        relevant_files = filter_parquet_files(start_date_str, end_date_str, data_dir)
        logger.info(f"relevant_files: {relevant_files}")
        
        full_df, file_count = load_data_to_df(data_dir, relevant_files)
        logger.info(f"Extracted {len(full_df)} rows from {file_count} files.")

        file_name = f"green_tripdata_{start_date.year}-{start_date.month:02d}_extracted.csv"
        output_path = os.path.join("data", "nyc", "processed", file_name)

        # Tambahkan kolom run_date (tanggal hari ini UTC+0)
        full_df["run_date"] = datetime.now(timezone.utc).date()
        full_df.to_csv(output_path, index=False)

        logger.info(f"Saved file to: {output_path}")

    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        raise


def filter_parquet_files(start_date, end_date, folder_path):
    # Convert start_date and end_date to datetime objects (set to first day of the month)
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Normalize to first day of the month
    start_date = start_date.replace(day=1)
    end_date = end_date.replace(day=1)
    
    files = os.listdir(folder_path)
    relevant_files = []
    
    for file in files:
        if file.endswith(".parquet"):
            # Extract the year and month from the filename
            file_date_str = file.split("_")[2].replace(".parquet", "")  # Extract YYYY-MM from filename
            file_year_month = datetime.strptime(file_date_str, "%Y-%m")            
            logger.debug(f"start_date: {start_date} <= file_year_month: {file_year_month} <= end_date: {end_date}:")

            # Check if file_year_month is within the start_date and end_date range
            if start_date <= file_year_month <= end_date:
                logger.debug(f"append: {file}")
                relevant_files.append(file)
            else:
                logger.debug(f"not append: {file}")
    
    return relevant_files


def load_data_to_df(folder_path, relevant_files):

    logger.debug(f"relevant_files: {relevant_files}")

    # Count the number of relevant files
    file_count = len(relevant_files)

    # Load the data from the relevant Parquet files
    dfs = []
    for file in relevant_files:
        file_path = os.path.join(folder_path, file)
        df = pd.read_parquet(file_path)
        dfs.append(df)
    
    # Concatenate all DataFrames into one
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df, file_count

