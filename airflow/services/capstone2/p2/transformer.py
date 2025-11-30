import os
import pandas as pd

from utils.logger import setup_logger
logger = setup_logger()

def transform_data(data_dir, filename, start_date, end_date):    
    try:    
        # Reading CSV
        file_path = os.path.join(data_dir, filename)
        full_df = pd.read_csv(file_path)

        # mengubah kolom lpep_pickup_datetime di dalam DataFrame full_df dari format string (teks) menjadi format datetime (format waktu Python).
        full_df['lpep_pickup_datetime'] = pd.to_datetime(full_df['lpep_pickup_datetime'])

        if full_df.empty:
            logger.warning("No data extracted for the given range. Exiting.")
            return

        filtered_df = filter_df_by_period(
            full_df,
            start_date,
            end_date,
            datetime_column='lpep_pickup_datetime',
            input_date_included=True
        )

        column_mapping = {
                "VendorID": "vendor_id",
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
                "RatecodeID": "rate_code_id",
                "PULocationID": "pickup_location_id",
                "DOLocationID": "dropoff_location_id",
        }

        # Rename columns based on the mapping
        filtered_df.rename(columns=column_mapping, inplace=True)

        file_name = f"green_tripdata_{start_date.year}-{start_date.month:02d}_transformed.csv"
        output_path = os.path.join("data", "nyc", "processed", file_name)

        # Tambahkan kolom run_date (tanggal hari ini UTC+0)
        filtered_df.to_csv(output_path, index=False)

        logger.info(f"Saved file to: {output_path}")
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        raise

def filter_df_by_period(
    df,
    start_date,
    end_date,
    datetime_column='lpep_pickup_datetime',
    input_date_included=True
):

    if datetime_column not in df.columns:
        logger.error(f"'{datetime_column}' column n/a in DataFrame.")

    df = df.copy()
    df[datetime_column] = pd.to_datetime(df[datetime_column])
    datetime_series = df[datetime_column].dt.date

    if input_date_included:
        mask = (datetime_series >= start_date.date()) & (datetime_series <= end_date.date())
    else:
        mask = (datetime_series > start_date.date()) & (datetime_series < end_date.date())

    filtered_df = df.loc[mask]
    return filtered_df

