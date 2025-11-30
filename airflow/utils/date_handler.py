# utils/filter.py

import pandas as pd
import os

from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

from utils.logger import setup_logger

logger = setup_logger()

def calculate_date_range(input_date_str, period, onward_period=0, backward_period=0):
    """
    Hitung start_date dan end_date berdasarkan input_date dan period.
    """
    input_date = pd.to_datetime(input_date_str)

    if onward_period == 0 and backward_period == 0:
        # logger.error("onward_period dan backward_period tidak boleh keduanya nol.")
        start_date = input_date
        end_date = input_date
    else:
        if period == 'daily':
            start_date = input_date - pd.Timedelta(days=backward_period)
            end_date = input_date + pd.Timedelta(days=onward_period)
        elif period == 'weekly':
            start_date = input_date - pd.Timedelta(weeks=backward_period)
            end_date = input_date + pd.Timedelta(weeks=onward_period)
        elif period == 'monthly':
            start_date = input_date - relativedelta(months=backward_period)
            end_date = input_date + relativedelta(months=onward_period)
        else:
            logger.error("Parameter 'period' harus 'daily', 'weekly', atau 'monthly'.")

    return start_date, end_date

def increment_date(current_date, frequency):
    """
    Increment the date based on frequency.

    Args:
        current_date (str): _description_
        frequency (str: 'daily', 'weekly', 'monthly', or 'first_day'

    Raises:
        ValueError: _description_

    Returns:
        _type_: _description_
    """
    if frequency == "daily":
        return current_date + timedelta(days=1)
    elif frequency == "weekly":
        return current_date + timedelta(weeks=1)
    elif frequency in ["monthly", "first_day"]:
        return current_date + relativedelta(months=1)
    else:
        raise ValueError(f"Unsupported frequency: {frequency}")


def get_previous_month_year(n_months_back: int = 3):
    # Get today's date
    today = datetime.today()
    
    # Calculate the date n months ago
    new_date = today - relativedelta(months=n_months_back)
    
    # Extract the year and month
    return new_date.month, new_date.year