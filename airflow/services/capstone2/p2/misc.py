import sys
from datetime import date, datetime, timedelta

from utils.date_handler import calculate_date_range
from utils.logger import get_logger

logger = get_logger()

def decide_start_end_date(config: dict) -> tuple:
    
    run_mode = config.get('run_mode')
    logger.info(f"Running in {run_mode} mode.")

    # Check if run mode is valid
    if run_mode not in ["auto", "manual", "range"]:
        logger.error(f"Invalid run_mode: {run_mode}")
        return

    start_date = config.get("start_date")
    logger.info(f"start_date: {start_date}")
    
    if run_mode == "manual":   
    
        end_date = config.get("end_date")

        # Convert string to datetime objects
        date_format="%Y-%m-%d"
        start_date = datetime.strptime(start_date, date_format)
        end_date = datetime.strptime(end_date, date_format)

        # Check if end_date is earlier
        if end_date < start_date:
            logger.error(f"ERROR: end_date ({end_date}) is earlier than start_date ({start_date}). Exiting program.")
            sys.exit(1)  # Exit with error code 1
        else:
            logger.debug(f"Dates are valid: {start_date} to {end_date}")

    period = config.get("period")
    onward_period = config.get("onward_period")
    backward_period = config.get("backward_period")

    if run_mode == "range":   
        start_date, end_date = calculate_date_range(
            input_date_str=start_date,
            period=period,
            onward_period=onward_period,
            backward_period=backward_period
        )
    
    if run_mode == "auto": # always n-month in backward_period   
        backward_period = config.get("backward_period")

        run_date = date.today()
        # first day of this month
        first_day_current_month = run_date.replace(day=1)

        # last day of previous month = one day before first day of this month
        end_date = first_day_current_month - timedelta(days=1)

        # first day of previous month
        start_date = end_date.replace(day=1)
        
    logger.info(f"start_date: {start_date}")
    logger.info(f"end_date: {end_date}")

    return start_date, end_date

    

