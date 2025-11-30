from airflow.models import TaskInstance
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context

from typing import Callable, Tuple, Dict

from services.capstone2.p2.misc import decide_start_end_date
from utils.logger import get_logger

logger = get_logger()

def decide_date_task(config: Dict) -> Callable[[Context], Tuple]:
    
    def _task(**kwargs) -> Tuple[int, int]:
        ti: TaskInstance = kwargs["ti"]
                
        # Assuming decide_year_month is defined elsewhere
        start_date, end_date = decide_start_end_date(config)

        # ✅ Convert to ISO8601 strings before pushing to XCom
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        ti.xcom_push(key="start_date", value=start_date_str)
        ti.xcom_push(key="end_date", value=end_date_str)

        logger.info(f"start_date: {start_date_str}, end_date: {end_date_str}")

        return start_date_str, end_date_str  # useful for logging/debugging
    return _task



