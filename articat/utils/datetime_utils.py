from datetime import date, datetime
from typing import Union


def convert_to_datetime(dt: Union[datetime, date]) -> datetime:
    # NOTE: isinstance(date.today(), datetime) is True
    return (
        datetime.combine(dt, datetime.min.time())
        if not isinstance(dt, datetime)
        else dt
    )
