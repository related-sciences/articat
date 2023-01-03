from datetime import date, datetime


def convert_to_datetime(dt: datetime | date) -> datetime:
    # NOTE: isinstance(date.today(), datetime) is True
    return (
        datetime.combine(dt, datetime.min.time())
        if not isinstance(dt, datetime)
        else dt
    )
