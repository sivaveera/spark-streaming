from pytz import timezone
from datetime import datetime, date, timedelta

us_tz = timezone('America/Los_Angeles')
# us_tz = timezone('EST')
us_time = datetime.now(us_tz)
print(us_time)


def get_current_datetime():
    return us_time.strftime('%Y-%m-%d %H:%M:%S')
