from src.utils.DatetimeUtils import *


class TestCommonUtils(object):

    def test_get_current_datetime_object_match(self):
        from pytz import timezone
        from datetime import datetime, date, timedelta
        us_tz = timezone('America/Los_Angeles')
        us_time = datetime.now(us_tz)

        expected_dt = us_time.strftime('%Y-%m-%d %H:%M:%S')

        curr_dt = get_current_datetime()
        assert (curr_dt == expected_dt)

    def test_get_current_datetime_not_null(self):
        from pytz import timezone
        from datetime import datetime, date, timedelta
        us_tz = timezone('America/Los_Angeles')
        us_time = datetime.now(us_tz)

        expected_dt = us_time.strftime('%Y-%m-%d %H:%M:%S')

        curr_dt = get_current_datetime()
        assert (curr_dt is not None)
