# Utility functions

def get_current_time(timezone: str = None):
    """Get current timestamp in configured timezone"""
    from datetime import datetime
    import pytz
    from config import TIMEZONE

    tz_name = timezone or TIMEZONE
    try:
        tz = pytz.timezone(tz_name)
        return datetime.now(tz)
    except:
        # Fallback to UTC if timezone is invalid
        return datetime.now(pytz.UTC)