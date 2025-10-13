# Utility functions

def get_current_time():
    """Get current timestamp in configured timezone"""
    from datetime import datetime
    import pytz
    from config import TIMEZONE

    try:
        tz = pytz.timezone(TIMEZONE)
        return datetime.now(tz)
    except:
        # Fallback to UTC if timezone is invalid
        return datetime.now(pytz.UTC)