from datetime import datetime, date
import dateutil.relativedelta
import calendar

def generate_months(start_date, end_date):
    """Generate a list of dates corresponding to the first day of each month between two dates.

    Args:
    start_date (datetime.date): The starting date.
    end_date (datetime.date): The ending date.

    Returns:
    list: A list of date objects for the first day of each month within the specified range.
    """
    current = start_date
    months = []
    while current <= end_date:
        months.append(current)
        # Increment month
        month = current.month
        year = current.year
        if month == 12:
            current = date(year + 1, 1, 1)
        else:
            current = date(year, month + 1, 1)
    return months

def get_start_end_of_month(date):
    """Return a datetime object for the first and last second  of the given month and year."""
    year = date.year
    month = date.month
    
    first_second = datetime(year, month, 1, 0, 0, 0)
    last_day = calendar.monthrange(year, month)[1]
    last_second = datetime(year, month, last_day, 23, 59, 59)
    return first_second.timestamp(), last_second.timestamp()