from datetime import datetime, date
import dateutil.relativedelta
import calendar
from pyspark.sql import SparkSession, functions as F


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

def get_data_within_timeframe(spark, table_name, month, time_col = 'event_time', unix_time = True):
    """
    Retrieves records from a specified Spark table within the given timeframe.

    Args:
    spark (SparkSession): The SparkSession object.
    table_name (str): The name of the Spark table to query.
    month (str): The start date of a month in datetime format.

    Returns:
    pyspark.sql.dataframe.DataFrame: A DataFrame containing the records within the specified timeframe.
    """
    # Convert dates to POSIX time (seconds since epoch)
    start_posix,stop_posix = get_start_end_of_month(month)

    # Load the table
    df = spark.table(table_name)
    
    if unix_time == True:
        # Filter records based on event_time posix column
        filtered_df = df.filter((F.col(time_col) >= start_posix) & (F.col(time_col) < stop_posix))
    else: 
        df = df.withColumn('unix_time', F.unix_timestamp(time_col))
        filtered_df = df.filter((F.col('unix_time') >= start_posix) & ((F.col('unix_time') < stop_posix)))
                                
    return filtered_df