# Copyright Dawn M. Foster <dawn@dawnfoster.com>
# MIT License

""" This file contains several functions that perform various date / datetime
calculations
"""

def get_last_month():
    """Gets the datetime for the month with the last complete month of data
    
    Returns
    -------
    last_month : datetime object
    """
    import datetime 

    current = datetime.date.today()

    first_current = current.replace(day=1)
    last_month = first_current - datetime.timedelta(days=1)

    return(last_month)

def get_dates(days):
    """ Gets the start and end date for the analysis based on the number
    of years to be analyzed (converted to days)"
    
    Parameters
    ----------
    days : int

    Returns
    -------
    start_date : str
    end_date : str
    """
    import datetime 

    last_month = get_last_month()
    end_date = "'" + str(last_month) + "'"

    start_month = last_month - datetime.timedelta(days=days)
    start = (start_month.replace(day=1) + datetime.timedelta(days=32)).replace(day=1) # convert to 1st day of following month
    start_date = "'" + str(start) + "'"

    return start_date, end_date

def convert_to_dt(start_date, end_date):
    """ Converts start and end dates to datetime objects

    Parameters
    ----------
    start_date : str
    end_date : str

    Returns
    -------
    start_dt : datetime
    end_dt : datetime
    
    """
    from datetime import datetime, timezone

    # inputs will be date strings, output tz aware datetime

    end_dt = datetime.strptime(end_date, "'%Y-%m-%d'").replace(tzinfo=timezone.utc)

    start_dt = datetime.strptime(start_date, "'%Y-%m-%d'").replace(tzinfo=timezone.utc)

    return start_dt, end_dt

def convert_dates(start_date, end_date):
    """ Converts start and end dates to datetime objects.

    This is different from the other function that does this, but
    I don't remember why :)

    Parameters
    ----------
    start_date : str
    end_date : str

    Returns
    -------
    start_dt : datetime
    end_dt : datetime
    
    """
    import datetime

    start_dt = datetime.datetime.strptime(start_date[1:11], '%Y-%m-%d')
    end_dt = datetime.datetime.strptime(end_date[1:11], '%Y-%m-%d')

    return start_dt, end_dt 