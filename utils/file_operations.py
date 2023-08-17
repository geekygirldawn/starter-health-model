# Copyright Dawn M. Foster <dawn@dawnfoster.com>
# MIT License

""" This file contains several functions that perform various file operations 
"""

def output_path(repo_name, org_name):
    """ Creates the path string where files will be located

    Parameters
    ----------
    repo_name : str
    org_name : str

    Returns
    -------
    path : str

    """
    import datetime
    from os.path import dirname, join
    from pathlib import Path
    from utils.date_calcs import get_last_month

    today = datetime.date.today()
    last_month = get_last_month()
    current_year_month = str(last_month.year) + '-' + '{:02d}'.format(last_month.month)

    current_dir = dirname(dirname(__file__)) # the double dirname is equivalent to ../
    rel_path = './output/' + current_year_month + '/' + org_name + '/' + repo_name 
    path = join(current_dir, rel_path)
    Path(path).mkdir(parents=True, exist_ok=True)

    return path

def output_filename(repo_name, org_name, metric_string): 
    """ Creates the string containing the filename where a graph will be created

    Parameters
    ----------
    repo_name : str
    org_name : str
    metric_str : str

    Returns
    -------
    filename : str
    """

    path = output_path(repo_name, org_name)

    filename = path + '/' + repo_name + '_' + metric_string + '.png'

    return filename