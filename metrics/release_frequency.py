# Copyright Dawn M. Foster <dawn@dawnfoster.com>
# MIT License

""" Contains functions used to gather data and graph the Release Frequency metric
"""

def get_release_data(repo_id, start_date, end_date, engine):
    """ Get release data from the Augur database

    Parameters
    ----------
    repo_id : str
    start_date : str
    end_date : str
    engine : sqlalchemy object

    Returns
    -------
    releases_df : dataframe
    """
    import pandas as pd

    releases_df = pd.DataFrame()

    release_query = f"""
                    SELECT
                        release_published_at as date
                    FROM
                        releases
                    WHERE 
                        repo_id = {repo_id}
                        AND release_published_at > {start_date}
                        AND release_published_at <= {end_date}
                        """
    releases_df = pd.read_sql_query(release_query, con=engine)

    return releases_df

def activity_release_data(repo_id, repo_name, org_name, start_date, end_date, engine):
    """ Takes release data and does some reformatting before graphing

    Parameters
    ----------
    repo_id : str
    repo_name : str
    org_name : str
    start_date : str
    end_date : str
    engine : sqlalchemy object

    Returns
    -------
    error_num : int
    error_text : str
    releases_df : dataframe
    start_dt : datetime
    end_dt : datetime
    title : str
    interpretation : str
    release_num : int
    """

    import datetime
    from utils.date_calcs import convert_dates

    try:
        releases_df = get_release_data(repo_id, start_date, end_date, engine)
        error_num = 0
        error_text = None
    except:
        return -1, 'NO DATA', None, None, None, None, None, None

    start_dt, end_dt = convert_dates(start_date, end_date)
    six_mos_dt = end_dt - datetime.timedelta(days=180)

    release_num = 0
    for release in releases_df['date']:
        if (release >= six_mos_dt and release <= end_dt):
            release_num+=1

    # return before creating plots if no release data in past 6 months
    if release_num == 0:
        return -1, 'NO DATA', None, None, None, None, None, None

    title = org_name + "/" + repo_name + "\nRelease Frequency: " + str(release_num) + " releases in the past 6 months."

    interpretation = 'Interpretation: Healthy projects will have frequent releases with security updates, bug fixes, and features.'

    return error_num, error_text, releases_df, start_dt, end_dt, title, interpretation, release_num

def activity_release_graph(repo_id, repo_name, org_name, start_date, end_date, engine):
    """ Graphs the release data returned from the activity_release_data function

    Parameters
    ----------
    repo_id : str
    repo_name : str
    org_name : str
    start_date : str
    end_date : str
    engine : sqlalchemy object

    Output
    ------
    Saves a png file in the location defined in the output_filename function.

    """
    import seaborn as sns
    import matplotlib
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    from utils.file_operations import output_filename

    error_num, error_text, releases_df, start_dt, end_dt, title, interpretation, release_num = activity_release_data(repo_id, repo_name, org_name, start_date, end_date, engine)

    if error_num == -1:
        return -1, 'NO DATA'

    matplotlib.use('Agg') #prevents from tying to send plot to screen
    sns.set(style="whitegrid", font_scale=2)

    fig, ax = plt.subplots()

    # the size of A4 paper
    fig.set_size_inches(24, 8)

    ax.set_xlim(start_dt, end_dt)
    ax.set_ylim(0,2)
    ax.yaxis.set_major_locator(ticker.MultipleLocator(1))
    ax.set(yticklabels=[])

    plottermonth = sns.lineplot(y=1, x='date', data=releases_df, marker="X", linewidth=0, markersize=20).set_title(title, fontsize=30)
    xlabel_str = 'Year Month\n\n' + interpretation
    plottermonthlabels = ax.set_xlabel(xlabel_str)

    filename = output_filename(repo_name, org_name, 'release_frequency')

    fig.savefig(filename, bbox_inches='tight', dpi=500)
    plt.close(fig)

    print('\nActivity Release metric for', org_name, '/', repo_name, '\nfrom', start_date, 'to', end_date, '\nsaved as', filename)
    print(release_num, 'releases in the past 6 months\n')