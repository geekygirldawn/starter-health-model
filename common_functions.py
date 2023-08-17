# Copyright 2023 CHAOSS Project
# Author: Dawn M. Foster <dawn@dawnfoster.com>

"""Common Functions
This file contains some common functions that are used within
the other scripts in this repo.

Note: The functions that pull the data from Augur are always separate
from the functions that graph the data so that you could choose to 
collect the data and use another tool to generate the graphs and
display the data. The data collection functions have names ending
in '_data' while graphing functions have names ending in '_graph'

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

def read_key(file_name):
    """Retrieves a GitHub API key from a file.
    
    Parameters
    ----------
    file_name : str

    Returns
    -------
    key : str
    """
    from os.path import dirname, join

    # Reads the first line of a file containing the GitHub API key
    # Usage: key = read_key('gh_key')

    current_dir = dirname(__file__)
    file2 = "./" + file_name
    file_path = join(current_dir, file2)

    with open(file_path, 'r') as kf:
        key = kf.readline().rstrip() # remove newline & trailing whitespace
    return key

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

    today = datetime.date.today()
    last_month = get_last_month()
    current_year_month = str(last_month.year) + '-' + '{:02d}'.format(last_month.month)

    current_dir = dirname(__file__)
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

def fork_archive(repo_name_orig, org_name, engine):
    """ Check whether a repo is a fork or an archived project.
    Parameters
    ----------
    repo_name_orig : str
    org_name : str
    engine : sqlalchemy database object

    Returns
    -------
    is_forked : Boolean
    is_archived : Boolean
    """

    import pandas as pd

    repo_git = "'" + 'https://github.com/' + org_name + '/' + repo_name_orig + "'"
    repo_name = "'" + repo_name_orig + "'"

    repo_df = pd.DataFrame()
    repo_df_query = f"""
            SELECT forked_from, repo_archived from repo
            WHERE repo_name = {repo_name}
            AND repo_git = {repo_git}
            """
    repo_df = pd.read_sql_query(repo_df_query, con=engine)
    forked = repo_df.forked_from[0]
    archived = repo_df.repo_archived[0]
    
    is_archived = 'ERROR'
    
    if forked != 'Parent not available':
        is_forked = True
    else:
        is_forked = False
    
    if archived == 1:
       is_archived = True
    elif archived == 0:
       is_archived = False

    return is_forked, is_archived

def repo_api_call(repo_name, org_name):
    """ Uses the GitHub REST API to get a repository object that can be
    used in other functions

    Parameters
    ----------
    repo_name : str
    org_name : str

    Returns
    -------
    repo : GitHub repository object
    """
    from github import Github
    import sys

    try:
        gh_key = read_key('gh_key')
        g = Github(gh_key)

        repo = g.get_repo(org_name + '/' + repo_name)

    except:
        print("Error making GH API call for", org_name, repo_name, "Rate limit remaining", g.rate_limiting[0])
        if g.rate_limiting[0] < 5:
            print("Exiting due to rate limit")
            sys.exit()
        else:
            repo = False

    return repo

def get_release_data(repo_api):
    """ Uses the GitHub REST API repository object to collect release data

    Parameters
    ----------
    repo_api : GitHub repository object

    Returns
    -------
    releases_df : dataframe
    """
    import pandas as pd

    releases = repo_api.get_releases()

    releases_df = pd.DataFrame(
        [x, x.tag_name, x.published_at, 1] for x in releases
    )
    releases_df.columns = ['release', 'name', 'date', 'releases']

    return releases_df

def activity_release_data(repo_name, org_name, start_date, end_date, repo_api):
    """ Gathers release data from the GitHub API

    Parameters
    ----------
    repo_name : str
    org_name : str
    start_date : str
    end_date : str
    repo_api : GitHub repository object

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

    try:
        releases_df = get_release_data(repo_api)
        error_num = 0
        error_text = None
    except:
        return -1, 'NO DATA', None, None, None, None, None, None, None, None

    start_dt, end_dt = convert_dates(start_date, end_date)
    six_mos_dt = end_dt - datetime.timedelta(days=180)

    release_num = 0
    for release in releases_df['date']:
        if (release >= six_mos_dt and release <= end_dt):
            release_num+=1

    # return before creating plots if no release data in past 6 months
    if release_num == 0:
        return -1, 'NO DATA', None, None, None, None, None, None, None, None

    title = org_name + "/" + repo_name + "\n" + str(release_num) + " releases in the past 6 months."

    interpretation = 'Interpretation: Healthy projects will have frequent releases with security updates, bug fixes, and features.'

    return error_num, error_text, releases_df, start_dt, end_dt, title, interpretation, release_num

def activity_release_graph(repo_name, org_name, start_date, end_date, repo_api):
    """ Graphs the release data returned from the activity_release_data function

    Parameters
    ----------
    repo_name : str
    org_name : str
    start_date : str
    end_date : str
    repo_api : GitHub repository object

    Output
    ------
    Saves a png file in the location defined in the output_filename function.

    """
    import seaborn as sns
    import matplotlib
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker

    error_num, error_text, releases_df, start_dt, end_dt, title, interpretation, release_num = activity_release_data(repo_name, org_name, start_date, end_date, repo_api)

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

    plottermonth = sns.lineplot(y='releases', x='date', data=releases_df, marker="X", linewidth=0, markersize=20).set_title(title, fontsize=30)
    xlabel_str = 'Year Month\n\n' + interpretation
    plottermonthlabels = ax.set_xlabel(xlabel_str)

    filename = output_filename(repo_name, org_name, 'activity_release')

    fig.savefig(filename, bbox_inches='tight')
    plt.close(fig)

    print('\nActivity Release metric for', org_name, '/', repo_name, '\nfrom', start_date, 'to', end_date, '\nsaved as', filename)
    print(release_num, 'releases in the past 6 months\n')

def monthly_prs_closed(repo_id, repo_name, start_date, end_date, engine):
    """ Gets data about the PRs closed for every month

    Parameters
    ----------
    repo_id : str
    repo_name : str
    start_date : str
    end_date : str
    engine : sqlalchemy object

    Returns
    -------
    pr_monthdf : dataframe
    """
    import pandas as pd

    pr_monthDF = pd.DataFrame()
    pr_monthquery = f"""
                    SELECT
                        * 
                    FROM
                        (
                        SELECT
                            date_part( 'year', month :: DATE ) AS YEAR,
                            date_part( 'month', month :: DATE ) AS month 
                        FROM
                            ( SELECT * FROM ( SELECT month :: DATE FROM generate_series ( TIMESTAMP {start_date}, TIMESTAMP {end_date}, INTERVAL '1 month' ) month ) d ) x 
                        ) y
                        LEFT OUTER JOIN (
                        SELECT
                            repo_id,
                            repo_name,
                            repo_group,
                            date_part( 'year', pr_created_at :: DATE ) AS YEAR,
                            date_part( 'month', pr_created_at :: DATE ) AS month,
                            COUNT ( pr_src_id ) AS total_prs_open_closed 
                        FROM
                            (
                            SELECT
                                repo.repo_id AS repo_id,
                                repo.repo_name AS repo_name,
                                repo_groups.rg_name AS repo_group,
                                pull_requests.pr_created_at AS pr_created_at,
                                pull_requests.pr_closed_at AS pr_closed_at,
                                pull_requests.pr_src_id AS pr_src_id
                            FROM
                                repo,
                                repo_groups,
                                pull_requests 
                            WHERE
                                repo.repo_group_id = repo_groups.repo_group_id 
                                AND repo.repo_id = pull_requests.repo_id 
                                AND repo.repo_id = {repo_id} 
                                AND pull_requests.pr_src_state = 'closed'  
                            ) L 
                        GROUP BY
                            L.repo_id,
                            L.repo_name,
                            L.repo_group,
                            YEAR,
                            month 
                        ORDER BY
                            repo_id,
                            YEAR,
                            month 
                        ) T USING ( month, YEAR ) 
                    ORDER BY
                        YEAR,
                        month;

        """
    pr_monthDFa = pd.read_sql_query(pr_monthquery, con=engine)

    pr_monthDFa[['repo_id']] = pr_monthDFa[['repo_id']].fillna(value=repo_id)
    
    # Hack to fill in repo_name where there are nan's
    pr_monthDFa[['repo_name']] = pr_monthDFa[['repo_name']].fillna(value=repo_name)
    
    pr_monthDF = pr_monthDFa
    pr_monthDF.set_index('repo_id', 'year', 'month')


    pr_monthDF[['total_prs_open_closed']] = pr_monthDF[['total_prs_open_closed']].fillna(0)

    pr_monthDF['year'] = pr_monthDF['year'].map(int)
    pr_monthDF['month'] = pr_monthDF['month'].map(int)
    pr_monthDF['month'] = pr_monthDF['month'].apply('{:0>2}'.format)
    pr_monthDF['yearmonth'] = pr_monthDF['year'].map(str) + '-' + pr_monthDF['month'].map(str)

    return pr_monthDF

def monthly_prs_all(repo_id, repo_name, start_date, end_date, engine):
    """ Gets data about all of the PRs each month (closed and open)

    Parameters
    ----------
    repo_id : str
    repo_name : str
    start_date : str
    end_date : str
    engine : sqlalchemy object

    Returns
    -------
    pr_monthDF : dataframe
    """

    import pandas as pd

    pr_monthDF = pd.DataFrame()

    pr_monthquery = f"""
                    SELECT
                        * 
                    FROM
                        (
                        SELECT
                            date_part( 'year', month :: DATE ) AS YEAR,
                            date_part( 'month', month :: DATE ) AS month 
                        FROM
                            ( SELECT * FROM ( SELECT month :: DATE FROM generate_series ( TIMESTAMP {start_date}, TIMESTAMP {end_date}, INTERVAL '1 month' ) month ) d ) x 
                        ) y
                        LEFT OUTER JOIN (
                        SELECT
                            repo_id,
                            repo_name,
                            repo_group,
                            date_part( 'year', pr_created_at :: DATE ) AS YEAR,
                            date_part( 'month', pr_created_at :: DATE ) AS month,
                            COUNT ( pr_src_id ) AS total_prs_open_closed 
                        FROM
                            (
                            SELECT
                                repo.repo_id AS repo_id,
                                repo.repo_name AS repo_name,
                                repo_groups.rg_name AS repo_group,
                                pull_requests.pr_created_at AS pr_created_at,
                                pull_requests.pr_closed_at AS pr_closed_at,
                                pull_requests.pr_src_id AS pr_src_id
                            FROM
                                repo,
                                repo_groups,
                                pull_requests 
                            WHERE
                                repo.repo_group_id = repo_groups.repo_group_id 
                                AND repo.repo_id = pull_requests.repo_id 
                                AND repo.repo_id = {repo_id} 
                            ) L 
                        GROUP BY
                            L.repo_id,
                            L.repo_name,
                            L.repo_group,
                            YEAR,
                            month 
                        ORDER BY
                            repo_id,
                            YEAR,
                            month 
                        ) T USING ( month, YEAR ) 
                    ORDER BY
                        YEAR,
                        month;

        """
    pr_monthDFa = pd.read_sql_query(pr_monthquery, con=engine)

    pr_monthDFa[['repo_id']] = pr_monthDFa[['repo_id']].fillna(value=repo_id)
    
    # Hack to fill in repo_name where there are nan's
    pr_monthDFa[['repo_name']] = pr_monthDFa[['repo_name']].fillna(value=repo_name)
    
    pr_monthDF = pr_monthDFa
    pr_monthDF.set_index('repo_id', 'year', 'month')

    pr_monthDF[['total_prs_open_closed']] = pr_monthDF[['total_prs_open_closed']].fillna(0)

    return pr_monthDF

def sustain_prs_by_repo_data(repo_id, repo_name, org_name, start_date, end_date, engine):
    """ Processes data from the queries in the monthly_prs_all and monthly_prs_closed
    functions and manipulates them into a format that can be used to easily graph them.

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
    pr_sustainDF : dataframe
    title : str
    interpretation : str
    month_num : int
    """

    import pandas as pd

    all_prsDF = monthly_prs_all(repo_id, repo_name, start_date, end_date, engine)

    # Return with no data if there are no PRs
    if all_prsDF['total_prs_open_closed'].sum() < 24:
        return -1, 'TOO FEW PRs', None, None, None, None, None, None
    else:
        error_num = 0
        error_text = None

    closed_prsDF = monthly_prs_closed(repo_id, repo_name, start_date, end_date, engine)

    pr_sustainDF = pd.DataFrame()

    pr_sustainDF['yearmonth'] = closed_prsDF['yearmonth']
    pr_sustainDF['repo_name'] = closed_prsDF['repo_name']
    pr_sustainDF['repo_id'] = closed_prsDF['repo_id']
    pr_sustainDF['closed_total'] = closed_prsDF['total_prs_open_closed']

    pr_sustainDF['all_total'] = all_prsDF['total_prs_open_closed']
    pr_sustainDF['diff'] = pr_sustainDF['all_total'] - pr_sustainDF['closed_total']
    pr_sustainDF['diff_per'] = pr_sustainDF['diff'] / pr_sustainDF['all_total']

    pr_sustainDF['repo_id'] = pr_sustainDF['repo_id'].map(int)
    pr_sustainDF.set_index('repo_id', 'yearmonth')

    month_num = 0
    m = 1
    for diff_per in pr_sustainDF['diff_per']:
        if (diff_per > 0.15 and m > 6):
            month_num+=1
        m+=1

    title = org_name + "/" + pr_sustainDF['repo_name'][0] + "\nSustains and Keeps up with Contributions"

    recent_mo_diff = pr_sustainDF['diff'][9] + pr_sustainDF['diff'][10] + pr_sustainDF['diff'][11]
    prev_mo_diff = pr_sustainDF['diff'][6] + pr_sustainDF['diff'][7] + pr_sustainDF['diff'][8]
    recent_yearmonth = pr_sustainDF['yearmonth'][9] + ' - ' + pr_sustainDF['yearmonth'][11]
    prev_yearmonth = pr_sustainDF['yearmonth'][6] + ' - ' + pr_sustainDF['yearmonth'][8]   

    magnitude = abs(prev_mo_diff - recent_mo_diff) / (prev_mo_diff + recent_mo_diff)

    if magnitude <= .1:
        trend = '\nTrend: Neutral - the ' + recent_yearmonth + ' gap is similar to the ' + prev_yearmonth + ' gap.'
    elif recent_mo_diff == prev_mo_diff:
        trend = '\nTrend: Neutral - the ' + recent_yearmonth + ' gap is similar to the ' + prev_yearmonth + ' gap.'
    elif recent_mo_diff < prev_mo_diff:
        trend = '\nTrend: Positive - the ' + recent_yearmonth + ' gap is smaller than the ' + prev_yearmonth + ' gap.' 
    elif recent_mo_diff > prev_mo_diff:
        trend = '\nTrend: Negative - the ' + recent_yearmonth + ' gap is larger than the ' + prev_yearmonth + ' gap.'
    else:
        trend = ''
    title += trend

    interpretation = 'Interpretation: Healthy projects will have little or no gap. A large or increasing gap requires attention.'

    return error_num, error_text, pr_sustainDF, title, interpretation, month_num  

def sustain_prs_by_repo_graph(repo_id, repo_name, org_name, start_date, end_date, engine):
    """ Graph the data returned by the sustain_prs_by_repo_data function

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
    import pandas as pd
    import seaborn as sns
    import matplotlib
    import matplotlib.pyplot as plt
    import datetime
    from matplotlib.ticker import MaxNLocator
    import warnings

    warnings.simplefilter("ignore") # Ignore fixed formatter warning.

    error_num, error_text, pr_sustainDF, title, interpretation, month_num = sustain_prs_by_repo_data(repo_id, repo_name, org_name, start_date, end_date, engine)

    if error_num == -1:
        return -1, 'TOO FEW PRs'

    matplotlib.use('Agg') #prevents from tying to send plot to screen
    sns.set_style('ticks')
    sns.set(style="whitegrid", font_scale=2)

    fig, ax = plt.subplots()
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))

    # the size of A4 paper
    fig.set_size_inches(24, 8)

    plottermonth = sns.lineplot(x='yearmonth', y='all_total', data=pr_sustainDF, sort=False, color='black', label='Total', linewidth=2.5)
    plottermonth = sns.lineplot(x='yearmonth', y='closed_total', data=pr_sustainDF, sort=False, color='green', label='Closed', linewidth=2.5, linestyle='dashed').set_title(title, fontsize=30)

    plottermonthlabels = ax.set_xticklabels(pr_sustainDF['yearmonth'],rotation=45)
    plottermonthlabels = ax.set_ylabel('Number of PRs')
    xlabel_str = 'Year Month\n\n' + interpretation
    plottermonthlabels = ax.set_xlabel(xlabel_str)

    filename = output_filename(repo_name, org_name, 'sustains_pr')

    fig.savefig(filename, bbox_inches='tight')
    plt.close(fig)

    print('\nSustaining and keeping up with contributions for', org_name, '/', repo_name, '\nfrom', start_date, 'to', end_date, '\nsaved as', filename)
    print('Number of months in the past 6 months with > 15% of PRs not closed', month_num, '\n')

def commit_author_data(repo_id, start_date, end_date, engine):
    """ Gets data about the number of commits from each author

    Parameters
    ----------
    repo_id : str
    start_date : str
    end_date : str
    engine : sqlalchemy object

    Returns
    -------
    authorDF : dataframe
    """
    import pandas as pd

    start_date, end_date = convert_to_dt(start_date, end_date)

    #Commit data - from humans excluding known bots
    commitsDF = pd.DataFrame()
    commitsquery = f"""
                    SELECT
                        DISTINCT(cmt_commit_hash),
                        contributors.cntrb_canonical,
                        canonical_full_names.cntrb_full_name AS canonical_full_name,
                        cmt_author_name, cmt_author_email, repo_id, cmt_author_timestamp 
                    FROM commits 
                        LEFT OUTER JOIN contributors ON cntrb_email = cmt_author_email left outer join 
                        (
                            SELECT distinct on (cntrb_canonical) cntrb_full_name, cntrb_canonical, data_collection_date
                            FROM contributors
                            WHERE cntrb_canonical = cntrb_email
                            order by cntrb_canonical
                        ) canonical_full_names on canonical_full_names.cntrb_canonical = contributors.cntrb_canonical
                    WHERE 
                        repo_id = {repo_id}
                        AND cmt_author_name NOT LIKE 'snyk%%'
                        AND cmt_author_name NOT LIKE '%%bot'
                        AND cmt_author_name NOT LIKE '%%Bot'
                        AND cmt_author_name NOT LIKE '%%BOT'
                        AND cmt_author_name NOT LIKE 'dependabot%%'
                        AND cmt_author_name NOT LIKE 'gerrit%%'
                        AND cmt_author_name NOT LIKE '%%utomation%%'
                        AND cmt_author_name NOT LIKE '%%ipeline%%'
                        AND cmt_author_name NOT LIKE '%%Cloud Foundry%%'
                        AND cmt_author_name != 'cfcr'
                        AND cmt_author_name != 'CFCR'
                        AND cmt_author_name != 'Travis CI'
                        AND cmt_author_name != 'Bitnami Containers'
                        AND cmt_author_name != 'Cloud Foundry London'
                        AND cmt_author_name != 'Spring Operator'
                        AND cmt_author_name != 'Spring Buildmaster'
                        AND cmt_author_name != 'pivotal-rabbitmq-ci'
                    ORDER BY
                        cntrb_canonical;
                    """
    
    all_commitsDF = pd.read_sql_query(commitsquery, con=engine)
    commitsDF = all_commitsDF[(all_commitsDF['cmt_author_timestamp'] >= start_date) & (all_commitsDF['cmt_author_timestamp'] <= end_date)]
    total_commits = commitsDF.cmt_commit_hash.nunique()    

    dedupDF = commitsDF.drop_duplicates(subset=['cmt_commit_hash'], inplace = False, keep="first")

    authorDF = pd.DataFrame()

    # Count values by email as a starting point to get number of commits.
    authorDF = dedupDF.cmt_author_email.value_counts()
    authorDF = authorDF.reset_index()
    authorDF.columns = ['name', 'commits']
    for email in authorDF['name']:
        name = dedupDF.loc[dedupDF['cmt_author_email'] == email]['cmt_author_name'].iloc[0]
        authorDF.loc[(authorDF.name == email),'name'] = name
    # Then group by name for people using the same name, but multiple email addresses
    # as the final aggregation before calculating percent
    authorDF = authorDF.groupby('name').sum().reset_index().sort_values('commits', ascending=False)
    authorDF['percent'] = authorDF['commits'] / total_commits     

    return authorDF

def contributor_risk_data(repo_id, repo_name, org_name, start_date, end_date, engine):
    """ Gathers data about the top contributors (by commit) - no more than 8 contributors

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
    names : list
    percents : list
    commits : list
    title : str
    interpretation : str
    num_people : int
    """
    import pandas as pd
    import textwrap

    authorDF = commit_author_data(repo_id, start_date, end_date, engine)

    cum_percent = 0
    people_list = []

    i = 1
    num_people = 0

    for item in authorDF.iterrows():
        name = item[1]['name']
        percent = item[1]['percent']
        commits = item[1]['commits']

        cum_percent += percent

        people_list.append([name, percent, commits])

        if (cum_percent > .70 and num_people == 0):
            num_people = i

        if i == 8:
            if cum_percent <= .70:
                num_people = i
            break
        i+=1

    risk_list = []

    for person in people_list:
        name = person[0]
        if len(name) > 15:
            new_name = textwrap.wrap(name, 15)
            name = '\n'.join(new_name)
        percent = person[1]
        commits = person[2]
        risk_list.append([name, percent, commits])

    # Exit early if num_people is 0
    if num_people == 0:
        return -1, 'NO DATA', None, None, None, None, None, None, None, None, None
    else:
        error_num = 0
        error_text = 'NA'

    names = [item[0] for item in risk_list]
    percents = [item[1] for item in risk_list]
    commits = [item[2] for item in risk_list]

    # reformat dates
    start = start_date.replace("'", '')
    end = end_date.replace("'", '')

    title = org_name + "/" + repo_name + "\nContributor Risk for commits from " + start + " to " + end + ": "

    interpretation = 'Interpretation: Healthy projects should have enough people making commits to\nsustain the project even if a top contributor leaves.'

    return error_num, error_text, names, percents, commits, title, interpretation, num_people

def contributor_risk_graph(repo_id, repo_name, org_name, start_date, end_date, engine):
    """ Graphs data from the contributor_risk_data function

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

    error_num, error_text, names, percents, commits, title, interpretation, num_people = contributor_risk_data(repo_id, repo_name, org_name, start_date, end_date, engine)

    if error_num == -1:
        return -1, 'NO DATA'

    matplotlib.use('Agg') #prevents from tying to send plot to screen
    sns.set_style('ticks')
    sns.set(style="whitegrid", font_scale=2)
 
    fig, ax = plt.subplots()
 
    # the size of A4 paper
    fig.set_size_inches(24, 8)

    risk_bar = sns.barplot(x=names, y=commits).set_title(title, fontsize=30)

    risk_bar_labels = ax.set_xticklabels(names, wrap=True)
    risk_bar_labels = ax.set_ylabel('Commits')
    xlabel_str = '\nKey Contributors\n\n' + interpretation
    risk_bar_labels = ax.set_xlabel(xlabel_str)

    i = 0
    for p in ax.patches:
        ax.annotate("{:.0%}".format(percents[i]), (p.get_x() + p.get_width() / 2., p.get_height()),
            ha='center', va='center', color='gray', xytext=(0, 20),
            textcoords='offset points')
        i+=1

    filename = output_filename(repo_name, org_name, 'contrib_risk_commits')

    fig.savefig(filename, bbox_inches='tight')
    plt.close(fig)

    print('\nContributor Risk for', org_name, '/', repo_name, '\nfrom', start_date, 'to', end_date, '\nsaved as', filename)
    print(num_people, 'people make up > 70% of the commits in the past year\n')

def response_time_db(repo_id, repo_name, start_date, end_date, engine):
    """ Gather data about PR reponse times

    Parameters
    ----------
    repo_id : str
    repo_name : str
    start_date : str
    end_date : str
    engine : sqlalchemy object

    Returns
    -------
    pr_all : dataframe
    """
    import pandas as pd
    import sqlalchemy as s

    pr_all = pd.DataFrame()

    # This query gets first_comment, merged, and closed times, since
    # merge and close can be first response for trivial PRs.
    pr_response = pd.DataFrame()

    pr_query = s.sql.text(f"""
                        SELECT pull_requests.pull_request_id, pull_requests.pr_created_at,
                               pull_requests.pr_merged_at, pull_requests.pr_closed_at,
                               MIN(message.msg_timestamp) AS first_comment_time
                        FROM repo, 
                               pull_requests left outer join pull_request_message_ref 
                               on pull_requests.pull_request_id = pull_request_message_ref.pull_request_id
                               left outer join message on pull_request_message_ref.pr_message_ref_src_comment_id = message.platform_msg_id and cntrb_id not in (select cntrb_id from contributors where cntrb_login like '%[bot]')
                        WHERE repo.repo_id = {repo_id}
                               AND repo.repo_id = pull_requests.repo_id                  
                               AND pull_requests.pr_created_at > {start_date}
                               AND pull_requests.pr_created_at <= {end_date}
                        GROUP BY pull_requests.pull_request_id
                        """)
    pr_response = pd.read_sql(pr_query, con=engine)
    
    # This query gets first review time
    pr_first_review = pd.DataFrame()

    pr_query = s.sql.text(f"""
                     SELECT 
                         pull_requests.pull_request_id,
                         MIN(pull_request_reviews.pr_review_submitted_at) as first_review
                     FROM 
                         repo, pull_request_reviews, pull_requests
                     WHERE 
                         repo.repo_id = {repo_id}
                         AND repo.repo_id = pull_requests.repo_id
                         AND pull_request_reviews.pull_request_id = pull_requests.pull_request_id                 
                         AND pull_requests.pr_created_at > {start_date}
                         AND pull_requests.pr_created_at <= {end_date}
                     GROUP BY
                         pull_requests.pull_request_id
                      """)
    pr_first_review = pd.read_sql(pr_query, con=engine)

    # combine dataframes and find the first response from all 4 sources
    pr_all = pd.merge(pr_response,pr_first_review,how='outer',on='pull_request_id')

    pr_all['repo_name'] = repo_name

    #This stack / groupby drops the missing values while calculating min
    pr_all['first_response_time'] = pr_all[['pr_merged_at','pr_closed_at', 'first_comment_time', 'first_review']].stack().dropna().groupby(level=0).min()

    return pr_all

def response_time_data(repo_id, repo_name, org_name, start_date, end_date, engine):
    """ Process the data from the queries in the response_time_db function to calculate
    which ones are in / out of guidelines for the number of business days specified

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
    first_response : dataframe
    title : str
    interpretation : str
    month_num : int
    """
    import pandas as pd
    import numpy as np
    import datetime
    from dateutil.relativedelta import relativedelta
    from pandas.tseries.offsets import BusinessDay

    pr_all = response_time_db(repo_id, repo_name, start_date, end_date, engine)

    bd = pd.tseries.offsets.BusinessDay(n = 2)

    # Don't gather data if less than 24 PRs
    # Or if non_null count is 0
    if len(pr_all) < 24:
        return -1, 'TOO FEW PRs', None, None, None, None, None, None
    elif pr_all['first_response_time'].count() == 0:
        return -1, 'PR COMMENTS MISSING', None, None, None, None, None, None
    else:
        error_num = 0
        error_text = None

    # Exit if diff can't be calculate (usu no responses)
    try:
#        pr_all['diff'] = pr_all.first_response_time.subtract(pr_all.pr_created_at, fill_value=0)
        pr_all['diff'] = pr_all.first_response_time - pr_all.pr_created_at
        pr_all['2_bus_days'] = pr_all.pr_created_at + bd
        pr_all['yearmonth'] = pr_all['pr_created_at'].dt.strftime('%Y-%m')
        pr_all['in_guidelines'] = np.where(pr_all['2_bus_days'] < pr_all['first_response_time'], 0, 1)
        error_num = 0
        error_text = 'NA'

    except:
        return -1, 'NO DATA', pr_all, '', '', '', -1, -1

    year_month_list = pr_all.yearmonth.unique()
    year_month_list.sort()
    first_response = pr_all.groupby(['repo_name', 'yearmonth'], as_index=False).sum()[['repo_name', 'yearmonth', 'in_guidelines']]

    # counts total number of PRs each month
    total_by_month = pr_all.groupby(['repo_name', 'yearmonth'], as_index=False).count()[['repo_name', 'yearmonth', 'pr_created_at']]

    first_response['total_prs'] = total_by_month['pr_created_at']
    first_response['out_guidelines'] = first_response['total_prs'] - first_response['in_guidelines']
    first_response['in_percent'] = first_response['in_guidelines'] / first_response['total_prs']
    first_response['out_percent'] = first_response['out_guidelines'] / first_response['total_prs']

    # This chunk adds rows to the dataframe for months where there was no activity
    # Only executes for dataframes containing less than 12 months

    if len(first_response) < 12:
        start_mo = datetime.datetime.strptime(start_date, "'%Y-%m-%d'").date()
        end_mo = datetime.datetime.strptime(end_date, "'%Y-%m-%d'").date()

        while start_mo < end_mo:
            year_month = str(start_mo)[0:7]
            if first_response[first_response['yearmonth'].str.contains(year_month)].empty:
                row = [repo_name, year_month, 0,0,0,0,0]
                first_response.loc[len(first_response)] = row

            start_mo += relativedelta(months=1)

        first_response = first_response.sort_values('yearmonth')

    month_num = 0
    six_months = str(datetime.date.today() + relativedelta(months=-7)) # 7 because we don't gather current partial month data
    for item in first_response.iterrows():
        year_month = item[1]['yearmonth']
        percent = item[1]['out_percent']
        if (percent > 0.15 and year_month >= six_months):
            month_num+=1

    title = org_name + "/" + repo_name + "\nTimely Responses"

    recent_mo = first_response['out_guidelines'][9] + first_response['out_guidelines'][10] + first_response['out_guidelines'][11]
    prev_mo = first_response['out_guidelines'][6] + first_response['out_guidelines'][7] + first_response['out_guidelines'][8]
    recent_yearmonth = first_response['yearmonth'][9] + ' - ' + first_response['yearmonth'][11] 
    prev_yearmonth = first_response['yearmonth'][6] + ' - ' + first_response['yearmonth'][8] 

    magnitude = abs(prev_mo - recent_mo) / (prev_mo + recent_mo)

    if magnitude <= .1:
        trend = '\nTrend: Neutral - the ' + recent_yearmonth + ' gap is similar to the ' + prev_yearmonth + ' gap.'
    elif recent_mo == prev_mo:
        trend = '\nTrend: Neutral - the ' + recent_yearmonth + ' gap is similar to the ' + prev_yearmonth + ' gap.'
    elif recent_mo < prev_mo:
        trend = '\nTrend: Positive - the ' + recent_yearmonth + ' gap is smaller than the ' + prev_yearmonth + ' gap.'
    elif recent_mo > prev_mo:
        trend = '\nTrend: Negative - the ' + recent_yearmonth + ' gap is larger than the ' + prev_yearmonth + ' gap.'
    else:
        trend = ''
    title += trend

    interpretation = 'Interpretation: Healthy projects will have little or no gap. A large or increasing gap requires attention.'
    
    return error_num, error_text, first_response, title, interpretation, month_num

def response_time_graph(repo_id, repo_name, org_name, start_date, end_date, engine):
    """ Graphs the data from the response_time_data function

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
    import pandas as pd
    import numpy as np
    import seaborn as sns
    import matplotlib
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator
    import warnings
    
    warnings.simplefilter("ignore") # Ignore fixed formatter warning.

    error_num, error_text, first_response, title, interpretation, month_num = response_time_data(repo_id, repo_name, org_name, start_date, end_date, engine)

    # Don't gather data if less than 24 PRs
    if error_num == -1:
        return -1, error_text

    sns.set_style('ticks')
    sns.set(style="whitegrid", font_scale=2)

    fig, ax = plt.subplots()
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))

    # the size of A4 paper
    fig.set_size_inches(24, 8)

    plottermonth = sns.lineplot(x='yearmonth', y='total_prs', data=first_response, sort=False, color='black', label='Total', linewidth=2.5)
    plottermonth = sns.lineplot(x='yearmonth', y='in_guidelines', data=first_response, sort=False, color='green', label='Response < 2 bus days', linewidth=2.5, linestyle='dashed').set_title(title, fontsize=30) 

    plottermonthlabels = ax.set_xticklabels(first_response['yearmonth'],rotation=45)
    plottermonthlabels = ax.set_ylabel('Number of PRs')
    interpretation_str = 'Year Month\n\n' + interpretation
    plottermonthlabels = ax.set_xlabel(interpretation_str)

    filename = output_filename(repo_name, org_name, 'first_response_pr')

    fig.savefig(filename, bbox_inches='tight')
    plt.close(fig)

    print('\nTime to first response for', org_name, '/', repo_name, '\nfrom', start_date, 'to', end_date, '\nsaved as', filename)
    print(month_num, 'months with more than 10% of pull requests not responded to within 2 business days in the past 6 months\n')



