# Copyright Dawn M. Foster <dawn@dawnfoster.com>
# MIT License

""" Contains functions used to gather data and graph the Change Request Closure
Ratio metric
"""

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
        return -1, 'TOO FEW PRs', None, None, None, None
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
    from utils.file_operations import output_filename

    warnings.simplefilter("ignore") # Ignore fixed formatter warning.

    error_num, error_text, pr_sustainDF, title, interpretation, month_num = sustain_prs_by_repo_data(repo_id, repo_name, org_name, start_date, end_date, engine)

    if error_num == -1:
        print("Closure Ratio: Too few PRs to calculate")
        return

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

    fig.savefig(filename, bbox_inches='tight', dpi=500)
    plt.close(fig)

    print('\nSustaining and keeping up with contributions for', org_name, '/', repo_name, '\nfrom', start_date, 'to', end_date, '\nsaved as', filename)
    print('Number of months in the past 6 months with > 15% of PRs not closed', month_num, '\n')
