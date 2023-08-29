# Copyright Dawn M. Foster <dawn@dawnfoster.com>
# MIT License

""" Contains functions used to gather data and graph the Time to First Response metric
"""

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
    from utils.file_operations import output_filename
    
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

    fig.savefig(filename, bbox_inches='tight', dpi=500)
    plt.close(fig)

    print('\nTime to first response for', org_name, '/', repo_name, '\nfrom', start_date, 'to', end_date, '\nsaved as', filename)
    print(month_num, 'months with more than 10% of pull requests not responded to within 2 business days in the past 6 months\n')



