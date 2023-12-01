# Copyright Dawn M. Foster <dawn@dawnfoster.com>
# MIT License

""" Contains functions used to gather data and graph the Bus Factor metric
"""
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
    #from utils.date_calcs import convert_to_dt

    #start_date, end_date = convert_to_dt(start_date, end_date)

    #Commit data - from humans excluding known bots
    commitsDF = pd.DataFrame()
    commitsquery = f"""
                    SELECT
                        DISTINCT(commits.cmt_commit_hash), commits.cmt_author_timestamp, contributors.cntrb_login
                    FROM
                        commits, contributors
                    WHERE 
                        commits.repo_id = {repo_id}
                        AND commits.cmt_ght_author_id = contributors.cntrb_id
                        AND commits.cmt_author_name NOT LIKE 'snyk%%'
                        AND commits.cmt_author_name NOT LIKE '%%bot'
                        AND commits.cmt_author_name NOT LIKE '%%Bot'
                        AND commits.cmt_author_name NOT LIKE '%%BOT'
                        AND commits.cmt_author_name NOT LIKE 'dependabot%%'
                        AND commits.cmt_author_name NOT LIKE 'gerrit%%'
                        AND commits.cmt_author_name NOT LIKE '%%utomation%%'
                        AND commits.cmt_author_name NOT LIKE '%%ipeline%%'
                        AND commits.cmt_author_name != 'Travis CI'
                        AND commits.cmt_author_timestamp >= {start_date}
                        AND commits.cmt_author_timestamp <= {end_date}
                    ORDER BY
                        contributors.cntrb_login;
                    """
    
    commitsDF = pd.read_sql_query(commitsquery, con=engine)
    total_commits = commitsDF.cmt_commit_hash.nunique()    

    authorDF = pd.DataFrame()

    # Count values by email as a starting point to get number of commits.
    authorDF = commitsDF.cntrb_login.value_counts()
    authorDF = authorDF.reset_index()
    authorDF.columns = ['name', 'commits']

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
        return -1, 'NO DATA', None, None, None, None, None, None
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

def contributor_risk_graph(repo_id, repo_name, org_name, start_date, end_date, engine, years):
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
    from utils.file_operations import output_filename

    error_num, error_text, names, percents, commits, title, interpretation, num_people = contributor_risk_data(repo_id, repo_name, org_name, start_date, end_date, engine)

    if error_num == -1:
        return "Error","Error"

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

    filename_str = 'bus_factor_y' + str(years)
    filename = output_filename(repo_name, org_name, filename_str)

    fig.savefig(filename, bbox_inches='tight', dpi=500)
    plt.close(fig)

    print('Bus Factor / Contributor Risk for', org_name, '/', repo_name, 'from', start_date, 'to', end_date, '\nsaved as', filename)
    print(num_people, 'people make up > 70% of the commits in the past year.')

    percent_str = '--'.join(str(x) for x in percents)
    return str(num_people), percent_str