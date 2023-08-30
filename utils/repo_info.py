# Copyright Dawn M. Foster <dawn@dawnfoster.com>
# MIT License

""" Contains functions that gather basic information about repositories.
"""

def get_repo_info(engine, repo_org, repo_name):
    """Retrieves the Augur repo_id (unique key) for a GitHub org/repo combination.

    Parameters
    ----------
    engine : sqlalchemy database object
    repo_org : str
    repo_name : str

    Returns
    -------
    repo_id : str
    """
    import sys
    import pandas as pd

    try:
        get_id_query = f"""
            SELECT
                repo.repo_id
            FROM
                repo, repo_groups
            WHERE
                repo.repo_group_id = repo_groups.repo_group_id
                AND LOWER(repo.repo_name) = LOWER('{repo_name}')
                AND LOWER(repo_groups.rg_name) = LOWER('{repo_org}');
            """

        repo_id_df = pd.read_sql_query(get_id_query, con=engine)

    except:
        print("Missing or invalid GitHub organization and repository name combination.")
        sys.exit()

    if len(repo_id_df) == 1:
        repo_id = repo_id_df.repo_id[0]
    else:
        print("Missing or invalid GitHub organization and repository name combination.")
        sys.exit()
        #quit()

    return repo_id

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

def get_org_repos(org_name, engine):
    """Retrieves the Augur repo_id (unique key) and repo_name for all repos in
       a GitHub org and stores them in a dataframe to return.

    Parameters
    ----------
    engine : sqlalchemy database object
    org_name : str

    Returns
    -------
    repoDF : dataframe
    """
    import pandas as pd

    repo_info_query = f"""
        SELECT
        repo.repo_id, repo.repo_name
        FROM
        repo, repo_groups
        WHERE
            repo_groups.repo_group_id = repo.repo_group_id AND
            rg_name = '{org_name}';
            """
    repoDF = pd.read_sql_query(repo_info_query, con=engine)

    return repoDF