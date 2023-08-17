# Copyright Dawn M. Foster <dawn@dawnfoster.com>
# MIT License

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