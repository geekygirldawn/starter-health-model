# Copyright Dawn M. Foster <dawn@dawnfoster.com>
# MIT License

""" Contains functions that make calls to the GitHub API.
"""

def read_key(file_name):
    """Retrieves a GitHub API key from a file.
    
    Parameters
    ----------
    file_name : str

    Returns
    -------
    key : str
    """

    with open(file_name, 'r') as kf:
        key = kf.readline().rstrip() # remove newline & trailing whitespace
    return key

def repo_api_call(repo_name, org_name, api_key):
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
        gh_key = read_key(api_key)
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