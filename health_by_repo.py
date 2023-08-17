# Copyright Dawn M. Foster <dawn@dawnfoster.com>
# MIT License

""" Started Project Health Metrics Model data for a single repository
This script uses data stored in an Augur PostgresQL database to gather
data for the metrics found in the Started Project Health Metrics Model:
https://chaoss.community/kb/metrics-model-starter-project-health/

Release data is gathered via the GitHub API.

Partial month data will never be reported. The last data reported will
be the most recent completed month.

Requirements
------------

Files required to run this script:
config.json
    {
        "connection_string": "sqlite:///:memory:",
        "database": "xxxxx",
        "host": "xxxx.xxxx.xx",
        "password": "xxxxx",
        "port": xxxx,
        "schema": "augur_data",
        "user": "xxxx",
        "user_type": "read_only"
    }
Replace the 'x's with values to connect to your Augur database

gh_key
A text file containing one GitHub personal access token

Usage
----- 

health_by_repo.py [-h] -o ORG_NAME -r REPO_NAME [-y YEARS] -c AUGUR_CONFIG -t API_KEY

options:
  -h, --help            show this help message and exit
  -o ORG_NAME, --org ORG_NAME
                        The name of the GitHub organization where your repo is found (required)
  -r REPO_NAME, --repo REPO_NAME
                        The name of a GitHub repository in that org where your PRs can be found (required)
  -y YEARS, --years YEARS
                        The number of years of data to collect (default to 1)
  -c AUGUR_CONFIG, --configfile AUGUR_CONFIG
                        The full file path to an Augur config.json file (required)
  -t API_KEY, --token API_KEY
                        The file where your GitHub personal access token can be found (required)

Output
------

* Messages are printed to the screen for each data gathering step for each repo
* Graphs are stored as png files in subdirectories of an "output" folder named like
  output/YYYY-MM/org_name/repo_name

"""
import argparse
from utils.augur_connect import augur_db_connect
from utils.repo_info import get_repo_info
from utils.date_calcs import get_dates
from utils.repo_info import fork_archive
from utils.github_api import repo_api_call
from metrics.release_frequency import activity_release_graph
from metrics.closure_ratio import sustain_prs_by_repo_graph
from metrics.first_response import response_time_graph
from metrics.bus_factor import contributor_risk_graph

# Gather options from command line arguments and store them in variables
parser = argparse.ArgumentParser()

parser.add_argument("-o", "--org", required=True, dest = "org_name", help="The name of the GitHub organization where your repo is found (required)")
parser.add_argument("-r", "--repo", required=True, dest = "repo_name", help="The name of a GitHub repository in that org where your PRs can be found (required)")
parser.add_argument("-y", "--years", required=False, dest = "years", type=int, default=1, help="The number of years of data to collect (default to 1)")
parser.add_argument("-c", "--configfile", required=True, dest = "augur_config", help="The full file path to an Augur config.json file (required)")
parser.add_argument("-t", "--token", required=True, dest = "api_key", help="The file where your GitHub personal access token can be found (required)")

args = parser.parse_args()
org_name = args.org_name
repo_name = args.repo_name
years = args.years
augur_config = args.augur_config
api_key = args.api_key

# Get the dates for the analysis using the years argument if provided
days = 365 * years
start_date, end_date = get_dates(days)

# Create the connection to the Augur database and get the Augur repo_id
engine = augur_db_connect(augur_config)
repo_id = get_repo_info(engine, org_name, repo_name)

# Check to see if the repo is Forked or Archived, since those impact 
# how you might interpret this data and print them to the screen
# In general, this model isn't intended to be used with forked
# or archived repos.
is_forked, is_archived = fork_archive(repo_name, org_name, engine)
print('Forked:', str(is_forked), '\nArchived:', str(is_archived))

# Get the GitHub API repository object used to gather release data.
repo_api = repo_api_call(repo_name, org_name, api_key)

# This section compares the Augur org / repo and renames them for repos that have been redirected
# using the GH API as the canonical source of data for the org and repo, rather than what's in Augur
# This fixes the problem of repos being renamed, but not updated in Augur.
full_name = org_name + '/' + repo_name
api_name = repo_api.full_name

if full_name.lower() != api_name.lower():
    org_name = api_name.split("/")[0]
    repo_name = api_name.split("/")[1]

# This section collects all of the data using the functions for each graph
# found in common_functions.py and creates the graphs for each metric

activity_release_graph(repo_name, org_name, start_date, end_date, repo_api)

sustain_prs_by_repo_graph(repo_id, repo_name, org_name, start_date, end_date, engine)

contributor_risk_graph(repo_id, repo_name, org_name, start_date, end_date, engine)

response_time_graph(repo_id, repo_name, org_name, start_date, end_date, engine)

