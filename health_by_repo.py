# Copyright Dawn M. Foster <dawn@dawnfoster.com>
# MIT License

""" Starter Project Health Metrics Model data gathered per repository
This script uses data stored in an Augur PostgresQL database to gather
data for the metrics found in the Started Project Health Metrics Model:
https://chaoss.community/kb/metrics-model-starter-project-health/

Partial month data will never be reported. The last data reported will
be the most recent completed month. 

If there are too few PRs, some charts will not be generated because
the charts only make sense with enough data points. 

If a GitHub organization and repository are both specified, data will
be gathered on that single repository only. 

If only a GitHub organization is specified, it will gather data about
every repository from that organization.

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

Usage
----- 

usage: health_by_repo.py [-h] -o ORG_NAME [-r REPO_NAME] [-y YEARS] [-b BUS_DAYS] -c AUGUR_CONFIG

  -h, --help            show this help message and exit
  -o ORG_NAME, --org ORG_NAME
                        The name of the GitHub organization for data collection on your repo(s) (required)
  -r REPO_NAME, --repo REPO_NAME
                        The name of a GitHub repository in that org where your PRs can be found. If no repo is specified, data will be
                        collected for all repos from the given org.
  -y YEARS, --years YEARS
                        The number of years of data to collect (default to 1)
  -b BUS_DAYS, --businessdays BUS_DAYS
                        The number of business days to use in the time to first response calculation (default to 2)
  -c AUGUR_CONFIG, --configfile AUGUR_CONFIG
                        The full file path to an Augur config.json file (required)

Output
------

* Messages are printed to the screen for each data gathering step for each repo
* Graphs are stored as png files in subdirectories of an "output" folder named like
  output/YYYY-MM/org_name/repo_name

"""
import argparse
import sys
import pandas as pd
from utils.augur_connect import augur_db_connect
from utils.date_calcs import get_dates
from utils.repo_info import get_repo_info, fork_archive, get_org_repos
from utils.file_operations import create_path_str
from metrics.release_frequency import activity_release_graph
from metrics.closure_ratio import sustain_prs_by_repo_graph
from metrics.first_response import response_time_graph
from metrics.bus_factor import contributor_risk_graph

# Gather options from command line arguments and store them in variables
parser = argparse.ArgumentParser()

parser.add_argument("-o", "--org", required=True, dest = "org_name", help="The name of the GitHub organization for data collection on your repo(s) (required)")
parser.add_argument("-r", "--repo", required=False, dest = "repo_name", default=None, help="The name of a GitHub repository in that org where your PRs can be found. If no repo is specified, data will be collected for all repos from the given org.")
parser.add_argument("-y", "--years", required=False, dest = "years", type=int, default=1, help="The number of years of data to collect (default to 1)")
parser.add_argument("-b", "--businessdays", required=False, dest = "bus_days", type=int, default=2, help="The number of business days to use in the time to first response calculation (default to 2)")
parser.add_argument("-c", "--configfile", required=True, dest = "augur_config", help="The full file path to an Augur config.json file (required)")

args = parser.parse_args()
org_name = args.org_name
repo_name = args.repo_name
years = args.years
bus_days = args.bus_days
augur_config = args.augur_config

# Print parameters to the screen
print('Parameters: Years =', years, 'Business Days', bus_days)

# Get the dates for the analysis using the years argument if provided
days = 365 * years
start_date, end_date = get_dates(days)

# Create the connection to the Augur database
engine = augur_db_connect(augur_config)

if repo_name == None:
    # This is the case where data is gathered on all repos from an org
    repoDF = get_org_repos(org_name, engine)
    print("multiple repos")

    # When gathering data on an org, it can be helpful to have a summary CSV
    path = create_path_str(org_name)
    output_filename = path + '/_' + org_name + '_output_yr_' + str(years) + '_bdays_' + str(bus_days) + '.csv'

    try:
        csv_output = open(output_filename, 'w')
        csv_output.write('org_name,repo_name,releases,first_resp_mos,closure_ratio_mos,bus_factor,bus_factor_percents,fork,archive\n')
    except:
        print('Could not write to csv file. Exiting')
        sys.exit(1)

else:
    # This is the case where data is gathered on a single org / repo combo
    repo_id = get_repo_info(engine, org_name, repo_name)
    repoDF = pd.DataFrame([[repo_id, repo_name]], columns=['repo_id', 'repo_name'])

# Collect data for every repo in repoDF

for repo in repoDF.iterrows():
    repo_id = repo[1]['repo_id']
    repo_name = repo[1]['repo_name']

    # Check to see if the repo is Forked or Archived, since those impact 
    # how you might interpret this data and print them to the screen
    # In general, this model isn't intended to be used with forked
    # or archived repos.
    is_forked, is_archived = fork_archive(repo_name, org_name, engine)
    print(org_name, repo_name, '- Forked:', str(is_forked), 'Archived:', str(is_archived))

    # This section collects all of the data using the functions for each graph
    # found in common_functions.py and creates the graphs for each metric
    # Skips archived repos

    if is_archived == False:
        releases = activity_release_graph(repo_id, repo_name, org_name, start_date, end_date, engine, years)

        closure_ratio_mos = sustain_prs_by_repo_graph(repo_id, repo_name, org_name, start_date, end_date, engine, years)

        bus_factor, bus_factor_percents = contributor_risk_graph(repo_id, repo_name, org_name, start_date, end_date, engine, years)

        first_resp_mos = response_time_graph(repo_id, repo_name, org_name, start_date, end_date, engine, bus_days, years)

        if len(repoDF) > 1:
            csv_line = org_name + ',' + repo_name + ',' + releases + ',' + first_resp_mos + ',' + closure_ratio_mos + ',' + bus_factor + ',' + bus_factor_percents + ',' + str(is_forked) + ',' + str(is_archived) + '\n'
            csv_output.write(csv_line)
    
    # Print a separator between repos
    print('-------------')
