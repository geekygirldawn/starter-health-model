# Starter Project Health Metrics Model Data
Uses an Augur database to generate metrics for the CHAOSS [Starter Project Health Metrics Model](https://chaoss.community/kb/metrics-model-starter-project-health/)

**health_by_repo.py**
Generates data about each of the 4 metrics in the model. See the docstring at the
top of this file to learn about requirements (config file and GitHub API key) along
with the arguments that need to be passed in when you run it.

Note: currently the release data is being pulled from the GitHub API, instead of
Augur. Eventually, this should be pulled from Augur to reduce dependence on the
GitHub API.

**common_functions.py**
This file contains all of the functions that do the real work to generate the data
and create a graph for each of the metrics in the model.

Some people might want to use these scripts to generate the data and pass that data
to other tools that can be used to generate those graphs, so the functions to gather
data and graph it are always separate. In some cases, the functions that make the 
Augur database calls are also separate when there was quite a bit of data manipulation
required for the graphs that I wanted to generate.

If you are only interested in the data, and not the graphs, please see these functions:
* Release Frequency: get_release_data and activity_release_data
* Change Request Closure Ratio: monthly_prs_closed, monthly_prs_all, sustain_prs_by_repo_data
* Bus Factor: commit_author_data, contributor_risk_data
* Time to First Response: response_time_db, response_time_data