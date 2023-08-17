# Starter Project Health Metrics Model Data
Uses an Augur database to generate metrics for the CHAOSS [Starter Project Health Metrics Model](https://chaoss.community/kb/metrics-model-starter-project-health/)

## health_by_repo.py

Generates data about each of the 4 metrics in the model. See the docstring at the
top of this file to learn about the config file requirements along
with the arguments that need to be passed in when you run it.

## metrics subdirectory

The metrics subdirectory contains all of the functions that do the real work to generate
the data and create a graph for each of the metrics in the model.

Some people might want to use these scripts to generate the data and pass that data
to other tools that can be used to generate those graphs, so the functions to gather
data and graph it are always separate. In some cases, the functions that make the 
Augur database calls are also separate when there was quite a bit of data manipulation
required for the graphs that I wanted to generate.

If you are only interested in the data, and not the graphs, please see these functions:
* Release Frequency (metrics/release_frequency.py): get_release_data, activity_release_data
* Change Request Closure Ratio (metrics/closure_ratio.py): monthly_prs_closed, monthly_prs_all, sustain_prs_by_repo_data
* Bus Factor (metrics/bus_factor.py): commit_author_data, contributor_risk_data
* Time to First Response (metrics/first_response): response_time_db, response_time_data