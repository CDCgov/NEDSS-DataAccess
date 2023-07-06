# SR8: State Map Report of Disease Cases Over Selected Time Period

## Introduction

Standard report SR8 or State Map Report of Disease Cases Over Selected Time Period: This report aggregates the total number of investigations for a state by county-level. This is plotted over a selected state's county map. 

This report has been recreated on Metabase to highlight the tool's functionality, features and capabilities. The tool's features such as SQL query editor, dashboard, variables, filters, charts have been used to recreate the reports.

The full report can be found on [SR8: State Map Report of Disease Cases Over Selected Time Period](https://cdc-nbs.atlassian.net/wiki/spaces/NM/pages/242384948/SR8+State+Map+Report+of+Disease+Cases+Over+Selected+Time+Period) Confluence page. 

## Query Explanation

This query accesses the PublicHealthCaseFact table in the ODSE database. The syntax of the query is modified to suit Metabase's Variable functionality. This functionality, depicted by the text within double curly braces {{sample_text}}, provides users with placeholder to filter on specific fields. 

The filters for this report are defined in the dashboard and query. In this query, Disease_value, State_value and Date_range are variables used to filter on the disease, state and dates contained within PublicHealthCaseFact. The state filter allows a single state to be selected and the date range filter specifies the time period of the data selection. The disease filter allows selection of one or more diseases.

These are defined in the WHERE clause for the query. Once the filter variables are defined, we are able to view the available data plotted on the selected state's county map. 

![Alabama-state-county-map](images/alabama-state-county-map-plot.png)

