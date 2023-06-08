# v16 CdEpiWeekly Morbidity and Mortality Weekly Report

Recreating the v16 CdEpiWeekly Morbidity and Mortality Weekly Report (MMRW) for Montana on Metabase using the Reporting Database. This report provides the aggregate count of most reportable conditions for a chosen week in comparison to the same week in the previous three years.

## v16: Report Details

Item: CdEpiWeekly

App Type: Task Server 

Run time: 20 seconds 

Run interval: 7pm Wed/Thurs

Database: MIDIS RDB

Description: Used to show local public health and DPHHS staff aggregate counts for the current MMWR week for most reportable conditions in comparison to same MMWR week in previous three years.

Tables: BmirdCases, CaseCounts, Conditions, ConfirmationMethods, ConfirmationMethodGroups, DPatients, FVarPams, GenericCases, HepatitisCases, Investigations, LdfData, MeaslesCases, NotificationEvents, PertussisCases, RdbDates, RubellaCases


##  Metabase Report Details
All the tables listed in this report are located in the Reporting Database (RDB). For this stage of development, the tables were selected based on their relevance to build the report after consideration of the available information. As we continue to develop the reports, the usage of the tables will differ. 
The current nexus point around which the query is built is the CASE_COUNTS table. This is a fact table that stores a record for each investigation in the RDB. It contains information about entities that are common across all investigations.

The reports have been generated using the Alabama April 2023 verson of RDB on a Metabase instance hosted on AWS EC2. More details on the [report](https://cdc-nbs.atlassian.net/wiki/spaces/NM/pages/181927941/v16+CdEpiWeekly+Morbidity+and+Mortality+Weekly+Report), including ER diagrams and report preview, can be found on the Confluence page. 



|  Database |  Table                    |
|-----------|---------------------------|
|  RDB      |  BmirdCases               | 
|  RDB      |  CaseCounts*               | 
|  RDB      |  Conditions*               | 
|  RDB      |  ConfirmationMethods      | 
|  RDB      |  ConfirmationMethodGroups | 
|  RDB      |  DPatients                | 
|  RDB      |  FVarPams                 | 
|  RDB      |  GenericCases             | 
|  RDB      |  HepatitisCases           | 
|  RDB      |  Investigations*           | 
|  RDB      |  LdfData                  | 
|  RDB      |  MeaslesCases             | 
|  RDB      |  NotificationEvents       | 
|  RDB      |  PertussisCases           | 
|  RDB      |  RdbDates*                 | 
|  RDB      |  RubellaCases             | 

 * *Selected tables*



