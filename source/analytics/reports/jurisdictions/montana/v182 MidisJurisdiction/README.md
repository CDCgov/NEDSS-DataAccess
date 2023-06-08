# v182 MidisJurisdiction Reconciliation Report

Recreating the v182 MidisJurisdiction Reconciliation Report for Montana on Metabase using the Reporting Database. This report provides an overview on the completeness and timeliness of data by reporting on the missing fields or incomplete status. The objective of the report is to reconcile missing data by the end of a two week period.

## Report Details

Item: MidisJurisdiction

App Type: Task Server 

Run time: 1h 17m

Run interval: 6:30am every Wed & Thur

Database: MIDIS RDB

Tables: BmirdCase, CaseCount, Condition, ConfirmationMethod, ConfirmationMethodGroup, DCaseManagement, DInterview, DInvAdministrative, DInvEpidemiology, DInvHiv, DInvLabFinding, DInvMedicalHistory, DInvRiskFactor, DInvTreatment, DInvVaccination, DPatient, FInterviewCase, FPageCase, FStdPageCase, GenericCase, HepatitisDatamart, Investigation, LdfDatum, NotificationEvent, PertussisCase, RdbDate, Treatment, TreatmentEvent, VarPamLdf

Description: Reconciliation report that is generated at the jurisdiction level. Excel workbook with multiple tabs. It used in DPHHS to calculate percent complete by data element. Run weekly for communicable disease epidemiologists to review but sent to the jurisidictions quarterly so they can address data quality issues. Uses the conditional formatting in Excel to perform the calculations. 

There are four tabs contained within report:

* Instructions: Instructions for how to interpret the report and timeline for actions to take post review. 
* Deliverables Snapshot:  This is the information that you will use to track your timeliness and completion goals.
* Line List:  The line list of cases supports the completeness/timeliness calculations on the deliverable snapshot ONLY.
* Quarterly Report: A state-wide view of data completeness and timeliness by jurisdiction.

##  Metabase Report Details

All the tables in this report are located in the Reporting Database (RDB). The reports have been generated using the Alabama April 2023 verson of RDB. 

For this stage of development, a select few tables were selected to build the report. The tables are selected based on their relevance to the required columns provided in report template. As we continue to develop the reports, the selection of the tables may differ. The current nexus point is based on F_PAGE_CASE table. This is a fact table consists metrics of the business process for page builder investigations. Facts are located at the center of a star schema or a snowflake schema surrounded by dimension tables denoted by "D_".

The reports have been generated using the Alabama April 2023 verson of RDB on a Metabase instance hosted on AWS EC2. More details on the [report](https://cdc-nbs.atlassian.net/wiki/spaces/NM/pages/198967325/v182+MidisJurisdiction+Reconciliation+Report), including ER diagrams and report preview, can be found on the Confluence page


|  Database |  Table                    |
|-----------|---------------------------|
|  RDB      |  BmirdCases               | 
|  RDB      |  CaseCounts               | 
|  RDB      |  Conditions*               | 
|  RDB      |  ConfirmationMethods      | 
|  RDB      |  ConfirmationMethodGroups | 
|  RDB      |  DCaseManagement          | 
|  RDB      |  DInterview               | 
|  RDB      |  DInvAdministrative       | 
|  RDB      |  DInvEpidemiology         | 
|  RDB      |  DInvHiv*                  | 
|  RDB      |  DInvLabFinding           | 
|  RDB      |  DInvMedicalHistory       | 
|  RDB      |  DInvRiskFactor*           | 
|  RDB      |  DInvTreatment*            | 
|  RDB      |  DInvVaccination          | 
|  RDB      |  DPatients*                | 
|  RDB      |  FInterviewCase           | 
|  RDB      |  FPageCase*                | 
|  RDB      |  FStdPageCase             | 
|  RDB      |  GenericCases             | 
|  RDB      |  HepatitisDatamart        | 
|  RDB      |  Investigations*           | 
|  RDB      |  LdfData                  | 
|  RDB      |  MeaslesCases             | 
|  RDB      |  NotificationEvents       | 
|  RDB      |  PertussisCases           | 
|  RDB      |  RdbDates                 | 
|  RDB      |  Treatment                | 
|  RDB      |  TreatmentEvent           | 
|  RDB      |  VarPamLdf                | 

 * *Selected tables*


