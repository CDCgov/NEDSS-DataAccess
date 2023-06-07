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

