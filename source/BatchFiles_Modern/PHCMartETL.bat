REM Where "-S SQL_server\instance", replace with your SQL Server name\instance name.
REM Where "-P P@ssword123!, replace with the related SQL user password.
REM Change the file path of the *.log files as required for your configuration.

sqlcmd -S cdc-nbs-alabama-rds-mssql.czya31goozkz.us-east-1.rds.amazonaws.com -U nbs_ods -P ods -d NBS_ODSE -Q "Exec NBS_ODSE.dbo.sp_PublicHealthCaseFact_DATAMART" > D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\PHCMartETL.log
