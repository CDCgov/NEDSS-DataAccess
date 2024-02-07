REM Where "-S SQL_server\instance", replace with your SQL Server name\instance name.
REM Where "-P P@ssword123!, replace with the related SQL user password.
REM Change the file path of the *.log files as required for your configuration.

sqlcmd -S cdc-nbs-alabama-rds-mssql.czya31goozkz.us-east-1.rds.amazonaws.com -U admin -P hW61W1Z5!xX1 -d nbs_classic -Q "Exec nbs_classic.dbo.sp_PublicHealthCaseFact_DATAMART" > D:\wildfly-10.0.0.Final\nedssdomain\Nedss\BatchFiles_Modern\PHCF_QA\PHCMartETL_Classic.log