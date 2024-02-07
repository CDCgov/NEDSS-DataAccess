@echo off
set date2=%date:~10,4%%date:~4,2%%date:~7,2%

@rem step 1 - delete files in rdbdata folder
del /Q D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\dw\etl\rdbdata\*.*

@rem step 2 - drop and recreate specific RDB tables:
sqlcmd -U nbs_rdb -P *** -S cdc-nbs-alabama-rds-mssql.czya31goozkz.us-east-1.rds.amazonaws.com -d rdb -i D:\wildfly-10.0.0.Final\nedssdomain\Nedss\BatchFiles\Refresh_SQL_RDB_tables.sql > D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log\Drop_Create_Tables.log

@rem step 3 – Cleanup Field Record and EpiLink (added in 5.4.2):
@rem sqlcmd -U nbs_rdb -P *** -S cdc-nbs-alabama-rds-mssql.czya31goozkz.us-east-1.rds.amazonaws.com -d NBS_ODSE -i D:\wildfly-10.0.0.Final\nedssdomain\Nedss\BatchFiles\CleanupFieldRecordAndEpiLink.sql > D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log\CleanupFieldRecordAndEpiLink.log

@rem step 4 - execute MasterETL1 program BEFRORE SSIS package:
%SAS_HOME%\sas.exe -sysin D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\dw\etl\src\MasterETL1.sas -nosyntaxcheck -print D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log\MasterETL1.lst -log D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log\MasterETL1.log -config %SAS_HOME%\SASV9.CFG -autoexec D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\autoexec.sas

@rem step 5 - execute SSIS code:
call D:\wildfly-10.0.0.Final\nedssdomain\Nedss\BatchFiles\exec_NBS_SSIS.bat

@rem step 6 - execute MasterETL2 program AFTER SSIS package:
%SAS_HOME%\sas.exe -sysin D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\dw\etl\src\MasterETL2.sas -nosyntaxcheck -print D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log\MasterETL2.lst -log D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log\MasterETL2.log -config %SAS_HOME%\SASV9.CFG -autoexec D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\autoexec.sas

@rem step 7 - execute RedefineTablespace.sql:
sqlcmd -U nbs_rdb -P *** -S cdc-nbs-alabama-rds-mssql.czya31goozkz.us-east-1.rds.amazonaws.com -d rdb -i D:\wildfly-10.0.0.Final\nedssdomain\Nedss\BatchFiles\RedefineTablespace.sql

@rem step 8 - execute SQL Stored Procedure RDB..sp_SLD_FINISH (commented out in 6.0.13): @rem sqlcmd -U nbs_rdb -P *** -S SQL_server/instance -d RDB -Q "Exec RDB.dbo.sp_SLD_FINISH" > D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log\SSIS_FINISH.log

@rem step 9 - execute the Dynamic Datamart ETL:
%SAS_HOME%\sas.exe -sysin D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\dw\dynamicDm\src\DynamicDataMartMaster.sas -nosyntaxcheck -log D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log\DynamicDataMart.log -config %SAS_HOME%\SASV9.CFG -autoexec D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\autoexec.sas