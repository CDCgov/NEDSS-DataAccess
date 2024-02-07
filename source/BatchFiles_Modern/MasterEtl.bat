@echo off
set date2=%date:~10,4%%date:~4,2%%date:~7,2%

@rem step 1 - delete files in rdbdata folder
@rem del /Q D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\dw\etl\rdbdata_modern\*.*

@rem step 2 - drop and recreate specific rdb_modern  tables:
sqlcmd -U nbs_rdb -P rdb -S <db_server> -d rdb_modern  -i D:\wildfly-10.0.0.Final\nedssdomain\Nedss\BatchFiles_Modern\Refresh_SQL_RDB_tables.sql > D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\Drop_Create_Tables.log

@rem step 3 – Cleanup Field Record and EpiLink (added in 5.4.2):--
@rem sqlcmd -U nbs_rdb -P rdb -S <db_server> -d NBS_ODSE -i D:\wildfly-10.0.0.Final\nedssdomain\Nedss\BatchFiles_Modern\CleanupFieldRecordAndEpiLink.sql > D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\CleanupFieldRecordAndEpiLink.log

@rem step 4 - execute MasterETL1 program BEFORE SLD ALL SQL PROC:
%SAS_HOME%\sas.exe -sysin D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\dw\etl\src_modern\MasterETL1.sas -nosyntaxcheck -print D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\MasterETL1.lst -log D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\MasterETL1.log -config %SAS_HOME%\SASV9.CFG -autoexec D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\autoexec-modern.sas

@rem step 5 - execute SSIS code: sp_SLD_All
call D:\wildfly-10.0.0.Final\nedssdomain\Nedss\BatchFiles_Modern\exec_NBS_SSIS.bat

@rem step 6 - execute MasterETL2 program AFTER SLD ALL SQL PROC::
@rem %SAS_HOME%\sas.exe -sysin D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\dw\etl\src_modern\MasterETL2.sas -nosyntaxcheck -print D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\MasterETL2.lst -log D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\MasterETL2.log -config %SAS_HOME%\SASV9.CFG -autoexec D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\autoexec-modern.sas

@rem step 7 - execute RedefineTablespace.sql:
@rem sqlcmd -U nbs_rdb -P rdb -S <db_server> -d rdb_modern  -i D:\wildfly-10.0.0.Final\nedssdomain\Nedss\BatchFiles_Modern\RedefineTablespace.sql

@rem step 8 - execute SQL Stored Procedure rdb_modern ..sp_SLD_FINISH (commented out in 6.0.13): @rem sqlcmd -U nbs_rdb -P rdb -S SQL_server/instance -d rdb_modern  -Q "Exec rdb_modern .dbo.sp_SLD_FINISH" > D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\SSIS_FINISH.log

@rem step 9 - execute the Dynamic Datamart ETL:
@rem %SAS_HOME%\sas.exe -sysin D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\dw\dynamicDm\src_modern\DynamicDataMartMaster.sas -nosyntaxcheck -log D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\DynamicDataMart.log -config %SAS_HOME%\SASV9.CFG -autoexec D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\autoexec-modern.sas