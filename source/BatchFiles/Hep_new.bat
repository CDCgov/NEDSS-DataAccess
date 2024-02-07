@rem Delete rdbdata only for hep
@rem step 1 - delete files in rdbdata folder
@rem del /Q D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\dw\etl\rdbdata_modern\*.*

@rem Refresh_SQL_RDB_tables
@rem step 2 - drop and recreate specific RDB tables:
@rem sqlcmd -U nbs_rdb -P rdb -S cdc-nbs-alabama-rds-mssql.czya31goozkz.us-east-1.rds.amazonaws.com -d rdb_modern -i D:\wildfly-10.0.0.Final\nedssdomain\Nedss\BatchFiles\Refresh_SQL_RDB_tables.sql > D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\Drop_Create_Tables.log

@rem call sp_ETL_PART1_Start
sqlcmd -S cdc-nbs-alabama-rds-mssql.czya31goozkz.us-east-1.rds.amazonaws.com -U nbs_rdb -P rdb -d rdb_modern -Q "Exec rdb_modern.dbo.sp_ETL_PART1_Start" > D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\sp_ETL_PART1_Start.log

@rem call PHCobservations.sas
%SAS_HOME%\sas.exe -sysin D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\dw\etl\src_modern\PHCobservations.sas -nosyntaxcheck -print D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\PHCobservations.lst -log D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\PHCobservations.log -config %SAS_HOME%\SASV9.CFG -autoexec D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\autoexec.sas

@rem call InvestigationDim.sas
%SAS_HOME%\sas.exe -sysin D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\dw\etl\src_modern\InvestigationDim.sas -nosyntaxcheck -print D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\InvestigationDim.lst -log D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\InvestigationDim.log -config %SAS_HOME%\SASV9.CFG -autoexec D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\autoexec.sas

@rem call the nbs_rdb_modern.sp_SLD_ALL_Hep
@rem sqlcmd -S cdc-nbs-alabama-rds-mssql.czya31goozkz.us-east-1.rds.amazonaws.com -U nbs_rdb -P rdb -d rdb_modern -Q "Exec rdb_modern.dbo.sp_SLD_ALL_Hep" > D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\sp_SLD_ALL_Hep.log


sqlcmd -S cdc-nbs-alabama-rds-mssql.czya31goozkz.us-east-1.rds.amazonaws.com -U nbs_rdb -P rdb -d rdb_modern -Q "Exec rdb_modern.dbo.sp_connection_test" > D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\sp_connection_test.log

sqlcmd -S cdc-nbs-alabama-rds-mssql.czya31goozkz.us-east-1.rds.amazonaws.com -U nbs_ods -P ods -d nbs_changedata -Q "Exec nbs_changedata.dbo.sp_connection_test" > D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log_modern\sp_connection_test_changedata.log