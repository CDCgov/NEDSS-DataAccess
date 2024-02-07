@echo on
set date2=%date:~10,4%%date:~4,2%%date:~7,2%

%SAS_HOME%\sas.exe -sysin D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\dw\dynamicDm\src\DynamicDataMartMaster.sas -nosyntaxcheck -log D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\log\DynamicDataMart.log -config  %SAS_HOME%\SASV9.CFG -autoexec D:\wildfly-10.0.0.Final\nedssdomain\Nedss\report\autoexec.sas



















