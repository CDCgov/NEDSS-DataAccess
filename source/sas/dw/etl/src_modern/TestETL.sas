%etllib;
OPTIONS COMPRESS=YES;
options fmtsearch=(nbsfmt);
%include etlpgm (etlmacro.sas);
%global etlerr;
%let etlerr=0;
 
%macro etlinit;
proc datasets lib=rdbdata nolist;
	delete 	
		condition
		codeset
	;
run;
quit;
%mend etlinit;
%etlinit;
%macro checkerr;
	%if &syserr > 4 %then %let etlerr=%eval(&etlerr+1);
	%put syserr=&syserr;
	%if &etlerr !=0 %then %do;
		%put dataset=&syslast;
		%put syserr=&syserr;
	%end;
%mend checkerr;


%macro MasterEtl;


%if  %SYSFUNC(LIBREF(nbs_ods)) NE 0 %then %goto liberr;
%if  %SYSFUNC(LIBREF(nbs_srt)) NE 0 %then %goto liberr;
%if  %SYSFUNC(LIBREF(nbs_rdb)) NE 0 %then %goto liberr;
%if  %SYSFUNC(LIBREF(nbsfmt)) NE 0 %then %goto liberr;

data rdbdata.codeset;
set nbs_rdb.codeset;
run;
data _null_;
if 0 then set rdbdata.codeset nobs=codeset_nobs;
call symput('codeset_nobs', put(codeset_nobs,22.));
stop;
run;
%if &codeset_nobs =0 %then %do;
%put &codeset_nobs;

Data _Null_;
	format Time Datetime20.;
	Time=datetime();
	put '	Start Time:   ' Time   Datetime20.;
	put '*****The Start codeset*********';
run;
%include etlpgm (codeset.sas);
%end;
Data _Null_;
	format Time1 Datetime20.;
	Time1=datetime();
	put '	End Time:   ' Time1   Datetime20.;
	put '*****The End codeset*********';
run;
Data _Null_;
	format Time Datetime20.;
	Time=datetime();
	put '	Start Time:   ' Time   Datetime20.;
	put '*****The Start etlformat*********';
run;
%include etlpgm (etlformat.sas);
Data _Null_;
	format Time1 Datetime20.;
	Time1=datetime();
	put '	End Time:   ' Time1   Datetime20.;
	put '*****The End etlformat*********';
run;


%include "&SAS_REPORT_HOME/custom/Custom_controller.sas";

%goto finish;

%liberr:
	%put Libname statement error &syslibrc &SYSMSG ;
	%let etlerr=1;	

%finish:

/*need to put more checking here*/
Data _Null_;
	length Etlmsg $20.;
	if  &etlerr = 0
		then Etlmsg='Successful';
		else  Etlmsg='Incomplete';
	put 'ETL Process Status: '  Etlmsg $char20.;

	format Start_Time End_Time Datetime20. Total_Time TOD8.;
	Start_Time=input("&SYSDATE9"||' '||"&SYSTIME",DATETIME20.);
	End_Time=datetime();
	Total_Time=End_Time-Start_time;
	put 'ETL Processing Time:';
	put '	Start Time: ' Start_Time Datetime20.;
	put '	End Time:   ' End_Time   Datetime20.;
	put '	Total Time: ' Total_Time time8.;
	put '*****The End*********';
run;

data rdbdata.etlstatus;
format etlendtime datetime20.;
etlendtime=datetime();
run;
%mend MasterEtl;
%MasterEtl;

%put &etlerr;
