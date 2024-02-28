USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_D_PROVIDER]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO


CREATE PROCEDURE [dbo].[sp_D_PROVIDER]
  @batch_id BIGINT
 as

  BEGIN
  
  --
--UPDATE ACTIVITY_LOG_DETAIL SET 
--START_DATE=DATETIME();

    DECLARE @RowCount_no INT ;
    DECLARE @Proc_Step_no FLOAT = 0 ;
    DECLARE @Proc_Step_Name VARCHAR(200) = '' ;
	DECLARE @batch_start_time datetime2(7) = null ;
	DECLARE @batch_end_time datetime2(7) = null ;
 
 BEGIN TRY
    
	SET @Proc_Step_no = 1;
	SET @Proc_Step_Name = 'SP_Start';

	

	
	BEGIN TRANSACTION;
	
    INSERT INTO [dbo].[job_flow_log] (
	        batch_id
		   ,[Dataflow_Name]
		   ,[package_Name]
		    ,[Status_Type] 
           ,[step_number]
           ,[step_name]
           ,[row_count]
           )
		   VALUES
           (
		   @batch_id
           ,'D_PROVIDER'
           ,'D_PROVIDER'
		   ,'START'
		   ,@Proc_Step_no
		   ,@Proc_Step_Name
           ,0
		   );
  
    COMMIT TRANSACTION;
	
	
	select @batch_start_time = batch_start_dttm,@batch_end_time = batch_end_dttm
	from [dbo].[job_batch_log]
	 where type_code='MasterETL'
		 and status_type = 'start'
     ;


	BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 2;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_INIT'; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_INIT', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_INIT;


		SELECT 
			PERSON.PERSON_UID  'PROVIDER_UID',
			PERSON.LOCAL_ID  'PROVIDER_LOCAL_ID',              
			PERSON.DESCRIPTION  'PROVIDER_GENERAL_COMMENTS',      
			PERSON.ELECTRONIC_IND  'PROVIDER_ENTRY_METHOD',
			PERSON.PERSON_PARENT_UID  'PROVIDER_MPR_UID', 
			PERSON.LAST_CHG_TIME  'PROVIDER_LAST_CHANGE_TIME',
			PERSON.ADD_TIME  'PROVIDER_ADD_TIME',
			PERSON.RECORD_STATUS_CD  'PROVIDER_RECORD_STATUS',
			PERSON.ADD_USER_ID,
			PERSON.LAST_CHG_USER_ID
		into dbo.TMP_S_PROVIDER_INIT
		FROM nbs_changedata.dbo.PERSON PERSON with ( nolock)
		WHERE PERSON.CD='PRV'
		--	AND PERSON.LAST_CHG_TIME> (SELECT MAX(ACTIVITY_LOG_MASTER_LAST.START_DATE) FROM  ACTIVITY_LOG_MASTER_LAST)
		--AND PERSON.LAST_CHG_TIME >= '2021-03-02 21:22:46'	AND PERSON.LAST_CHG_TIME <  '2021-03-24 14:52:21'
		AND PERSON.LAST_CHG_TIME >= @batch_start_time	AND PERSON.LAST_CHG_TIME <  @batch_end_time

		;




				--CREATE TABLE PROVIDER_UID_COLL AS 
      		    
						     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 3;
			SET @PROC_STEP_NAME = ' GENERATING TMP_PROVIDER_UID_COLL'; 

		IF OBJECT_ID('dbo.TMP_PROVIDER_UID_COLL', 'U') IS NOT NULL  
				 drop table dbo.TMP_PROVIDER_UID_COLL;


				SELECT PROVIDER_UID  
				into dbo.TMP_PROVIDER_UID_COLL
				FROM dbo.TMP_S_PROVIDER_INIT with ( nolock)
				;

				-- CREATE TABLE  S_INITPROVIDER AS 

				    
						     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 4;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_INITPROVIDER'; 

		IF OBJECT_ID('dbo.TMP_S_INITPROVIDER', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_INITPROVIDER;

				SELECT A.*, 
						B.user_first_nm AS  'ADD_USER_FIRST_NAME',
						B.user_last_nm AS  'ADD_USER_LAST_NAME', 
						C.user_first_nm AS  'CHG_USER_FIRST_NAME',
						C.user_last_nm AS  'CHG_USER_LAST_NAME',
						Cast (null as  [varchar](50))  as PROVIDER_ADDED_BY ,
					    Cast (null as  [varchar](50))  as PROVIDER_LAST_UPDATED_BY 
				into dbo.TMP_S_INITPROVIDER  
				FROM dbo.TMP_S_PROVIDER_INIT A with (nolock) 
					LEFT OUTER JOIN nbs_changedata.dbo.Auth_user B with (nolock) ON A.ADD_USER_ID=B.NEDSS_ENTRY_ID
					LEFT OUTER JOIN nbs_changedata.dbo.Auth_user C with (nolock) ON A.ADD_USER_ID=C.NEDSS_ENTRY_ID
					;


				/*
				DATA S_INITPROVIDER;
				SET S_INITPROVIDER;
  					IF PROVIDER_RECORD_STATUS = '' THEN PROVIDER_RECORD_STATUS = 'ACTIVE';
  					IF PROVIDER_RECORD_STATUS = 'SUPERCEDED' THEN PROVIDER_RECORD_STATUS = 'INACTIVE' ;
  					IF PROVIDER_RECORD_STATUS = 'LOG_DEL' THEN PROVIDER_RECORD_STATUS = 'INACTIVE' ;
  					IF LENGTH(COMPRESS(ADD_USER_FIRST_NAME))> 0 AND LENGTHN(COMPRESS(ADD_USER_LAST_NAME))>0 THEN PROVIDER_ADDED_BY= TRIM(ADD_USER_LAST_NAME)|| ', ' ||TRIM(ADD_USER_FIRST_NAME);
					ELSE IF LENGTHN(COMPRESS(ADD_USER_FIRST_NAME))> 0 THEN PROVIDER_ADDED_BY= TRIM(ADD_USER_FIRST_NAME);
					ELSE IF LENGTHN(COMPRESS(ADD_USER_LAST_NAME))> 0 THEN PROVIDER_ADDED_BY= TRIM(ADD_USER_LAST_NAME);
					IF LENGTH(COMPRESS(CHG_USER_FIRST_NAME))> 0 AND LENGTHN(COMPRESS(CHG_USER_LAST_NAME))>0 THEN PROVIDER_LAST_UPDATED_BY= TRIM(CHG_USER_LAST_NAME)|| ', ' ||TRIM(CHG_USER_FIRST_NAME);
					ELSE IF LENGTHN(COMPRESS(CHG_USER_FIRST_NAME))> 0 THEN PROVIDER_LAST_UPDATED_BY= TRIM(CHG_USER_FIRST_NAME);
					ELSE IF LENGTHN((CHG_USER_LAST_NAME))> 0 THEN PROVIDER_LAST_UPDATED_BY= TRIM(CHG_USER_LAST_NAME);
				RUN;
				
				*/

  					update dbo.TMP_S_INITPROVIDER set  PROVIDER_RECORD_STATUS = 'ACTIVE'   where  PROVIDER_RECORD_STATUS = '' ;
  					update dbo.TMP_S_INITPROVIDER set  PROVIDER_RECORD_STATUS = 'INACTIVE' where  PROVIDER_RECORD_STATUS = 'SUPERCEDED'  ;
  					update dbo.TMP_S_INITPROVIDER set  PROVIDER_RECORD_STATUS = 'INACTIVE'where  PROVIDER_RECORD_STATUS = 'LOG_DEL'  ;

  				--	IF LENGTH(COMPRESS(ADD_USER_FIRST_NAME))> 0 AND LENGTHN(COMPRESS(ADD_USER_LAST_NAME))>0 
						--  THEN PROVIDER_ADDED_BY= TRIM(ADD_USER_LAST_NAME)|| ', ' ||TRIM(ADD_USER_FIRST_NAME);
						--ELSE IF LENGTHN(COMPRESS(ADD_USER_FIRST_NAME))> 0 THEN PROVIDER_ADDED_BY= TRIM(ADD_USER_FIRST_NAME);
					 --   ELSE IF LENGTHN(COMPRESS(ADD_USER_LAST_NAME))> 0 THEN PROVIDER_ADDED_BY= TRIM(ADD_USER_LAST_NAME);

                      update dbo.TMP_S_INITPROVIDER 
					    set PROVIDER_ADDED_BY = CAST(( Case
						                            when len(rtrim(ADD_USER_FIRST_NAME)) > 0 and len(rtrim(ADD_USER_LAST_NAME))> 0 
													    then rtrim(ADD_USER_LAST_NAME)+', '+rtrim(ADD_USER_FIRST_NAME)
													when len(rtrim(ADD_USER_FIRST_NAME)) > 0  
													    then rtrim(ADD_USER_FIRST_NAME)
                                                    when  len(rtrim(ADD_USER_LAST_NAME))> 0 
													    then rtrim(ADD_USER_LAST_NAME)
													else ''
                                                  END
												) as varchar(50))
						;


					--IF LENGTH(COMPRESS(CHG_USER_FIRST_NAME))> 0 AND LENGTHN(COMPRESS(CHG_USER_LAST_NAME))>0 THEN PROVIDER_LAST_UPDATED_BY= TRIM(CHG_USER_LAST_NAME)|| ', ' ||TRIM(CHG_USER_FIRST_NAME);
					--ELSE IF LENGTHN(COMPRESS(CHG_USER_FIRST_NAME))> 0 THEN PROVIDER_LAST_UPDATED_BY= TRIM(CHG_USER_FIRST_NAME);
					--ELSE IF LENGTHN(COMPRESS(CHG_USER_LAST_NAME))> 0 THEN PROVIDER_LAST_UPDATED_BY= TRIM(CHG_USER_LAST_NAME);

					
                      update dbo.TMP_S_INITPROVIDER 
					    set PROVIDER_LAST_UPDATED_BY = CAST(( Case
						                            when len(rtrim(CHG_USER_FIRST_NAME)) > 0 and len(rtrim(CHG_USER_LAST_NAME))> 0 
													    then rtrim(CHG_USER_LAST_NAME)+', '+rtrim(CHG_USER_FIRST_NAME)
													when len(rtrim(CHG_USER_FIRST_NAME)) > 0  
													    then rtrim(CHG_USER_FIRST_NAME)
                                                    when  len(rtrim(CHG_USER_LAST_NAME))> 0 
													    then rtrim(CHG_USER_LAST_NAME)
													else ''
                                                  END
												) as varchar(50))
						;



				-- CREATE TABLE S_PROVIDER_NAME AS 
				 
						     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 5;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_NAME'; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_NAME', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_NAME;

				
				SELECT FIRST_NM,
						LAST_NM,
						MIDDLE_NM,
						NM_SUFFIX,
						NM_PREFIX,
						NM_DEGREE,
						NM_USE_CD,
						pn.PERSON_UID AS   'PROVIDER_UID_NAME',
						Cast (null as  [varchar](50))  PROVIDER_FIRST_NAME ,
						Cast (null as  [varchar](50))  PROVIDER_LAST_NAME ,
						Cast (null as  [varchar](50))  PROVIDER_MIDDLE_NAME ,
						Cast (null as  [varchar](50))  PROVIDER_ALIAS_NICKNAME ,
						Cast (null as  [varchar](50))  PROVIDER_NAME_SUFFIX ,
						Cast (null as  [varchar](50))  PROVIDER_NAME_PREFIX ,
						Cast (null as  [varchar](50))  PROVIDER_NAME_DEGREE 
				 into dbo.TMP_S_PROVIDER_NAME
				 FROM  dbo.TMP_PROVIDER_UID_COLL tpuc WITH ( NOLOCK)  
				   INNER JOIN nbs_changedata.dbo.PERSON_NAME pn  WITH ( NOLOCK)	ON tpuc.PROVIDER_UID=pn.PERSON_UID
				 WHERE NM_USE_CD ='L' 
				--ORDER BY PERSON_NAME.PERSON_UID, NM_USE_CD
				;

				
				/*
				IF FIRST.PROVIDER_UID THEN IF NM_USE_CD= 'L' THEN PROVIDER_FIRST_NAME=FIRST_NM;
				IF NM_USE_CD= 'L' THEN PROVIDER_LAST_NAME=LAST_NM;
				IF NM_USE_CD= 'L' THEN PROVIDER_MIDDLE_NAME=MIDDLE_NM;
				IF NM_USE_CD= 'L' THEN NM_SUFFIX=NM_SUFFIX;
				IF NM_USE_CD= 'L' THEN NM_PREFIX=NM_PREFIX;
				IF NM_USE_CD= 'L' THEN NM_DEGREE=NM_DEGREE;	
				IF LAST.PROVIDER_UID;
				*/


				
		update [dbo].TMP_S_PROVIDER_NAME 
		  SET  
			  PROVIDER_FIRST_NAME=FIRST_NM,
			  PROVIDER_LAST_NAME=LAST_NM,
			  PROVIDER_MIDDLE_NAME=MIDDLE_NM
			where NM_USE_CD= 'L'
			;

				/* --VS
				PROVIDER_NAME_SUFFIX=PUT(NM_SUFFIX, $DEM107F.);
				PROVIDER_NAME_PREFIX=PUT(NM_PREFIX, $DEM101F.);
				PROVIDER_NAME_DEGREE=PUT(NM_DEGREE, $DEM108F.);
				--*/


			update dbo.[TMP_S_PROVIDER_NAME] 
			   set dbo.[TMP_S_PROVIDER_NAME].PROVIDER_NAME_SUFFIX = SUBSTRING(cvg.[code_short_desc_txt], 1, 50)
			  FROM nbs_odse.dbo.NBS_question nq with ( nolock),
				   [NBS_SRTE].dbo.[Codeset] cd with ( nolock),
				   [NBS_SRTE].dbo.[Code_value_general] cvg with ( nolock),
				  dbo.[TMP_S_PROVIDER_NAME] sir 
			  where nq.question_identifier = ( 'DEM107')
					  and   cd.code_set_group_id = nq.code_set_group_id
					  and   cvg.code_set_nm = cd.code_set_nm
					  and   sir.NM_SUFFIX = cvg.code
					  and   sir.NM_SUFFIX is not null
			 ;

			 

			update dbo.[TMP_S_PROVIDER_NAME] 
			   set dbo.[TMP_S_PROVIDER_NAME].PROVIDER_NAME_PREFIX =   SUBSTRING(cvg.[code_short_desc_txt], 1, 50)
			  FROM nbs_odse.dbo.NBS_question nq with ( nolock),
				   [NBS_SRTE].dbo.[Codeset] cd with ( nolock),
				   [NBS_SRTE].dbo.[Code_value_general] cvg with ( nolock),
				  dbo.[TMP_S_PROVIDER_NAME] sir 
			  where nq.question_identifier = ( 'DEM101')
					  and   cd.code_set_group_id = nq.code_set_group_id
					  and   cvg.code_set_nm = cd.code_set_nm
					  and   sir.NM_PREFIX = cvg.code
					  and   sir.NM_PREFIX is not null
			 ;
				
				
				
				
/*
			update dbo.[TMP_S_PROVIDER_NAME] 
			   set dbo.[TMP_S_PROVIDER_NAME].PROVIDER_NAME_DEGREE = cvg.[code_short_desc_txt]
			  FROM nbs_odse.dbo.NBS_question nq,
				   [NBS_SRTE].dbo.[Codeset] cd,
				   [NBS_SRTE].dbo.[Code_value_general] cvg,
				  dbo.[TMP_S_PROVIDER_NAME] sir 
			  where nq.question_identifier = ( 'DEM108')
					  and   cd.code_set_group_id = nq.code_set_group_id
					  and   cvg.code_set_nm = cd.code_set_nm
					  and   sir.NM_DEGREE = cvg.code
					  and   sir.NM_DEGREE is not null
			 ;
				
*/

     			update dbo.[TMP_S_PROVIDER_NAME] 
			   set dbo.[TMP_S_PROVIDER_NAME].PROVIDER_NAME_DEGREE = NM_DEGREE
			   where NM_DEGREE is not null
			   ;



				
				/* --VS
				DROP 
				FIRST_NM LAST_NM MIDDLE_NM NM_SUFFIX NM_USE_CD NM_DEGREE;
				RUN; 
				*/

				--CREATE TABLE S_PROVIDER_WITH_NM AS 
				
				
						     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 6;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_WITH_NM'; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_WITH_NM', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_WITH_NM;

				
				SELECT si.*, spn.*
				into dbo.TMP_S_PROVIDER_WITH_NM
				FROM dbo.TMP_S_INITPROVIDER  si
				   LEFT OUTER JOIN dbo.TMP_S_PROVIDER_NAME spn with ( nolock) ON si.PROVIDER_UID= spn.PROVIDER_UID_NAME
				;
				 

				--PROC DATASETS LIBRARY = WORK NOLIST; DELETE S_INITPROVIDER S_PROVIDER_NAME; RUN; 
				
				
				
				
				--CREATE TABLE S_POSTAL_LOCATOR AS
				
				
						     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 7;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_POSTAL_LOCATOR'; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_POSTAL_LOCATOR', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_POSTAL_LOCATOR;

				with lst as (
				SELECT distinct
						pl.CITY_DESC_TXT AS  'PROVIDER_CITY',             
						pl.CNTRY_CD	AS  'PROVIDER_COUNTRY',            
						pl.CNTY_CD	AS  'PROVIDER_COUNTY_CODE',              
						pl.STATE_CD	AS  'PROVIDER_STATE_CODE',               
						rtrim(pl.STREET_ADDR1) AS  'PROVIDER_STREET_ADDRESS_1',
						rtrim(pl.STREET_ADDR2)	AS  'PROVIDER_STREET_ADDRESS_2',
						pl.ZIP_CD AS  'PROVIDER_ZIP',
						sc.CODE_DESC_TXT AS  'PROVIDER_STATE_DESC',
						substring(ccv.CODE_DESC_TXT ,1,50) AS  'PROVIDER_COUNTY_DESC',
						cc.CODE_SHORT_DESC_TXT AS  'PROVIDER_COUNTRY_DESC',
						elp.LOCATOR_DESC_TXT AS  'PROVIDER_ADDRESS_COMMENTS',
						elp.ENTITY_UID as ENTITY_UID_POSTAL,
						Cast (null as  [varchar](50))  PROVIDER_STATE ,
						Cast (null as  [varchar](50))  PROVIDER_COUNTY
				  	  , ROW_NUMBER() OVER (
									 PARTITION BY elp.ENTITY_UID 
									 ORDER BY pl.POSTAL_LOCATOR_UID DESC
         					   ) AS [ROWNO]    
				 
				FROM  dbo.TMP_PROVIDER_UID_COLL puc
						LEFT OUTER JOIN nbs_changedata.dbo.ENTITY_LOCATOR_PARTICIPATION elp with ( nolock) ON puc.PROVIDER_UID= elp.ENTITY_UID and elp.record_status_cd='ACTIVE'
						LEFT OUTER JOIN nbs_changedata.dbo.POSTAL_LOCATOR pl with ( nolock) ON elp.LOCATOR_UID=pl.POSTAL_LOCATOR_UID  
						LEFT OUTER JOIN NBS_SRTE.dbo.STATE_CODE sc with ( nolock) ON sc.STATE_CD=pl.STATE_CD
						LEFT OUTER JOIN NBS_SRTE.dbo.COUNTRY_CODE cc with ( nolock) ON cc.CODE=pl.CNTRY_CD
						LEFT OUTER JOIN NBS_SRTE.dbo.STATE_COUNTY_CODE_VALUE ccv with ( nolock) ON ccv.CODE=pl.CNTY_CD	
				WHERE elp.USE_CD='WP'
					AND elp.CD='O'
					AND elp.CLASS_CD='PST'
				)
				 select * 
				into dbo.TMP_S_PROVIDER_POSTAL_LOCATOR
				from lst
				where rowno = 1
				;

              				
				alter table dbo.TMP_S_PROVIDER_POSTAL_LOCATOR
				    drop column ROWNO ;

				/*
				DATA S_POSTAL_LOCATOR;
				SET S_POSTAL_LOCATOR;
				IF LENGTHN(TRIM(PROVIDER_STATE_DESC))>1 THEN PROVIDER_STATE=PROVIDER_STATE_DESC;
				IF LENGTHN(TRIM(PROVIDER_COUNTY_DESC))>1 THEN PROVIDER_COUNTY=PROVIDER_COUNTY_DESC;
				IF LENGTHN(TRIM(PROVIDER_COUNTRY_DESC))>1 THEN PROVIDER_COUNTRY=PROVIDER_COUNTRY_DESC;
				RUN;
				*/
				ALTER TABLE dbo.TMP_S_PROVIDER_POSTAL_LOCATOR ALTER COLUMN PROVIDER_COUNTRY VARCHAR(50) NULL;
				ALTER TABLE dbo.TMP_S_PROVIDER_POSTAL_LOCATOR ALTER COLUMN PROVIDER_COUNTY_DESC VARCHAR(50) NULL;


				update dbo.TMP_S_PROVIDER_POSTAL_LOCATOR set PROVIDER_STATE=PROVIDER_STATE_DESC    where len(rtrim(PROVIDER_STATE_DESC))>= 1;
				update dbo.TMP_S_PROVIDER_POSTAL_LOCATOR set PROVIDER_COUNTY=PROVIDER_COUNTY_DESC  where len(rtrim(PROVIDER_COUNTY_DESC))>= 1;
				update dbo.TMP_S_PROVIDER_POSTAL_LOCATOR set PROVIDER_COUNTRY=PROVIDER_COUNTRY_DESC where len(rtrim(PROVIDER_COUNTRY_DESC))>= 1;

				
				
				
				--PROC SORT DATA=S_POSTAL_LOCATOR NODUPKEY; BY ENTITY_UID; RUN;
				
				
				
				
				-- CREATE TABLE S_PROVIDER_TELE_LOCATOR_OFFICE AS
		
					     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 8;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_TELE_LOCATOR_OFFICE'; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_TELE_LOCATOR_OFFICE', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_TELE_LOCATOR_OFFICE;

			
				with lst as(

			SELECT DISTINCT
					elp.ENTITY_UID as ENTITY_UID_OFFICE,
					tl.EXTENSION_TXT AS  'PROVIDER_PHONE_EXT_WORK',        
					tl.PHONE_NBR_TXT AS  'PROVIDER_PHONE_WORK', 
					tl.EMAIL_ADDRESS AS  'PROVIDER_EMAIL_WORK',
					elp.LOCATOR_DESC_TXT AS  'PROVIDER_PHONE_COMMENTS'
					, ROW_NUMBER() OVER (
									 PARTITION BY elp.ENTITY_UID 
									 ORDER BY tl.TELE_LOCATOR_UID DESC
         					   ) AS [ROWNO]    
				FROM dbo.TMP_PROVIDER_UID_COLL puc
					INNER JOIN nbs_changedata.dbo.ENTITY_LOCATOR_PARTICIPATION elp with ( nolock) ON puc.PROVIDER_UID= elp.ENTITY_UID and elp.record_status_cd='ACTIVE'
					INNER JOIN nbs_changedata.dbo.TELE_LOCATOR tl with ( nolock) ON elp.LOCATOR_UID=tl.TELE_LOCATOR_UID
				WHERE elp.USE_CD='WP'
					AND elp.CD='O'
					AND elp.CLASS_CD='TELE'
					AND elp.RECORD_STATUS_CD='ACTIVE'
					)
				select 
					ENTITY_UID_OFFICE,
					PROVIDER_PHONE_EXT_WORK,        
					PROVIDER_PHONE_WORK, 
					PROVIDER_EMAIL_WORK,
					PROVIDER_PHONE_COMMENTS
				into dbo.TMP_S_PROVIDER_TELE_LOCATOR_OFFICE
				from lst
				where rowno = 1
					;
		
				
				--PROC SORT DATA=S_PROVIDER_TELE_LOCATOR_OFFICE NODUPKEY; BY ENTITY_UID; RUN;
				
				
				
				--CREATE TABLE S_PROVIDER_TELE_LOCATOR_CELL AS
		
		
					     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 4;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_TELE_LOCATOR_CELL'; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_TELE_LOCATOR_CELL', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_TELE_LOCATOR_CELL;

		
		     with lst as(
				SELECT DISTINCT
					elp.ENTITY_UID as ENTITY_UID_CELL ,
					tl.PHONE_NBR_TXT AS  'PROVIDER_PHONE_CELL'
					, ROW_NUMBER() OVER (
									 PARTITION BY elp.ENTITY_UID 
									 ORDER BY tl.TELE_LOCATOR_UID DESC
         					   ) AS [ROWNO]    
				FROM dbo.TMP_PROVIDER_UID_COLL puc with( nolock)
					INNER JOIN nbs_changedata.dbo.ENTITY_LOCATOR_PARTICIPATION elp with ( nolock) ON puc.PROVIDER_UID= elp.ENTITY_UID
					INNER JOIN nbs_changedata.dbo.TELE_LOCATOR tl with ( nolock) ON elp.LOCATOR_UID=tl.TELE_LOCATOR_UID
				WHERE elp.CD='CP'
				 AND elp.CLASS_CD='TELE'
			  )
			   	select *
				into dbo.TMP_S_PROVIDER_TELE_LOCATOR_CELL
				from lst
				where rowno = 1
				;
		
		      				
				alter table dbo.TMP_S_PROVIDER_TELE_LOCATOR_CELL
				    drop column ROWNO ;
		
				
		--		PROC SORT DATA=S_PROVIDER_TELE_LOCATOR_CELL NODUPKEY; BY ENTITY_UID; RUN;
				
				
				
		--		CREATE TABLE S_PROVIDER_LOCATOR AS 
				
				
					     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 7;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_LOCATOR'; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_LOCATOR', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_LOCATOR;

		
				
				SELECT S_pl.*, tlo.*, tlc.*, puc.PROVIDER_UID as PROVIDER_UID_LOCATOR
				into dbo.TMP_S_PROVIDER_LOCATOR
				FROM dbo.TMP_PROVIDER_UID_COLL puc
				 LEFT OUTER JOIN  dbo.TMP_S_PROVIDER_TELE_LOCATOR_OFFICE  tlo ON	puc.PROVIDER_UID= tlo.ENTITY_UID_OFFICE
				 LEFT OUTER JOIN dbo.TMP_S_PROVIDER_TELE_LOCATOR_CELL tlc ON	puc.PROVIDER_UID=tlc.ENTITY_UID_CELL
				 LEFT OUTER JOIN  dbo.TMP_S_PROVIDER_POSTAL_LOCATOR S_pl ON puc.PROVIDER_UID=S_pl.ENTITY_UID_POSTAL
				;
				

				/*
				
				PROC SORT DATA=S_PROVIDER_LOCATOR NODUPKEY; BY PROVIDER_UID; RUN;
				PROC DATASETS LIBRARY = WORK NOLIST;DELETE S_POSTAL_LOCATOR S_PROVIDER_TELE_LOCATOR_OFFICE S_PROVIDER_TELE_LOCATOR_CELL;RUN;
			*/
				
				
				--CREATE TABLE QEC_ENTITY_ID AS
				
				
					     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 8;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_QEC_ENTITY_ID'; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_QEC_ENTITY_ID', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_QEC_ENTITY_ID;

		
				
				 SELECT 
				   DISTINCT PROVIDER_UID as PROVIDER_UID_QEC ,
				    ROOT_EXTENSION_TXT,
				   ASSIGNING_AUTHORITY_CD  
				 into dbo.TMP_S_PROVIDER_QEC_ENTITY_ID
				 FROM dbo.TMP_PROVIDER_UID_COLL puc
				 LEFT OUTER JOIN nbs_changedata.dbo.ENTITY_ID	with ( nolock) ON puc.PROVIDER_UID=ENTITY_ID.ENTITY_UID and Entity_id.record_status_cd='ACTIVE'
				       AND ENTITY_ID.TYPE_CD = 'QEC';
				
				
				--PROC SORT DATA=QEC_ENTITY_ID NODUPKEY; BY PROVIDER_UID; RUN;
				
				
				
				-- CREATE TABLE PRN_ENTITY_ID AS 
				
				
					     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 9;
			SET @PROC_STEP_NAME = ' GENERATING TMP_PRN_ENTITY_ID'; 

		IF OBJECT_ID('dbo.TMP_PRN_ENTITY_ID', 'U') IS NOT NULL  
				 drop table dbo.TMP_PRN_ENTITY_ID;

		
			with lst as (	
				SELECT DISTINCT 
				    PROVIDER_UID as PROVIDER_UID_PRN, 
					ROOT_EXTENSION_TXT, 
					ASSIGNING_AUTHORITY_CD 
                	, ROW_NUMBER() OVER (
									 PARTITION BY puc.PROVIDER_UID
									 ORDER BY entity_id_seq DESC
         					   ) AS [ROWNO]    
 
            	FROM dbo.TMP_PROVIDER_UID_COLL puc
				     LEFT OUTER JOIN nbs_changedata.dbo.ENTITY_ID with ( nolock) ON puc.PROVIDER_UID=ENTITY_ID.ENTITY_UID and ENTITY_ID.record_status_cd ='ACTIVE'
				        AND ENTITY_ID.TYPE_CD = 'PRN'
			)
			
			select 		PROVIDER_UID_PRN, 
					ROOT_EXTENSION_TXT, 
					ASSIGNING_AUTHORITY_CD
			   into dbo.TMP_PRN_ENTITY_ID
			from lst
			where rowno = 1			
			;
				
				--PROC SORT DATA=PRN_ENTITY_ID NODUPKEY; BY PROVIDER_UID; RUN;
				
				
				
				--CREATE TABLE S_PROVIDER_FINAL AS 
				
				
					     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 10;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_FINAL'; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_FINAL', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_FINAL;

		
				SELECT  distinct pwm.*,
					   spl.*, 
					   qe.ROOT_EXTENSION_TXT AS  'PROVIDER_QUICK_CODE', 
					   pe.ROOT_EXTENSION_TXT AS  'PROVIDER_REGISTRATION_NUM',
					   pe.ASSIGNING_AUTHORITY_CD AS  'PROVIDER_REGISRATION_NUM_AUTH'
                into  dbo.TMP_S_PROVIDER_FINAL
				FROM dbo.TMP_S_PROVIDER_WITH_NM pwm
						LEFT OUTER JOIN  dbo.TMP_S_PROVIDER_LOCATOR spl ON pwm.PROVIDER_UID=spl.PROVIDER_UID_LOCATOR
						LEFT OUTER JOIN dbo.TMP_S_PROVIDER_QEC_ENTITY_ID	qe ON  pwm.PROVIDER_UID= qe.PROVIDER_UID_QEC
						LEFT OUTER JOIN dbo.TMP_PRN_ENTITY_ID	pe ON  pwm.PROVIDER_UID= pe.PROVIDER_UID_PRN
				;

						update dbo.TMP_S_PROVIDER_FINAL
						set  PROVIDER_QUICK_CODE= null
						where rtrim(ltrim(PROVIDER_QUICK_CODE)) = ''
											
						update dbo.TMP_S_PROVIDER_FINAL
						set  PROVIDER_REGISRATION_NUM_AUTH= null
						where rtrim(ltrim(PROVIDER_REGISRATION_NUM_AUTH)) = ''
							update dbo.TMP_S_PROVIDER_FINAL
						set  PROVIDER_STREET_ADDRESS_1= null
						where rtrim(ltrim(PROVIDER_STREET_ADDRESS_1)) = ''
							update dbo.TMP_S_PROVIDER_FINAL
						set  PROVIDER_STREET_ADDRESS_2= null
						where rtrim(ltrim(PROVIDER_STREET_ADDRESS_2)) = ''
							update dbo.TMP_S_PROVIDER_FINAL
						set  PROVIDER_COUNTY_CODE= null
						where rtrim(ltrim(PROVIDER_COUNTY_CODE)) = ''
							update dbo.TMP_S_PROVIDER_FINAL
						set  PROVIDER_ADDRESS_COMMENTS= null
						where rtrim(ltrim(PROVIDER_ADDRESS_COMMENTS)) = ''
							update dbo.TMP_S_PROVIDER_FINAL
						set  PROVIDER_PHONE_WORK= null
						where rtrim(ltrim(PROVIDER_PHONE_WORK)) = ''
							update dbo.TMP_S_PROVIDER_FINAL
						set  PROVIDER_PHONE_EXT_WORK= null
						where rtrim(ltrim(PROVIDER_PHONE_EXT_WORK)) = ''
							update dbo.TMP_S_PROVIDER_FINAL
						set  PROVIDER_PHONE_COMMENTS= null
						where rtrim(ltrim(PROVIDER_PHONE_COMMENTS)) = ''
						
				/*
				PROC SORT DATA=S_PROVIDER_FINAL NODUPKEY; BY PROVIDER_UID; RUN;
				%DBLOAD (S_PROVIDER, S_PROVIDER_FINAL);
				*/
				
				
		    			     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 11;
			SET @PROC_STEP_NAME = ' GENERATING S_PROVIDER'; 

		IF OBJECT_ID('dbo.S_PROVIDER', 'U') IS NOT NULL  
				 drop table dbo.S_PROVIDER;

		/*
				select distinct *
				into dbo.S_PROVIDER
				from dbo.TMP_S_PROVIDER_FINAL
				;
      */

	         select distinct [PROVIDER_UID]
					  ,[PROVIDER_LOCAL_ID]
					  ,[PROVIDER_RECORD_STATUS]
					  ,[PROVIDER_NAME_PREFIX]
					  ,[PROVIDER_FIRST_NAME]
					  ,[PROVIDER_MIDDLE_NAME]
					  ,[PROVIDER_LAST_NAME]
					  ,[PROVIDER_NAME_SUFFIX]
					  ,[PROVIDER_NAME_DEGREE]
					  ,[PROVIDER_GENERAL_COMMENTS]
					  ,[PROVIDER_QUICK_CODE]
					  ,[PROVIDER_REGISTRATION_NUM]
					  ,[PROVIDER_REGISRATION_NUM_AUTH]
					  ,[PROVIDER_STREET_ADDRESS_1]
					  ,[PROVIDER_STREET_ADDRESS_2]
					  ,[PROVIDER_CITY]
					  ,[PROVIDER_STATE]
					  ,[PROVIDER_STATE_CODE]
					  ,[PROVIDER_ZIP]
					  ,[PROVIDER_COUNTY]
					  ,[PROVIDER_COUNTY_CODE]
					  ,[PROVIDER_COUNTRY]
					  ,[PROVIDER_ADDRESS_COMMENTS]
					  ,[PROVIDER_PHONE_WORK]
					  ,[PROVIDER_PHONE_EXT_WORK]
					  ,[PROVIDER_EMAIL_WORK]
					  ,[PROVIDER_PHONE_COMMENTS]
					  ,[PROVIDER_PHONE_CELL]
					  ,[PROVIDER_ENTRY_METHOD]
					  ,[PROVIDER_LAST_CHANGE_TIME]
					  ,[PROVIDER_ADD_TIME]
					  ,[PROVIDER_ADDED_BY]
					  ,[PROVIDER_LAST_UPDATED_BY]
             	into dbo.S_PROVIDER
				from dbo.TMP_S_PROVIDER_FINAL
				;
                 

			--	PROC DATASETS LIBRARY = WORK NOLIST;DELETE S_PROVIDER_WITH_NM S_PROVIDER_LOCATOR QEC_ENTITY_ID PROVIDER_UID_COLL PRN_ENTITY_ID;RUN;
			
				
				
		--		CREATE TABLE L_PROVIDER_N AS

			
					     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 12;
			SET @PROC_STEP_NAME = ' GENERATING TMP_L_PROVIDER_N'; 

		IF OBJECT_ID('dbo.TMP_L_PROVIDER_N', 'U') IS NOT NULL  
				 drop table dbo.TMP_L_PROVIDER_N;

				 	
					CREATE TABLE [dbo].TMP_L_PROVIDER_N
					(
					[PROVIDER_id]  [int] IDENTITY(1,1) NOT NULL,
					[PROVIDER_UID_N] [bigint] NOT NULL,  
					[PROVIDER_KEY] [numeric](18, 0) NULL
					 ) ON [PRIMARY]
					 ;

					insert into [dbo].[TMP_L_PROVIDER_N]([PROVIDER_UID_N],[PROVIDER_KEY])
						
					SELECT DISTINCT [PROVIDER_UID],null
					  FROM dbo.S_PROVIDER
					EXCEPT 
					SELECT PROVIDER_UID ,null
					  FROM  dbo.L_PROVIDER
					  ;
				
					IF NOT EXISTS (SELECT * FROM L_PROVIDER WHERE PROVIDER_KEY=1) 
							BEGIN
							   INSERT INTO L_PROVIDER (PROVIDER_KEY,PROVIDER_UID) VALUES (1,0);
							END
					IF NOT EXISTS (SELECT * FROM D_PROVIDER WHERE PROVIDER_KEY=1) 
							BEGIN
							   INSERT INTO D_PROVIDER (PROVIDER_KEY) VALUES (1);
							END


				UPDATE dbo.TMP_L_PROVIDER_N 
				   SET PROVIDER_KEY= PROVIDER_ID + coalesce((SELECT MAX(PROVIDER_KEY) FROM dbo.L_PROVIDER),0)
				   ;


			--	CREATE TABLE L_PROVIDER_E AS

			
					     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO =13;
			SET @PROC_STEP_NAME = ' GENERATING TMP_L_PROVIDER_E'; 

		IF OBJECT_ID('dbo.TMP_L_PROVIDER_E', 'U') IS NOT NULL  
				 drop table dbo.TMP_L_PROVIDER_E;

		
				SELECT DISTINCT sp.PROVIDER_UID as PROVIDER_UID_E ,
					    lp.PROVIDER_KEY
				into dbo.TMP_L_PROVIDER_E
				FROM dbo.S_PROVIDER sp ,
				     dbo.L_PROVIDER lp
				WHERE sp.PROVIDER_UID= lp.PROVIDER_UID
				;
				
				


				
				--%DBLOAD (L_PROVIDER, L_PROVIDER_N);
				
			  insert into dbo.L_PROVIDER
					   select distinct [PROVIDER_KEY] ,
							[PROVIDER_UID_N]
						from dbo.TMP_L_PROVIDER_N
					   ;

				
			
	
		--		CREATE TABLE D_PROVIDER_N AS
		
		
					     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 14;
			SET @PROC_STEP_NAME = ' GENERATING TMP_D_PROVIDER_N'; 

		IF OBJECT_ID('dbo.TMP_D_PROVIDER_N', 'U') IS NOT NULL  
				 drop table dbo.TMP_D_PROVIDER_N;

		 
					SELECT  distinct * 
					into dbo.TMP_D_PROVIDER_N
					FROM dbo.S_PROVIDER  sp , 
					     dbo.TMP_L_PROVIDER_N lpn
				WHERE sp.PROVIDER_UID=lpn.PROVIDER_UID_N
				;
	
	
		--		CREATE TABLE D_PROVIDER_E AS
		
		
					     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 15;
			SET @PROC_STEP_NAME = ' GENERATING TMP_D_PROVIDER_E'; 

		IF OBJECT_ID('dbo.TMP_D_PROVIDER_E', 'U') IS NOT NULL  
				 drop table dbo.TMP_D_PROVIDER_E;

		 
					SELECT distinct * 
					into dbo.TMP_D_PROVIDER_E
					FROM dbo.S_PROVIDER sp, 
					     dbo.TMP_L_PROVIDER_E lpe
				WHERE sp.PROVIDER_UID=lpe.PROVIDER_UID_E
				;
				
		
					     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 16;
			SET @PROC_STEP_NAME = ' UPDATING D_PROVIDER'; 

			
				
				--PROC SORT DATA=D_PROVIDER_N NODUPKEY; BY PROVIDER_KEY;RUN;
		
		
			/* -- VS	
				DATA dbo.D_PROVIDER;
				 MODIFY dbo.D_PROVIDER D_PROVIDER_E;
				 BY PROVIDER_KEY;
				RUN;
			*/
			update dbo.D_PROVIDER
  			 set [PROVIDER_UID]	=	 tpe.[PROVIDER_UID]	,
				 [PROVIDER_KEY]	=	tpe.[PROVIDER_KEY]	,
				 [PROVIDER_LOCAL_ID]	=	tpe.[PROVIDER_LOCAL_ID]	,
				 [PROVIDER_RECORD_STATUS]	=	tpe.[PROVIDER_RECORD_STATUS]	,
				 [PROVIDER_NAME_PREFIX]	=	tpe.[PROVIDER_NAME_PREFIX]	,
				 [PROVIDER_FIRST_NAME]	=	tpe.[PROVIDER_FIRST_NAME]	,
				 [PROVIDER_MIDDLE_NAME]	=	tpe.[PROVIDER_MIDDLE_NAME]	,
				 [PROVIDER_LAST_NAME]	=	tpe.[PROVIDER_LAST_NAME]	,
				 [PROVIDER_NAME_SUFFIX]	=	tpe.[PROVIDER_NAME_SUFFIX]	,
				 [PROVIDER_NAME_DEGREE]	=	tpe.[PROVIDER_NAME_DEGREE]	,
				 [PROVIDER_GENERAL_COMMENTS]	=	tpe.[PROVIDER_GENERAL_COMMENTS]	,
				 [PROVIDER_QUICK_CODE]	=	substring(tpe.[PROVIDER_QUICK_CODE] ,1,50)	,
				 [PROVIDER_REGISTRATION_NUM]	=	substring(tpe.[PROVIDER_REGISTRATION_NUM] ,1,50)	,
				 [PROVIDER_REGISRATION_NUM_AUTH]	=	substring(tpe.[PROVIDER_REGISRATION_NUM_AUTH] ,1,50)	,
				 [PROVIDER_STREET_ADDRESS_1]	=	substring(tpe.[PROVIDER_STREET_ADDRESS_1],1,50),
				 [PROVIDER_STREET_ADDRESS_2]	=	substring(tpe.[PROVIDER_STREET_ADDRESS_2],1,50),
				 [PROVIDER_CITY]	=	substring(tpe.[PROVIDER_CITY] ,1,50)	,
				 [PROVIDER_STATE]	=	tpe.[PROVIDER_STATE]	,
				 [PROVIDER_STATE_CODE]	=	tpe.[PROVIDER_STATE_CODE]	,
				 [PROVIDER_ZIP]	=	tpe.[PROVIDER_ZIP]	,
				 [PROVIDER_COUNTY]	=	tpe.[PROVIDER_COUNTY]	,
				 [PROVIDER_COUNTY_CODE]	=	tpe.[PROVIDER_COUNTY_CODE]	,
				 [PROVIDER_COUNTRY]	=	tpe.[PROVIDER_COUNTRY]	,
				 [PROVIDER_ADDRESS_COMMENTS]	=	tpe.[PROVIDER_ADDRESS_COMMENTS]	,
				 [PROVIDER_PHONE_WORK]	=	tpe.[PROVIDER_PHONE_WORK]	,
				 [PROVIDER_PHONE_EXT_WORK]	=	 tpe.[PROVIDER_PHONE_EXT_WORK]	,
				 [PROVIDER_EMAIL_WORK]	=	substring(tpe.[PROVIDER_EMAIL_WORK] ,1,50)	,
				 [PROVIDER_PHONE_COMMENTS]	=	tpe.[PROVIDER_PHONE_COMMENTS]	,
				 [PROVIDER_PHONE_CELL]	=	tpe.[PROVIDER_PHONE_CELL]	,
				 [PROVIDER_ENTRY_METHOD]	=	tpe.[PROVIDER_ENTRY_METHOD]	,
				 [PROVIDER_LAST_CHANGE_TIME]	=	tpe.[PROVIDER_LAST_CHANGE_TIME]	,
				 [PROVIDER_ADD_TIME]	=	tpe.[PROVIDER_ADD_TIME]	,
				 [PROVIDER_ADDED_BY]	=	tpe.[PROVIDER_ADDED_BY]	,
				 [PROVIDER_LAST_UPDATED_BY]	=	tpe.[PROVIDER_LAST_UPDATED_BY]	
				from dbo.TMP_D_PROVIDER_E tpe
				Where tpe.PROVIDER_KEY  = D_PROVIDER.PROVIDER_KEY
				;


				
					     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 17;
			SET @PROC_STEP_NAME = ' INSERTING NEW ENTERIES D_PROVIDER'; 

			/*
		IF OBJECT_ID('dbo.TMP_D_PROVIDER_E', 'U') IS NOT NULL  
				 drop table dbo.TMP_D_PROVIDER_E;
				 */
		

			
				
			--	%DBLOAD (D_PROVIDER, D_PROVIDER_N);
			
		INSERT INTO [dbo].[D_PROVIDER]
           ([PROVIDER_UID]
           ,[PROVIDER_KEY]
           ,[PROVIDER_LOCAL_ID]
           ,[PROVIDER_RECORD_STATUS]
           ,[PROVIDER_NAME_PREFIX]
           ,[PROVIDER_FIRST_NAME]
           ,[PROVIDER_MIDDLE_NAME]
           ,[PROVIDER_LAST_NAME]
           ,[PROVIDER_NAME_SUFFIX]
           ,[PROVIDER_NAME_DEGREE]
           ,[PROVIDER_GENERAL_COMMENTS]
           ,[PROVIDER_QUICK_CODE]
           ,[PROVIDER_REGISTRATION_NUM]
           ,[PROVIDER_REGISRATION_NUM_AUTH]
           ,[PROVIDER_STREET_ADDRESS_1]
           ,[PROVIDER_STREET_ADDRESS_2]
           ,[PROVIDER_CITY]
           ,[PROVIDER_STATE]
           ,[PROVIDER_ZIP]
           ,[PROVIDER_COUNTY]
           ,[PROVIDER_COUNTRY]
           ,[PROVIDER_ADDRESS_COMMENTS]
           ,[PROVIDER_PHONE_WORK]
           ,[PROVIDER_PHONE_EXT_WORK]
           ,[PROVIDER_EMAIL_WORK]
           ,[PROVIDER_PHONE_COMMENTS]
           ,[PROVIDER_PHONE_CELL]
           ,[PROVIDER_ENTRY_METHOD]
           ,[PROVIDER_LAST_CHANGE_TIME]
           ,[PROVIDER_ADD_TIME]
           ,[PROVIDER_ADDED_BY]
           ,[PROVIDER_LAST_UPDATED_BY]
           ,[PROVIDER_STATE_CODE]
           ,[PROVIDER_COUNTY_CODE])
     			SELECT [PROVIDER_UID]
				  ,[PROVIDER_KEY]
				  ,[PROVIDER_LOCAL_ID]
				  ,[PROVIDER_RECORD_STATUS]
				  ,[PROVIDER_NAME_PREFIX]
				  ,[PROVIDER_FIRST_NAME]
				  ,[PROVIDER_MIDDLE_NAME]
				  ,[PROVIDER_LAST_NAME]
				  ,[PROVIDER_NAME_SUFFIX]
				  ,[PROVIDER_NAME_DEGREE]
				  ,[PROVIDER_GENERAL_COMMENTS]
				  ,case when cast ( PROVIDER_QUICK_CODE as varchar(50))= '' then null else cast ( PROVIDER_QUICK_CODE as varchar(50)) end
				  ,case when cast ( [PROVIDER_REGISTRATION_NUM] as varchar(50))= '' then null else cast ( [PROVIDER_REGISTRATION_NUM] as varchar(50)) end
				  ,case when cast ( [PROVIDER_REGISRATION_NUM_AUTH] as varchar(50))= '' then null else cast ( [PROVIDER_REGISRATION_NUM_AUTH] as varchar(50)) end
				  ,case when cast ( [PROVIDER_STREET_ADDRESS_1] as varchar(50))= '' then null else cast ( [PROVIDER_STREET_ADDRESS_1] as varchar(50)) end
				  ,case when cast ( [PROVIDER_STREET_ADDRESS_2] as varchar(50))= '' then null else cast ( [PROVIDER_STREET_ADDRESS_2] as varchar(50)) end
				  ,case when cast ( [PROVIDER_CITY] as varchar(50))= '' then null else cast ( [PROVIDER_CITY] as varchar(50)) end
				  ,[PROVIDER_STATE]
				  ,[PROVIDER_ZIP]
				  ,[PROVIDER_COUNTY]
				  ,[PROVIDER_COUNTRY]
				  ,case when [PROVIDER_ADDRESS_COMMENTS]= '' then null else [PROVIDER_ADDRESS_COMMENTS] end
				  ,case when [PROVIDER_PHONE_WORK]= '' then null else [PROVIDER_PHONE_WORK] end
				  ,case when [PROVIDER_PHONE_EXT_WORK] = '' then null else [PROVIDER_PHONE_EXT_WORK] end
				  ,case when cast ( [PROVIDER_EMAIL_WORK] as varchar(50))= '' then null else cast ( [PROVIDER_EMAIL_WORK] as varchar(50)) end
				  ,case when [PROVIDER_PHONE_COMMENTS]= '' then null else [PROVIDER_PHONE_COMMENTS] end
				  ,[PROVIDER_PHONE_CELL]
				  ,[PROVIDER_ENTRY_METHOD]
				  ,[PROVIDER_LAST_CHANGE_TIME]
				  ,[PROVIDER_ADD_TIME]
				  ,[PROVIDER_ADDED_BY]
				  ,[PROVIDER_LAST_UPDATED_BY]
				  ,[PROVIDER_STATE_CODE]
				  ,case when [PROVIDER_COUNTY_CODE]= '' then null else [PROVIDER_COUNTY_CODE] end

			  FROM [dbo].[TMP_D_PROVIDER_N]
			  ;

				

					     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO [DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PROVIDER','D_PROVIDER','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
		
		--PROC DATASETS LIBRARY = WORK NOLIST;DELETE L_PROVIDER_E L_PROVIDER_N D_PROVIDER_E D_PROVIDER_N S_PROVIDER_FINAL;RUN;

		
		

			IF OBJECT_ID('dbo.TMP_S_PROVIDER_INIT', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_INIT; 

		IF OBJECT_ID('dbo.TMP_PROVIDER_UID_COLL', 'U') IS NOT NULL  
				 drop table dbo.TMP_PROVIDER_UID_COLL; 

		IF OBJECT_ID('dbo.TMP_S_INITPROVIDER', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_INITPROVIDER; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_NAME', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_NAME; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_WITH_NM', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_WITH_NM; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_POSTAL_LOCATOR', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_POSTAL_LOCATOR; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_TELE_LOCATOR_OFFICE', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_TELE_LOCATOR_OFFICE; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_TELE_LOCATOR_CELL', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_TELE_LOCATOR_CELL; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_LOCATOR', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_LOCATOR; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_QEC_ENTITY_ID', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_QEC_ENTITY_ID; 

		IF OBJECT_ID('dbo.TMP_PRN_ENTITY_ID', 'U') IS NOT NULL  
				 drop table dbo.TMP_PRN_ENTITY_ID; 

		IF OBJECT_ID('dbo.TMP_S_PROVIDER_FINAL', 'U') IS NOT NULL  
				 drop table dbo.TMP_S_PROVIDER_FINAL; 


		IF OBJECT_ID('dbo.TMP_L_PROVIDER_N', 'U') IS NOT NULL  
				 drop table dbo.TMP_L_PROVIDER_N; 

		IF OBJECT_ID('dbo.TMP_L_PROVIDER_E', 'U') IS NOT NULL  
				 drop table dbo.TMP_L_PROVIDER_E; 

		IF OBJECT_ID('dbo.TMP_D_PROVIDER_N', 'U') IS NOT NULL  
				 drop table dbo.TMP_D_PROVIDER_N; 

		IF OBJECT_ID('dbo.TMP_D_PROVIDER_E', 'U') IS NOT NULL  
				 drop table dbo.TMP_D_PROVIDER_E; 


   


    BEGIN TRANSACTION ;
	
	SET @Proc_Step_no = 20;
	SET @Proc_Step_Name = 'SP_COMPLETE'; 


	INSERT INTO [dbo].[job_flow_log] (
		    batch_id
			,[Dataflow_Name]
		   ,[package_Name]
		    ,[Status_Type] 
           ,[step_number]
           ,[step_name]
           ,[row_count]
           )
		   VALUES
           (
		   @batch_id,
           'D_PROVIDER'
           ,'D_PROVIDER'
		   ,'COMPLETE'
		   ,@Proc_Step_no
		   ,@Proc_Step_name
           ,@RowCount_no
		   );
  
	
	COMMIT TRANSACTION;
  END TRY

  BEGIN CATCH
  
     
     IF @@TRANCOUNT > 0   ROLLBACK TRANSACTION;
 
  
	
	DECLARE @ErrorNumber INT = ERROR_NUMBER();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorState INT = ERROR_STATE();
 
	
    INSERT INTO [dbo].[job_flow_log] (
		    batch_id
		   ,[Dataflow_Name]
		   ,[package_Name]
		    ,[Status_Type] 
           ,[step_number]
           ,[step_name]
           ,[Error_Description]
		   ,[row_count]
           )
		   VALUES
           (
           @batch_id
           ,'D_PROVIDER'
           ,'D_PROVIDER'
		   ,'ERROR'
		   ,@Proc_Step_no
		   ,'ERROR - '+ @Proc_Step_name
           , 'Step -' +CAST(@Proc_Step_no AS VARCHAR(3))+' -' +CAST(@ErrorMessage AS VARCHAR(500))
           ,0
		   );
  

      return -1 ;

	END CATCH
	
END

;



GO
