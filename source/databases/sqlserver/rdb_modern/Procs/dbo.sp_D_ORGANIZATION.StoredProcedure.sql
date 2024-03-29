USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_D_ORGANIZATION]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[sp_D_ORGANIZATION]
  @batch_id BIGINT
 as

  BEGIN
  

	      
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
							batch_id         ---------------@batch_id
						   ,[Dataflow_Name]  --------------'D_ORGANIZATION'
						   ,[package_Name]   --------------'D_ORGANIZATION'
						   ,[Status_Type]    ---------------START
						   ,[step_number]    ---------------@Proc_Step_no
						   ,[step_name]   ------------------@Proc_Step_Name=sp_start
						   ,[row_count] --------------------0
						   )
						   VALUES
						   (
						   @batch_id
						   ,'D_ORGANIZATION'
						   ,'D_ORGANIZATION'
						   ,'START'
						   ,@Proc_Step_no
						   ,@Proc_Step_Name
						   ,0
						   );
		  
		COMMIT TRANSACTION;
			
			
				SELECT @batch_start_time = batch_start_dttm,@batch_end_time = batch_end_dttm
				FROM [dbo].[job_batch_log]
				where  status_type = 'start' 
			    and  type_code='MasterETL';

------------------------------------------2. Create Table TMP_S_INITORGANIZATION_INIT_TEMP-----------------------//////////--------------
		BEGIN TRANSACTION;

									SET @PROC_STEP_NO = 2;
									SET @PROC_STEP_NAME = ' GENERATING  TMP_S_INITORGANIZATION_INIT_TEMP'; 

								   IF OBJECT_ID('dbo.TMP_S_INITORGANIZATION_INIT_TEMP', 'U') IS NOT NULL   
									 drop table dbo.TMP_S_INITORGANIZATION_INIT_TEMP  ;

						SELECT 
						ORGANIZATION.ORGANIZATION_UID       AS 'ORGANIZATION_UID',
						LTRIM(RTRIM(ORGANIZATION.LOCAL_ID)) AS  'ORGANIZATION_LOCAL_ID',              
						LTRIM(RTRIM(SUBSTRING(ORGANIZATION.DESCRIPTION,1,1000))) AS 'ORGANIZATION_GENERAL_COMMENTS',      
						LTRIM(RTRIM(SUBSTRING(ORGANIZATION.ELECTRONIC_IND,1,1))) AS  'ORGANIZATION_ENTRY_METHOD',
						ORGANIZATION.ADD_TIME AS  'ORGANIZATION_ADD_TIME',
						ORGANIZATION.LAST_CHG_TIME AS 'ORGANIZATION_LAST_CHANGE_TIME',
						LTRIM(RTRIM(NAICS.CODE_SHORT_DESC_TXT)) AS  'ORGANIZATION_STAND_IND_CLASS',
						LTRIM(RTRIM(SUBSTRING(ORGANIZATION.RECORD_STATUS_CD,1,20))) AS  'ORGANIZATION_RECORD_STATUS',
						ORGANIZATION.ADD_USER_ID,
						ORGANIZATION.LAST_CHG_USER_ID
						INTO dbo.TMP_S_INITORGANIZATION_INIT_TEMP
						FROM nbs_changedata.dbo.ORGANIZATION ORGANIZATION with(nolock)
						LEFT OUTER JOIN NBS_SRTE.dbo.NAICS_INDUSTRY_CODE NAICS with (Nolock)
						ON  NAICS.CODE=ORGANIZATION.STANDARD_INDUSTRY_CLASS_CD 
						WHERE ORGANIZATION.LAST_CHG_TIME >= @batch_start_time AND ORGANIZATION.LAST_CHG_TIME<  @batch_end_time;
						
						SELECT @ROWCOUNT_NO = @@ROWCOUNT;
					
						INSERT INTO [DBO].[JOB_FLOW_LOG] 
						(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
						VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

		COMMIT TRANSACTION;
	
		------------------------3. Create a table TMP_ORGANIZATION_UID_COLL-----------------------------------------------------------

			
		BEGIN TRANSACTION;

							SET @PROC_STEP_NO = 3;
							SET @PROC_STEP_NAME = ' GENERATING TMP_ORGANIZATION_UID_COLL'; 
							
						
							IF OBJECT_ID('dbo.TMP_ORGANIZATION_UID_COLL', 'U') IS NOT NULL   
								 drop table dbo.TMP_ORGANIZATION_UID_COLL ;	
			
					
							SELECT ORGANIZATION_UID AS 'ORGANIZATION_UID'
							INTO dbo.TMP_ORGANIZATION_UID_COLL
							FROM dbo.TMP_S_INITORGANIZATION_INIT_TEMP with (nolock);

							SELECT @ROWCOUNT_NO = @@ROWCOUNT;
					
					
							INSERT INTO [DBO].[JOB_FLOW_LOG] 
							(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
							VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							CREATE NONCLUSTERED INDEX IDX_TMP_ORGANIZATION_UID_COLL
							ON [dbo].[TMP_ORGANIZATION_UID_COLL] ([ORGANIZATION_UID])
					 

		COMMIT TRANSACTION;
							-----------4.CREATE TABLE  TMP_S_INITORGANIZATION-------------------////////////------------------------
		BEGIN TRANSACTION

							SET @PROC_STEP_NO = 4;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_INITORGANIZATION'; 



							IF OBJECT_ID('dbo.TMP_S_INITORGANIZATION', 'U') IS NOT NULL   
								DROP TABLE dbo.TMP_S_INITORGANIZATION  ;

							SELECT A.*, 
					        B.user_first_nm AS  'ADD_USER_FIRST_NAME',
						    B.user_last_nm AS  'ADD_USER_LAST_NAME', 
						    C.user_first_nm AS  'CHG_USER_FIRST_NAME',
						    C.user_last_nm AS  'CHG_USER_LAST_NAME',
                            CAST (null as  [varchar](50))  as ORGANIZATION_ADDED_BY,
							CAST (null as  [varchar](50))  as ORGANIZATION_LAST_UPDATED_BY 
							INTO dbo.TMP_S_INITORGANIZATION
							FROM dbo.TMP_S_INITORGANIZATION_INIT_TEMP A
							LEFT OUTER JOIN nbs_changedata.dbo.Auth_user B With (Nolock)ON A.ADD_USER_ID=B.NEDSS_ENTRY_ID
							LEFT OUTER JOIN nbs_changedata.dbo.Auth_user C With (Nolock)ON A.ADD_USER_ID=C.NEDSS_ENTRY_ID;
						

							CREATE NONCLUSTERED INDEX [IDX_TMP_S_INITORGANIZATION]
							ON [dbo].[TMP_S_INITORGANIZATION] ([ORGANIZATION_RECORD_STATUS])
			
						------UPDATE ON  TMP_S_INITORGANIZATION Set

							UPDATE   dbo.TMP_S_INITORGANIZATION
							SET [ORGANIZATION_RECORD_STATUS] = 'ACTIVE'
							WHERE [ORGANIZATION_RECORD_STATUS] = '';

							UPDATE   dbo.TMP_S_INITORGANIZATION
							SET [ORGANIZATION_RECORD_STATUS] = 'INACTIVE'
							WHERE [ORGANIZATION_RECORD_STATUS] = 'SUPERCEDED';

							UPDATE   dbo.TMP_S_INITORGANIZATION
							SET [ORGANIZATION_RECORD_STATUS] = 'INACTIVE'
							WHERE [ORGANIZATION_RECORD_STATUS] = 'LOG_DEL';

							

							 ---Changed on 4/5/2021
						UPDATE dbo.TMP_S_INITORGANIZATION
							SET ORGANIZATION_ADDED_BY = CAST(( CASE
															WHEN LEN(rtrim(ADD_USER_LAST_NAME)) > 0 and LEN(rtrim(ADD_USER_FIRST_NAME))> 0 
																THEN rtrim(ADD_USER_LAST_NAME)+', '+ rtrim(ADD_USER_FIRST_NAME)
															WHEN LEN(rtrim(ADD_USER_FIRST_NAME)) > 0  
																THEN rtrim(ADD_USER_FIRST_NAME)
															WHEN LEN(rtrim(ADD_USER_LAST_NAME))> 0 
																then rtrim(ADD_USER_LAST_NAME)
															ELSE ''
														  END
														) as varchar(50));
							
							UPDATE dbo.TMP_S_INITORGANIZATION
							SET ORGANIZATION_LAST_UPDATED_BY = CAST(( CASE
															WHEN LEN(rtrim(CHG_USER_LAST_NAME)) > 0 and LEN(rtrim(CHG_USER_FIRST_NAME))> 0 
																THEN rtrim(CHG_USER_LAST_NAME)+', '+ rtrim(CHG_USER_FIRST_NAME)
															WHEN LEN(rtrim(CHG_USER_FIRST_NAME)) > 0  
																THEN rtrim(CHG_USER_FIRST_NAME)
															WHEN LEN(rtrim(CHG_USER_LAST_NAME))> 0 
																then rtrim(CHG_USER_LAST_NAME)
															ELSE ''
														  END
														) as varchar(50));
						   
		    
						
							SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							INSERT INTO [DBO].[JOB_FLOW_LOG] 
							(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
							VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

		COMMIT TRANSACTION;
		------------------------------------5. CREATE TABLE TMP_S_ORGANIZATION_NAME------------------	
				
		BEGIN TRANSACTION;

							SET @PROC_STEP_NO = 5;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_ORGANIZATION_NAME'; 

							IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_NAME', 'U') IS NOT NULL  
								 drop table dbo.TMP_S_ORGANIZATION_NAME;
								
								SELECT 
								LTRIM(RTRIM(SUBSTRING (NM_TXT,1,50))) AS  'ORGANIZATION_NAME',
									   C.ORGANIZATION_UID AS  'ORGANIZATION_UID' 
								INTO  dbo.TMP_S_ORGANIZATION_NAME
								FROM  dbo.TMP_ORGANIZATION_UID_COLL C
								LEFT JOIN nbs_changedata.dbo.ORGANIZATION_NAME O with (nolock)
								ON O.ORGANIZATION_UID=C.ORGANIZATION_UID
								ORDER BY O.ORGANIZATION_UID;
								
							SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							INSERT INTO [DBO].[JOB_FLOW_LOG] 
							(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
							VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

		COMMIT TRANSACTION;
					
		-------------------------------------6. CREATE TABLE TMP_S_ORGANIZATION_WITH_NM----------------------------/////////-------------------------	
		BEGIN TRANSACTION					
							SET @PROC_STEP_NO = 6;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_ORGANIZATION_WITH_NM'; 

								IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_WITH_NM', 'U') IS NOT NULL  
								drop table dbo.TMP_S_ORGANIZATION_WITH_NM;
							
							SELECT 
							S.[ORGANIZATION_LOCAL_ID],
							S.[ORGANIZATION_GENERAL_COMMENTS],
							S.[ORGANIZATION_ENTRY_METHOD],
							S.[ORGANIZATION_ADD_TIME],
							S.[ORGANIZATION_LAST_CHANGE_TIME],
							S.[ORGANIZATION_STAND_IND_CLASS],
							S.[ORGANIZATION_RECORD_STATUS],
							S.[ADD_USER_ID],
							S.[LAST_CHG_USER_ID],
							S.[ADD_USER_FIRST_NAME],
							S.[ADD_USER_LAST_NAME],
							S.[CHG_USER_FIRST_NAME],
							S.[ORGANIZATION_ADDED_BY],
							S.[ORGANIZATION_LAST_UPDATED_BY],
							SN.[ORGANIZATION_NAME],
							SN.[ORGANIZATION_UID]
							INTO dbo.TMP_S_ORGANIZATION_WITH_NM
							FROM dbo.TMP_S_INITORGANIZATION  S
							LEFT OUTER JOIN dbo.TMP_S_ORGANIZATION_NAME SN ON S.ORGANIZATION_UID= SN.ORGANIZATION_UID;
								

								SELECT @ROWCOUNT_NO = @@ROWCOUNT;

								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO); 
								
					

		COMMIT TRANSACTION;
		----------------------------------7. CREATE TABLE TMP_S_ORGANIZATION_POSTAL_LOCATOR----------------//////////---------------------------
		

		BEGIN TRANSACTION

							SET @PROC_STEP_NO = 7;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_ORGANIZATION_POSTAL_LOCATOR'; 

								IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_POSTAL_LOCATOR', 'U') IS NOT NULL  
								DROP TABLE  dbo.TMP_S_ORGANIZATION_POSTAL_LOCATOR;


							with CTE as (
							SELECT DISTINCT
							LTRIM(rtrim(SUBSTRING(PL.CITY_DESC_TXT,1,50)))      AS  'ORGANIZATION_CITY',             
							LTRIM(rtrim(SUBSTRING(PL.CNTRY_CD,1,20)))	        AS  'ORGANIZATION_COUNTRY',            
							LTRIM(rtrim(SUBSTRING(PL.CNTY_CD,1,20)))	AS  'ORGANIZATION_COUNTY_CODE',              
							LTRIM(rtrim(SUBSTRING(PL.STATE_CD,1,20)))	AS 'ORGANIZATION_STATE_CODE',               
							LTRIM(rtrim(SUBSTRING(PL.STREET_ADDR1,1,50))) AS  'ORGANIZATION_STREET_ADDRESS_1',
							LTRIM(rtrim(SUBSTRING(PL.STREET_ADDR2,1,50)))	AS 'ORGANIZATION_STREET_ADDRESS_2',
							LTRIM(rtrim(SUBSTRING(PL.ZIP_CD,1,10))) AS  'ORGANIZATION_ZIP',
							LTRIM(rtrim(SC.CODE_DESC_TXT)) AS  'ORGANIZATION_STATE_DESC',--------ORGANIZATION_STATE var(50)
							LTRIM(rtrim(SUBSTRING(CCV.CODE_DESC_TXT,1,50))) AS  'ORGANIZATION_COUNTY_DESC',
							LTRIM(rtrim(SUBSTRING(CC.CODE_SHORT_DESC_TXT,1,50))) AS  'ORGANIZATION_COUNTRY_DESC',
							LTRIM(rtrim(ELP.LOCATOR_DESC_TXT)) AS  'ORGANIZATION_ADDRESS_COMMENTS',
							LTRIM(rtrim(ELP.ENTITY_UID)) as ENTITY_UID_POSTAL_LOCATOR,
							Cast (null as  [varchar](50))  ORGANIZATION_STATE ,
						    Cast (null as  [varchar](50))  ORGANIZATION_COUNTY ,
							ROW_NUMBER() OVER (
									 PARTITION BY elp.ENTITY_UID 
									 ORDER BY pl.POSTAL_LOCATOR_UID DESC
         					   ) AS [ROWNO]   
							
							FROM dbo.TMP_ORGANIZATION_UID_COLL OUC
							LEFT OUTER JOIN nbs_changedata.dbo.ENTITY_LOCATOR_PARTICIPATION ELP with(nolock) ON OUC.ORGANIZATION_UID= ELP.ENTITY_UID and ELP.record_status_cd='Active'---added 4/12/21
							LEFT OUTER JOIN nbs_changedata.dbo.POSTAL_LOCATOR PL  with(nolock) ON ELP.LOCATOR_UID=PL.POSTAL_LOCATOR_UID  
							LEFT OUTER JOIN NBS_SRTE.dbo.STATE_CODE SC  with(nolock) ON SC.STATE_CD=PL.STATE_CD
							LEFT OUTER JOIN NBS_SRTE.dbo.COUNTRY_CODE CC  with(nolock) ON CC.CODE=PL.CNTRY_CD
							LEFT OUTER JOIN NBS_SRTE.dbo.STATE_COUNTY_CODE_VALUE CCV  with(nolock) ON CCV.CODE=PL.CNTY_CD	
							WHERE ELP.USE_CD='WP'
							AND ELP.CD='O'
							AND ELP.CLASS_CD='PST'	
								)
							 select * INTO dbo.TMP_S_ORGANIZATION_POSTAL_LOCATOR  from CTE where rowno = 1;

							 alter table dbo.TMP_S_ORGANIZATION_POSTAL_LOCATOR
				             drop column ROWNO ;
								
							UPDATE dbo.TMP_S_ORGANIZATION_POSTAL_LOCATOR SET ORGANIZATION_STATE=ORGANIZATION_STATE_DESC   WHERE  LEN(RTRIM(ORGANIZATION_STATE_DESC))>= 1;
							UPDATE dbo.TMP_S_ORGANIZATION_POSTAL_LOCATOR SET ORGANIZATION_COUNTY=ORGANIZATION_COUNTY_DESC WHERE LEN(RTRIM(ORGANIZATION_COUNTY_DESC))>= 1;
							---UPDATE dbo.TMP_S_ORGANIZATION_POSTAL_LOCATOR SET ORGANIZATION_COUNTRY=ORGANIZATION_COUNTRY_DESC WHERE LEN(RTRIM(ORGANIZATION_COUNTRY_DESC))>= 1;
			 	          	UPDATE dbo.TMP_S_ORGANIZATION_POSTAL_LOCATOR SET ORGANIZATION_COUNTRY=SUBSTRING(LTRIM(RTRIM(ORGANIZATION_COUNTRY_DESC)),1,20) WHERE LEN(RTRIM(ORGANIZATION_COUNTRY_DESC))>= 1;
			 	   
						  
						  SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO); 
							
							
							COMMIT TRANSACTION;
	

		----------------------------------8. CREATE TABLE TMP_S_ORGANIZATION_TELE_LOCATOR_FAX-------------------------////////////----------------		
  
		BEGIN TRANSACTION					
							SET @PROC_STEP_NO = 8;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_TELE_LOCATOR_FAX'; 

								IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_TELE_LOCATOR_FAX', 'U') IS NOT NULL  
								DROP TABLE dbo.TMP_S_ORGANIZATION_TELE_LOCATOR_FAX;
								
							
							with CTE as (
							SELECT DISTINCT
							ELP.ENTITY_UID as ENTITY_UID_FAX,-----------------------------TMP_S_ORGANIZATION_TELE_LOCATOR_FAX
							LTRIM(RTRIM(SUBSTRING(TL.PHONE_NBR_TXT,1,20))) AS 'ORGANIZATION_FAX',
							ROW_NUMBER() OVER (
									 PARTITION BY elp.ENTITY_UID 
									 ORDER BY TL.TELE_LOCATOR_UID DESC
         					   ) AS [ROWNO]  


							FROM dbo.TMP_ORGANIZATION_UID_COLL OUC
							INNER JOIN nbs_changedata.dbo.ENTITY_LOCATOR_PARTICIPATION ELP with(Nolock) ON OUC.ORGANIZATION_UID= ELP.ENTITY_UID and ELP.record_status_cd='Active'---added 4/15/21
							INNER JOIN nbs_changedata.dbo.TELE_LOCATOR TL with (Nolock) ON ELP.LOCATOR_UID=TL.TELE_LOCATOR_UID 
							WHERE ELP.USE_CD='WP'
							AND ELP.CD='FAX'
							AND ELP.class_cd='TELE'
							)

							SELECT * INTO dbo.TMP_S_ORGANIZATION_TELE_LOCATOR_FAX
							FROM CTE where ROWNO =1

							 alter table dbo.TMP_S_ORGANIZATION_TELE_LOCATOR_FAX
				             drop column ROWNO ;


							SELECT @ROWCOUNT_NO = @@ROWCOUNT;

								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

		COMMIT TRANSACTION;

		---------------------------------9. CREATE TABLE TMP_S_ORGANIZATION_TELE_LOCATOR_OFFICE-------------------------------------////////////-----------					


		BEGIN TRANSACTION					
							SET @PROC_STEP_NO = 9;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_ORGANIZATION_TELE_LOCATOR_OFFICE'; 

								IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_TELE_LOCATOR_OFFICE', 'U') IS NOT NULL  
								DROP TABLE dbo.TMP_S_ORGANIZATION_TELE_LOCATOR_OFFICE;
								

                            With CTE as (
							SELECT DISTINCT
								ELP.ENTITY_UID as ENTITY_UID_TELE_LOCATOR,------------------TMP_S_ORGANIZATION_TELE_LOCATOR_OFFICE
								LTRIM(RTRIM(SUBSTRING(TL.EXTENSION_TXT,1,20))) AS  'ORGANIZATION_PHONE_EXT_WORK',        
								LTRIM(RTRIM(SUBSTRING (TL.PHONE_NBR_TXT,1,20))) AS  'ORGANIZATION_PHONE_WORK', 
								Case When LEN(RTRIM(LTRIM(SUBSTRING(TL.EMAIL_ADDRESS,1,20))))=0 then null else EMAIL_ADDRESS end  AS  'ORGANIZATION_EMAIL',
								RTRIM(LTRIM(ELP.LOCATOR_DESC_TXT)) AS 'ORGANIZATION_PHONE_COMMENTS',
								ROW_NUMBER() OVER (
									 PARTITION BY elp.ENTITY_UID 
									 ORDER BY TL.TELE_LOCATOR_UID DESC
         					   ) AS [ROWNO] 

								FROM dbo.TMP_ORGANIZATION_UID_COLL OUC
								INNER JOIN nbs_changedata.dbo.ENTITY_LOCATOR_PARTICIPATION ELP with(Nolock)ON OUC.ORGANIZATION_UID=ELP.ENTITY_UID and ELP.record_status_cd='Active'----added  4/12/21
								INNER JOIN nbs_changedata.dbo.TELE_LOCATOR TL with(Nolock) ON ELP.LOCATOR_UID=TL.TELE_LOCATOR_UID 
								WHERE ELP.USE_CD='WP'
								AND ELP.CD='PH'
								AND ELP.CLASS_CD='TELE'
								)

								Select * INTO dbo.TMP_S_ORGANIZATION_TELE_LOCATOR_OFFICE
								From CTE where ROWNO =1
								
								alter table  dbo.TMP_S_ORGANIZATION_TELE_LOCATOR_OFFICE
				                drop column ROWNO ;

						    	SELECT @ROWCOUNT_NO = @@ROWCOUNT;

								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

								

			COMMIT TRANSACTION;
	
		-------------------------------------------10. CREATE TABLE TMP_S_ORGANIZATION_LOCATOR-----------------------------------/////////////---------

			BEGIN TRANSACTION					
							SET @PROC_STEP_NO = 10;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_ORGANIZATION_LOCATOR'; 

								IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_LOCATOR', 'U') IS NOT NULL  
								DROP TABLE dbo.TMP_S_ORGANIZATION_LOCATOR;
								
						
								
							----SELECT  ---TLO.*,PL.*,LF.*,OUC.ORGANIZATION_UID 
							SELECT
							      CASE WHEN LEN(TLO.Organization_Phone_ext_work)=0 then NULL Else Organization_Phone_ext_work end as Organization_Phone_ext_work,
								   TLO.Organization_Phone_Work,
								   CASE WHEN LEN(TLO.Organization_Email)=0 then NULL ELSE Organization_Email end as Organization_Email ,
	                               CASE WHEN LEN(TLO.Organization_Phone_Comments)=0 then NULL ELSE  Organization_Phone_Comments end as Organization_Phone_Comments,
								   PL.ORGANIZATION_CITY	,
								   PL.ORGANIZATION_COUNTRY	,
								   PL.ORGANIZATION_COUNTY_CODE,	
								   PL.ORGANIZATION_STATE_CODE,	
								   CASE WHEN LEN(PL.ORGANIZATION_STREET_ADDRESS_1)=0 then null else  ORGANIZATION_STREET_ADDRESS_1 end as ORGANIZATION_STREET_ADDRESS_1 ,
	                                CASE WHEN LEN(PL.ORGANIZATION_STREET_ADDRESS_2)=0 then null else  ORGANIZATION_STREET_ADDRESS_2 end as ORGANIZATION_STREET_ADDRESS_2,
								   PL.ORGANIZATION_ZIP	,
								   PL.ORGANIZATION_STATE_DESC	,
								   PL.ORGANIZATION_COUNTY_DESC	,
								   PL.ORGANIZATION_COUNTRY_DESC	,
								   CASE WHEN LEN(PL.ORGANIZATION_ADDRESS_COMMENTS)=0 then null else  PL.ORGANIZATION_ADDRESS_COMMENTS end as ORGANIZATION_ADDRESS_COMMENTS ,
								   PL.ORGANIZATION_STATE	,
								   PL.ORGANIZATION_COUNTY,
								   LF.ORGANIZATION_FAX,
								   OUC.ORGANIZATION_UID
							
							INTO dbo.TMP_S_ORGANIZATION_LOCATOR 
							FROM dbo.TMP_ORGANIZATION_UID_COLL OUC
							LEFT OUTER JOIN  dbo.TMP_S_ORGANIZATION_TELE_LOCATOR_OFFICE TLO with(Nolock)ON OUC.ORGANIZATION_UID=TLO.ENTITY_UID_TELE_LOCATOR
							LEFT OUTER JOIN  dbo.TMP_S_ORGANIZATION_POSTAL_LOCATOR		PL  With(NoLock)ON OUC.ORGANIZATION_UID=PL.ENTITY_UID_POSTAL_LOCATOR
				            LEFT OUTER JOIN  dbo.TMP_S_ORGANIZATION_TELE_LOCATOR_FAX	LF  With(Nolock)ON OUC.ORGANIZATION_UID=LF.ENTITY_UID_FAX
								

							SELECT @ROWCOUNT_NO = @@ROWCOUNT;

								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  


								

			COMMIT TRANSACTION;
		----------------------------------------11. CREATE TABLE TMP_S_ORGANIZATION_QEC_ENTITY_ID----------------////////---------------------
							
			BEGIN TRANSACTION					
							SET @PROC_STEP_NO = 11;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_ORGANIZATION_QEC_ENTITY_ID'; 

								IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_QEC_ENTITY_ID', 'U') IS NOT NULL  
								DROP TABLE dbo.TMP_S_ORGANIZATION_QEC_ENTITY_ID;
								
								
							SELECT DISTINCT ORGANIZATION_UID as ORGANIZATION_UID_QEC,
							 LTRIM(RTRIM(Substring(ROOT_EXTENSION_TXT, 1,50))) as ROOT_EXTENSION_TXT,
							 ASSIGNING_AUTHORITY_CD 
							INTO dbo.TMP_S_ORGANIZATION_QEC_ENTITY_ID
							FROM dbo.TMP_ORGANIZATION_UID_COLL OUC
							LEFT OUTER JOIN nbs_changedata.dbo.ENTITY_ID  With (Nolock) ON OUC.ORGANIZATION_UID=ENTITY_ID.ENTITY_UID
							AND ENTITY_ID.TYPE_CD = 'QEC';
							
							---Eliminate Duplication
							SELECT * INTO #TempQEC_ENTITY_ID FROM
							(Select * from dbo.TMP_S_ORGANIZATION_QEC_ENTITY_ID) as T
							
							
							IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_QEC_ENTITY_ID', 'U') IS NOT NULL  
							DROP TABLE  dbo.TMP_S_ORGANIZATION_QEC_ENTITY_ID


							 Select * into  dbo.TMP_S_ORGANIZATION_QEC_ENTITY_ID from(
							 Select distinct Result.* from  #TempQEC_ENTITY_ID PL
                             Cross apply (Select top 1 * from  #TempQEC_ENTITY_ID
						     where  #TempQEC_ENTITY_ID.ORGANIZATION_UID_QEC =PL.ORGANIZATION_UID_QEC) result)A



							SELECT @ROWCOUNT_NO = @@ROWCOUNT;

								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

								Drop table  #TempQEC_ENTITY_ID;
			COMMIT TRANSACTION;
							
		---------------------------------------------12. CREATE TABLE TMP_FI_ENTITY_ID--------------------------------------------//////----------------
			BEGIN TRANSACTION					
							SET @PROC_STEP_NO = 12;
							SET @PROC_STEP_NAME = 'GENERATING TMP_FI_ENTITY_ID'; 

								IF OBJECT_ID('dbo.TMP_FI_ENTITY_ID', 'U') IS NOT NULL  
								DROP TABLE dbo.TMP_FI_ENTITY_ID;
								

								 With CTE as (
							          SELECT DISTINCT ORGANIZATION_UID as ORGANIZATION_UID_FI ,
								            LTRIM(RTRIM(SUBSTRING(ROOT_EXTENSION_TXT,1,50))) as ROOT_EXTENSION_TXT ,
								            ASSIGNING_AUTHORITY_CD ,
								            EI.record_status_cd ,
								            EI.TYPE_CD,
								            ROW_NUMBER() OVER (
									        PARTITION BY EI.ENTITY_UID 
									       ORDER BY  EI.ENTITY_UID DESC
         					              )AS [ROWNO] ------------------------------------------------added 4/19/21

								FROM dbo.TMP_ORGANIZATION_UID_COLL OUC
								LEFT OUTER JOIN nbs_changedata.dbo.ENTITY_ID EI With (Nolock)ON OUC.ORGANIZATION_UID=EI.ENTITY_UID AND record_status_cd='ACTIVE'
								AND EI.TYPE_CD = 'FI' 

								)

								Select * INTO dbo.TMP_FI_ENTITY_ID
								From CTE where ROWNO =1

								alter table  dbo.TMP_FI_ENTITY_ID
				                drop column ROWNO ;

							SELECT @ROWCOUNT_NO = @@ROWCOUNT;

								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

								
			COMMIT TRANSACTION;

		------------------------------------------13. CREATE TABLE TMP_CLIA_ENTITY_ID--------------------------------------------//////////--------------
			BEGIN TRANSACTION					
							SET @PROC_STEP_NO = 13;
							SET @PROC_STEP_NAME = ' GENERATING TMP_CLIA_ENTITY_ID'; 

								IF OBJECT_ID('dbo.TMP_CLIA_ENTITY_ID', 'U') IS NOT NULL  
								DROP TABLE dbo.TMP_CLIA_ENTITY_ID;
								
								SELECT DISTINCT ORGANIZATION_UID as ORGANIZATION_UI_CLIA ,
								LTRIM(RTRIM(SUBSTRING(ROOT_EXTENSION_TXT,1,50))) as ROOT_EXTENSION_TXT,
								 ASSIGNING_AUTHORITY_CD 
								INTO dbo.TMP_CLIA_ENTITY_ID
								FROM dbo.TMP_ORGANIZATION_UID_COLL OUC
								LEFT OUTER JOIN nbs_changedata.dbo.ENTITY_ID EI  With (Nolock) ON OUC.ORGANIZATION_UID=EI.ENTITY_UID 
								AND EI.TYPE_CD = 'CLIA';
								
								
									---Eliminate Duplication
							SELECT * INTO #Temp_CLIA_ENTITY_ID FROM
							(Select * from dbo.TMP_CLIA_ENTITY_ID) as T
							
							
							IF OBJECT_ID('dbo.TMP_CLIA_ENTITY_ID', 'U') IS NOT NULL  
							DROP TABLE dbo.TMP_CLIA_ENTITY_ID


							 Select * into  dbo.TMP_CLIA_ENTITY_ID from(
							 Select distinct Result.* from  #Temp_CLIA_ENTITY_ID PL
                             Cross apply (Select top 1 * from   #Temp_CLIA_ENTITY_ID
						     where  #Temp_CLIA_ENTITY_ID.ORGANIZATION_UI_CLIA =PL.ORGANIZATION_UI_CLIA) result)A

								SELECT @ROWCOUNT_NO = @@ROWCOUNT;

								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

								Drop Table #Temp_CLIA_ENTITY_ID;
			COMMIT TRANSACTION;

		  ----------------------------------------14. CREATE TABLE TMP_S_ORGANIZATION_FINAL------///////////----------------------------------------

		 
		  BEGIN TRANSACTION					
							SET @PROC_STEP_NO = 14;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_ORGANIZATION_FINAL'; 

								IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_FINAL', 'U') IS NOT NULL  
								DROP TABLE dbo.TMP_S_ORGANIZATION_FINAL;
			
 
									SELECT  DISTINCT 

									            NM.ORGANIZATION_UID,
												NM.ORGANIZATION_LOCAL_ID,	
												NM.ORGANIZATION_RECORD_STATUS,	
												NM.ORGANIZATION_NAME,	
												NM.ORGANIZATION_GENERAL_COMMENTS,
											    CASE WHEN LEN(QEC.ROOT_EXTENSION_TXT)=0 then NULL Else QEC.ROOT_EXTENSION_TXT end as ORGANIZATION_QUICK_CODE,
												---QEC.ROOT_EXTENSION_TXT AS 'ORGANIZATION_QUICK_CODE', 
												NM.ORGANIZATION_STAND_IND_CLASS,	
												FI.ROOT_EXTENSION_TXT AS  'ORGANIZATION_FACILITY_ID',
												FI.ASSIGNING_AUTHORITY_CD AS 'ORGANIZATION_FACILITY_ID_AUTH', ----FI_ENTITY_ID_CD
												L.ORGANIZATION_STREET_ADDRESS_1	,
											    L.ORGANIZATION_STREET_ADDRESS_2	,
											    isnull(NULLIF(L.ORGANIZATION_CITY,''),NULL) as ORGANIZATION_CITY,
												isnull(NULLIF(L.ORGANIZATION_STATE,''),NULL) as ORGANIZATION_STATE,
												isnull(NULLIF(L.ORGANIZATION_STATE_CODE,''),NULL) as ORGANIZATION_STATE_CODE,
												isnull(NULLIF(L.ORGANIZATION_ZIP,''),NULL) as ORGANIZATION_ZIP,
												isnull(NULLIF(L.ORGANIZATION_COUNTY,''),NULL) as ORGANIZATION_COUNTY,
											    isnull(NULLIF(L.ORGANIZATION_COUNTY_CODE,''),NULL) as ORGANIZATION_COUNTY_CODE,
												isnull(NULLIF(L.ORGANIZATION_COUNTRY,''),NULL) as ORGANIZATION_COUNTRY,
												isnull(NULLIF(L.ORGANIZATION_ADDRESS_COMMENTS,''),NULL) as ORGANIZATION_ADDRESS_COMMENTS,
												isnull(NULLIF(L.ORGANIZATION_PHONE_WORK,''),NULL) as 	ORGANIZATION_PHONE_WORK,
												isnull(NULLIF(L.ORGANIZATION_PHONE_EXT_WORK,''),NULL) as 	ORGANIZATION_PHONE_EXT_WORK,
												isnull(NULLIF(L.ORGANIZATION_EMAIL,''),NULL) as ORGANIZATION_EMAIL,
												L.ORGANIZATION_PHONE_COMMENTS,	
                                                NM.ORGANIZATION_ENTRY_METHOD,	
												NM.ORGANIZATION_LAST_CHANGE_TIME,	
												NM.ORGANIZATION_ADD_TIME,	
											    NM.ORGANIZATION_ADDED_BY,	
											    NM.ORGANIZATION_LAST_UPDATED_BY	,
											    isnull(NULLIF(L.ORGANIZATION_FAX,''),NULL) as ORGANIZATION_FAX
		
									INTO dbo.TMP_S_ORGANIZATION_FINAL
									FROM dbo.TMP_S_ORGANIZATION_WITH_NM NM
									LEFT OUTER JOIN dbo.TMP_S_ORGANIZATION_LOCATOR L with (nolock)ON NM.ORGANIZATION_UID=L.ORGANIZATION_UID
									LEFT OUTER JOIN dbo.TMP_S_ORGANIZATION_QEC_ENTITY_ID QEC with (nolock)ON  NM.ORGANIZATION_UID= QEC.ORGANIZATION_UID_QEC
									LEFT OUTER JOIN dbo.TMP_FI_ENTITY_ID FI with (NOLOCK) ON NM.ORGANIZATION_UID= FI.ORGANIZATION_UID_FI;


								SELECT @ROWCOUNT_NO = @@ROWCOUNT;

								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  
								
								
			COMMIT TRANSACTION;

		 ----------------------------------------15. CREATE TABLE S_ORGANIZATION----------STAGING TABLE UPDATE
	

					  BEGIN TRANSACTION					
							SET @PROC_STEP_NO = 15; 
							SET @PROC_STEP_NAME = ' GENERATING S_ORGANIZATION'; 

								IF OBJECT_ID('dbo.S_ORGANIZATION', 'U') IS NOT NULL  
								DROP TABLE dbo.S_ORGANIZATION;

				
								SELECT *
								into dbo.S_ORGANIZATION
								from dbo.TMP_S_ORGANIZATION_FINAL;
						

							
								SELECT @ROWCOUNT_NO = @@ROWCOUNT;

								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

								UPDATE S_ORGANIZATION
							---	SET FI_ENTITY_ID_CD = code_short_desc_txt
								SET ORGANIZATION_FACILITY_ID_AUTH = code_short_desc_txt
								FROM  dbo.S_ORGANIZATION
								INNER JOIN  nbs_srte.dbo.code_value_general With (NOLOCK)on code_value_general.code = dbo.S_ORGANIZATION.ORGANIZATION_FACILITY_ID_AUTH
								where code_set_nm='EI_AUTH_ORG'


			COMMIT TRANSACTION;

----------------------------------------16. CREATE TABLE TMP_L_ORGANIZATION_N --------------------------------------				
			BEGIN TRANSACTION;

							SET @PROC_STEP_NO = 16;
							SET @PROC_STEP_NAME = ' GENERATING TMP_L_ORGANIZATION_N'; 

							IF OBJECT_ID('dbo.TMP_L_ORGANIZATION_N', 'U') IS NOT NULL  
							DROP TABLE dbo.TMP_L_ORGANIZATION_N;

						
							
							CREATE TABLE [dbo].TMP_L_ORGANIZATION_N
							(
							[ORGANIZATION_id]  [int] IDENTITY(1,1) NOT NULL,
							[ORGANIZATION_UID_N] [bigint] NOT NULL,  
							[ORGANIZATION_KEY] [numeric](18, 0) NULL
							 ) ON [PRIMARY]
							 ;
			
							INSERT INTO [dbo].TMP_L_ORGANIZATION_N([ORGANIZATION_UID_N],[ORGANIZATION_KEY])
								
							SELECT DISTINCT [ORGANIZATION_UID],NULL
							  FROM dbo.S_ORGANIZATION
							EXCEPT 
							SELECT ORGANIZATION_UID ,null
							  FROM  [dbo].[L_ORGANIZATION];
						
				---Incremental Increase Process
							UPDATE dbo.TMP_L_ORGANIZATION_N
							SET ORGANIZATION_KEY= ORGANIZATION_ID + COALESCE((SELECT MAX(ORGANIZATION_KEY) FROM [dbo].[L_ORGANIZATION]),0);
						

								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;

				-------------------------------------17. CREATE TABLE TMP_L_ORGANIZATION_E ----------------------------------------------------				
	
			BEGIN TRANSACTION;

								SET @PROC_STEP_NO = 17;
								SET @PROC_STEP_NAME = ' GENERATING TMP_L_ORGANIZATION_E'; 

									IF OBJECT_ID('dbo.TMP_L_ORGANIZATION_E', 'U') IS NOT NULL  
									DROP TABLE dbo.TMP_L_ORGANIZATION_E;
									
									
										/* Added below code to Insert a null Row in L_Organization*/ --- 2021-01-13
								IF NOT EXISTS (SELECT * FROM L_ORGANIZATION WHERE ORGANIZATION_KEY=1) 
							BEGIN
							   INSERT INTO L_ORGANIZATION (ORGANIZATION_KEY,Organization_UID) VALUES (1,0);
							END

								SELECT SO.ORGANIZATION_UID as ORGANIZATION_UID_E ,
									   LO.[ORGANIZATION_KEY]
								INTO dbo.TMP_L_ORGANIZATION_E
								FROM dbo.S_ORGANIZATION SO ,---Staging
								     dbo.L_ORGANIZATION LO  ---LookUp
								WHERE SO.[ORGANIZATION_UID]= LO.ORGANIZATION_UID;
					
										--%DBLOAD (L_ORGANIZATIOJN, L_ORGANIZATION_N);

								INSERT INTO dbo.L_ORGANIZATION
								SELECT DISTINCT  [ORGANIZATION_KEY] ,[ORGANIZATION_UID_N]
								FROM  dbo.TMP_L_ORGANIZATION_N;	
					
								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

								
								
						COMMIT TRANSACTION;

			-----------------------------------18. CREATE TABLE TMP_D_ORGANIZATION_N---------------------------
	
			BEGIN TRANSACTION;

							SET @PROC_STEP_NO = 18;
							SET @PROC_STEP_NAME = ' GENERATING TMP_D_ORGANIZATION_N'; 

							IF OBJECT_ID('dbo.TMP_D_ORGANIZATION_N', 'U') IS NOT NULL  
							DROP TABLE dbo.TMP_D_ORGANIZATION_N;

							SELECT  DISTINCT * 
							INTO dbo.TMP_D_ORGANIZATION_N
							FROM dbo.S_ORGANIZATION  SO , 
								 dbo.TMP_L_ORGANIZATION_N LON 
							WHERE SO.ORGANIZATION_UID=LON.ORGANIZATION_UID_N;

								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO); 

			COMMIT TRANSACTION;
		

		---------------------------------------19. CREATE TABLE TMP_D_ORGANIZATION_E----------
			BEGIN TRANSACTION

							SET @PROC_STEP_NO = 19;
							SET @PROC_STEP_NAME = ' GENERATING TMP_D_ORGANIZATION_E'; 

							IF OBJECT_ID('dbo.TMP_D_ORGANIZATION_E', 'U') IS NOT NULL  
							DROP TABLE  dbo.TMP_D_ORGANIZATION_E;
	
							SELECT distinct * 
							into dbo.TMP_D_ORGANIZATION_E
							FROM dbo.S_ORGANIZATION SO, 
								 dbo.TMP_L_ORGANIZATION_E LOE
							WHERE SO.ORGANIZATION_UID=LOE.ORGANIZATION_UID_E;

						
						   SELECT @ROWCOUNT_NO = @@ROWCOUNT;

								
								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO); 
       Commit Transaction;

		---------------------------20. Updating Records dbo.D_ORGANIZATION if there are any updates in any fields----------------------------------------------


	   Begin Transaction
	   
							SET @PROC_STEP_NO = 20;
							SET @PROC_STEP_NAME = ' Updating dbo.D_ORGANIZATION'; 

							Update  dbo.D_ORGANIZATION
						 Set  [ORGANIZATION_KEY]               = E.ORGANIZATION_KEY,
						      [ORGANIZATION_UID]               = E.ORGANIZATION_UID,
						      [ORGANIZATION_LOCAL_ID]          = E.ORGANIZATION_LOCAL_ID,
						      [ORGANIZATION_RECORD_STATUS]     = E.ORGANIZATION_RECORD_STATUS,
						      [ORGANIZATION_NAME]              = CASE WHEN (substring(E.ORGANIZATION_NAME,1,50)) is null then null else substring(E.ORGANIZATION_NAME,1,50) end,
						      [ORGANIZATION_GENERAL_COMMENTS]  = E.ORGANIZATION_GENERAL_COMMENTS,
						      ORGANIZATION_QUICK_CODE          = CASE WHEN (substring(E.ORGANIZATION_QUICK_CODE,1,50)) is null then null else substring(E.ORGANIZATION_QUICK_CODE,1,50) end,
						      [ORGANIZATION_STAND_IND_CLASS]   = E.ORGANIZATION_STAND_IND_CLASS,
						      [ORGANIZATION_FACILITY_ID]	   = CASE when (substring(E.ORGANIZATION_FACILITY_ID,1,50)) is null then null else substring(E.ORGANIZATION_FACILITY_ID,1,50) end,
						      [ORGANIZATION_FACILITY_ID_AUTH]  = CASE WHEN (substring(E.ORGANIZATION_FACILITY_ID_AUTH,1,50)) is null then null else substring(E.ORGANIZATION_FACILITY_ID_AUTH,1,50) end,
						      [ORGANIZATION_STREET_ADDRESS_1]  = substring(E.[ORGANIZATION_STREET_ADDRESS_1] ,1,50),
						      [ORGANIZATION_STREET_ADDRESS_2]  = substring(E.[ORGANIZATION_STREET_ADDRESS_2] ,1,50),
						      [ORGANIZATION_CITY]			   = substring(E.[ORGANIZATION_CITY],1,50),
						      [ORGANIZATION_STATE]             = E.[ORGANIZATION_STATE],
						      [ORGANIZATION_STATE_CODE]        = E.[ORGANIZATION_STATE_CODE],
						      [ORGANIZATION_ZIP]               = E.[ORGANIZATION_ZIP] ,
					    	  [ORGANIZATION_COUNTY]            = E.[ORGANIZATION_COUNTY] ,
						      [ORGANIZATION_COUNTY_CODE]       = E.[ORGANIZATION_COUNTY_CODE] ,
						      [ORGANIZATION_COUNTRY]           = E.[ORGANIZATION_COUNTRY],
						
                              [ORGANIZATION_ADDRESS_COMMENTS]  =  E.[ORGANIZATION_ADDRESS_COMMENTS],
						 
						      [ORGANIZATION_PHONE_WORK]        =  E.[ORGANIZATION_PHONE_WORK] ,
						      [ORGANIZATION_PHONE_EXT_WORK]    =  E.[ORGANIZATION_PHONE_EXT_WORK] ,
						      [ORGANIZATION_EMAIL]			   = substring(E.[ORGANIZATION_EMAIL],1,50),
						      [ORGANIZATION_PHONE_COMMENTS]    =  E.[ORGANIZATION_PHONE_COMMENTS] ,

						     [ORGANIZATION_ENTRY_METHOD]       =  E.[ORGANIZATION_ENTRY_METHOD] , 
						     [ORGANIZATION_LAST_CHANGE_TIME]   =  E.[ORGANIZATION_LAST_CHANGE_TIME],
					         [ORGANIZATION_ADD_TIME]           =  E.[ORGANIZATION_ADD_TIME] ,
						     [ORGANIZATION_ADDED_BY]           =  E.[ORGANIZATION_ADDED_BY]  ,
						     [ORGANIZATION_LAST_UPDATED_BY]    =  E.[ORGANIZATION_LAST_UPDATED_BY],

						     [ORGANIZATION_FAX]				   =  E.[ORGANIZATION_FAX]

					   From  dbo.TMP_D_ORGANIZATION_E E
					    Where E.ORGANIZATION_Key  = D_ORGANIZATION.ORGANIZATION_KEY


					   SELECT @ROWCOUNT_NO = @@ROWCOUNT;
	
								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO); 

			COMMIT TRANSACTION;
		 
--------------------------------------21.-----Final ---Inserting into dbo.D_ORGANIZATION-----------------------------------------------
	   Begin Transaction
	   

							SET @PROC_STEP_NO = 21;
							SET @PROC_STEP_NAME = ' Inserting new entries D_ORGANIZATION'; 

							IF OBJECT_ID('dbo.TMP_D_ORGANIZATION_E', 'U') IS NOT NULL  
							DROP TABLE  dbo.TMP_D_ORGANIZATION_E;

							/* Added below code to Insert a null Row in D_Organization*/

	                      IF NOT EXISTS (SELECT * FROM D_ORGANIZATION WHERE ORGANIZATION_KEY=1) 
							BEGIN
							   INSERT INTO D_ORGANIZATION (ORGANIZATION_KEY) VALUES (1);
							END

						INSERT INTO [dbo].[D_ORGANIZATION]
						   ([ORGANIZATION_KEY]
						   ,[ORGANIZATION_UID]
						   ,[ORGANIZATION_LOCAL_ID]
						   ,[ORGANIZATION_RECORD_STATUS]
						   ,[ORGANIZATION_NAME]
						   ,[ORGANIZATION_GENERAL_COMMENTS]
						   ,[ORGANIZATION_QUICK_CODE]
						   ,[ORGANIZATION_STAND_IND_CLASS]
						   ,[ORGANIZATION_FACILITY_ID]
						   ,[ORGANIZATION_FACILITY_ID_AUTH]
						   ,[ORGANIZATION_STREET_ADDRESS_1]
						   ,[ORGANIZATION_STREET_ADDRESS_2]
						   ,[ORGANIZATION_CITY]
						   ,[ORGANIZATION_STATE]
						   ,[ORGANIZATION_STATE_CODE]
						   ,[ORGANIZATION_ZIP]
						   ,[ORGANIZATION_COUNTY]
						   ,[ORGANIZATION_COUNTY_CODE]
						   ,[ORGANIZATION_COUNTRY]
						   ,[ORGANIZATION_ADDRESS_COMMENTS]
						   ,[ORGANIZATION_PHONE_WORK]
						   ,[ORGANIZATION_PHONE_EXT_WORK]
						   ,[ORGANIZATION_EMAIL]
						   ,[ORGANIZATION_PHONE_COMMENTS]
						   ,[ORGANIZATION_ENTRY_METHOD]
						   ,[ORGANIZATION_LAST_CHANGE_TIME]
						   ,[ORGANIZATION_ADD_TIME]
						   ,[ORGANIZATION_ADDED_BY]
						   ,[ORGANIZATION_LAST_UPDATED_BY]
						   ,[ORGANIZATION_FAX])
						SELECT 
						
						   [ORGANIZATION_KEY]
						  ,[ORGANIZATION_UID] ---as ORGANIZATION_UID
						  ,[ORGANIZATION_LOCAL_ID]
						  ,[ORGANIZATION_RECORD_STATUS]
						
						  ,cast(ORGANIZATION_NAME as varchar(50)) as ORGANIZATION_NAME
						  ,[ORGANIZATION_GENERAL_COMMENTS]
						  ,isnull(NULLIF(cast([ORGANIZATION_QUICK_CODE] as varchar(50)),''),NULL) as ORGANIZATION_QUICK_CODE
					
						  ,[ORGANIZATION_STAND_IND_CLASS] 
						  ,cast([ORGANIZATION_FACILITY_ID] as varchar(50)) as ORGANIZATION_FACILITY_ID
					
						  ,cast(ORGANIZATION_FACILITY_ID_AUTH as varchar(50)) as ORGANIZATION_FACILITY_ID_AUTH
						  ,case when cast ([ORGANIZATION_STREET_ADDRESS_1] as varchar(50)) is null then null else cast([ORGANIZATION_STREET_ADDRESS_1] as varchar(50)) end
						  ,case when cast ([ORGANIZATION_STREET_ADDRESS_2] as varchar(50)) is null then null else cast([ORGANIZATION_STREET_ADDRESS_2] as varchar(50)) end
						  ,isnull(NULLIF(cast([ORGANIZATION_CITY] as varchar(50)),''),NULL) as ORGANIZATION_CITY
						  ,isnull(NULLIF([ORGANIZATION_STATE],''),NULL) as ORGANIZATION_STATE
						  ,isnull(NULLIF([ORGANIZATION_STATE_CODE],''),NULL) as ORGANIZATION_STATE_CODE
						  ,isnull(NULLIF(cast([ORGANIZATION_ZIP] as varchar(10)),''),NULL) as ORGANIZATION_ZIP
						  ,isnull(NULLIF([ORGANIZATION_COUNTY],''),NULL) as ORGANIZATION_COUNTY
						  ,isnull(NULLIF([ORGANIZATION_COUNTY_CODE] ,''),NULL) as ORGANIZATION_COUNTY_CODE
						  ,isnull(NULLIF([ORGANIZATION_COUNTRY],''),NULL) as ORGANIZATION_COUNTRY
						
                          ,case when [ORGANIZATION_ADDRESS_COMMENTS] is null then null else RTRIM(LTRIM([ORGANIZATION_ADDRESS_COMMENTS])) end
						 
						  ,case when [ORGANIZATION_PHONE_WORK]is  null then null else [ORGANIZATION_PHONE_WORK] end
						  ,case when [ORGANIZATION_PHONE_EXT_WORK] is null then null else [ORGANIZATION_PHONE_EXT_WORK] end 
						
						  ,isnull(NULLIF(cast([ORGANIZATION_EMAIL] as varchar(50)),''),NULL) as  ORGANIZATION_EMAIL
						  ,case when [ORGANIZATION_PHONE_COMMENTS] is null then null else RTRIM(LTRIM([ORGANIZATION_PHONE_COMMENTS])) end

						  ,[ORGANIZATION_ENTRY_METHOD]
						  ,[ORGANIZATION_LAST_CHANGE_TIME] 
					      ,[ORGANIZATION_ADD_TIME] 
						  ,[ORGANIZATION_ADDED_BY]
						  ,[ORGANIZATION_LAST_UPDATED_BY]

						  ,[ORGANIZATION_FAX]
						 
					  FROM [dbo].[TMP_D_ORGANIZATION_N];
					  
					   SELECT @ROWCOUNT_NO = @@ROWCOUNT;

								
								INSERT INTO [DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_ORGANIZATION','D_ORGANIZATION','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO); 

	
			COMMIT TRANSACTION;


----------------------------------Dropping all the TMP Tables used--------------------

IF OBJECT_ID('dbo.TMP_S_INITORGANIZATION_INIT_TEMP', 'U') IS NOT NULL   
drop table dbo.TMP_S_INITORGANIZATION_INIT_TEMP  ;
IF OBJECT_ID('dbo.TMP_ORGANIZATION_UID_COLL', 'U') IS NOT NULL   
drop table dbo.TMP_ORGANIZATION_UID_COLL ;	
IF OBJECT_ID('dbo.TMP_S_INITORGANIZATION', 'U') IS NOT NULL   
DROP TABLE dbo.TMP_S_INITORGANIZATION  ;
IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_NAME', 'U') IS NOT NULL  
drop table dbo.TMP_S_ORGANIZATION_NAME;
IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_WITH_NM', 'U') IS NOT NULL  
drop table dbo.TMP_S_ORGANIZATION_WITH_NM;
IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_POSTAL_LOCATOR', 'U') IS NOT NULL  
DROP TABLE  dbo.TMP_S_ORGANIZATION_POSTAL_LOCATOR;
IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_TELE_LOCATOR_FAX', 'U') IS NOT NULL  
DROP TABLE dbo.TMP_S_ORGANIZATION_TELE_LOCATOR_FAX;
IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_TELE_LOCATOR_OFFICE', 'U') IS NOT NULL  
DROP TABLE dbo.TMP_S_ORGANIZATION_TELE_LOCATOR_OFFICE;
IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_LOCATOR', 'U') IS NOT NULL  
DROP TABLE dbo.TMP_S_ORGANIZATION_LOCATOR;
IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_QEC_ENTITY_ID', 'U') IS NOT NULL  
DROP TABLE dbo.TMP_S_ORGANIZATION_QEC_ENTITY_ID;
IF OBJECT_ID('dbo.TMP_FI_ENTITY_ID', 'U') IS NOT NULL  
DROP TABLE dbo.TMP_FI_ENTITY_ID;
IF OBJECT_ID('dbo.TMP_CLIA_ENTITY_ID', 'U') IS NOT NULL  
DROP TABLE dbo.TMP_CLIA_ENTITY_ID;
IF OBJECT_ID('dbo.TMP_S_ORGANIZATION_FINAL', 'U') IS NOT NULL  
DROP TABLE dbo.TMP_S_ORGANIZATION_FINAL;

IF OBJECT_ID('dbo.TMP_L_ORGANIZATION_N', 'U') IS NOT NULL  
DROP TABLE dbo.TMP_L_ORGANIZATION_N;
IF OBJECT_ID('dbo.TMP_L_ORGANIZATION_E', 'U') IS NOT NULL  
DROP TABLE dbo.TMP_L_ORGANIZATION_E;
IF OBJECT_ID('dbo.TMP_D_ORGANIZATION_N', 'U') IS NOT NULL  
DROP TABLE dbo.TMP_D_ORGANIZATION_N;
IF OBJECT_ID('dbo.TMP_D_ORGANIZATION_E', 'U') IS NOT NULL  
DROP TABLE  dbo.TMP_D_ORGANIZATION_E;

--------------------------------------------------------------------------------------------



			BEGIN TRANSACTION ;
			
				SET @Proc_Step_no = 22;
				SET @Proc_Step_Name = 'SP_COMPLETE'; 


				INSERT INTO [dbo].[job_flow_log] 
						(
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
					   'D_ORGANIZATION'
					   ,'D_ORGANIZATION'
					   ,'COMPLETE'
					   ,@Proc_Step_no
					   ,@Proc_Step_name
					   ,@RowCount_no
					   );
		  
			
			COMMIT TRANSACTION;
 END TRY
-----------------------------------------

  BEGIN CATCH
  
     
				IF @@TRANCOUNT > 0   ROLLBACK TRANSACTION;
 
				DECLARE @ErrorNumber INT = ERROR_NUMBER();
				DECLARE @ErrorLine INT = ERROR_LINE();
				DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
				DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
				DECLARE @ErrorState INT = ERROR_STATE();
			 
	
				INSERT INTO [dbo].[job_flow_log] 
					  (
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
					   ,'D_ORGANIZATION'
					   ,'D_ORGANIZATIONR'
					   ,'ERROR'
					   ,@Proc_Step_no
					   ,'ERROR - '+ @Proc_Step_name
					   , 'Step -' +CAST(@Proc_Step_no AS VARCHAR(3))+' -' +CAST(@ErrorMessage AS VARCHAR(500))
					   ,0
					   );
  

			return -1 ;

	END CATCH
	
END;



GO
