CREATE PROCEDURE dbo.sp_nrt_organization_postprocessing @id_list nvarchar(max)
AS 
 BEGIN 

BEGIN TRY

	/* Logging */
	declare @rowcount bigint;
	declare @log_id bigint;
	insert into dbo.nrt_batch_log
		( 
		procedure_name, 
		param_id_list,
		status
		)
		Values
		('sp_nrt_organization_postprocessing',
		@id_list,
		'START'
		);
	set @log_id = @@IDENTITY;

	/* Temp organization table creation*/
	select 
	ORGANIZATION_KEY,
	nrt.organization_uid as ORGANIZATION_UID,
	local_id as ORGANIZATION_LOCAL_ID,
	record_status as ORGANIZATION_RECORD_STATUS,
	nrt.organization_name as ORGANIZATION_NAME,
	general_comments as ORGANIZATION_GENERAL_COMMENTS,
	quick_code as ORGANIZATION_QUICK_CODE,
	stand_ind_class as ORGANIZATION_STAND_IND_CLASS,
	facility_id as ORGANIZATION_FACILITY_ID,
	facility_id_auth as ORGANIZATION_FACILITY_ID_AUTH,
	street_address_1 as ORGANIZATION_STREET_ADDRESS_1,
	street_address_2 as ORGANIZATION_STREET_ADDRESS_2,
	city as ORGANIZATION_CITY,
	state as ORGANIZATION_STATE,
	state_code as ORGANIZATION_STATE_CODE,
	zip as ORGANIZATION_ZIP,
	county as ORGANIZATION_COUNTY,
	county_code as ORGANIZATION_COUNTY_CODE,
	country as ORGANIZATION_COUNTRY,
	address_comments as ORGANIZATION_ADDRESS_COMMENTS,
	phone_work as ORGANIZATION_PHONE_WORK,
	phone_ext_work as ORGANIZATION_PHONE_EXT_WORK,
	email as ORGANIZATION_EMAIL,
	phone_comments as ORGANIZATION_PHONE_COMMENTS,
	fax as ORGANIZATION_FAX,
	entry_method as ORGANIZATION_ENTRY_METHOD,
	add_user_name as ORGANIZATION_ADDED_BY,
	add_time as ORGANIZATION_ADD_TIME,
	last_chg_user_name as ORGANIZATION_LAST_UPDATED_BY,
	last_chg_time as ORGANIZATION_LAST_CHANGE_TIME
	into #temp_org_table
	from dbo.nrt_organization nrt
	left join dbo.d_organization o on o.organization_uid = nrt.organization_uid
	where    
	nrt.organization_uid in (SELECT value FROM STRING_SPLIT(@id_list, ','))
	

	/* D_Organization Update Operation */
	BEGIN TRANSACTION;
	update dbo.d_organization
		set	[ORGANIZATION_KEY]             = org.ORGANIZATION_KEY,
	      [ORGANIZATION_UID]               = org.ORGANIZATION_UID,
	      [ORGANIZATION_LOCAL_ID]          = org.ORGANIZATION_LOCAL_ID,
	      [ORGANIZATION_RECORD_STATUS]     = org.ORGANIZATION_RECORD_STATUS,
	      [ORGANIZATION_NAME]              = CASE WHEN (substring(org.ORGANIZATION_NAME,1,50)) is null then null else substring(org.ORGANIZATION_NAME,1,50) end,
	      [ORGANIZATION_GENERAL_COMMENTS]  = org.ORGANIZATION_GENERAL_COMMENTS,
	     ORGANIZATION_QUICK_CODE          = CASE WHEN (substring(org.ORGANIZATION_QUICK_CODE,1,50)) is null then null else substring(org.ORGANIZATION_QUICK_CODE,1,50) end,
	      [ORGANIZATION_STAND_IND_CLASS]   = org.ORGANIZATION_STAND_IND_CLASS,
	      [ORGANIZATION_FACILITY_ID]	   = CASE when (substring(org.ORGANIZATION_FACILITY_ID,1,50)) is null then null else substring(org.ORGANIZATION_FACILITY_ID,1,50) end,
	      [ORGANIZATION_FACILITY_ID_AUTH]  = CASE WHEN (substring(org.ORGANIZATION_FACILITY_ID_AUTH,1,50)) is null then null else substring(org.ORGANIZATION_FACILITY_ID_AUTH,1,50) end,
	      [ORGANIZATION_STREET_ADDRESS_1]  = substring(org.[ORGANIZATION_STREET_ADDRESS_1] ,1,50),
	      [ORGANIZATION_STREET_ADDRESS_2]  = substring(org.[ORGANIZATION_STREET_ADDRESS_2] ,1,50),
	      [ORGANIZATION_CITY]			   = substring(org.[ORGANIZATION_CITY],1,50),
	      [ORGANIZATION_STATE]             = org.[ORGANIZATION_STATE],
	      [ORGANIZATION_STATE_CODE]        = org.[ORGANIZATION_STATE_CODE],
	      [ORGANIZATION_ZIP]               = org.[ORGANIZATION_ZIP] ,
    	  [ORGANIZATION_COUNTY]            = org.[ORGANIZATION_COUNTY] ,
	      [ORGANIZATION_COUNTY_CODE]       = org.[ORGANIZATION_COUNTY_CODE] ,
	      [ORGANIZATION_COUNTRY]           = org.[ORGANIZATION_COUNTRY],
          [ORGANIZATION_ADDRESS_COMMENTS]  =  org.[ORGANIZATION_ADDRESS_COMMENTS],
	      [ORGANIZATION_PHONE_WORK]        =  org.[ORGANIZATION_PHONE_WORK] ,
	      [ORGANIZATION_PHONE_EXT_WORK]  =  org.[ORGANIZATION_PHONE_EXT_WORK] ,
	      [ORGANIZATION_EMAIL]			   = substring(org.[ORGANIZATION_EMAIL],1,50),
	      [ORGANIZATION_PHONE_COMMENTS]    =  org.[ORGANIZATION_PHONE_COMMENTS] ,
	      [ORGANIZATION_ENTRY_METHOD]       =  org.[ORGANIZATION_ENTRY_METHOD] , 
	      [ORGANIZATION_LAST_CHANGE_TIME]   =  org.[ORGANIZATION_LAST_CHANGE_TIME],
          [ORGANIZATION_ADD_TIME]           =  org.[ORGANIZATION_ADD_TIME] ,
	      [ORGANIZATION_ADDED_BY]           =  org.[ORGANIZATION_ADDED_BY]  ,
	      [ORGANIZATION_LAST_UPDATED_BY]    =  org.[ORGANIZATION_LAST_UPDATED_BY],
	      [ORGANIZATION_FAX]				   =  org.[ORGANIZATION_FAX]
		from #temp_org_table org
		inner join dbo.d_organization o on org.organization_uid = o.organization_uid 
		and org.organization_key = o.organization_key
		and o.organization_key is not null;

	/* Logging */
	set @rowcount=@@rowcount 
	if @rowcount > 0
		update dbo.nrt_batch_log
		set log_detail='Num of Organization keys updated: ' + cast(@rowcount as nvarchar(max))+ '.'
		where batch_id=@log_id; 
	else 
		update dbo.nrt_batch_log
		set log_detail='No Organization Updates.'
		where batch_id=@log_id; 
	
	/* D_Organization Insert Operation */	
	-- delete from the key table to generate new keys for the resulting new data to be inserted
	begin try
	
	delete from dbo.nrt_organization_key ;
	insert into dbo.nrt_organization_key(organization_uid) 
	select organization_uid from #temp_org_table where organization_key is null order by organization_uid;

	insert into dbo.d_organization
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
			SELECT  k.d_organization_key  as ORGANIZATION_KEY  
			  ,org.[ORGANIZATION_UID] 
			  ,org.[ORGANIZATION_LOCAL_ID]
			  ,org.[ORGANIZATION_RECORD_STATUS]
			  ,cast(org.ORGANIZATION_NAME as varchar(50)) as ORGANIZATION_NAME
			  ,org.[ORGANIZATION_GENERAL_COMMENTS]
			  ,isnull(NULLIF(cast(org.[ORGANIZATION_QUICK_CODE] as varchar(50)),''),NULL) as ORGANIZATION_QUICK_CODE
			  ,org.[ORGANIZATION_STAND_IND_CLASS] 
			  ,cast(org.[ORGANIZATION_FACILITY_ID] as varchar(50)) as ORGANIZATION_FACILITY_ID
			  ,cast(org.ORGANIZATION_FACILITY_ID_AUTH as varchar(50)) as ORGANIZATION_FACILITY_ID_AUTH
			  ,case when cast (org.[ORGANIZATION_STREET_ADDRESS_1] as varchar(50)) is null then null else cast(org.[ORGANIZATION_STREET_ADDRESS_1] as varchar(50)) end
			  ,case when cast (org.[ORGANIZATION_STREET_ADDRESS_2] as varchar(50)) is null then null else cast(org.[ORGANIZATION_STREET_ADDRESS_2] as varchar(50)) end
			  ,isnull(NULLIF(cast(org.[ORGANIZATION_CITY] as varchar(50)),''),NULL) as ORGANIZATION_CITY
			  ,isnull(NULLIF(org.[ORGANIZATION_STATE],''),NULL) as ORGANIZATION_STATE
			  ,isnull(NULLIF(org.[ORGANIZATION_STATE_CODE],''),NULL) as ORGANIZATION_STATE_CODE
			  ,isnull(NULLIF(cast(org.[ORGANIZATION_ZIP] as varchar(10)),''),NULL) as ORGANIZATION_ZIP
			  ,isnull(NULLIF(org.[ORGANIZATION_COUNTY],''),NULL) as ORGANIZATION_COUNTY
			  ,isnull(NULLIF(org.[ORGANIZATION_COUNTY_CODE] ,''),NULL) as ORGANIZATION_COUNTY_CODE
			  ,isnull(NULLIF(org.[ORGANIZATION_COUNTRY],''),NULL) as ORGANIZATION_COUNTRY
              ,case when org.[ORGANIZATION_ADDRESS_COMMENTS] is null then null else RTRIM(LTRIM(org.[ORGANIZATION_ADDRESS_COMMENTS])) end
			  ,case when org.[ORGANIZATION_PHONE_WORK]is  null then null else org.[ORGANIZATION_PHONE_WORK] end
			  ,case when org.[ORGANIZATION_PHONE_EXT_WORK] is null then null else org.[ORGANIZATION_PHONE_EXT_WORK] end 
			  ,isnull(NULLIF(cast(org.[ORGANIZATION_EMAIL] as varchar(50)),''),NULL) as  ORGANIZATION_EMAIL
			  ,case when org.[ORGANIZATION_PHONE_COMMENTS] is null then null else RTRIM(LTRIM(org.[ORGANIZATION_PHONE_COMMENTS])) end
			  ,org.[ORGANIZATION_ENTRY_METHOD]
			  ,org.[ORGANIZATION_LAST_CHANGE_TIME] 
		      ,org.[ORGANIZATION_ADD_TIME] 
			  ,org.[ORGANIZATION_ADDED_BY]
			  ,org.[ORGANIZATION_LAST_UPDATED_BY]
			  ,org.[ORGANIZATION_FAX]
			  FROM #temp_org_table org
			  join dbo.nrt_organization_key k on org.organization_uid = k.organization_uid
			  where org.organization_key is null;
			  			  
		end try
		begin catch
		 IF @@TRANCOUNT > 0   ROLLBACK TRANSACTION;
  
    	
    	/* Logging */
	    update dbo.nrt_batch_log
		set 
			batch_end_time=GETDATE(),
			status='ERROR',
				error_log=ERROR_MESSAGE()
			where batch_id = @log_id; 

    	
      	return ERROR_MESSAGE();
		end catch
		
		/* Logging */
		set @rowcount=@@rowcount 	  
		if @rowcount > 0
			update dbo.nrt_batch_log
			set log_detail=log_detail+' Num of Organization keys inserted: ' + cast(@rowcount as nvarchar(max))+ '.'
		where batch_id=@log_id; 
		else 
			update dbo.nrt_batch_log
			set log_detail=log_detail+' No Organization Inserts.'
			where batch_id=@log_id;
	
		select 'Success'; 
		update dbo.nrt_batch_log
		set 
			batch_end_time=GETDATE(),
			status='COMPLETE'
		where batch_id=@log_id; 
		
		COMMIT TRANSACTION;
			
END TRY

 BEGIN CATCH
  
     
     IF @@TRANCOUNT > 0   ROLLBACK TRANSACTION;
  
    	DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE(); 
    	
    	/* Logging */
	    update dbo.nrt_batch_log
		set 
			batch_end_time=GETDATE(),
			status='ERROR',
				error_log=@ErrorMessage
			where batch_id = @log_id; 

    	/*
    	declare @errorstr varchar(max)
		set @errorstr = coalesce(@ErrorMessage+', ID List:' , @id_list) 
		return @errorstr;
		*/
      	return @ErrorMessage;

	END CATCH
	
END