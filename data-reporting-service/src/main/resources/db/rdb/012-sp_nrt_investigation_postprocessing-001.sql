CREATE PROCEDURE dbo.sp_nrt_investigation_postprocessing @id_list nvarchar(max)
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
		status,
		log_detail
		)
		Values
		('sp_nrt_investigation_postprocessing',
		@id_list,
		'START',
		'Details:'
		);
	set @log_id = @@IDENTITY;

	/* Temp investigation table creation*/
	select 
		INVESTIGATION_KEY,
		public_health_case_uid as CASE_UID,
		program_jurisdiction_oid as CASE_OID,
		nrt.local_id as INV_LOCAL_ID,
		nrt.shared_ind as INV_SHARE_IND,
		nrt.outbreak_name as OUTBREAK_NAME,
		nrt.investigation_status as INVESTIGATION_STATUS,
		nrt.inv_case_status as INV_CASE_STATUS,
		nrt.case_type_cd as CASE_TYPE,
		nrt.txt as INV_COMMENTS,
		nrt.jurisdiction_cd as JURISDICTION_CD,
		nrt.jurisdiction_nm as JURISDICTION_NM,
		nrt.earliest_rpt_to_phd_dt as EARLIEST_RPT_TO_PHD_DT,
		nrt.effective_from_time as ILLNESS_ONSET_DT,
		nrt.effective_to_time as ILLNESS_END_DT,
		nrt.rpt_form_cmplt_time as INV_RPT_DT,
		nrt.activity_from_time as INV_START_DT,
		nrt.rpt_src_cd_desc as RPT_SRC_CD_DESC,
		nrt.rpt_to_county_time as EARLIEST_RPT_TO_CNTY_DT,
		nrt.rpt_to_state_time as EARLIEST_RPT_TO_STATE_DT,
		nrt.mmwr_week as CASE_RPT_MMWR_WK,
		nrt.mmwr_year as CASE_RPT_MMWR_YR,
		nrt.disease_imported_ind as DISEASE_IMPORTED_IND,
		nrt.imported_from_country as IMPORT_FRM_CNTRY,
		nrt.imported_from_state as IMPORT_FRM_STATE,
		nrt.imported_from_county as IMPORT_FRM_CNTY,
		nrt.imported_city_desc_txt as IMPORT_FRM_CITY,
		nrt.earliest_rpt_to_cdc_dt as EARLIEST_RPT_TO_CDC_DT,
		nrt.rpt_source_cd as RPT_SRC_CD,
		nrt.imported_country_cd as IMPORT_FRM_CNTRY_CD,
		nrt.imported_state_cd as IMPORT_FRM_STATE_CD,
		nrt.imported_county_cd as IMPORT_FRM_CNTY_CD,
		nrt.import_frm_city_cd as IMPORT_FRM_CITY_CD,
		nrt.diagnosis_time as DIAGNOSIS_DT,
		nrt.hospitalized_admin_time as HSPTL_ADMISSION_DT,
		nrt.hospitalized_discharge_time as HSPTL_DISCHARGE_DT,
		nrt.hospitalized_duration_amt as HSPTL_DURATION_DAYS,
		nrt.outbreak_ind_val as OUTBREAK_IND,
		nrt.hospitalized_ind as HSPTLIZD_IND,
		nrt.inv_state_case_id as INV_STATE_CASE_ID,
		nrt.city_county_case_nbr as CITY_COUNTY_CASE_NBR,
		nrt.transmission_mode as TRANSMISSION_MODE,
		nrt.record_status_cd as RECORD_STATUS_CD,
		nrt.pregnant_ind as PATIENT_PREGNANT_IND,
		nrt.die_frm_this_illness_ind as DIE_FRM_THIS_ILLNESS_IND,
		nrt.day_care_ind as DAYCARE_ASSOCIATION_IND,
		nrt.food_handler_ind as FOOD_HANDLR_IND,
		nrt.deceased_time as INVESTIGATION_DEATH_DATE,
		case when isnumeric(nrt.pat_age_at_onset)=1 then cast(nrt.pat_age_at_onset as int)
			else null
		end as PATIENT_AGE_AT_ONSET,
		nrt.pat_age_at_onset_unit as PATIENT_AGE_AT_ONSET_UNIT,
		nrt.investigator_assigned_time as INV_ASSIGNED_DT,
		nrt.detection_method_desc_txt as DETECTION_METHOD_DESC_TXT,
		case when isnumeric(nrt.effective_duration_amt)=1 then cast(nrt.effective_duration_amt as int)
			else null
		end as ILLNESS_DURATION,
		nrt.illness_duration_unit as ILLNESS_DURATION_UNIT,
		nrt.contact_inv_txt as CONTACT_INV_COMMENTS,
		nrt.contact_inv_priority as CONTACT_INV_PRIORITY,
		nrt.infectious_from_date as CONTACT_INFECTIOUS_FROM_DATE,
		nrt.infectious_to_date as CONTACT_INFECTIOUS_TO_DATE,
		nrt.contact_inv_status as CONTACT_INV_STATUS,
		nrt.activity_to_time as INV_CLOSE_DT,
		nrt.program_area_description as PROGRAM_AREA_DESCRIPTION,
		nrt.add_time as ADD_TIME,
		nrt.last_chg_time as LAST_CHG_TIME,
		nrt.add_user_name as INVESTIGATION_ADDED_BY,
		nrt.last_chg_user_name as INVESTIGATION_LAST_UPDATED_BY,
		nrt.referral_basis as REFERRAL_BASIS,
		nrt.curr_process_state as CURR_PROCESS_STATE,
		nrt.inv_priority_cd as INV_PRIORITY_CD,
		nrt.coinfection_id as COINFECTION_ID,
		nrt.legacy_case_id as LEGACY_CASE_ID,
		nrt.outbreak_name as OUTBREAK_NAME_DESC
	into #temp_inv_table
	from dbo.nrt_investigation nrt
	left join dbo.investigation i on i.case_uid = nrt.public_health_case_uid
	where    
	nrt.public_health_case_uid in (SELECT value FROM STRING_SPLIT(@id_list, ','));
	

	/* Investigation Update Operation */
	BEGIN TRANSACTION;
	update dbo.INVESTIGATION 
		set [INVESTIGATION_KEY] = inv.INVESTIGATION_KEY,
			[CASE_OID] = inv.CASE_OID,
			[CASE_UID] = inv.CASE_UID,
			[INV_LOCAL_ID] = inv.INV_LOCAL_ID,
			[INV_SHARE_IND] = inv.INV_SHARE_IND,
			[OUTBREAK_NAME] = inv.OUTBREAK_NAME,
			[INVESTIGATION_STATUS] = inv.INVESTIGATION_STATUS,
			[INV_CASE_STATUS] = inv.INV_CASE_STATUS,
			[CASE_TYPE] = inv.CASE_TYPE,
			[INV_COMMENTS] = inv.INV_COMMENTS,
			[JURISDICTION_CD] = inv.JURISDICTION_CD,
			[JURISDICTION_NM] = inv.JURISDICTION_NM,
			[EARLIEST_RPT_TO_PHD_DT] = inv.EARLIEST_RPT_TO_PHD_DT,
			[ILLNESS_ONSET_DT] = inv.ILLNESS_ONSET_DT,
			[ILLNESS_END_DT] = inv.ILLNESS_END_DT,
			[INV_RPT_DT] = inv.INV_RPT_DT,
			[INV_START_DT] = inv.INV_START_DT,
			[RPT_SRC_CD_DESC] = inv.RPT_SRC_CD_DESC,
			[EARLIEST_RPT_TO_CNTY_DT] = inv.EARLIEST_RPT_TO_STATE_DT,
			[CASE_RPT_MMWR_WK] = inv.CASE_RPT_MMWR_WK,
			[CASE_RPT_MMWR_YR] = inv.CASE_RPT_MMWR_YR,
			[DISEASE_IMPORTED_IND] = inv.DISEASE_IMPORTED_IND,
			[IMPORT_FRM_CNTRY] = inv.IMPORT_FRM_CNTRY,
			[IMPORT_FRM_STATE] = inv.IMPORT_FRM_STATE,
			[IMPORT_FRM_CNTY] = inv.IMPORT_FRM_CNTY,
			[IMPORT_FRM_CITY] = inv.IMPORT_FRM_CITY,
			[EARLIEST_RPT_TO_CDC_DT] = inv.EARLIEST_RPT_TO_CDC_DT,
			[RPT_SRC_CD] = inv.RPT_SRC_CD,
			[IMPORT_FRM_CNTRY_CD] = inv.IMPORT_FRM_CNTRY_CD,
			[IMPORT_FRM_STATE_CD] = inv.IMPORT_FRM_STATE_CD,
			[IMPORT_FRM_CNTY_CD] = inv.IMPORT_FRM_CNTY_CD,
			[IMPORT_FRM_CITY_CD] = inv.IMPORT_FRM_CITY_CD,
			[DIAGNOSIS_DT] = inv.DIAGNOSIS_DT,
			[HSPTL_ADMISSION_DT] = inv.HSPTL_ADMISSION_DT,
			[HSPTL_DISCHARGE_DT] = inv.HSPTL_DISCHARGE_DT,
			[HSPTL_DURATION_DAYS] = inv.HSPTL_DURATION_DAYS,
			[OUTBREAK_IND] = inv.OUTBREAK_IND,
			[HSPTLIZD_IND] = inv.HSPTLIZD_IND,
			[INV_STATE_CASE_ID] = inv.INV_STATE_CASE_ID,
			[CITY_COUNTY_CASE_NBR] = inv.CITY_COUNTY_CASE_NBR,
			[TRANSMISSION_MODE] = inv.TRANSMISSION_MODE,
			[RECORD_STATUS_CD] = inv.RECORD_STATUS_CD,
			[PATIENT_PREGNANT_IND] = inv.PATIENT_PREGNANT_IND,
			[DIE_FRM_THIS_ILLNESS_IND] = inv.DIE_FRM_THIS_ILLNESS_IND,
			[DAYCARE_ASSOCIATION_IND] = inv.DAYCARE_ASSOCIATION_IND,
			[FOOD_HANDLR_IND] = inv.FOOD_HANDLR_IND,
			[INVESTIGATION_DEATH_DATE] = inv.INVESTIGATION_DEATH_DATE,
			[PATIENT_AGE_AT_ONSET] = inv.PATIENT_AGE_AT_ONSET,
			[PATIENT_AGE_AT_ONSET_UNIT] = inv.PATIENT_AGE_AT_ONSET_UNIT,
			[INV_ASSIGNED_DT] = inv.INV_ASSIGNED_DT,
			[DETECTION_METHOD_DESC_TXT] = inv.DETECTION_METHOD_DESC_TXT,
			[ILLNESS_DURATION] = inv.ILLNESS_DURATION,
			[ILLNESS_DURATION_UNIT] = inv.ILLNESS_DURATION_UNIT,
			[CONTACT_INV_COMMENTS] = inv.CONTACT_INV_COMMENTS,
			[CONTACT_INV_PRIORITY] = inv.CONTACT_INV_PRIORITY,
			[CONTACT_INFECTIOUS_FROM_DATE] = inv.CONTACT_INFECTIOUS_FROM_DATE,
			[CONTACT_INFECTIOUS_TO_DATE] = inv.CONTACT_INFECTIOUS_TO_DATE,
			[CONTACT_INV_STATUS] = inv.CONTACT_INV_STATUS,
			[PROGRAM_AREA_DESCRIPTION] = inv.PROGRAM_AREA_DESCRIPTION,
			[ADD_TIME] =  inv.ADD_TIME,
			[LAST_CHG_TIME] = inv.LAST_CHG_TIME,
			[INVESTIGATION_ADDED_BY] = inv.INVESTIGATION_ADDED_BY,
			[INVESTIGATION_LAST_UPDATED_BY] = inv.INVESTIGATION_LAST_UPDATED_BY,
			[REFERRAL_BASIS] = inv.REFERRAL_BASIS,
			[CURR_PROCESS_STATE] = inv.CURR_PROCESS_STATE,
			[INV_PRIORITY_CD] = inv.INV_PRIORITY_CD,
			[COINFECTION_ID] = inv.COINFECTION_ID,
			[OUTBREAK_NAME_DESC] = inv.OUTBREAK_NAME_DESC
		from #temp_inv_table inv
		inner join dbo.investigation i on inv.case_uid = i.case_uid 
		and inv.investigation_key = i.investigation_key
		and i.investigation_key is not null;

	/* Logging */
	set @rowcount=@@rowcount 
	if @rowcount > 0
		update dbo.nrt_batch_log
		set log_detail=log_detail+'Num of Investigation keys updated: ' + cast(@rowcount as nvarchar(max))+ '.'
		where batch_id=@log_id; 
	else 
		update dbo.nrt_batch_log
		set log_detail=log_detail+'No Investigation Updates.'
		where batch_id=@log_id; 
	
	/* Investigation Insert Operation */
	
	-- delete from the key table to generate new keys for the resulting new data to be inserted
	delete from dbo.nrt_investigation_key ;
	insert into dbo.nrt_investigation_key(case_uid) 
	select public_health_case_uid from #temp_inv_table where investigation_key is null order by public_health_case_uid;
	
	insert into dbo.INVESTIGATION 
			([INVESTIGATION_KEY],
			[CASE_OID],
			[CASE_UID],
			[INV_LOCAL_ID],
			[INV_SHARE_IND],
			[OUTBREAK_NAME],
			[INVESTIGATION_STATUS],
			[INV_CASE_STATUS],
			[CASE_TYPE],
			[INV_COMMENTS],
			[JURISDICTION_CD],
			[JURISDICTION_NM],
			[EARLIEST_RPT_TO_PHD_DT],
			[ILLNESS_ONSET_DT],
			[ILLNESS_END_DT],
			[INV_RPT_DT],
			[INV_START_DT],
			[RPT_SRC_CD_DESC],
			[EARLIEST_RPT_TO_CNTY_DT],
			[CASE_RPT_MMWR_WK],
			[CASE_RPT_MMWR_YR],
			[DISEASE_IMPORTED_IND],
			[IMPORT_FRM_CNTRY],
			[IMPORT_FRM_STATE],
			[IMPORT_FRM_CNTY],
			[IMPORT_FRM_CITY],
			[EARLIEST_RPT_TO_CDC_DT],
			[RPT_SRC_CD],
			[IMPORT_FRM_CNTRY_CD],
			[IMPORT_FRM_STATE_CD],
			[IMPORT_FRM_CNTY_CD],
			[IMPORT_FRM_CITY_CD],
			[DIAGNOSIS_DT],
			[HSPTL_ADMISSION_DT],
			[HSPTL_DISCHARGE_DT],
			[HSPTL_DURATION_DAYS],
			[OUTBREAK_IND],
			[HSPTLIZD_IND],
			[INV_STATE_CASE_ID],
			[CITY_COUNTY_CASE_NBR],
			[TRANSMISSION_MODE],
			[RECORD_STATUS_CD],
			[PATIENT_PREGNANT_IND],
			[DIE_FRM_THIS_ILLNESS_IND],
			[DAYCARE_ASSOCIATION_IND],
			[FOOD_HANDLR_IND],
			[INVESTIGATION_DEATH_DATE],
			[PATIENT_AGE_AT_ONSET],
			[PATIENT_AGE_AT_ONSET_UNIT],
			[INV_ASSIGNED_DT],
			[DETECTION_METHOD_DESC_TXT],
			[ILLNESS_DURATION],
			[ILLNESS_DURATION_UNIT],
			[CONTACT_INV_COMMENTS],
			[CONTACT_INV_PRIORITY],
			[CONTACT_INFECTIOUS_FROM_DATE],
			[CONTACT_INFECTIOUS_TO_DATE],
			[CONTACT_INV_STATUS],
			[PROGRAM_AREA_DESCRIPTION],
			[ADD_TIME],
			[LAST_CHG_TIME],
			[INVESTIGATION_ADDED_BY] ,
			[INVESTIGATION_LAST_UPDATED_BY],
			[REFERRAL_BASIS],
			[CURR_PROCESS_STATE],
			[INV_PRIORITY_CD],
			[COINFECTION_ID],
			[OUTBREAK_NAME_DESC]
           )
			select
			  k.[d_INVESTIGATION_KEY] as INVESTIGATION_KEY ,
				inv.CASE_OID,
				inv.CASE_UID,
				inv.INV_LOCAL_ID,
				inv.INV_SHARE_IND,
				inv.OUTBREAK_NAME,
				inv.INVESTIGATION_STATUS,
				inv.INV_CASE_STATUS,
				inv.CASE_TYPE,
				inv.INV_COMMENTS,
				inv.JURISDICTION_CD,
				inv.JURISDICTION_NM,
				inv.EARLIEST_RPT_TO_PHD_DT,
				inv.ILLNESS_ONSET_DT,
				inv.ILLNESS_END_DT,
				inv.INV_RPT_DT,
				inv.INV_START_DT,
				inv.RPT_SRC_CD_DESC,
				inv.EARLIEST_RPT_TO_STATE_DT,
				inv.CASE_RPT_MMWR_WK,
				inv.CASE_RPT_MMWR_YR,
				inv.DISEASE_IMPORTED_IND,
				inv.IMPORT_FRM_CNTRY,
				inv.IMPORT_FRM_STATE,
				inv.IMPORT_FRM_CNTY,
				inv.IMPORT_FRM_CITY,
				inv.EARLIEST_RPT_TO_CDC_DT,
				inv.RPT_SRC_CD,
				inv.IMPORT_FRM_CNTRY_CD,
				inv.IMPORT_FRM_STATE_CD,
				inv.IMPORT_FRM_CNTY_CD,
				inv.IMPORT_FRM_CITY_CD,
				inv.DIAGNOSIS_DT,
				inv.HSPTL_ADMISSION_DT,
				inv.HSPTL_DISCHARGE_DT,
				inv.HSPTL_DURATION_DAYS,
				inv.OUTBREAK_IND,
				inv.HSPTLIZD_IND,
				inv.INV_STATE_CASE_ID,
				inv.CITY_COUNTY_CASE_NBR,
				inv.TRANSMISSION_MODE,
				inv.RECORD_STATUS_CD,
				inv.PATIENT_PREGNANT_IND,
				inv.DIE_FRM_THIS_ILLNESS_IND,
				inv.DAYCARE_ASSOCIATION_IND,
				inv.FOOD_HANDLR_IND,
				inv.INVESTIGATION_DEATH_DATE,
				inv.PATIENT_AGE_AT_ONSET,
				inv.PATIENT_AGE_AT_ONSET_UNIT,
				inv.INV_ASSIGNED_DT,
				inv.DETECTION_METHOD_DESC_TXT,
				inv.ILLNESS_DURATION,
				inv.ILLNESS_DURATION_UNIT,
				inv.CONTACT_INV_COMMENTS,
				inv.CONTACT_INV_PRIORITY,
				inv.CONTACT_INFECTIOUS_FROM_DATE,
				inv.CONTACT_INFECTIOUS_TO_DATE,
				inv.CONTACT_INV_STATUS,
				inv.PROGRAM_AREA_DESCRIPTION,
				inv.ADD_TIME,
				inv.LAST_CHG_TIME,
				inv.INVESTIGATION_ADDED_BY,
				inv.INVESTIGATION_LAST_UPDATED_BY,
				inv.REFERRAL_BASIS,
				inv.CURR_PROCESS_STATE,
				inv.INV_PRIORITY_CD,
				inv.COINFECTION_ID,
				inv.OUTBREAK_NAME_DESC
			  FROM #temp_inv_table inv
			  join dbo.nrt_investigation_key k on inv.case_uid = k.case_uid 			 		   
			  where inv.investigation_key is null;
			  
			  
		/* Logging */
		set @rowcount=@@rowcount 	  
		if @rowcount > 0
			update dbo.nrt_batch_log
			set log_detail=log_detail+' Num of Investigation keys inserted: ' + cast(@rowcount as nvarchar(max))+ '.'
		where batch_id=@log_id; 
		else 
			update dbo.nrt_batch_log
			set log_detail=log_detail+' No Investigation Inserts.'
			where batch_id=@log_id;
		
	
	
		/* Temp Confimation Method Table*/
		select distinct 
			nrt.PUBLIC_HEALTH_CASE_UID, 
			i.INVESTIGATION_KEY,
			nrt.CONFIRMATION_METHOD_CD,			
			nrt.CONFIRMATION_METHOD_DESC_TXT,
			nrt.CONFIRMATION_METHOD_TIME as CONFIRMATION_DT,
			cm.CONFIRMATION_METHOD_KEY
		into #temp_cm_table
		from dbo.nrt_investigation_confirmation nrt
		left join dbo.confirmation_method cm on cm.confirmation_method_cd=nrt.confirmation_method_cd
		left join dbo.investigation i on i.case_uid = nrt.public_health_case_uid
		where nrt.public_health_case_uid in (select value FROM STRING_SPLIT(@id_list, ','));
		
	
		-- if confirmation_method_key for the cd exists get the key or insert a new row to rdb.confirmation_method
	
		/*Update Operation for confirmation_method and group*/
		update cm
			set cm.CONFIRMATION_METHOD_DESC = cmt.CONFIRMATION_METHOD_DESC_TXT 
		from #temp_cm_table cmt
		inner join dbo.confirmation_method cm on cmt.confirmation_method_key = cm.confirmation_method_key
			and cmt.CONFIRMATION_METHOD_KEY is not null;
		
		update cmg
			set cmg.CONFIRMATION_DT = cmt.CONFIRMATION_DT
		from #temp_cm_table cmt
		inner join dbo.confirmation_method_group cmg on cmt.investigation_key = cmg.investigation_key
												and cmt.confirmation_method_key = cmg.confirmation_method_key
			and cmt.CONFIRMATION_METHOD_KEY is not null;
	
		/* Logging */
		set @rowcount=@@rowcount 
		if @rowcount > 0
			update dbo.nrt_batch_log
			set log_detail= log_detail+' Num of CMG keys updated: ' + cast(@rowcount as nvarchar(max))+ '.'
			where batch_id=@log_id; 
		else 
			update dbo.nrt_batch_log
			set log_detail=log_detail+' No CMG Updates.'
			where batch_id=@log_id; 
		

	
		-- generate new CONFIRMATION_METHOD_KEY for the correspondinf cd
		delete from dbo.nrt_confirmation_method_key;
	
		insert into dbo.nrt_confirmation_method_key(confirmation_method_cd) 
		select distinct confirmation_method_cd from #temp_cm_table 
			where CONFIRMATION_METHOD_KEY is null 
				and cmt.confirmation_method_cd not in (select confirmation_method_cd from dbo.confirmation_method)
		order by INVESTIGATION_KEY;

		/* Insert confirmation_method */
		Insert into dbo.confirmation_method(CONFIRMATION_METHOD_KEY,confirmation_method_cd,CONFIRMATION_METHOD_DESC)
		Select distinct cmk.CONFIRMATION_METHOD_KEY, cmt.confirmation_method_cd, cmt.CONFIRMATION_METHOD_DESC_TXT 
		from #temp_cm_table cmt
		join dbo.nrt_confirmation_method_key cmk on cmk.confirmation_method_cd = cmt.confirmation_method_cd
		where cmt.CONFIRMATION_METHOD_KEY is  null 
		and cmt.confirmation_method_cd not in (select confirmation_method_cd from dbo.confirmation_method)
		order by INVESTIGATION_KEY;
	

		/* Insert CONFIRMATION_METHOD_GROUP */
		insert into dbo.CONFIRMATION_METHOD_GROUP ([INVESTIGATION_KEY],	[CONFIRMATION_METHOD_KEY],[CONFIRMATION_DT])
		select cmt.INVESTIGATION_KEY,	cmk.CONFIRMATION_METHOD_KEY,	cmt.CONFIRMATION_DT
		  from #temp_cm_table cmt
		  join ndbo.rt_confirmation_method_key cmk on cmt.confirmation_method_cd = cmk.confirmation_method_cd
		  join dbo.confirmation_method_group cmg on cmt.investigation_key = cmg.investigation_key 
		  where cmt.CONFIRMATION_METHOD_KEY is null;
			  
		/* Logging */
		set @rowcount=@@rowcount 	  
		if @rowcount > 0
			update dbo.nrt_batch_log
			set log_detail=log_detail+' Num of CMG keys inserted: ' + cast(@rowcount as nvarchar(max))+ '.'
		where batch_id=@log_id; 
		else 
			update dbo.nrt_batch_log
			set log_detail=log_detail+' No CMG Inserts.'
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
    	PRINT 'ERROR: ' +  @ErrorMessage + 'ID list:' + @id_list;
    	declare @errorstr varchar(max)
		set @errorstr = coalesce(@ErrorMessage+', ID List:' , @id_list) 
		return @errorstr;
		*/
      	return @ErrorMessage;

	END CATCH
	
END