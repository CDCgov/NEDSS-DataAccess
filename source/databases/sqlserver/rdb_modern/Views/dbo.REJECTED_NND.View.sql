USE [rdb_modern]
GO
/****** Object:  View [dbo].[REJECTED_NND]    Script Date: 1/17/2024 8:39:44 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO



create view [dbo].[REJECTED_NND] as
SELECT 
  c.condition_desc,
  c.condition_cd,
  rd.date_mm_dd_yyyy,
  n.notification_status,
  n.notification_comments,
  i.inv_case_status,
  i.inv_local_id,
  i.case_rpt_mmwr_wk,
  i.case_rpt_mmwr_yr,
  p.patient_local_id as person_local_id,
  p.patient_first_name as person_first_nm,
  p.patient_last_name as person_last_nm,
  i.jurisdiction_nm,
  i.investigation_status,
  i.case_oid AS 'program_jurisdiction_oid'
FROM 
  rdb..notification_event ne,
  rdb..notification n,
  rdb..condition c,
  rdb..investigation i,
  rdb..d_patient p,
  rdb..rdb_date rd
WHERE 
  ne.notification_key = n.notification_key  and
  ne.notification_submit_dt_key = rd.date_key  and
  ne.condition_key = c.condition_key  and
  ne.investigation_key = i.investigation_key  and
  ne.patient_key = p.patient_key  and
  n.notification_status = 'REJECTED'  and
  i.case_type !=  'S' 
GO
