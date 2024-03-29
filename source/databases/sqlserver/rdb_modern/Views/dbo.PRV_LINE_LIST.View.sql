USE [rdb_modern]
GO
/****** Object:  View [dbo].[PRV_LINE_LIST]    Script Date: 1/17/2024 8:39:44 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create VIEW [dbo].[PRV_LINE_LIST] as    
SELECT 
  prv.provider_local_id as prv_local_id,
  prv.provider_last_name as prv_last_nm,
  prv.provider_first_name as prv_first_nm,
  prv.provider_quick_code as prv_quick_cd,
  prv.provider_entry_method as prv_electronic_ind,
  prv.provider_state as state_short_desc,
  prv.provider_county as cnty_short_desc,
  prv.provider_city as city_short_desc,
  prv.provider_zip as zip_cd_5,
  prv.provider_street_address_1 as street_addr_1,
  prv.provider_street_address_2 as street_addr_2,
  prv.provider_phone_work as phone_nbr,
  prv.provider_phone_ext_work as phone_ext,
  prv.provider_county_code as cnty_fips,
  prv.provider_state_code as state_fips
FROM 
  rdb.dbo.d_provider prv
WHERE 
  prv.provider_key > '1'    
GO
