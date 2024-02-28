USE [rdb_modern]
GO
/****** Object:  View [dbo].[ORG_LINE_LIST]    Script Date: 1/17/2024 8:39:44 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO

create VIEW [dbo].[ORG_LINE_LIST] as    
SELECT 
  org.organization_local_id as org_local_id,
  org.organization_name as org_nm,
  org.organization_quick_code as org_quick_cd,
  org.organization_entry_method as org_electronic_ind,
  org.organization_state as state_short_desc,
  org.organization_county as cnty_short_desc,
  org.organization_city as city_short_desc,
  org.organization_zip as zip_cd_5,
  org.organization_street_address_1 as street_addr_1,
  org.organization_street_address_2 as street_addr_2,
  org.organization_phone_work as phone_nbr,
  org.organization_phone_ext_work as phone_ext,
  org.organization_county_code as cnty_fips,
  org.organization_state_code as state_fips
FROM 
  rdb..d_organization org
WHERE 
  org.organization_key > '1'    
GO
