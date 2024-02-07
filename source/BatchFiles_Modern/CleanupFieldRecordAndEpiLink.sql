USE [NBS_ODSE]
GO

/*ND-14810 DEV: Create Stored Proc to 'Recreate' NBS191 (Epi-Link ID - Lot Number) and NBS160 (Field Record Number) data from history (SQL)*/

EXEC	spAddMissingFieldRecEpiLinkToCaseMgt
		@pmode = 'PROD'
GO

------------------------------------------------------------------------------------------------------------------------------------------

--2. Run the following script to update Epi link id's from Field Record numbers for cases left behind after the first stored procedure

update nbs_odse..case_management set epi_link_id=field_record_number where epi_link_id is null and field_record_number is not null
GO

------------------------------------------------------------------------------------------------------------------------------------------

--3. Run spAddMissingFieldRecEpiLinkFromCaseId

EXEC	spAddMissingFieldRecEpiLinkFromCaseId
		@pmode = 'PROD'
GO