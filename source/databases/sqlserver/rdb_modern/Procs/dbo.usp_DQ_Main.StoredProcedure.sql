USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[usp_DQ_Main]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS OFF
GO
SET QUOTED_IDENTIFIER OFF
GO
/*** Data Validation Data Mart Execution Guide ***/

/*** The following procedure can be executed in SQL Server ***/
/***  dbo.usp_DQ_MAIN @need_error, @need_warning, @need_ELR, @min_last_chg_time ***/
/*** The procedure takes four parameters. ***/
/*** Parameter 1: Represents whether or not errors are needed. 0 represents 'no'; 1 represents 'yes' ***/
/*** Parameter 2: Represents whether or not warnings are needed. 0 represents 'no'; 1 represents 'yes' ***/
/*** Parameter 3: Represents whether or not ELRs are needed. 0 represents 'no'; 1 represents 'yes' ***/
/*** Parameter 4: Represents the date threshold (2 Options). Option 1: NULL = ALL Dates; Option 2: Valid Date ('12/01/2008','mm/dd/yyyy') = Date entered up to today's date***/

-- Examples:

/* The following will create a data mart with all errors, all warnings, including ELRs, for ALL dates */
--EXEC usp_DQ_Main

/* The following will return the same result as above. It will create a data mart with all errors, all warnings, including ELRs, for ALL dates */
--EXEC usp_DQ_Main 1,1,1,NULL

/* The following will create a data mart with all errors, no warnings, including ELRs, for ALL dates */
--EXEC usp_DQ_Main 1,0,1,NULL

/* The following will create a data mart with no errors, all warnings, excluding ELRs, for records on or after 12/01/2008 */
--EXEC usp_DQ_Main 0,1,0, '12/01/2008'

-- =============================================
-- Author:		Hong Zhang
-- Create date: 20090128 (R2.1)
-- Description:	This procedure process all invesigation/lab test/morbidigy errors and warnings data,
--							It calls four subprocedures: usp_DQ_Set_Investigation, 
--							usp_DQ_Get_Investigation, usp_DQ_Get_Labtest,
--							usp_DQ_Get_Morbidity to process data step by step.
--							by default, it gets all data. however, users can specify the params to pick subset 
--							by setting the params as 1 or 0, or by setting a minimum last change date.	
--------------------------------------------------------------------------------------------------------
-- Modify Date: 20100805 (R4.0.2)
-- Modified By: Hong Zhang
-- Modification Description: The sub procedures were modified: 
--                                              1) In DQ_ALL_CASE table, removed PERSON_RACE_KEY column, added PATIENT_RACE_CALCULATED column;
--                                              2) Because PERSON table was removed in RDB, now the Patient info is obtained from D_PATIENT table;
--                                                 and Provider info is obtained from D_PROVIDER table. 
--                                              3) State, County and Race info for patients is obtained from D_PATIENT directly instead of using multiple joins.
--                                              4) The data in column for gender in D_PATIENT is different from that in PERSON table, so a little parsing was added.                
-- =============================================
CREATE  PROCEDURE [dbo].[usp_DQ_Main] 
@need_error			tinyint=1,
@need_warning	 tinyint=1,
@need_ELR			 tinyint=1,
@min_last_chg_time	datetime=null

AS

BEGIN
	
	SET NOCOUNT ON
	PRINT 'Main Process Started...'+ CONVERT(varchar(20), getdate(),120) 

	/* user param entry validation */
	IF @need_error NOT IN (0,1) OR @need_warning NOT IN (0,1) OR @need_ELR NOT IN (0,1)
	BEGIN
		RAISERROR ('The ''@need_xxx'' parameters must be either 0 or 1!', 16, 1)
		RETURN
	END

	IF @need_error = 0 AND @need_warning = 0
	BEGIN
		RAISERROR ('The ''@need_error'' and ''@need_warning'' parameters cannot be both zero. At least one of them needs to be 1!', 16, 2)
		RETURN
	END	 

	/* write all investigation table data into the main dq_all_case table */
	EXEC	 usp_DQ_Set_Investigation
				@min_last_chg_time=@min_last_chg_time

	/* Clean up Data_Validation table that contains all the data validations */
	TRUNCATE TABLE data_validation

	/* Get Investigation data from dq_all_case table into data_validation */
	EXEC	 usp_DQ_Get_Investigation
		@need_error = @need_error,
		@need_warning = @need_warning

	/* Get lab test data into data_validation */
	EXEC  usp_DQ_Get_Labtest
		@need_error = @need_error,
		@need_warning = @need_warning,
		@need_ELR = @need_ELR,
		@min_last_chg_time=@min_last_chg_time

	/* Get morbidity data into data_validation */
	EXEC	 usp_DQ_Get_Morbidity
		@need_error = @need_error,
		@need_warning = @need_warning,
		@min_last_chg_time=@min_last_chg_time

	PRINT 'Main Process Completed...'+ CONVERT(varchar(20), getdate(),120) 

END

GO
