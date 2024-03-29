USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_nbs_batch_wait_complete]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_nbs_batch_wait_complete]
as

BEGIN
		DECLARE @status_counter INT = 1;


		select   @status_counter = count(*)
		 from [dbo].[job_batch_log]
							 where status_type = 'start' ;

 
		WHILE @status_counter > 0 
		BEGIN
 	
			WAITFOR DELAY '00:01:00';

		   select   @status_counter = count(*)
		      from [dbo].[job_batch_log]
			  where status_type = 'start' ;
		END
END
GO
