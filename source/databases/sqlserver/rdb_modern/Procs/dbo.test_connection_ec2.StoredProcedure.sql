USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[test_connection_ec2]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[test_connection_ec2]
AS 
SELECT top 1 * from job_flow_log;
GO
