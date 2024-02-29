USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_ETL_TEST_4]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE  PROCEDURE [dbo].[sp_ETL_TEST_4] as
BEGIN
    EXEC dbo.sp_S_INV_ADMINISTRATIVE -9;
    EXEC dbo.sp_S_INV_CLINICAL -9;
    EXEC dbo.sp_S_INV_COMPLICATION -9;
    EXEC dbo.sp_S_INV_CONTACT -9;
END
;
GO
