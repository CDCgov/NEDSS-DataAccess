USE [rdb_modern]
GO
/****** Object:  View [dbo].[V_EVENT_METRIC]    Script Date: 1/17/2024 8:39:44 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[V_EVENT_METRIC]
AS
     SELECT B.FIRST_NM,
            B.LAST_NM,
            B.PROVIDER_QUICK_CODE,
            A.ADD_USER_ID,
            A.ADD_TIME,
            A.EVENT_TYPE,
            A.PROG_AREA_CD,
            A.LOCAL_ID,
			A.ELECTRONIC_IND
     FROM RDB.dbo.EVENT_METRIC A
          INNER JOIN RDB.dbo.USER_PROFILE B ON A.ADD_USER_ID = B.NEDSS_ENTRY_ID
     WHERE EVENT_TYPE IN('PHCINVFORM', 'LABREPORT', 'MORBREPORT', 'CONTACT');
GO
