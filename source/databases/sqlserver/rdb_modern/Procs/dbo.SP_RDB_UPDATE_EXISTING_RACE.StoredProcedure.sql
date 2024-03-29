USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[SP_RDB_UPDATE_EXISTING_RACE]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[SP_RDB_UPDATE_EXISTING_RACE] 
AS
BEGIN

		--Logic for Single Race
		--Updating Calculated Race Details Column
		--CASE 1: When are is only a single Known Race
		UPDATE [dbo].[D_PATIENT]
		SET PATIENT_RACE_CALC_DETAILS=PATIENT_RACE_ALL
		WHERE PATIENT_RACE_ALL in ('American Indian or Alaska Native','Asian','Black or African American','Native Hawaiian or Other Pacific Islander','White','Other Race')
		
		--Logic for Single Race
		--Updating Calculated Race Details Column
		--CASE 2: When are is only a single Null Flavor Race
		UPDATE [dbo].[D_PATIENT]
		SET PATIENT_RACE_CALC_DETAILS='Unknown'
		WHERE PATIENT_RACE_ALL in ('Unknown','not asked','Refused to Answer')
		
		--Logic for Multi Race
		--Updating Calculated Race Details Column
		--CASE 1: When there are only 2 races as a part of Multi-Race(irrespective of known or null)
		UPDATE [dbo].[D_PATIENT]
		SET PATIENT_RACE_CALC_DETAILS = (CASE	
											 WHEN PATIENT_RACE_ALL like '%Unknown%' OR PATIENT_RACE_ALL like '%not asked%' OR PATIENT_RACE_ALL like '%Refused to Answer%' THEN (CASE
																													WHEN PATIENT_RACE_ALL like '%American Indian or Alaska Native%' THEN 'American Indian or Alaska Native'
																													WHEN PATIENT_RACE_ALL like '%Asian%' THEN 'Asian'
																													WHEN PATIENT_RACE_ALL like '%Black or African American%' THEN 'Black or African American'
																													WHEN PATIENT_RACE_ALL like '%Native Hawaiian or Other Pacific Islander%' THEN 'Native Hawaiian or Other Pacific Islander'
																													WHEN PATIENT_RACE_ALL like '%White%' THEN 'White'
																													WHEN PATIENT_RACE_ALL like '%Other Race%' THEN 'Other Race'
																													ELSE 'Unknown'
																													END) 
											 ELSE PATIENT_RACE_ALL
											 END)
		WHERE LEN(PATIENT_RACE_ALL)-LEN(REPLACE(PATIENT_RACE_ALL,'|',''))=1
			
		--Logic for Multi Race
		--Updating Calculated Race Details Column
		--CASE 2: When there are more then 2 known races as a part of Multi-Race
		UPDATE [dbo].[D_PATIENT]
		SET PATIENT_RACE_CALC_DETAILS = (CASE	
											 WHEN PATIENT_RACE_ALL like '% | Unknown%' THEN (CASE
																						   WHEN PATIENT_RACE_ALL like '%American Indian or Alaska Native%' OR PATIENT_RACE_ALL like '%Asian%' OR PATIENT_RACE_ALL like '%Black or African American%' OR PATIENT_RACE_ALL like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_ALL like '%White%' OR PATIENT_RACE_ALL like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE(' | Unknown',PATIENT_RACE_ALL))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_ALL like '% | Not Asked%' THEN (CASE
																						   WHEN PATIENT_RACE_ALL like '%American Indian or Alaska Native%' OR PATIENT_RACE_ALL like '%Asian%' OR PATIENT_RACE_ALL like '%Black or African American%' OR PATIENT_RACE_ALL like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_ALL like '%White%' OR PATIENT_RACE_ALL like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE(' | Not Asked',PATIENT_RACE_ALL))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_ALL like '% | Refused to Answer%' THEN (CASE
																						   WHEN PATIENT_RACE_ALL like '%American Indian or Alaska Native%' OR PATIENT_RACE_ALL like '%Asian%' OR PATIENT_RACE_ALL like '%Black or African American%' OR PATIENT_RACE_ALL like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_ALL like '%White%' OR PATIENT_RACE_ALL like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE(' | Refused to Answer',PATIENT_RACE_ALL))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_ALL like '%Unknown | %' THEN (CASE
																						   WHEN PATIENT_RACE_ALL like '%American Indian or Alaska Native%' OR PATIENT_RACE_ALL like '%Asian%' OR PATIENT_RACE_ALL like '%Black or African American%' OR PATIENT_RACE_ALL like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_ALL like '%White%' OR PATIENT_RACE_ALL like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE('Unknown | ',PATIENT_RACE_ALL))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_ALL like '%Not Asked | %' THEN (CASE
																						   WHEN PATIENT_RACE_ALL like '%American Indian or Alaska Native%' OR PATIENT_RACE_ALL like '%Asian%' OR PATIENT_RACE_ALL like '%Black or African American%' OR PATIENT_RACE_ALL like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_ALL like '%White%' OR PATIENT_RACE_ALL like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE('Not Asked | ',PATIENT_RACE_ALL))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_ALL like '%Refused to Answer | %' THEN (CASE
																						   WHEN PATIENT_RACE_ALL like '%American Indian or Alaska Native%' OR PATIENT_RACE_ALL like '%Asian%' OR PATIENT_RACE_ALL like '%Black or African American%' OR PATIENT_RACE_ALL like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_ALL like '%White%' OR PATIENT_RACE_ALL like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE('Refused to Answer | ',PATIENT_RACE_ALL))
																						   ELSE 'Unknown'
																						   END)
											 ELSE PATIENT_RACE_ALL
											 END)
		WHERE LEN(PATIENT_RACE_ALL)-LEN(REPLACE(PATIENT_RACE_ALL,'|',''))>1
		
		--Logic for Multi Race
		--Updating Calculated Race Details Column
		--CASE 3.1: When there are more then 2 Null Flavor races as a part of Multi-Race
		UPDATE [dbo].[D_PATIENT]
		SET PATIENT_RACE_CALC_DETAILS = (CASE	
											 WHEN PATIENT_RACE_CALC_DETAILS like '%Unknown%' OR PATIENT_RACE_CALC_DETAILS like '%not asked%' OR PATIENT_RACE_CALC_DETAILS like '%Refused to Answer%' THEN (CASE
																													WHEN PATIENT_RACE_CALC_DETAILS like '%American Indian or Alaska Native%' THEN 'American Indian or Alaska Native'
																													WHEN PATIENT_RACE_CALC_DETAILS like '%Asian%' THEN 'Asian'
																													WHEN PATIENT_RACE_CALC_DETAILS like '%Black or African American%' THEN 'Black or African American'
																													WHEN PATIENT_RACE_CALC_DETAILS like '%Native Hawaiian or Other Pacific Islander%' THEN 'Native Hawaiian or Other Pacific Islander'
																													WHEN PATIENT_RACE_CALC_DETAILS like '%White%' THEN 'White'
																													WHEN PATIENT_RACE_CALC_DETAILS like '%Other Race%' THEN 'Other Race'
																													ELSE 'Unknown'
																													END) 
											 ELSE PATIENT_RACE_CALC_DETAILS
											 END)
		WHERE LEN(PATIENT_RACE_CALC_DETAILS)-LEN(REPLACE(PATIENT_RACE_CALC_DETAILS,'|',''))=1
		UPDATE [dbo].[D_PATIENT]
		SET PATIENT_RACE_CALC_DETAILS = (CASE	
											 WHEN PATIENT_RACE_CALC_DETAILS like '% | Unknown%' THEN (CASE
																						   WHEN PATIENT_RACE_CALC_DETAILS like '%American Indian or Alaska Native%' OR PATIENT_RACE_CALC_DETAILS like '%Asian%' OR PATIENT_RACE_CALC_DETAILS like '%Black or African American%' OR PATIENT_RACE_CALC_DETAILS like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_CALC_DETAILS like '%White%' OR PATIENT_RACE_CALC_DETAILS like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE(' | Unknown',PATIENT_RACE_CALC_DETAILS))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_CALC_DETAILS like '% | Not Asked%' THEN (CASE
																						   WHEN PATIENT_RACE_CALC_DETAILS like '%American Indian or Alaska Native%' OR PATIENT_RACE_CALC_DETAILS like '%Asian%' OR PATIENT_RACE_CALC_DETAILS like '%Black or African American%' OR PATIENT_RACE_CALC_DETAILS like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_CALC_DETAILS like '%White%' OR PATIENT_RACE_CALC_DETAILS like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE(' | Not Asked',PATIENT_RACE_CALC_DETAILS))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_CALC_DETAILS like '% | Refused to Answer%' THEN (CASE
																						   WHEN PATIENT_RACE_CALC_DETAILS like '%American Indian or Alaska Native%' OR PATIENT_RACE_CALC_DETAILS like '%Asian%' OR PATIENT_RACE_CALC_DETAILS like '%Black or African American%' OR PATIENT_RACE_CALC_DETAILS like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_CALC_DETAILS like '%White%' OR PATIENT_RACE_CALC_DETAILS like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE(' | Refused to Answer',PATIENT_RACE_CALC_DETAILS))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_CALC_DETAILS like '%Unknown | %' THEN (CASE
																						   WHEN PATIENT_RACE_CALC_DETAILS like '%American Indian or Alaska Native%' OR PATIENT_RACE_CALC_DETAILS like '%Asian%' OR PATIENT_RACE_CALC_DETAILS like '%Black or African American%' OR PATIENT_RACE_CALC_DETAILS like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_CALC_DETAILS like '%White%' OR PATIENT_RACE_CALC_DETAILS like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE('Unknown | ',PATIENT_RACE_CALC_DETAILS))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_CALC_DETAILS like '%Not Asked | %' THEN (CASE
																						   WHEN PATIENT_RACE_CALC_DETAILS like '%American Indian or Alaska Native%' OR PATIENT_RACE_CALC_DETAILS like '%Asian%' OR PATIENT_RACE_CALC_DETAILS like '%Black or African American%' OR PATIENT_RACE_CALC_DETAILS like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_CALC_DETAILS like '%White%' OR PATIENT_RACE_CALC_DETAILS like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE('Not Asked | ',PATIENT_RACE_CALC_DETAILS))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_CALC_DETAILS like '%Refused to Answer | %' THEN (CASE
																						   WHEN PATIENT_RACE_CALC_DETAILS like '%American Indian or Alaska Native%' OR PATIENT_RACE_CALC_DETAILS like '%Asian%' OR PATIENT_RACE_CALC_DETAILS like '%Black or African American%' OR PATIENT_RACE_CALC_DETAILS like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_CALC_DETAILS like '%White%' OR PATIENT_RACE_CALC_DETAILS like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE('Refused to Answer | ',PATIENT_RACE_CALC_DETAILS))
																						   ELSE 'Unknown'
																						   END)
											 ELSE PATIENT_RACE_CALC_DETAILS
											 END)
		WHERE LEN(PATIENT_RACE_CALC_DETAILS)-LEN(REPLACE(PATIENT_RACE_CALC_DETAILS,'|',''))>1
		
		--Logic for Multi Race
		--Updating Calculated Race Details Column
		--CASE 3.2: When there are more then 2 Null Flavor races as a part of Multi-Race
		UPDATE [dbo].[D_PATIENT]
		SET PATIENT_RACE_CALC_DETAILS = (CASE	
											 WHEN PATIENT_RACE_CALC_DETAILS like '%Unknown%' OR PATIENT_RACE_CALC_DETAILS like '%not asked%' OR PATIENT_RACE_CALC_DETAILS like '%Refused to Answer%' THEN (CASE
																													WHEN PATIENT_RACE_CALC_DETAILS like '%American Indian or Alaska Native%' THEN 'American Indian or Alaska Native'
																													WHEN PATIENT_RACE_CALC_DETAILS like '%Asian%' THEN 'Asian'
																													WHEN PATIENT_RACE_CALC_DETAILS like '%Black or African American%' THEN 'Black or African American'
																													WHEN PATIENT_RACE_CALC_DETAILS like '%Native Hawaiian or Other Pacific Islander%' THEN 'Native Hawaiian or Other Pacific Islander'
																													WHEN PATIENT_RACE_CALC_DETAILS like '%White%' THEN 'White'
																													WHEN PATIENT_RACE_CALC_DETAILS like '%Other Race%' THEN 'Other Race'
																													ELSE 'Unknown'
																													END) 
											 ELSE PATIENT_RACE_CALC_DETAILS
											 END)
		WHERE LEN(PATIENT_RACE_CALC_DETAILS)-LEN(REPLACE(PATIENT_RACE_CALC_DETAILS,'|',''))=1
		UPDATE [dbo].[D_PATIENT]
		SET PATIENT_RACE_CALC_DETAILS = (CASE	
											 WHEN PATIENT_RACE_CALC_DETAILS like '% | Unknown%' THEN (CASE
																						   WHEN PATIENT_RACE_CALC_DETAILS like '%American Indian or Alaska Native%' OR PATIENT_RACE_CALC_DETAILS like '%Asian%' OR PATIENT_RACE_CALC_DETAILS like '%Black or African American%' OR PATIENT_RACE_CALC_DETAILS like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_CALC_DETAILS like '%White%' OR PATIENT_RACE_CALC_DETAILS like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE(' | Unknown',PATIENT_RACE_CALC_DETAILS))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_CALC_DETAILS like '% | Not Asked%' THEN (CASE
																						   WHEN PATIENT_RACE_CALC_DETAILS like '%American Indian or Alaska Native%' OR PATIENT_RACE_CALC_DETAILS like '%Asian%' OR PATIENT_RACE_CALC_DETAILS like '%Black or African American%' OR PATIENT_RACE_CALC_DETAILS like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_CALC_DETAILS like '%White%' OR PATIENT_RACE_CALC_DETAILS like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE(' | Not Asked',PATIENT_RACE_CALC_DETAILS))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_CALC_DETAILS like '% | Refused to Answer%' THEN (CASE
																						   WHEN PATIENT_RACE_CALC_DETAILS like '%American Indian or Alaska Native%' OR PATIENT_RACE_CALC_DETAILS like '%Asian%' OR PATIENT_RACE_CALC_DETAILS like '%Black or African American%' OR PATIENT_RACE_CALC_DETAILS like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_CALC_DETAILS like '%White%' OR PATIENT_RACE_CALC_DETAILS like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE(' | Refused to Answer',PATIENT_RACE_CALC_DETAILS))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_CALC_DETAILS like '%Unknown | %' THEN (CASE
																						   WHEN PATIENT_RACE_CALC_DETAILS like '%American Indian or Alaska Native%' OR PATIENT_RACE_CALC_DETAILS like '%Asian%' OR PATIENT_RACE_CALC_DETAILS like '%Black or African American%' OR PATIENT_RACE_CALC_DETAILS like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_CALC_DETAILS like '%White%' OR PATIENT_RACE_CALC_DETAILS like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE('Unknown | ',PATIENT_RACE_CALC_DETAILS))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_CALC_DETAILS like '%Not Asked | %' THEN (CASE
																						   WHEN PATIENT_RACE_CALC_DETAILS like '%American Indian or Alaska Native%' OR PATIENT_RACE_CALC_DETAILS like '%Asian%' OR PATIENT_RACE_CALC_DETAILS like '%Black or African American%' OR PATIENT_RACE_CALC_DETAILS like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_CALC_DETAILS like '%White%' OR PATIENT_RACE_CALC_DETAILS like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE('Not Asked | ',PATIENT_RACE_CALC_DETAILS))
																						   ELSE 'Unknown'
																						   END)
											 WHEN PATIENT_RACE_CALC_DETAILS like '%Refused to Answer | %' THEN (CASE
																						   WHEN PATIENT_RACE_CALC_DETAILS like '%American Indian or Alaska Native%' OR PATIENT_RACE_CALC_DETAILS like '%Asian%' OR PATIENT_RACE_CALC_DETAILS like '%Black or African American%' OR PATIENT_RACE_CALC_DETAILS like '%Native Hawaiian or Other Pacific Islander%' OR PATIENT_RACE_CALC_DETAILS like '%White%' OR PATIENT_RACE_CALC_DETAILS like '%Other Race%' THEN (SELECT dbo.REMOVE_NULL_RACE('Refused to Answer | ',PATIENT_RACE_CALC_DETAILS))
																						   ELSE 'Unknown'
																						   END)
											 ELSE PATIENT_RACE_CALC_DETAILS
											 END)
		WHERE LEN(PATIENT_RACE_CALC_DETAILS)-LEN(REPLACE(PATIENT_RACE_CALC_DETAILS,'|',''))>1
		
		--Logic for Multi Race
		--Updating Calculated Race Column
		UPDATE [dbo].[D_PATIENT]
		SET PATIENT_RACE_CALCULATED = (CASE	
										   WHEN PATIENT_RACE_CALC_DETAILS like '%Unknown%' THEN 'Unknown'
										   ELSE 'Multi-Race'
										   END)
		WHERE LEN(PATIENT_RACE_CALC_DETAILS)-LEN(REPLACE(PATIENT_RACE_CALC_DETAILS,'|',''))>0
		
		--Logic for Single Race
		--Updating Calculated Race Column
		--CASE 1: When there is only a single Known Race
		UPDATE [dbo].[D_PATIENT]
		SET PATIENT_RACE_CALCULATED = PATIENT_RACE_CALC_DETAILS
		WHERE PATIENT_RACE_CALC_DETAILS in ('American Indian or Alaska Native','Asian','Black or African American','Native Hawaiian or Other Pacific Islander','White','Other Race')
		
		--Logic for Single Race
		--Updating Calculated Race Column
		--CASE 2: When there is only a single Null Flavor Race
		UPDATE [dbo].[D_PATIENT]
		SET PATIENT_RACE_CALCULATED = 'Unknown'
		WHERE PATIENT_RACE_CALC_DETAILS in ('Unknown','not asked','Refused to Answer')
		
		--Updating [HEPATITIS_DATAMART].[PAT_RACE]  
		--Matching [HEPATITIS_DATAMART].[PAT_UID] = [dbo].[D_PATIENT].[PATIENT_UID]
		UPDATE [dbo].[HEPATITIS_DATAMART]
		SET [dbo].[HEPATITIS_DATAMART].[PAT_RACE]=[dbo].[D_PATIENT].[PATIENT_RACE_CALCULATED]
		FROM [dbo].[D_PATIENT]
		JOIN [dbo].[HEPATITIS_DATAMART] ON [dbo].[HEPATITIS_DATAMART].[PATIENT_UID] = [dbo].[D_PATIENT].[PATIENT_UID]

		--Updating [HEP100].[RACE]  
		--Matching [HEP100].[PATIENT_UID] = [dbo].[D_PATIENT].[PATIENT_UID]
		UPDATE [dbo].[HEP100]
		SET [dbo].[HEP100].[RACE]=[dbo].[D_PATIENT].[PATIENT_RACE_CALC_DETAILS]
		FROM [dbo].[D_PATIENT]
		JOIN [dbo].[HEP100] ON [dbo].[HEP100].[PATIENT_UID] = [dbo].[D_PATIENT].[PATIENT_UID]

END
GO
