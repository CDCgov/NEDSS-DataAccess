USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[SP_UPDATE_EVENT_DATE]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[SP_UPDATE_EVENT_DATE]
     @batch_id BIGINT = 9999,
	 @Dataflow_Name  VARCHAR (100),
     @vTableName VARCHAR (100)  ,
     @vColumnName VARCHAR (100) = 'EVENT_DATE',
	 @ERROR_MSG varchar(100) OUTPUT

AS

   DECLARE @RET INT =null ;
	DECLARE @Table_RowCount_no INT ;
    DECLARE @Proc_Step_no FLOAT = 0 ;
    DECLARE @Proc_Step_Name VARCHAR(200) = '' ;
	DECLARE @batch_start_time datetime2(7) = null ;
	DECLARE @batch_end_time datetime2(7) = null ;
 
   BEGIN
    BEGIN TRY
    
	


	if (@vTableName IS NOT NULL) and  (  @vColumnName IS NOT NULL)
     BEGIN
	   

				DECLARE @SQL nvarchar(1000)
 
				DECLARE @SQL_UPD nvarchar(1000)
    
				
		           SET @SQL = N'SELECT @RET=  OBJECT_ID('''+@vTableName+''', ''U'')'

					EXEC SP_EXECUTESQL  @Query  = @SQL
						  , @Params = N'@RET INT OUTPUT'
						  , @RET = @RET OUTPUT

					--SELECT 'EXISIT',@RET
		         
				 if @RET is null
				   begin
				     SET @ERROR_MSG =  ' Table not found';


         				 --IF @@TRANCOUNT > 0   ROLLBACK TRANSACTION;

						INSERT INTO [dbo].[job_flow_log] (
								batch_id
							   ,[Dataflow_Name]
							   ,[package_Name]
								,[Status_Type] 
							   ,[step_number]
							   ,[step_name]
							   ,[Error_Description]
							   ,[row_count]
							   )
							   VALUES
							   (
							   @batch_id
							   ,@Dataflow_Name	
							   ,'SP_UPDATE_EVENT_DATE'
							   ,'ERROR'
							   ,@Proc_Step_no
							   ,'ERROR - '+ @Proc_Step_name
							   , 'Step -' + ' Table not found'
							   ,0
							   );
					 
					 return -1 ;
				 end;

				BEGIN TRANSACTION; 

				  set @RET = null;
				
				   SET @SQL = N'SELECT @RET=object_id  FROM sys.columns   WHERE Name = N''SPECIMEN_COLLECTION_DT''  AND Object_ID = Object_ID('''+'dbo.TMP_GEN_PATINFO_INV_PHY_RPTSRC_COND'+''') ';

				   --select @SQL

    			   EXEC SP_EXECUTESQL  @Query  = @SQL
						  , @Params = N'@RET INT OUTPUT'
						  , @RET = @RET OUTPUT

					--SELECT 'EXISIT',@RET
   	         
				 if @RET is not null
				  BEGIN
                   --select ' SPECIMEN_COLLECTION_DT Column Exists';

				   SET @SQL = 'update '+ @vTableName+
											' set 	EVENT_DATE = SPECIMEN_COLLECTION_DT' +
											' where  SPECIMEN_COLLECTION_DT is not null '  +
											' and  EVENT_DATE is null ' 
											;
 
				   EXEC (@SQL);
                  END
				  ;



                set @RET = null;
				
				   SET @SQL = N'SELECT @RET=object_id  FROM sys.columns   WHERE Name = N''ILLNESS_ONSET_DT''  AND Object_ID = Object_ID('''+@vTableName+''') ';

    			   EXEC SP_EXECUTESQL  @Query  = @SQL
						  , @Params = N'@RET INT OUTPUT'
						  , @RET = @RET OUTPUT

					--SELECT 'EXISIT',@RET
   	         
				 if @RET is not null
				  BEGIN
                  --select 'Column Exists';

				   SET @SQL = 'update '+ @vTableName+
											' set 	EVENT_DATE = ILLNESS_ONSET_DT' +
											' where  ILLNESS_ONSET_DT is not null '  +
											' and  EVENT_DATE is null ' 
											;
 
				   EXEC (@SQL);
                  END
				  ;


				  
                set @RET = null;
				
				   SET @SQL = N'SELECT @RET=object_id  FROM sys.columns   WHERE Name = N''DIAGNOSIS_DT''  AND Object_ID = Object_ID('''+@vTableName+''') ';

    			   EXEC SP_EXECUTESQL  @Query  = @SQL
						  , @Params = N'@RET INT OUTPUT'
						  , @RET = @RET OUTPUT

					--SELECT 'EXISIT',@RET
   	         
				 if @RET is not null
				  BEGIN
   
				     SET @SQL = 'update '+ @vTableName+
											' set EVENT_DATE = DIAGNOSIS_DT ' +
											' where  DIAGNOSIS_DT is not null  ' +
											' and  EVENT_DATE is null ' ;
										;

     
				     EXEC (@SQL);


                   end
				   ;
				    

               set @RET = null;
				
				DECLARE @SQL_LIST varchar(4000);

				set @SQL_LIST = '';


			  
                
				   SET @SQL = N'SELECT @RET=object_id  FROM sys.columns   WHERE Name = N''EARLIEST_RPT_TO_STATE_DT''  AND Object_ID = Object_ID('''+@vTableName+''') ';

    			   EXEC SP_EXECUTESQL  @Query  = @SQL
						  , @Params = N'@RET INT OUTPUT'
						  , @RET = @RET OUTPUT

					--SELECT 'EXISIT',@RET
   	         
				 if @RET is not null SET @SQL_LIST = @SQL_LIST + ', (EARLIEST_RPT_TO_STATE_DT) ';



				   SET @SQL = N'SELECT @RET=object_id  FROM sys.columns   WHERE Name = N''EARLIEST_RPT_TO_CNTY_DT''  AND Object_ID = Object_ID('''+@vTableName+''') ';

    			   EXEC SP_EXECUTESQL  @Query  = @SQL
						  , @Params = N'@RET INT OUTPUT'
						  , @RET = @RET OUTPUT

					--SELECT 'EXISIT',@RET
   	         
				 if @RET is not null SET @SQL_LIST = @SQL_LIST + ', (EARLIEST_RPT_TO_CNTY_DT) ';

				 

				   SET @SQL = N'SELECT @RET=object_id  FROM sys.columns   WHERE Name = N''INV_RPT_DT''  AND Object_ID = Object_ID('''+@vTableName+''') ';

    			   EXEC SP_EXECUTESQL  @Query  = @SQL
						  , @Params = N'@RET INT OUTPUT'
						  , @RET = @RET OUTPUT

					--SELECT 'EXISIT',@RET
   	         
				 if @RET is not null SET @SQL_LIST = @SQL_LIST + ', (INV_RPT_DT) ';

				 
				   SET @SQL = N'SELECT @RET=object_id  FROM sys.columns   WHERE Name = N''INV_START_DT''  AND Object_ID = Object_ID('''+@vTableName+''') ';

    			   EXEC SP_EXECUTESQL  @Query  = @SQL
						  , @Params = N'@RET INT OUTPUT'
						  , @RET = @RET OUTPUT

					--SELECT 'EXISIT',@RET
   	         
				 if @RET is not null SET @SQL_LIST = @SQL_LIST + ', (INV_START_DT) ';


				  
				   SET @SQL = N'SELECT @RET=object_id  FROM sys.columns   WHERE Name = N''CONFIRMATION_DT''  AND Object_ID = Object_ID('''+@vTableName+''') ';

    			   EXEC SP_EXECUTESQL  @Query  = @SQL
						  , @Params = N'@RET INT OUTPUT'
						  , @RET = @RET OUTPUT

					--SELECT 'CONFIRMATION_DT EXISIT',@RET
   	         
				 if @RET is not null SET @SQL_LIST = @SQL_LIST + ', (CONFIRMATION_DT) ';


                  
				   SET @SQL = N'SELECT @RET=object_id  FROM sys.columns   WHERE Name = N''HSPTL_ADMISSION_DT''  AND Object_ID = Object_ID('''+@vTableName+''') ';

    			   EXEC SP_EXECUTESQL  @Query  = @SQL
						  , @Params = N'@RET INT OUTPUT'
						  , @RET = @RET OUTPUT

					--SELECT 'EXISIT',@RET
   	         
				 if @RET is not null SET @SQL_LIST = @SQL_LIST + ', (HSPTL_ADMISSION_DT) ';


				 
				   SET @SQL = N'SELECT @RET=object_id  FROM sys.columns   WHERE Name = N''HSPTL_DISCHARGE_DT''  AND Object_ID = Object_ID('''+@vTableName+''') ';

    			   EXEC SP_EXECUTESQL  @Query  = @SQL
						  , @Params = N'@RET INT OUTPUT'
						  , @RET = @RET OUTPUT

					--SELECT 'EXISIT',@RET
   	          
				 if @RET is not null SET @SQL_LIST = @SQL_LIST + ', (HSPTL_DISCHARGE_DT) ';




			  set @SQL_LIST =  rtrim(substring(@SQL_LIST,2,len(@SQL_LIST)));

     
	 /*
				set @SQL_LIST                    =  	' (EARLIEST_RPT_TO_STATE_DT) ' + 
														', (INV_RPT_DT) ' + 
														', (INV_START_DT) ' + 
														', (HSPTL_ADMISSION_DT) ' + 
														', (HSPTL_DISCHARGE_DT) ' + 
														', (INV_ADD_TIME) '
														;

 */

				
				SET @SQL = 'update '+ @vTableName+
											' set EVENT_DATE =  ( select min(min_dt) from ( values ' +    
												 @SQL_LIST	 +
												') as Fields(min_dt))' +   
										' where  EVENT_DATE is null '

					EXEC (@SQL);

					

				set @RET = null;
				  
                
				   SET @SQL = N'SELECT @RET=object_id  FROM sys.columns   WHERE Name = N''INV_ADD_TIME''  AND Object_ID = Object_ID('''+@vTableName+''') ';

    			   EXEC SP_EXECUTESQL  @Query  = @SQL
						  , @Params = N'@RET INT OUTPUT'
						  , @RET = @RET OUTPUT

					--SELECT 'EXISIT',@RET
   	         
				 if @RET is not null
				  BEGIN
   
				     SET @SQL = 'update '+ @vTableName+
											' set EVENT_DATE = INV_ADD_TIME ' +
											' where  INV_ADD_TIME IS NOT null ' +
											' and  EVENT_DATE is null ' ;
										;

     
				     EXEC (@SQL);


                   end
				   ;
				    





 
 			COMMIT TRANSACTION;
         END
      ELSE
	     BEGIN
		          select 'Table not specified'
				  return -1 ;
		 END

	  END TRY

	 BEGIN CATCH
  
   
				 IF @@TRANCOUNT > 0   ROLLBACK TRANSACTION;
	
				DECLARE @ErrorNumber INT = ERROR_NUMBER();
				DECLARE @ErrorLine INT = ERROR_LINE();
				DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
				DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
				DECLARE @ErrorState INT = ERROR_STATE();
 
	          


				INSERT INTO [dbo].[job_flow_log] (
						batch_id
					   ,[Dataflow_Name]
					   ,[package_Name]
						,[Status_Type] 
					   ,[step_number]
					   ,[step_name]
					   ,[Error_Description]
					   ,[row_count]
					   )
					   VALUES
					   (
					   @batch_id
					   ,@Dataflow_Name	
					   ,'SP_UPDATE_EVENT_DATE'
					   ,'ERROR'       
					   ,@Proc_Step_no
					   ,'ERROR - '+ @Proc_Step_name
					   , 'Step -' +CAST(@Proc_Step_no AS VARCHAR(3))+' -' +CAST(@ErrorMessage AS VARCHAR(500))
					   ,0
					   );

                  
				  return -1 ;
				END CATCH

END

;



GO
