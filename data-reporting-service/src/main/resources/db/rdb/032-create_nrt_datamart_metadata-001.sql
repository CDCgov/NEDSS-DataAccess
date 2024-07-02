USE RDB;
IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name = 'nrt_datamart_metadata' and xtype = 'U')
CREATE TABLE dbo.nrt_datamart_metadata (
   condition_cd varchar(20) NOT NULL,
   condition_desc_txt varchar(300) NULL,
   Datamart varchar(18) NOT NULL,
   Stored_Procedure varchar(36) NOT NULL
);