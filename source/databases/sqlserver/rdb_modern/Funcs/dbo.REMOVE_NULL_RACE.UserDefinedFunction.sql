USE [rdb_modern]
GO
/****** Object:  UserDefinedFunction [dbo].[REMOVE_NULL_RACE]    Script Date: 1/17/2024 9:38:18 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION [dbo].[REMOVE_NULL_RACE](@nullVar varchar(500),@concatRaceVar varchar(500))
RETURNS varchar(500)
AS
BEGIN
DECLARE
	@CalculatedRace nvarchar(255),
	@pos1  int,
	@pos2  int;
	select @pos1 = CHARINDEX (@nullVar, @concatRaceVar,1);
	select @pos2 = @pos1+LEN(@nullVar);
	IF @pos1 = 0 
		Select @CalculatedRace = @concatRaceVar;
	ELSE
		Select @CalculatedRace = SUBSTRING(@concatRaceVar,1,@pos1 - 1 ) + SUBSTRING(@concatRaceVar,@POS2,LEN(@concatRaceVar) )

RETURN @CalculatedRace
END
GO
