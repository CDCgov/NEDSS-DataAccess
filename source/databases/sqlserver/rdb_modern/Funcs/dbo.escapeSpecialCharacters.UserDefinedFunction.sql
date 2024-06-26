USE [rdb_modern]
GO
/****** Object:  UserDefinedFunction [dbo].[escapeSpecialCharacters]    Script Date: 1/17/2024 9:38:18 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[escapeSpecialCharacters] (@valueToEscape VARCHAR(250))
RETURNS VARCHAR(250)
AS BEGIN
    DECLARE @valueEscaped VARCHAR(250)
	SET @valueEscaped = (SELECT REPLACE(@valueToEscape, CHAR(13) + CHAR(10), ' '));--Return Character
	SET @valueEscaped = (SELECT REPLACE(@valueEscaped, CHAR(9), ' '));--Tab
	SET @valueEscaped = (SELECT REPLACE(@valueEscaped, CHAR(8), ' '));--Backspace: \b-> CHAR(8)
	SET @valueEscaped = (SELECT REPLACE(@valueEscaped, CHAR(10), ' '));--Line feed: char(10)
	SET @valueEscaped = (SELECT REPLACE(@valueEscaped, CHAR(12), ' '));--form feed: \f -> CHAR(12)
	SET @valueEscaped = (SELECT REPLACE(@valueEscaped, CHAR(27), ' '));--Escape: CHAR(27)??
	SET @valueEscaped = (SELECT REPLACE(@valueEscaped, '''', ''''''));	--': ''
	SET @valueEscaped = (SELECT REPLACE(@valueEscaped, CHAR(92), '\\'));--\ = CHAR(92): \\
	SET @valueEscaped = (SELECT REPLACE(@valueEscaped, '"', '\"'));--": \"
	SET @valueEscaped = (SELECT REPLACE(@valueEscaped, '%', '"%"'));--%: "%"

    RETURN @valueEscaped
END

GO
