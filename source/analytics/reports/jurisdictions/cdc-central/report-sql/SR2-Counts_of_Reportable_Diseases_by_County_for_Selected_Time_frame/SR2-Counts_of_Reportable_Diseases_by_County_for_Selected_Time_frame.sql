-- Converted from Metabase Query Builder
-- PHC_code_desc is the Disease column
SELECT "dbo"."PublicHealthCaseFact"."PHC_code_desc" AS "PHC_code_desc", "dbo"."PublicHealthCaseFact"."county" AS "county", count(*) AS "count"
FROM "dbo"."PublicHealthCaseFact"
WHERE ("dbo"."PublicHealthCaseFact"."county" IS NOT NULL
   AND ("dbo"."PublicHealthCaseFact"."county" <> ''
    OR "dbo"."PublicHealthCaseFact"."county" IS NULL))
GROUP BY "dbo"."PublicHealthCaseFact"."PHC_code_desc", "dbo"."PublicHealthCaseFact"."county"
ORDER BY "dbo"."PublicHealthCaseFact"."PHC_code_desc" ASC, "dbo"."PublicHealthCaseFact"."county" ASC