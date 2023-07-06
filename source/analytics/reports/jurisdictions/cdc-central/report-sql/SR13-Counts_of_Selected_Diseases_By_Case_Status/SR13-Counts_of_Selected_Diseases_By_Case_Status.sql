-- Converted from Metabase Query Builder
SELECT "source"."PHC_code_desc" AS "PHC_code_desc", "source"."case_class_desc_txt" AS "case_class_desc_txt", count(*) AS "count"
FROM (SELECT "dbo"."PublicHealthCaseFact"."event_date" AS "event_date", "dbo"."PublicHealthCaseFact"."PHC_code_desc" AS "PHC_code_desc", "dbo"."PublicHealthCaseFact"."case_class_desc_txt" AS "case_class_desc_txt" FROM "dbo"."PublicHealthCaseFact"
WHERE ("dbo"."PublicHealthCaseFact"."case_class_desc_txt" IS NOT NULL
   AND ("dbo"."PublicHealthCaseFact"."case_class_desc_txt" <> ''
    OR "dbo"."PublicHealthCaseFact"."case_class_desc_txt" IS NULL))) "source"
GROUP BY "source"."PHC_code_desc", "source"."case_class_desc_txt"
ORDER BY "source"."PHC_code_desc" ASC, "source"."case_class_desc_txt" ASC