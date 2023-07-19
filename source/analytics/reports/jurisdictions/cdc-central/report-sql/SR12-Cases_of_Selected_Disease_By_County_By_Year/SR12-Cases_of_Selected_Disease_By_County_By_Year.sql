-- Converted from Metabase Query Builder
SELECT "source"."state" AS "state", "source"."county" AS "county", "source"."Event Year" AS "Event Year", count(*) AS "count"
FROM (SELECT "dbo"."PublicHealthCaseFact"."county" AS "county", "dbo"."PublicHealthCaseFact"."event_date" AS "event_date", "dbo"."PublicHealthCaseFact"."state" AS "state", datepart(year, "dbo"."PublicHealthCaseFact"."event_date") AS "Event Year" FROM "dbo"."PublicHealthCaseFact") "source"
WHERE ("source"."county" IS NOT NULL
   AND ("source"."county" <> ''
    OR "source"."county" IS NULL) AND "source"."state" IS NOT NULL AND ("source"."state" <> '' OR "source"."state" IS NULL) AND "source"."Event Year" IS NOT NULL)
GROUP BY "source"."state", "source"."county", "source"."Event Year"
ORDER BY "source"."Event Year" DESC, "source"."state" ASC, "source"."county" ASC