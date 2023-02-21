DROP TABLE IF EXISTS sttgaz.dds_business_days_calendar;
CREATE TABLE sttgaz.dds_business_days_calendar AS
WITH 
	sq1 AS(
		SELECT "Год/Месяц", 'Январь' AS "Месяц (название)", 1 AS "Месяц (номер)", "Январь" AS "День" 
		FROM sttgaz.stage_calendar
		UNION
		SELECT "Год/Месяц", 'Февраль', 2,  "Февраль" 
		FROM sttgaz.stage_calendar
		UNION
		SELECT "Год/Месяц", 'Март', 3, "Март" 
		FROM sttgaz.stage_calendar
		UNION
		SELECT "Год/Месяц", 'Апрель', 4, "Апрель" 
		FROM sttgaz.stage_calendar
		UNION
		SELECT "Год/Месяц", 'Май', 5, "Май" 
		FROM sttgaz.stage_calendar
		UNION
		SELECT "Год/Месяц", 'Июнь', 6, "Июнь" 
		FROM sttgaz.stage_calendar
		UNION
		SELECT "Год/Месяц", 'Июль', 7, "Июль" 
		FROM sttgaz.stage_calendar
		UNION
		SELECT "Год/Месяц", 'Август', 8, "Август" 
		FROM sttgaz.stage_calendar
		UNION
		SELECT "Год/Месяц", 'Сентябрь', 9, "Сентябрь" 
		FROM sttgaz.stage_calendar
		UNION
		SELECT "Год/Месяц", 'Октябрь', 10, "Октябрь" 
		FROM sttgaz.stage_calendar
		UNION
		SELECT "Год/Месяц", 'Ноябрь', 11, "Ноябрь" 
		FROM sttgaz.stage_calendar
		UNION
		SELECT "Год/Месяц", 'Декабрь', 12, "Декабрь" 
		FROM sttgaz.stage_calendar
	),
	sq2 AS(
		SELECT 
			EXPLODE(
				"Год/Месяц",
				"Месяц (название)",
				"Месяц (номер)",
				STRING_TO_ARRAY(REGEXP_REPLACE(Replace("День", '+', ''), '[0-9]+\*', ''))::ARRAY[INT]
			) OVER(PARTITION BEST) AS ("Год", "Месяц (название)", "Месяц (номер)", position, "День")
		FROM sq1
		ORDER BY "Год", "День"	
	),
	sq3 AS(
		SELECT
			("Год"::varchar || '-' || "Месяц (номер)"::varchar || '-' || "День"::varchar)::date AS "Дата",
			"Год",
			"Месяц (название)",
			"Месяц (номер)",
			"День",
			'Выходной' AS "Примечание"
		FROM sq2
		WHERE "День" IS NOT NULL
	),
	sq4 AS(
        SELECT DISTINCT ts::date AS "Дата"
        FROM (
        	SELECT '2022-01-01 00:00:00'::TIMESTAMP as tm 
        	UNION ALL 
        	SELECT LAST_DAY(
        		(EXTRACT(YEAR FROM NOW())::VARCHAR ||'-'||'12'||'-'||'1')::date
        	)
        ) as t
        TIMESERIES ts as '1 DAY' OVER (ORDER BY t.tm)	
	)
SELECT
	sq4."Дата",
	COALESCE("Год"::numeric, EXTRACT(YEAR FROM sq4."Дата")) AS "Год",
	COALESCE("Месяц (номер)"::numeric, EXTRACT(MONTH FROM sq4."Дата")) AS "Месяц (номер)",
	COALESCE("День"::numeric, EXTRACT(DAY FROM sq4."Дата")) AS "День",
	CASE
		WHEN "Месяц (номер)" = 1 AND ("День" = 1 OR "День" = 7) THEN 'Праздник'
		WHEN "Месяц (номер)" = 2 AND "День" = 23 THEN 'Праздник'
		WHEN "Месяц (номер)" = 3 AND "День" = 8 THEN 'Праздник'
		WHEN "Месяц (номер)" = 5 AND ("День" = 1 OR "День" = 9) THEN 'Праздник'
		WHEN "Месяц (номер)" = 6 AND "День" = 12 THEN 'Праздник'
		WHEN "Месяц (номер)" = 11 AND "День" = 4 THEN 'Праздник'
		ELSE "Примечание"
	END "Примечание"
FROM sq4
LEFT JOIN sq3
	USING("Дата");