DROP TABLE IF EXISTS {{ target_table }}; 

CREATE TABLE {{ target_table }} AS (
    SELECT 
		TO_CHAR(TO_DATE(date,'YYYY-MM-DD'),'YYYY-MM') AS year_month,
		city,
		COUNT(CASE WHEN TRIM(UPPER(weather_condition)) = TRIM(UPPER('sunny')) THEN 1 END) AS sunny_day_count_by_month,
		COUNT(CASE WHEN
			  TRIM(UPPER(weather_condition)) = TRIM(UPPER('light drizzle')) OR
			  TRIM(UPPER(weather_condition)) = TRIM(UPPER('patchy rain possible')) OR
			  TRIM(UPPER(weather_condition)) = TRIM(UPPER('light rain')) OR
			  TRIM(UPPER(weather_condition)) = TRIM(UPPER('light rain shower')) OR  
			  TRIM(UPPER(weather_condition)) = TRIM(UPPER('moderate rain')) OR 
			  TRIM(UPPER(weather_condition)) = TRIM(UPPER('moderate or heavy rain shower')) OR
			  TRIM(UPPER(weather_condition)) = TRIM(UPPER('heavy rain')) 
			THEN 1 END) AS rain_day_count_by_month,
		COUNT(CASE WHEN
			  TRIM(UPPER(weather_condition)) = TRIM(UPPER('overcast')) OR
			  TRIM(UPPER(weather_condition)) = TRIM(UPPER('cloudy')) OR
			  TRIM(UPPER(weather_condition)) = TRIM(UPPER('partly cloudy'))
			THEN 1 END) AS cloudy_day_count_by_month,
		COUNT(CASE WHEN
			  TRIM(UPPER(weather_condition)) LIKE TRIM(UPPER('%thunder%'))
			THEN 1 END) AS thunder_day_count_by_month
	FROM {{ source_table }}
	GROUP BY year_month, city
	ORDER BY year_month
);