DROP TABLE IF EXISTS {{ target_table }}; 

CREATE TABLE {{ target_table }} AS (
    SELECT 
        date AS day,
        city,
        max_temp_celcius,
        RANK() OVER (PARTITION BY date ORDER BY max_temp_celcius DESC) AS rank_of_city_max_temp_by_day,
        min_temp_celcius,
        RANK() OVER (PARTITION BY date ORDER BY min_temp_celcius DESC) AS rank_of_city_min_temp_by_day,
        max_wind_kph,
        RANK() OVER (PARTITION BY date ORDER BY max_wind_kph DESC) AS rank_of_city_max_wind_kph_by_day,
        total_rain_mm,
        RANK() OVER (PARTITION BY date ORDER BY total_rain_mm DESC) AS rank_of_city_total_rain_mm_by_day
    FROM {{ source_table }}
    ORDER BY day ASC
);