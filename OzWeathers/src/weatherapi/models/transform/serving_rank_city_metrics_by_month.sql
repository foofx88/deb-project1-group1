DROP TABLE IF EXISTS {{ target_table }}; 

CREATE TABLE {{ target_table }} AS (
    WITH cte_weather AS (
        SELECT 
            TO_CHAR(TO_DATE(date,'YYYY-MM-DD'),'YYYY-MM') AS year_month,
            city,
            ROUND(CAST(AVG(max_temp_celcius) AS NUMERIC),2) AS avg_max_temp_celcuis_by_month,
            ROUND(CAST(AVG(min_temp_celcius) AS NUMERIC),2) AS avg_min_temp_celcuis_by_month,
            ROUND(CAST(AVG(max_wind_kph) AS NUMERIC),2) AS avg_max_wind_kph_by_month,
            ROUND(CAST(SUM(total_rain_mm) AS NUMERIC),2) AS total_rain_mm_by_month
        FROM {{ source_table }}
        GROUP BY year_month, city
    )
    SELECT 
        year_month,
        city,
        avg_max_temp_celcuis_by_month,
        RANK() OVER (PARTITION BY year_month ORDER BY avg_max_temp_celcuis_by_month DESC) AS rank_of_city_avg_max_temp_by_month,
        avg_min_temp_celcuis_by_month,
        RANK() OVER (PARTITION BY year_month ORDER BY avg_min_temp_celcuis_by_month DESC) AS rank_of_city_avg_min_temp_by_month,
        avg_max_wind_kph_by_month,
        RANK() OVER (PARTITION BY year_month ORDER BY avg_max_wind_kph_by_month DESC) AS rank_of_city_avg_max_wind_kph_by_month,
        total_rain_mm_by_month,
        RANK() OVER (PARTITION BY year_month ORDER BY total_rain_mm_by_month DESC) AS rank_of_city_total_rain_mm_by_month
    FROM cte_weather
    ORDER BY year_month ASC
);
