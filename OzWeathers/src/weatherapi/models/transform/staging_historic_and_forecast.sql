SELECT
'historic' AS from_table,
"date",
"City_Name" AS city,
"day.maxtemp_c" AS max_temp_celcius,
"day.mintemp_c" AS min_temp_celcius,
"day.maxwind_kph" AS max_wind_kph,
"day.totalprecip_mm" AS total_precip_mm,
"day.avgvis_km" AS average_visibility_km,
"day.avghumidity" AS average_humity,
"day.condition.text" AS weather_condition,
"day.uv" AS uv_rating,
"astro.sunrise" AS sunrise,
"astro.sunset" AS sunset,
"astro.moonrise" AS moonrise,
"astro.moonset" AS moonset,
"astro.moon_phase",
"astro.moon_illumination"
FROM raw_historic

UNION ALL

SELECT
'forecast' AS from_table,
"date",
"City_Name" AS city,
"day.maxtemp_c" AS max_temp_celcius,
"day.mintemp_c" AS min_temp_celcius,
"day.maxwind_kph" AS max_wind_kph,
"day.totalprecip_mm" AS total_precip_mm,
"day.avgvis_km" AS average_visibility_km,
"day.avghumidity" AS average_humity,
"day.condition.text" AS weather_condition,
"day.uv" AS uv_rating,
"astro.sunrise" AS sunrise,
"astro.sunset" AS sunset,
"astro.moonrise" AS moonrise,
"astro.moonset" AS moonset,
"astro.moon_phase",
"astro.moon_illumination"
FROM raw_forecast

ORDER BY city, date ASC