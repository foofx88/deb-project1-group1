drop table if exists {{target_table}};

create table {{target_table}} as (

--Creating a List using CTE for days that are not Sunny
WITH ideal_indoor as (SELECT DISTINCT(upper(weather_condition)) as indoor_time
	FROM staging_historic_and_forecast
	WHERE NOT trim(upper(weather_condition))=trim(upper('Sunny')))

SELECT
cast (date as date) as date, --casting date string to date
city,
weather_condition,
CASE WHEN trim(upper(moon_phase))=trim(upper('Full Moon')) THEN
	CASE WHEN trim(upper(weather_condition))=trim(upper('Sunny')) THEN 'yes'
	ELSE 'no' END
ELSE 'no'
END AS werewolf,
CASE WHEN trim(upper(weather_condition))=trim(upper('Sunny')) OR trim(upper(weather_condition))=trim(upper('Partly Cloudy')) THEN
	CASE WHEN min_temp_celcius >= 10.0 AND max_temp_celcius <= 30.0 THEN 'yes'
	ELSE 'no' END
	WHEN trim(upper(weather_condition))=trim(upper('Overcast')) OR trim(upper(weather_condition))= trim(upper('Cloudy')) THEN
		CASE WHEN min_temp_celcius >= 10.0 AND max_temp_celcius <= 30.0 THEN 'maybe'
		ELSE 'no' END
	ELSE 'no'
END AS picnics,
CASE WHEN upper(weather_condition) IN (SELECT * FROM ideal_indoor) THEN 'yes'
ELSE 'no'
END AS indoor_activity,
CASE WHEN max_wind_kph BETWEEN 9.0 AND 23.0 THEN 'yes'
ELSE 'no'
END AS sailing,
CASE WHEN max_temp_celcius <= 25.0 THEN 
	CASE WHEN average_humity BETWEEN 30.0 AND 60.0 THEN 'yes'
	ELSE 'no'
	END
ELSE 'no'
END AS happy,
CASE WHEN CAST(moon_illumination AS INT) >= 80 THEN 'yes'
ELSE 'no'
END AS moonbathing,
(CAST (sunset AS TIME(0)) - CAST (sunrise AS TIME(0))) as day_duration


FROM {{source_table}} --can also use just forecasting data to get all the above activities

);