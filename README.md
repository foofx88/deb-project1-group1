<a id="top"></a>

<h1 align="left"> OzWeathers </h1>
<h3 align="left"> A Comprehensive Dataset to forecast various activities in most populated cities in Australia</h3>

</br>

<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary><b>Table of Contents</b></summary>
  <ol>
    <li><a href="#about-the-project"> About The Project</a></li>
    <li><a href="#setups">Setups</a></li>
    <li><a href="#folder-structure">Folder Structure</a></li>
    <li><a href="#theprocess">How it's done</a></li>
      <ul>
        <li><a href="#sad">Solution Architecture Diagram</a></li>
        <li><a href="#vat">View All Tables</a></li>
        <li><a href="#extract">Extract</a></li>
        <li><a href="#transform">Transform</a></li>
        <li><a href="#load">Load</a></li>
      </ul>
    </li>
    <li><a href="#discussion">Discussion, Lesson Learnt and Future Improvement</a></li>
    <li><a href="#contributors">Contributors & Contributions</a></li>
  </ol>
</details>

<hr style="background:#3944BC;">

<!-- ABOUT THE PROJECT -->

<h2 id="about-the-project">About The Project</h2>

<p align="justify"> 
  This project aims to create a useful weather dataset for the top populated cities in Australia. We all know that weather can be unpredictable especially when planning for any outdoor activities. Our group created a weather dataset from <a href="https://www.weatherapi.com/">WeatherApi</a> with reference of the top populated cities from <a href="https://countriesnow.space/">CountriesNow</a>. This dataset would support any data analyst on presenting current weather data with it's suitable activities and past forecast data would be used by ML Data Scientist for future activities/weather forecasting.
</p>

<p align="center">
  <img src="https://www.australiaday.com.au/assets/sampleimages/page/submit-your-event/An-Australian-flag-flying-on-a-sunny-day.jpg" alt="The Great Australian Outdoors" width="60%" height="60%">        
  <!--figcaption>Caption goes here</figcaption-->
</p>

<hr style="background:#3944BC;">
<!-- WHAT WE USE -->
<h2 id="setups">Setups</h2>

This project is built with Python version 3.9.

The following packages and tools are used in this project:

- Jupyter Notebook
- Pandas
- Requests
- SQLAlchemy
- Postgres
- Jinja
- Docker
- AWS Services (S3, RDS, EC2)

Requirement list can be found in the project folder within <a href="#folder-structure">src</a>.

<u>To run the pipeline locally:</u>
Conda Activate "your venv"

pip install -r requirement.txt

Obtain API key from <a href="https://www.weatherapi.com/">WeatherApi</a>

Setup Postgres, AWS account (RDS, S3) and add in appropriate credentials in config.bat

Run set_python_path.bat

Run main weatherapi_pipeline.py

<hr style="background:#3944BC;">

<!-- FOLDER STRUCTURE -->
<h2 id="folder-structure">Folder Structure</h2>

    code
    .
    │
    ├── OzWeather
    │   ├── src
    │   │   ├── database
    │   │   │   ├── __init__.py
    │   │   │   └── postgres.py
    │   │   ├── utility
    │   │   |   ├── __init__.py
    │   │   |   └── metadata_logging.py
    │   │   └── weatherapi
    │   │       ├── etl
    |   |       |   ├── __init__.py
    |   |       |   ├── extract.py
    |   |       |   ├── load.py
    |   |       |   └── transform.py
    │   │       ├── models
    |   |       |   └── transform
    |   |       |         ├── staging_forecast.sql
    |   |       |         ├── staging_historic.sql
    |   |       |         ├── staging_historic_and_forecast.sql
    |   |       |         ├── serving_activities.sql
    |   |       |         ├── serving_rank_city_metrics_by_day.sql
    |   |       |         ├── serving_rank_city_metrics_by_month.sql
    |   |       |         ├── serving_weather_condition_count_by_month.sql
    |   |       |         └── serving_weather_condition_count_by_years.sql
    |   |       |         
    │   │       ├── pipeline
    |   |       |   ├── __init__.py
    |   |       |   └── weatherapi_pipeline.py
    │   │       ├── test
    |   |       |   ├── __init__.py            
    |   |       |   └── test_cities_api.py
    |   |       |
    |   |       ├── __init__.py
    │   |       └── config.yaml
    │   │
    │   ├── requirements.txt
    │   └── set_python_path.bat
    │
    │
    ├── Snippets
    ├── Dockerfile
    └─── README.md


<hr style="background:#3944BC;">
<!-- THE WHOLE PROCESS OF THE PROJECT -->
<h2 id="theprocess">How it's Done</h2>
<p> 
We built an ELTL pipeline with an Extract and Load Class without the @static method.<br>
<img src="https://github.com/foofx88/deb-project1-group1/blob/main/snips/without_static_method.PNG" alt="No static method was used" width="100%"> <br>
We did not utilise a Extract_load_pipeline but we have incorporate that into our main pipeline.
In other words, there are only dag nodes for the load and transform classes. <br>
<img src="https://github.com/foofx88/deb-project1-group1/blob/main/snips/dag(extract).PNG" alt="DAG Extract" width="100%"> <br>
<img src="https://github.com/foofx88/deb-project1-group1/blob/main/snips/dag(load_transform).PNG" alt="DAG Load and Transform" width="100%"> <br>

<hr style="background:#3944BC;">

<h3 id="sad">Solution Architecture Diagram</h3>
<img src="https://github.com/foofx88/deb-project1-group1/blob/main/snips/SAD_Ozweathers.png" alt="Solution Architecture Diagram" width="100%"> 

<hr style="background:#3944BC;">

<h3 id="vat">View all tables</h3>
<img src="https://github.com/foofx88/deb-project1-group1/blob/main/snips/alltables.png" alt="View All Tables" width="100%"> 
</p>

<hr style="background:#3944BC;">

<h2 id="extract">Extract</h2>
<p align="justify"> 
We've extracted data from 2 APIs - <a href="https://www.weatherapi.com/">WeatherApi</a> and <a href="https://countriesnow.space/">CountriesNow</a><br>
The <i>extract_cities</i> funciton gets the top 10 most populated cities in Australia. In order for the WeatherAPI to work seamlessly, We have excluded any cities with a space or symbols in their names. This table then acts as a reference table for the <i>extract_weather_forecast</i> and <i>extract_weather_historic</i> function.<br>
<i>extract_weather_forecast</i> function extracts 7 days worth of forecasting data for each cities whereas <i>extract_weather_historic</i> function gets weather data all the way back from a year ago for all the cities. Incremental Extract is performed on the historic data with the reference of an incremental value. The incremental value is stored in a CSV file "historic_log.csv" on S3 and the value referenced from the last recorded date from the extraction. For every run, the function refers back to the log file and starts the extraction process a day after the incremental value date. The justification for this is due to the fact that the data already exists for the log date and we would want to get data for the next day. We've utilized a <i>while</i> loop to stop the historic data extraction if it reaches current date, any data that is today and onwards would be covered by the forecast data. The <i>extract_from_api</i> acts as the main function which bundles everything together and gets used in the main pipeline.
<br>

<b><i>"We will get today's historic data tomorrow when it becomes yesterday's. - FX</i></b>  

<hr style="background:#3944BC;">

<h2 id="load">Load</h2>

<p align="justify"> 
Extracted data are loaded into AWS RDS PostgresSQL database with the help of SQLAlchemy.
We have also upserting extracted raw historic data in chunks. As the data is extracted daily, the cities and forecast table would be overwritten to always get the most updated data whereas the raw_historic is an upsert.
</p>

<hr style="background:#3944BC;">

<h2 id="transform">Transform</h2>
<p align="justify"> 
Transformations are executed with the help of SQL models with Jinja templating after the load. For the <i>staging_forecast</i> and <i>staging_historic</i> table, we've only selected the columns that are relevant and renamed them accordingly. We then join them together to form the <i>staging_historic_and_forecast</i> table with a UNION ALL, this way, we have cities weather data for all the dates. The <i>staging_historic_and_forecast</i> table is then used for all the other 5 serving tables:

<i>serving_activities</i>
Using weather data to create an activities table. Using multiple CASE statements to determine if an activity is suitable for a particular date in its corresponding city. Activities includes werewolf, picnics, indoor activity, sailing, being happy and moonbathing.

<i>serving_rank_city_metrics_by_day</i>
Using weather data to get ranking of each cities according to their highest and lowest temperature, wind speeds and total amount of rain. We utilised Window Function with RANK() for this.

<i>serving_rank_city_metrics_by_month</i>
To get the ranking by Month. We've first created a CTE to calculate the average of each required columns (max_temp_celcius, min_temp_celcius, max_wind_kph, total_rain_mm) and then grouped them by year_month and city. This is then RANK() with Window function, using the year_month as partition.

<i>serving_weather_condition_count_by_month</i>
For this table, we wanted to get the frequency of weather condition by the month. From the weather data, we COUNT() each occurances and classify them into main categories - sunny_day, rainy_day, cloudy_day, thunder_day. These are then grouped by year_month and city, ordering by year_month

<i>serving_weather_condition_count_by_years</i>
Similar transformation is done to get the weather condition by the year with one significant difference where the aggregated data is grouped by year instead of year_month and is ordered by year. 

<b>For all the tables, we have used the following techniques to transform the data:</b>
<ul>
<li>Renaming - rename column headers from raw to staging tables</li>
<li>Data type casting - date string to date, columns data to numeric and int</li>
<li>Unions - Union of raw_historic & raw_forecast</li>
<li>Aggregation function - avg() temperature, rank() for metrics, count() frequencies of weather condition, sum() total amount of rain</li>
<li>Window function - partition by year_month</li>
<li>Calculation - duration of moonbathing time</li>
<li>Filtering - where claused used when creating list for CTE on days that are not sunny.</li>
<li>Grouping - group by year, year_month, city</li>
<li>Sorting - Order By</li>
</ul>

</p>

<hr style="background:#3944BC;">
<!-- RESULTS AND DISCUSSION -->
<h2 id="discussion">Discussion, Lesson Learnt and Future Improvement</h2>

<p align="justify">

Current iteration docker image is built and pushed onto ECR and task instance created to run the pipeline on EC2. <br>
<b>What have we learned in this project and what can be done better:</b> 
<ul> 
  <li>Never underestimate how long it will take to troubleshoot an error. And when it comes to testing and troubleshooting, print() helps!</li>
  <li>Always incorporate good error catching <br>
  <img src="https://github.com/foofx88/deb-project1-group1/blob/main/snips/error_catching.PNG" alt="Error Catching" width="60%" height="60%">  </li>
  <li>We need to incorporate S3 List object on the get_incremental_value function as Boto3 doesnt provide a response if the key is not present/not the right name.</li>
  <li>The current uses boto3 client to perform function which requires user credentials. Can possibly be using read with the right S3 policy. </li>
  <li>Incremental value currently written during the data extract, however, if the process fails, the value will still be written to the logs. Ideally, this should happen after the upsert to database. </li>
  <li>When a table is using the overwrite methods, there may be a timing issue where the table is not dropped fast enough before the new creation.</li>
  <li>Figure out Metadatalogging, why it stopped after we've uploaded onto EC2. Identify a way to incorporate local timezone to the logs</li>
  <li>Noticed an issue when reading from RDS, even when the table exists and we know that there are data, querying from PgAdmin will not return any results initially, then in the span of a minute, the table columns exists and so are the data <br>
  <img src="https://github.com/foofx88/deb-project1-group1/blob/main/snips/database_issue.gif" alt="Phantom Table" width="60%" height="60%">  </li>
</ul>

<b>What have we learned while using Docker:</b> 
</ul>
    To execute the following Docker commands to avoid having multiple containers:
    <ul>
    <li>Build docker image with no cache: <i>docker build --no-cache . -t IMAGE_NAME</i></li>
    <li>Stop the existing docker container: <i>docker stop CONTAINER_NAME</i></li>
    <li>Remove the existing docker container: <i>docker rm CONTAINER_NAME</i></li>
    <li>Start docker container by name: <i>docker run --restart=unless-stopped --name CONTAINER_NAME IMAGE_NAME</i></li>
    </ul>

</ul>
    To execute the following Docker commands when tagging and pushing to ECR:
    <ul>
    <li>Build image for ECR: <i>docker tag LOCAL_IMAGE_NAME : LOCAL_IMAGE_TAG AWS_ECR_URI : LOCAL_IMAGE_TAG</i></li>
    <li>Push image to ECR: <i>docker push AWS_ECR_URI : LOCAL_IMAGE_TAG</i></li>
</ul>

</p>

<hr style="background:#3944BC;">
<!-- CONTRIBUTORS -->
<h2 id="contributors">Contributors & Contributions</h2>

<p>
  <i>All participants in this project are professional individuals enrolled in <a href="https://www.dataengineercamp.com">Data Engineering Camp</a> </i> <br> <br>
  
<table>
<tr>
<th>Name</th>
<th>GitHub</th>
<th>Contributions</th>
</tr>
<tr>
 <td> <b>Luke Huntley</b></td>
<td><a href="https://github.com/LuckyLukeAtGitHub">LuckyLukeAtGitHub</a></td>
<td>Transform(ETL), Load(ETL), MainPipeline, Testing, Docker Image Build, AWS resource creation (IAM, RDS)</td>
</tr>

<tr>
 <td> <b>Fang Xuan Foo</b></td>
<td><a href="https://github.com/foofx88">foofx88</a></td>
<td>Extract(ETL), Transform(ETL), AWS - S3, ECR, EC2, Troubleshoot, Pull request review, Project Documentation</td>
</tr>

<tr>
 <td> <b>Muhammad Mursalin</b> </td>
  <td><a href="https://github.com/doctormachine">doctormachine</a> </td>
  <td>Initial Pull request review</td>
 </tr>

<tr>
  <td><b>Rohith Korupalli</b></td>
<td>  <a href="https://github.com/korupalli">korupalli</a></td>
<td>Initial Weather API Extract</td>
</tr>
</table>
</p>

<i>Team members partook on testing their own individual parts, cross check and supplied content for project documentation. </i>
<i>This was the First project for the ETL part of the course in the <a href="https://www.dataengineercamp.com">Data Engineering Camp</a>.</i> <br>
<i style="font-size:10px;">No werewolves were harmed in the process of getting this dataset.</i>


<a href="#top"> Go back up🔼</a>
