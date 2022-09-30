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
        <li><a href="#extract">Extract</a></li>
        <li><a href="#transform">Transform</a></li>
        <li><a href="#load">Load</a></li>
      </ul>
    </li>
    <li><a href="#discussion">Results and Discussion</a></li>
    <li><a href="#contributors">Contributors</a></li>
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
- AWS Services (S3, RDS, EC2)

Requirement list can be found in the project folder within <a href="#folder-structure">src</a>.

pip install -r requirement.txt

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
    │   │   |   ├── incremental_logging.py
    │   │   |   └── metadata_logging.py
    │   │   └── weatherapi
    │   │       ├── etl
    |   |       |   ├── __init__.py
    |   |       |   ├── extract_cities.py
    |   |       |   ├── extract.py
    |   |       |   ├── load.py
    |   |       |   └── transform.py
    │   │       ├── models
    |   |       |   └── extract
    │   │       ├── pipeline
    |   |       |   ├── __init__.py
    |   |       |   ├── extract_load_pipeline.py
    |   |       |   └── weatherapi_pipeline.py
    │   │       ├── test
    |   |       |    └── test_extract_load.py
    |   |       ├── __init__.py
    │   |       └── config.yaml
    │   │
    │   ├── requirements.txt
    │   └── set_python_path.bat
    │
    └─── README.md

<hr style="background:#3944BC;">
<!-- THE WHOLE PROCESS OF THE PROJECT -->
<h2 id="theprocess">How it's Done</h2>
<p> 
  Describe the Dataset here

</p>

<hr style="background:#3944BC;">

<h2 id="extract">Extract</h2>

<p align="justify"> 
Extract Process here<br>

<hr style="background:#3944BC;">

<h2 id="transform">Transform</h2>

<p align="justify"> 
Transform Process here
</p>
<hr style="background:#3944BC;">

<h2 id="load">Load</h2>

<p align="justify"> 
Load Process here
</p>

<hr style="background:#3944BC;">
<!-- RESULTS AND DISCUSSION -->
<h2 id="discussion">Discussion, Lesson Learnt and Future Improvement</h2>

<p align="justify">
<ul>
  <li>
    What we have done for this iteration <br>
  </li>
  <li>
    What have we learned in this project
  </li>
  <li>
    Any Suggested improvement for future iteration
  </li>
  <li>
  Insert something else here or remove
  </li>
</ul>
</p>

<hr style="background:#3944BC;">
<!-- CONTRIBUTORS -->
<h2 id="contributors">Contributors and Contributions</h2>

<p>
  <i>All participants in this project are professional individuals enrolled in <a href="https://www.dataengineercamp.com">Data Engineering Camp</a> </i> <br> <br>
  
<table>
<tr>
<th>Name</th>
<th>GitHub</th>
<th>Contributions</th>
</tr>

<tr>
 <td> <b>Muhammad Mursalin</b> </td>
  <td><a href="https://github.com/doctormachine">doctormachine</a> </td>
  <td>Pull request review, Load(ETL), Docker Image Build and Troubleshooting</td>
 </tr>

<tr>
  <td><b>Rohith Korupalli</b></td>
<td>  <a href="https://github.com/korupalli">korupalli</a></td>
<td>Weather API Extract, Load(ETL), MetaData Logging</td>
</tr>
<tr>
 <td> <b>Luke Huntley</b></td>
<td><a href="https://github.com/LuckyLukeAtGitHub">LuckyLukeAtGitHub</a></td>
<td>Transform(ETL), Docker Image Build and Troubleshooting, AWS resource creation (IAM, RDS)</td>
</tr>
<tr>
 <td> <b>Fang Xuan Foo</b></td>
<td><a href="https://github.com/foofx88">foofx88</a></td>
<td>Cities API Extract, Transform(ETL), MetaData Logging, Project Documentation</td>
</tr>

</table>
</p>

<i>All team members partook on testing their own individual parts, cross check and supplied content for project documentation. </i>
<i>This was the First project for the ETL part of the course in the <a href="https://www.dataengineercamp.com">Data Engineering Camp</a>.</i> <br>
<i style="font-size:10px;">No werewolves were harmed in the process of getting this dataset.</i>

<a href="#top"><p style="text-align:right">Go back up🔼<p></a>
