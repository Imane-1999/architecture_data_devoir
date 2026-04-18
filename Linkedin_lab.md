# 🧊 Projet : Analyse des Offres d'Emploi LinkedIn avec Snowflake:

Chaque jour, des milliers d’entreprises et de particuliers utilisent LinkedIn pour recruter et identifier de nouveaux talents. Dans ce projet, nous allons exploiter un jeu de données composé de plusieurs milliers d’offres d’emploi. Afin de pouvoir analyser ces données, nous commencerons par importer les fichiers aux formats CSV et JSON dans des tables au sein de la base de données Snowflake, ce qui permettra de faciliter leur manipulation et leur exploration.

## 🎯 Objectif
> Dans le cadre de ce projet, nous exploitons un jeu de données issu de LinkedIn contenant des offres d’emploi.
Nous utilisons Snowflake pour stocker, organiser et manipuler les données.
L’analyse est réalisée afin d’identifier des tendances du marché du travail.
Streamlit est utilisé pour créer une interface interactive de visualisation.
L’objectif est de transformer les données en informations claires et utiles.
## 📁 Jeu de Données
Cet atelier consiste à manipuler les données issu de LinkedIn contenant des offres d’emploi.
Les fichiers sont disponibles dans le bucket S3 public suivant : **s3://snowflake-lab-bucket/**

Voici la liste des fichiers csv que nous allons charger:  

* benefits.csv  
* companies.json 
* company_industries.json  
* company_specialities.json
* employee_counts.csv
* job_industries.json
* job_postings.csv
* job_skills.csv

## 1) Créer une base de données

```
-- Create Databse
CREATE  DATABASE IF NOT EXISTS  LINKEDIN;
```
## 2) Créer un schema de données BRONZE
```
-- Create Schema BRONZE
CREATE SCHEMA IF NOT EXISTS LINKEDIN.BRONZE;

-- définir le context
use database LINKEDIN;
use schema BRONZE;
```
## 3) Créer un stage vers les données sur aws
```
--- création du stage

CREATE OR REPLACE STAGE LINKEDIN.BRONZE.linkedin_stage
URL = 's3://snowflake-lab-bucket/';
```
## 4) Création des tables et chargement des données

**Benefits :** 

|Column |                      Description  |
|--------|-----------------------------------|  
|job_id	    |The job ID|
|inferred	|Whether the benefit was explicitly tagged or inferred through text by LinkedIn|
|type	    |Type of benefit provided (401K, Medical Insurance, etc)|

```
--- Création de la table benefits

create table if not exists linkedin.bronze.benefits(
job_id string,
inferred string, 
type string
);
-- Copy the data into table
COPY INTO linkedin.bronze.benefits
FROM @linkedin_stage/benefits.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
-- Check table
select * from benefits;
```
**Companies :**

|Column |                      Description  |
|--------|-----------------------------------|  
|company_id	    |The company ID as defined by LinkedIn|
|name	        |Company name|
|description	|Company description|
|company_size	|Company grouping based on number of employees (0 Smallest - 7 Largest)|
|state	        |State of company headquarters|
|country	    |Country of company headquarters|
|city	        |City of company headquarters|
|zip_code	    |ZIP code of company's headquarters|
|address	    |Address of company's headquarters|
|url	        |Link to company's LinkedIn page|
```
---Creation de la table "companies"

create table if not exists linkedin.bronze.companies_json(
data variant
);

COPY INTO linkedin.bronze.companies_json
FROM @linkedin_stage/companies.json
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);

create table if not exists linkedin.bronze.companies AS
SELECT
    data:company_id::string AS company_id,
    data:name::string AS name,
    data:description::string AS description,
    data:company_size::integer AS company_size,
    data:state::string AS state,
    data:country::string AS country,
    data:city::string AS city,
    data:zip_code::string AS zip_code,
    data:address::string AS address,
    data:url::string AS url
FROM linkedin.bronze.companies_json;

select * from companies;

drop table linkedin.bronze.companies_json;
```
**Company_industries :**

|Column |                      Description  |
|--------|-----------------------------------| 
|company_id	|The company ID (references companies table and primary key)|
|industry	|The industry ID |

```
--- Création de la table compnay_industries.json

create table if not exists linkedin.bronze.company_industries_json(
data variant
);

COPY INTO linkedin.bronze.company_industries_json
FROM @linkedin_stage/company_industries.json
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);

create table if not exists linkedin.bronze.company_industries AS
SELECT
    data:company_id::string AS company_id,
    data:industry::string AS industry,
FROM linkedin.bronze.company_industries_json;

select *
from company_industries;

drop table linkedin.bronze.company_industries_json;
```

**Company_specialities :**

|Column |                      Description  |
|--------|-----------------------------------| 
|company_id	|The company ID (references companies table and primary key)|
|speciality	|The speciality name|
```
--- Création de la table "company_specialities"

create table if not exists linkedin.bronze.company_specialities_json(
data variant
);

COPY INTO linkedin.bronze.company_specialities_json
FROM @linkedin_stage/company_specialities.json
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);

create table if not exists linkedin.bronze.company_specialities AS
SELECT
    data:company_id::string AS company_id,
    data:speciality::string AS speciality,
FROM linkedin.bronze.company_specialities_json;

drop table linkedin.bronze.company_specialities_json;

select *
from company_specialities;
```
**Employee_counts :**   

|Column |                      Description  |
|--------|-----------------------------------|  
|company_id	The |company ID|
|employee_count	|Number of employees at company|
|follower_count	|Number of company followers on LinkedIn|
|time_recorded	|Unix time of data collection|
```
--- Création de table employee_counts

create table if not exists linkedin.bronze.employee_counts(
company_id string,
employee_count integer,
follower_count integer,
time_recorded string 
);

COPY INTO linkedin.bronze.employee_counts
FROM @linkedin_stage/employee_counts.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');;

select *
from employee_counts;
```
**Job_Industries :**  

|Column |                      Description  |
|--------|-----------------------------------| 
|job_id	        |The job ID (references jobs table and primary key)|
|industry_id	|The industry ID |

```
-- Création de table job_industries

create table if not exists linkedin.bronze.job_industries_json(
data variant
);

COPY INTO linkedin.bronze.job_industries_json
FROM @linkedin_stage/job_industries.json
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);

create table if not exists linkedin.bronze.job_industries AS
SELECT
    data:job_id::string AS job_id,
    data:industry_id::string AS industry_id,
FROM linkedin.bronze.job_industries_json;

drop table linkedin.bronze.job_industries_json;

select *
from job_industries;
```
**Jobs_posting :** 

|Column |                      Description  |
|--------|-----------------------------------|  
|job_id                    | The job ID as defined by LinkedIn (https://www.linkedin.com/jobs/view/{job_id})|
|company_name               | nmae for the company associated with the job posting (maps to companies.csv)  |
|title	                   | Job title  |
|description	           |     Job description  |
|max_salary	               | Maximum salary  |
|med_salary	               | Medium salary  |
|min_salary	               | Minimum salary  |
|pay_period	               | Pay period for salary (Hourly, Monthly, Yearly)  |
|formatted_work_type	   |     Type of work (Fulltime, Parttime, Contract)  |
|location	               | Job location  |
|applies	               |     Number of applications that have been submitted  |
|original_listed_time	   | Original time the job was listed  |
|remote_allowed	           | Whether job permits remote work  |
|views	                   | Number of times the job posting has been viewed  |
|job_posting_url	       |     URL to the job posting on a platform  |
|application_url	       |     URL where applications can be submitted |  
|application_type	       | Type of application process (offsite, complex/simple onsite)  |
|expiry	                   | Expiration date or time for the job listing  |
|closed_time	           |     Time to close job listing  |
|formatted_experience_level | job experience level (entry, associate, executive, etc)  |
|skills_desc	           |    Description detailing required skills for job  |
|listed_time	           |   Time when the job was listed  |
|posting_domain	           | Domain of the website with application  |
|sponsored	               |Whether the job listing is sponsored or promoted  |
|work_type	               | Type of work associated with the job  |
|currency	               | Currency in which the salary is provided  |
|compensation_type	       | Type of compensation for the job  |

```
--- Création de la table job_postings

create table if not exists linkedin.bronze.job_postings(
job_id string,
company_name string,
title string,
description string,
max_salary integer,
med_salart integer,
min_salary integer,
pay_period string,
formatted_work_type string,
location string,
applies integer,
original_listed_time string,
remote_allowed string,
views integer,
job_posting_url string,
application_url string,
application_type string,
expiry string,
close_time string,
formatted_experience_level string,
skills_desc string,
listed_time string,
posting_domain string,
sponsored string,
work_type string,
currency string,
compensation_type string
);

COPY INTO linkedin.bronze.job_postings
FROM @linkedin_stage/job_postings.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');;

select *
from job_postings;

```
**Job_Skills :**

|Column |                      Description  |
|--------|-----------------------------------| 
|job_id	    |The job ID (references jobs table and primary key)|
|skill_abr	|The skill abbreviation|

```
--- Création de la table job_skills

create table if not exists linkedin.bronze.job_skills(
job_id string,
skill_abr string
);

COPY INTO linkedin.bronze.job_skills
FROM @linkedin_stage/job_skills.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');;

select *
from job_skills;

```
