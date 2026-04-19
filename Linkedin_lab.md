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

select * from company_industries;

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

select * from job_industries;
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

select * from job_postings;

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

select * from job_skills;

```
## 5) Nettoyage des données SILVER
**Créer  un schema de données SILVER**
```

-- Create Schema BRONZE

CREATE SCHEMA IF NOT EXISTS LINKEDIN.SILVER;

-- définir le context

use database LINKEDIN;
use schema SILVER;
```
**Jobs_posting :** 
```
create table if not exists linkedin.silver.job_postings as (
select 
job_id,
company_name::integer as company_id,
UPPER(REGEXP_REPLACE(title, '\\([^)]*\\)|\\[[^]]*\\]|\\{[^}]*\\}',  '')) as title,
description,
max_salary,
med_salart,
min_salary,
pay_period,
formatted_work_type,
location, 
applies,
to_timestamp(original_listed_time::NUMBER, 3) as original_listed_time,
remote_allowed::float = 1 as remote_allowed,
views,
job_posting_url,
application_url,
to_timestamp(expiry::NUMBER, 3) as expiry,
to_timestamp(close_time::NUMBER, 3) as close_time,
UPPER(formatted_experience_level) as formated_experience_level,
skills_desc,
to_timestamp(listed_time::NUMBER, 3) as listed_time,
posting_domain,
sponsored::boolean as sponsored, 
work_type,
currency,
compensation_type
from linkedin.bronze.job_postings
);


```
**Companies :**
```

---------------------------------------------------------------------------------------------------
create table if not exists linkedin.silver.companies as (
select
company_id,
UPPER(name) as name,
description,
company_size,
nullif(state, '0') as state,
country,
nullif(city, '0') as city,
nullif(zip_code, '0') as zip_code,
address,
url
from linkedin.bronze.companies
);
```
**Benefits :** 
```

-----------------------------------------------------------------------------------------
create table if not exists linkedin.silver.benefits as (
select 
job_id,
inferred::boolean as inferred,
type
from linkedin.bronze.benefits
);

```
**Company_industries :**
```

-------------------------------------------------------------------------------------------
create table if not exists linkedin.silver.company_industries as (
select 
company_id,
upper(industry) as industry
from linkedin.bronze.company_industries
);

```
**Company_specialities :**
```

-------------------------------------------------------------------------------------------
create table if not exists linkedin.silver.company_specialities as (
select 
company_id,
upper(speciality) as speciality
from linkedin.bronze.company_specialities
);
```
**Employee_counts :**   
```

---------------------------------------------------------------------------------------
create table if not exists linkedin.silver.employee_counts as (
select 
company_id,
employee_count,
follower_count,
to_timestamp(time_recorded::NUMBER, 3) as time_recorded
from linkedin.bronze.employee_counts
);
```
**Job_Industries :**  
```

----------------------------------------------------------------------------------------
create table if not exists linkedin.silver.job_industries as (
select 
job_id,
industry_id
from linkedin.bronze.job_industries
);

```
**Job_Skills :**
```

----------------------------------------------------------------------------------------
create table if not exists linkedin.silver.skills as (
select 
job_id,
skill_abr
from linkedin.bronze.job_skills
);
```
## 6) Analyse des données

```

create schema if not exists linkedin.gold;

-------------------------------------------------------------------------------------
create or replace table linkedin.gold.fact_job_analysis_by_industry as (
select 
jp.title,
ci.industry,
count(jp.job_id) as total_postings,
max(jp.max_salary) as peak_salary,
avg(jp.max_salary) as avg_max_salary
from linkedin.silver.job_postings jp
left join linkedin.silver.company_industries ci
on jp.company_id = ci.company_id
group by jp.title, ci.industry
);

-------------------------------------------------------------------------------------
create table if not exists linkedin.gold.fact_postings_by_company_size as (
select 
c.company_size,
count(jp.job_id) as job_count
from linkedin.silver.job_postings jp
join linkedin.silver.companies c on jp.company_id = c.company_id
where c.company_size is not null
group by c.company_size
order by c.company_size
);

-------------------------------------------------------------------------------------
create or replace table linkedin.gold.fact_postings_by_industry as (
select 
ci.industry,
count(jp.job_id) as total_offers
from linkedin.silver.job_postings jp
join linkedin.silver.company_industries ci on jp.company_id = ci.company_id
group by ci.industry
order by total_offers desc
);

-------------------------------------------------------------------------------------
create table if not exists linkedin.gold.fact_postings_by_work_type as (
select 
formatted_work_type,
count(job_id) as job_count
from linkedin.silver.job_postings
where formatted_work_type is not null
group by formatted_work_type
order by job_count desc
);
```
## 7) Créer l'application web d'analyse de marché de l'emploi :

```
import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session

# Configuration de base de l'app
st.set_page_config(layout="wide", page_title="LinkedIn Job Insights")

st.title("Rapport LinkedIn : Marché de l'emploi")
st.markdown("Analyse interactive basée sur les données de Linkedin.")

# Récupération de la session Snowflake
session = get_active_session()

# On met les données en cache pour éviter de requêter Snowflake à chaque clic
@st.cache_data
def fetch_data(query):
    return session.sql(query).to_pandas()

# Chargement initial des vues globales
df_taille_entreprise = fetch_data("SELECT * FROM linkedin.gold.fact_postings_by_company_size")
df_secteurs = fetch_data("SELECT * FROM linkedin.gold.fact_postings_by_industry ORDER BY TOTAL_OFFERS DESC LIMIT 20")
df_type_contrat = fetch_data("SELECT * FROM linkedin.gold.fact_postings_by_work_type")

# Organisation de l'interface par onglets
onglet1, onglet2, onglet3 = st.tabs([
    "Rôles & Salaires", 
    "Tailles & Contrats", 
    "Top Secteurs"
])

with onglet1:
    st.header("Analyse des postes et rémunérations par secteur")
    
    # On récupère la liste des secteurs pour alimenter la liste déroulante
    liste_secteurs = fetch_data("SELECT DISTINCT INDUSTRY FROM linkedin.gold.fact_job_analysis_by_industry WHERE INDUSTRY IS NOT NULL ORDER BY INDUSTRY")
    
    secteur_choisi = st.selectbox("Filtrez par secteur d'activité :", liste_secteurs['INDUSTRY'])
    
    if secteur_choisi:
        secteur_safe = secteur_choisi.replace("'", "''")
        
        requete = f"""
            SELECT TITLE, TOTAL_POSTINGS, PEAK_SALARY, AVG_MAX_SALARY
            FROM linkedin.gold.fact_job_analysis_by_industry
            WHERE INDUSTRY = '{secteur_safe}'
        """
        df_filtre = fetch_data(requete)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader(f"Top 10 des postes - {secteur_choisi}")
            top_postes = df_filtre.nlargest(10, 'TOTAL_POSTINGS')
            
            graph_postes = alt.Chart(top_postes).mark_bar(color='#0077b5').encode(
                x=alt.X('TOTAL_POSTINGS:Q', title="Nombre d'offres"),
                y=alt.Y('TITLE:N', sort='-x', title='Titre du poste'),
                tooltip=['TITLE', 'TOTAL_POSTINGS']
            ).properties(height=400)
            
            st.altair_chart(graph_postes, use_container_width=True)

        with col2:
            st.subheader(f"Top 10 des salaires - {secteur_choisi}")
            # On exclut les postes sans salaire renseignée avant de trier
            df_salaires = df_filtre.dropna(subset=['PEAK_SALARY'])
            top_salaires = df_salaires.nlargest(10, 'PEAK_SALARY')
            
            graph_salaires = alt.Chart(top_salaires).mark_bar(color='#00a0dc').encode(
                x=alt.X('PEAK_SALARY:Q', title='Salaire Max (USD)'),
                y=alt.Y('TITLE:N', sort='-x', title='Titre du poste'),
                tooltip=['TITLE', 'PEAK_SALARY', 'TOTAL_POSTINGS']
            ).properties(height=400)
            
            st.altair_chart(graph_salaires, use_container_width=True)

with onglet2:
    col1, col2 = st.columns(2)
    
    with col1:
        st.header("Offres par taille d'entreprise")
        st.markdown("*(0 = TPE, 7 = Multinationale)*")
        
        graph_taille = alt.Chart(df_taille_entreprise).mark_bar(color='#313335').encode(
            x=alt.X('COMPANY_SIZE:O', title='Catégorie de taille'),
            y=alt.Y('JOB_COUNT:Q', title="Volume d'offres"),
            tooltip=['COMPANY_SIZE', 'JOB_COUNT']
        ).properties(height=400)
        
        st.altair_chart(graph_taille, use_container_width=True)
        
    with col2:
        st.header("Offres par type de contrat")
        st.markdown("*(Temps plein, stage, freelance...)*")
        
        graph_contrats = alt.Chart(df_type_contrat).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field="JOB_COUNT", type="quantitative"),
            color=alt.Color(field="FORMATTED_WORK_TYPE", type="nominal", title="Type de contrat"),
            tooltip=['FORMATTED_WORK_TYPE', 'JOB_COUNT']
        ).properties(height=400)
        
        st.altair_chart(graph_contrats, use_container_width=True)

with onglet3:
    st.header("Secteurs les plus dynamiques")
    st.markdown("Top 20 des industries qui publient le plus d'offres.")
    
    # On limite aux 20 premières pour garder un graphique lisible et repondre à la demande d'analyse
    graph_ind = alt.Chart(df_secteurs.head(20)).mark_bar(color='#86B817').encode(
        x=alt.X('INDUSTRY:N', sort='-y', title="Secteur d'activité", axis=alt.Axis(labelAngle=-45)),
        y=alt.Y('TOTAL_OFFERS:Q', title="Nombre total d'offres"),
        tooltip=['INDUSTRY', 'TOTAL_OFFERS']
    ).properties(height=500)
    
    st.altair_chart(graph_ind, use_container_width=True)
```
