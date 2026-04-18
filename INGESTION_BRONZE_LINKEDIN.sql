--- création du stage

CREATE OR REPLACE STAGE LINKEDIN.BRONZE.linkedin_stage
URL = 's3://snowflake-lab-bucket/';

--- Création de la table benefits

create table if not exists linkedin.bronze.benefits(
job_id string,
inferred string, 
type string
);

COPY INTO linkedin.bronze.benefits
FROM @linkedin_stage/benefits.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

select *
from benefits;

-----------------------------------------------------------
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

select *
from companies;

drop table linkedin.bronze.companies_json;

-------------------------------------------------------------------
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

-------------------------------------------------------------
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

--------------------------------------------------------------
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

-------------------------------------------------------------
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

--------------------------------------------------------------------
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

-----------------------------------------------------------------
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
