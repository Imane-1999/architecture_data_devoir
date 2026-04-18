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

-----------------------------------------------------------------------------------------
create table if not exists linkedin.silver.benefits as (
select 
job_id,
inferred::boolean as inferred,
type
from linkedin.bronze.benefits
);

-------------------------------------------------------------------------------------------
create table if not exists linkedin.silver.company_industries as (
select 
company_id,
upper(industry) as industry
from linkedin.bronze.company_industries
);

-------------------------------------------------------------------------------------------
create table if not exists linkedin.silver.company_specialities as (
select 
company_id,
upper(speciality) as speciality
from linkedin.bronze.company_specialities
);

---------------------------------------------------------------------------------------
create table if not exists linkedin.silver.employee_counts as (
select 
company_id,
employee_count,
follower_count,
to_timestamp(time_recorded::NUMBER, 3) as time_recorded
from linkedin.bronze.employee_counts
);

----------------------------------------------------------------------------------------
create table if not exists linkedin.silver.job_industries as (
select 
job_id,
industry_id
from linkedin.bronze.job_industries
);

----------------------------------------------------------------------------------------
create table if not exists linkedin.silver.skills as (
select 
job_id,
skill_abr
from linkedin.bronze.job_skills
);

