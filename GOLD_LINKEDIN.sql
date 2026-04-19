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