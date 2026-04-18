create schema if not exists linkedin.gold;

-------------------------------------------------------------------------------------
create table if not exists linkedin.gold.fact_job_analysis_by_industry as (
select 
jp.title,
ji.industry_id,
count(jp.job_id) as total_postings,
max(jp.max_salary) as peak_salary,
avg(jp.max_salary) as avg_max_salary
from linkedin.silver.job_postings jp
join linkedin.silver.job_industries ji on jp.job_id = ji.job_id
group by jp.title, ji.industry_id
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
create table if not exists linkedin.gold.fact_postings_by_industry as (
select 
ji.industry_id,
count(jp.job_id) as total_offers
from linkedin.silver.job_postings jp
join linkedin.silver.job_industries ji on jp.job_id = ji.job_id
group by ji.industry_id
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