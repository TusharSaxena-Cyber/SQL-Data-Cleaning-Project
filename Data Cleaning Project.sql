-- Data Cleaning Project

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns

-- Removing Duplicates

Create table layoffs_staging
like layoffs;

select *
from layoffs_staging;


insert layoffs_staging
select *
from layoffs;

select *,
row_number() over(
partition by Company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;


with duplicate_cte as
(
select *,
row_number() over(
partition by Company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions
) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;


select *
from layoffs_staging
where company = 'Casper';


with duplicate_cte as
(
select *,
row_number() over(
partition by Company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions
) as row_num
from layoffs_staging
)
Delete 
from duplicate_cte
where row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(
partition by Company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions
) as row_num
from layoffs_staging;


select *
from layoffs_staging2;


-- Standardizing Data


select company, TRIM(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%';

select distinct industry
from layoffs_staging2;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2;

-- Null and Blank Values

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set industry = null
where industry = '';


select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company = 'airbnb';

select T1.industry, T2.industry
from layoffs_staging2 T1
join layoffs_staging2 T2
	on T1.company = T2.company
    and T1.location = T2.location
where (T1.industry is null or T1.industry = '')
and T2.industry is not null;

update layoffs_staging2 T1
join layoffs_staging2 T2
	on T1.company = T2.company
set T1.industry = T2.industry
where (T1.industry is null or T1.industry = '')
and T2.industry is not null;

select industry, company
from layoffs_staging2
where industry is null;


select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

-- Removing extra columns

alter table layoffs_staging2
drop column row_num;

select *
from layoffs_staging2;

-- ============================================================
-- 📈 Final Insights & Project Reflection
-- ============================================================
-- This project demonstrates a complete end-to-end SQL data cleaning process
-- on a global layoffs dataset. Through the use of staging tables, CTEs, and
-- window functions, duplicates were removed, data inconsistencies corrected,
-- and missing values handled systematically.

-- The cleaned dataset is now standardized, consistent, and ready for analysis
-- or visualization in tools like Tableau or Power BI. It can be used to explore
-- key questions such as:
--   • Which industries or regions were most affected by layoffs?
--   • How do funding levels relate to workforce reductions?
--   • What trends appear over time across different sectors?

-- Reflection:
-- This project reinforced the importance of thorough data preparation in analytics.
-- Even the most advanced visualizations or models rely on well-structured, accurate data.
-- By transforming raw data into a clean and reliable format, we enable meaningful insights
-- and more confident decision-making.
-- ============================================================
