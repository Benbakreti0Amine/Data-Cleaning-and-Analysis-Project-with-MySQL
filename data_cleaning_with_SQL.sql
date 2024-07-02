use world_layoff;
select *  from layoffs;

-- 1.remove duplicates
-- 2.standarize data (spellings mistakes...)
-- 3.null and blanks (we can remove them or leave them)
-- 4.remove any columns (maybe irrelevant)

create table layoffs_staging like layoffs; -- we don't work with raw (original) database in case anything happens

select * from layoffs_staging;
insert into layoffs_staging
select * from layoffs;

-- 1.remove duplicates
with duplicate_cte as (
select *,row_number() OVER(partition by company,location,industry,total_laid_off,
percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging
) 
select * 
from duplicate_cte
where row_num > 1;

select * from layoffs_staging;

CREATE TABLE `layoffs_staging_2` (
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

insert into layoffs_staging_2
select *,row_number() OVER(partition by company,location,industry,total_laid_off,
percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

DELETE
from layoffs_staging_2
where row_num > 1;

select *
from layoffs_staging_2
where row_num > 1;

select *
from layoffs_staging_2;

-- first step done

-- 2.standarize data (finding mistakes and fixing them,ex: spellings mistakes...)

select company,trim(company)
from layoffs_staging_2;

update layoffs_staging_2
set company = trim(company);

-- nriglo company name 

select distinct company
from layoffs_staging_2
order by 1;

select *
from layoffs_staging_2
where industry like 'crypto%';

update layoffs_staging_2
set industry = 'Crypto'
where industry like 'crypto%';

-- nriglo country 

select distinct location
from layoffs_staging_2
order by 1;

select distinct country, trim(trailing '.' from country)
from layoffs_staging_2
order by 1;

update layoffs_staging_2
set country = trim(trailing '.' from country);

select `date` , str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging_2;

update layoffs_staging_2
set `date` = str_to_date(`date`, '%m/%d/%Y');

Alter table layoffs_staging_2
modify column `date` DATE;
-- -------------------
select * 
from layoffs_staging_2
where total_laid_off is null
and percentage_laid_off is null;

DELETE
from layoffs_staging_2
where total_laid_off is null
and percentage_laid_off is null;

select * 
from layoffs_staging_2
where industry is null 
or industry = '';

select * 
from layoffs_staging_2
where company='Airbnb';

update layoffs_staging_2
set industry = null
where industry = '';

select * 
from layoffs_staging_2 t1
join layoffs_staging_2 t2
where t1.company = t2.company
and t1.industry is null 
and t2.industry is not null;

update layoffs_staging_2 t1
join layoffs_staging_2 t2
set t1.industry = t2.industry
where t1.company = t2.company
and t1.industry is null 
and t2.industry is not null;

select * 
from layoffs_staging_2;

alter table  layoffs_staging_2 
drop column row_num;

-- some data analysis :

select country , sum(total_laid_off)
from layoffs_staging_2
group by country 
order by 2 desc;


with Company_year(company,sum_total_laidoff,yearr) as
(
select company,Sum(total_laid_off), year(`date`) As yearr
from layoffs_staging_2
where total_laid_off is not null and year(`date`) is not null 
group by company,year(`date`)
),Company_year_rank as(
select *, dense_rank() over( partition by yearr order by  sum_total_laidoff desc) as ranking
from Company_year
)
select * 
from Company_year_rank
where ranking < 5 ;



















