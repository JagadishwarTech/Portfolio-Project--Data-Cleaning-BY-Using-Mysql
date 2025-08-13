#------ Data cleaning--------
 
 
 
 SELECT * FROM layoffs;
 
 #------1. Remove Duplicates
 #------2.Standardize the Data
 #------3.Null Values or blank values
 #------4.Remove Any Columns
 
 CREATE TABLE layoffs_staging
 LIKE layoffs;
 
 
 Select * from layoffs_staging;
 
 Insert layoffs_staging
 select * from layoffs;
 
 
 select * ,
 Row_number() over
 (partition by company, industry, total_laid_off,percentage_laid_off,`date`) AS Row_num
 from layoffs_staging;
 
 
 WITH duplicate_cte AS 
 (
 select * ,
 Row_number() over(
 partition by company,location, industry, total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS Row_num
 from layoffs_staging
 )
 select * 
 from duplicate_cte
 where row_num > 1;
 
 select *
 from layoffs_staging
 where company ='Casper';
 
 
 
 
  WITH duplicate_cte AS 
 (
 select * ,
 Row_number() over(
 partition by company,location, industry, total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS Row_num
 from layoffs_staging
 )
 DELETE
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

Insert into layoffs_staging2
 select * ,
 Row_number() over(
 partition by company,location, industry, total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS Row_num
 from layoffs_staging; 
 
 
select * from layoffs_staging2
where row_num > 1;

DELETE from layoffs_staging2
where row_num > 1;
 
select * from layoffs_staging2 ;
  
  
  
#------2.Standardize the Data
   
select company,(TRIM(company))
from layoffs_staging2;
   
update layoffs_staging2
SET company = TRIM(company);
   
select DISTINCT  industry
from layoffs_staging2
order by 1;





 
 select distinct industry
 from layoffs_staging2;
 
 update layoffs_staging2
 set industry ='Crypto'
 where industry LIKE 'crypto%';
 
 
 
 select distinct country
 from layoffs_staging2
 order by 1;
 
 
 select * from layoffs_staging2
 where country LIKE 'United States%'
 order by 1;
 
 select distinct country,TRIM(TRAILING '.' from country)
 from layoffs_staging2
 order by 1;
 
 update layoffs_staging2
 set country = TRIM(TRAILING '.' from country)
 where country = 'United States';
 
 select `date`
 from layoffs_staging;
 
  update layoffs_staging2
  set `date` = str_to_date(`date`,'%m/%d/%Y');
  
  
  ALTER TABLE layoffs_staging2
  MODIFY COLUMN `date` DATE;
  
  
  select *
  from layoffs_staging2
  where total_laid_off IS NULL
  AND percentage_laid_off IS NULL;
  
  
    
UPDATE layoffs_staging2
SET Industry = NULL
WHERE TRIM(Industry) = '';

  
  
  
  
  select *
  from layoffs_staging2
  where industry IS NULL
  or industry = ' ';
  
  select *
  from layoffs_staging2
  where company LIKE 'Bally%';
  
  
  
  select t1.industry,t2.industry
  from layoffs_staging2 t1
  join layoffs_staging2 t2
      on t1.company=t2.company
	where (t1.industry is null or t1.industry='')
    and t2.industry is not null;
    
    
    
    update layoffs_staging2 t1
     join layoffs_staging2 t2
       on t1.company=t2.company
	set t1.industry=t2.industry
    where t1.industry is null 
    and t2.industry is not null;
    
    
    select * from layoffs_staging2;
    
    
    
      select *
  from layoffs_staging2
  where total_laid_off IS NULL
  AND percentage_laid_off IS NULL;
  
  
  delete 
  from layoffs_staging2
   where total_laid_off IS NULL
  AND percentage_laid_off IS NULL;
  
  select * from layoffs_staging2;
  
  alter table layoffs_staging2
  drop column row_num;
    
        
  
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 