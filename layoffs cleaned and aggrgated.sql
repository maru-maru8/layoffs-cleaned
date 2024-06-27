USE practice;
SELECT * FROM layoffs;

SELECT *,  ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS rn
FROM layoffs;

WITH cte AS 
(SELECT *,  ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS rn
FROM layoffs)
SELECT * 
FROM cte
WHERE rn > 1;

CREATE TABLE `layoffs2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `rn` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs2
SELECT *,  ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS rn
FROM layoffs;

DELETE
FROM layoffs2
WHERE rn > 1;

SELECT *
FROM layoffs2;

SELECT company
FROM layoffs2;

SELECT company, TRIM(company)
FROM layoffs2;

UPDATE layoffs2
SET company = TRIM(company);

SELECT * FROM layoffs2;

SELECT DISTINCT(location)
FROM layoffs2
ORDER BY location;

SELECT DISTINCT(industry)
FROM layoffs2
ORDER BY industry;

SELECT DISTINCT(industry)
FROM layoffs2
WHERE industry LIKE 'crypto%';

UPDATE layoffs2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT(industry)
FROM layoffs2
ORDER BY industry;

UPDATE layoffs2
SET industry = NULL
WHERE industry = '';

SELECT * FROM layoffs2;

SELECT DISTINCT(country)
FROM layoffs2
ORDER BY country;

SELECT DISTINCT(country), TRIM(TRAILING '.'FROM country)
FROM layoffs2
WHERE country LIKE 'United States%';

UPDATE layoffs2
SET country = TRIM(TRAILING '.'FROM country)
WHERE country LIKE 'United States%';

SELECT * FROM layoffs2;

ALTER TABLE layoffs2
DROP COLUMN rn;

DELETE
FROM layoffs2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs2
WHERE industry IS NULL;

SELECT *
FROM layoffs2 AS t1 INNER JOIN layoffs2 AS t2
ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs2 t1
INNER JOIN layoffs2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT company, industry
FROM layoffs2
ORDER BY company;
SELECT `date` FROM layoffs2;

SELECT 
`date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs2;

UPDATE layoffs2
SET `date` =STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs2
MODIFY COLUMN `date` DATE;

SELECT * FROM layoffs2;

THE AMOUNT OF LAY OFFS;
SELECT company, COUNT(total_laid_off)
FROM layoffs2
GROUP BY company;

SELECT *
FROM layoffs2;

AVERAGE LAYOFF PERCENTAGE BY COUNTRY AND INDUSTRY;
SELECT
company,
AVG(total_laid_off) OVER(PARTITION BY company)AS avg_layoff_company,
industry,
AVG(total_laid_off) OVER(PARTITION BY industry) AS avg_layoff_industry
FROM layoffs2
group by company, industry; 

FINDING layoffs and funds raised for top industries;
SELECT
industry,
SUM(total_laid_off) AS total_layoffs,
SUM(funds_raised_millions) AS total_raised
FROM layoffs2
GROUP by industry
ORDER BY total_layoffs DESC
LIMIT 4;

SELECT * FROM layoffs2;

monthy layoffs trend by country;
SELECT
country,
EXTRACT(year FROM `date`) AS year,
EXTRACT(month FROM `date`) AS month,
AVG(total_laid_off) AS avg_laid_off
FROM layoffs2
GROUP BY country, year, month
ORDER BY country, year, month;

COORELATION BETWEEN FUNDS RAISED AND LAYOFFS;
SELECT
EXTRACT(year FROM `date`) AS year,
ROUND(AVG(funds_raised_millions),1) AS avg_funds,
ROUND(AVG(total_laid_off),1) AS laid_off
FROM layoffs2
GROUP BY year
ORDER BY year DESC;

FINDING WHICH COMPANY HAD THE MOST LAYOFFS;
SELECT company, SUM(total_laid_off) AS total_layoffs
FROM layoffs2
GROUP BY company
HAVING SUM(total_laid_off) IS NOT NULL
ORDER BY total_layoffs DESC
LIMIT 1;

TOTAL LAYOFFS BY INDUSTRY;
SELECT industry, SUM(total_laid_off) AS total_layoffs
FROM layoffs2
GROUP BY industry;

SEEING WHICH COMPANY HAD THE LEAST AMOUNT OF LAYOFFS
SELECT company, SUM(total_laid_off) AS total_layoffs
FROM layoffs2
GROUP BY company
HAVING SUM(total_laid_off) IS NOT NULL
ORDER BY total_layoffs 
LIMIT 1;

SEEING WHAT COMPANY HAD THE THIRD HIGHEST LAYOFFS;
SELECT company, SUM(total_laid_off) AS total_layoffs
FROM layoffs2
GROUP BY company
HAVING SUM(total_laid_off) IS NOT NULL
ORDER BY total_layoffs DESC
LIMIT 2,1;

find all the even numbers;
SELECT industry, total_laid_off
FROM layoffs2
WHERE total_laid_off%2 = 0
ORDER BY total_laid_off;

SELECT company
FROM layoffs
WHERE company LIKE 'A%';

SELECT company
FROM layoffs
WHERE company LIKE '%A';

SELECT company
FROM layoffs
WHERE company LIKE '%A%';

SELECT company
FROM layoffs
WHERE company NOT LIKE '%A%';

SELECT * FROM layoffs2;

seeing the total amount of people fired by month and year;
SELECT company, 
SUM(total_laid_off) AS total, 
EXTRACT(month FROM `date`) AS `month`,
EXTRACT(year FROM `date`) AS `year`
FROM layoffs2
GROUP BY company, `month`, `year`
HAVING SUM(total_laid_off) IS NOT NULL
ORDER BY `year`, `month` DESC;

PERCENTAGE LAYOFFS BY COUNTRY;
SELECT country, ROUND(AVG(percentage_laid_off),2) AS avg_percent_laidoff
FROM layoffs2
GROUP BY country
HAVING ROUND(AVG(percentage_laid_off),2) IS NOT NULL
ORDER BY avg_percent_laidoff DESC;

TOTAL LAYOFFS AND TOTAL FUNDS RAISED;
SELECT company, SUM(total_laid_off) AS total_layoffs, SUM(funds_raised_millions) AS total_funds_raised
FROM layoffs2
GROUP BY company
HAVING  SUM(total_laid_off) IS NOT NULL AND SUM(funds_raised_millions) IS NOT NULL;

LAYOFFS PER LOCATION;
SELECT company, location, avg(total_laid_off) AS avg_laid_off
FROM layoffs2
WHERE location = 'Tokyo';


