/*
===================================================================
SQL Project: Layoff Data Analysis
Dataset: Layoffs Dataset (Public Dataset from: 
https://www.kaggle.com/datasets/swaptr/layoffs-2022)
Table Name: layoff_staging
===================================================================
Description:
This script performs an exploratory data analysis (EDA) on a dataset 
containing information about layoffs across various companies, industries, 
and locations. The analysis covers the following key aspects:

1. Overview of total layoffs, layoff percentages, and notable companies
   with 100% layoff rates.
2. Aggregation of layoffs by company, location, country, industry, and year.
3. Identification of companies with the largest layoffs, both overall and 
   on a yearly basis, along with ranking.
4. Analysis of layoffs by company stage, funding raised, and industry sector.
5. Calculation of rolling totals of layoffs to track trends over time.

The objective of this analysis is to identify patterns and insights regarding
layoff events, helping businesses and policymakers understand the dynamics 
of workforce reductions during significant periods.
===================================================================
Author: Muhammad Sajid Salman
*/

-- Preview the dataset
SELECT * 
From layoff_staging;

-- Retrieve the maximum number of total layoffs
SELECT MAX(total_laid_off)
From layoff_staging;

-- Analyze the range of layoff percentages across the dataset
SELECT MAX(percentage_laid_off), MIN(percentage_laid_off)
From layoff_staging
WHERE percentage_laid_off IS NOT NULL;

-- Identify companies that experienced a 100% layoff rate
SELECT *
From layoff_staging
WHERE percentage_laid_off = 1;

-- Analyze companies with 100% layoff rates, ordered by the amount of funds raised
SELECT *
From layoff_staging
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Retrieve companies with the largest single layoff event
SELECT company, total_laid_off
FROM layoff_staging
ORDER BY total_laid_off DESC
LIMIT 5;

-- Summarize total layoffs by company across all recorded events
SELECT company, SUM(total_laid_off)
From layoff_staging
GROUP BY company
ORDER BY SUM(total_laid_off) DESC
LIMIT 10;

-- Summarize total layoffs by location
SELECT location, SUM(total_laid_off)
From layoff_staging
GROUP BY location
ORDER BY SUM(total_laid_off) DESC
LIMIT 10;

-- Summarize total layoffs by country
SELECT country, SUM(total_laid_off)
From layoff_staging
GROUP BY country
ORDER BY SUM(total_laid_off) DESC;

-- Summarize layoffs by year
SELECT YEAR(date), SUM(total_laid_off)
From layoff_staging
GROUP BY YEAR(date)
ORDER BY YEAR(date) ASC;

-- Summarize total layoffs by industry
SELECT industry, SUM(total_laid_off)
From layoff_staging
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC;

-- Summarize total layoffs by company stage
SELECT stage, SUM(total_laid_off)
From layoff_staging
GROUP BY stage
ORDER BY SUM(total_laid_off) DESC;

-- Analyze total layoffs per company by year and rank them annually
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS year, SUM(total_laid_off) AS total_laid_off
  From layoff_staging
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS 
(
  SELECT company, year, total_laid_off, DENSE_RANK() OVER (PARTITION BY year ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, year, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND year IS NOT NULL
ORDER BY year ASC, total_laid_off DESC;

-- Calculate the total layoffs per month
SELECT SUBSTRING(date,1,7) AS month, SUM(total_laid_off) AS total_laid_off
From layoff_staging
GROUP BY month
ORDER BY month ASC;

-- Compute a rolling total of layoffs per month using a CTE
WITH DATE_CTE AS 
(
  SELECT SUBSTRING(date,1,7) AS month, SUM(total_laid_off) AS total_laid_off
  From layoff_staging
  GROUP BY month
  ORDER BY month ASC
)
SELECT month, SUM(total_laid_off) OVER (ORDER BY month ASC) AS rolling_total_layoffs
FROM DATE_CTE
ORDER BY month ASC;
