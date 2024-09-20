/*
===================================================================
SQL Project: Layoff Data Cleaning and Preparation
Dataset: Layoffs Dataset (Public Dataset from: 
https://www.kaggle.com/datasets/swaptr/layoffs-2022)
Table Name: layoff_staging
===================================================================
Description:
This script focuses on the data cleaning and preparation of a layoff dataset 
to ensure its accuracy, consistency, and usability. The data cleaning process 
consists of eight main steps, which will be further expanded into a total of 
19 detailed steps during the coding phase.

1. Creating a staging table to preserve the raw data for backup.
2. Checking for and removing duplicate records using window functions.
3. Standardizing data by addressing nulls and fixing formatting issues.
4. Standardizing industry names and correcting inconsistencies.
5. Fixing country name issues, ensuring uniformity across records.
6. Formatting the date field by converting it into a proper date format.
7. Checking for and handling missing values where necessary.
8. Removing unnecessary columns and rows that add no analytical value.

The goal of these steps is to prepare a clean and structured dataset for 
further analysis, ensuring that the data is ready to be used for uncovering 
insights into layoff trends and workforce reductions during critical periods.
===================================================================
Author: Muhammad Sajid Salman
*/

-- Step 1: View the raw data from the 'layoff' table
SELECT * 
FROM layoff;

-- Step 2: Create a staging table for data cleaning
-- The staging table will be used to perform all transformations while preserving the original dataset.
CREATE TABLE layoff.layoff_staging 
LIKE layoff;

-- Insert data into the staging table from the original 'layoff' table
INSERT layoff_staging 
SELECT * FROM layoff;

-- Data Cleaning Process Overview:
-- 1. Remove duplicates
-- 2. Standardize and fix data inconsistencies
-- 3. Handle missing values
-- 4. Remove unnecessary columns and rows

-- Step 3: Remove Duplicates
-- Checking for duplicates in the staging table using ROW_NUMBER
SELECT company, industry, total_laid_off, `date`,
    ROW_NUMBER() OVER (
        PARTITION BY company, industry, total_laid_off, `date`
    ) AS row_num
FROM layoff_staging;

-- Step 4: Identify and list duplicate records
SELECT *
FROM (
    SELECT company, industry, total_laid_off, `date`,
        ROW_NUMBER() OVER (
            PARTITION BY company, industry, total_laid_off, `date`
        ) AS row_num
    FROM layoff_staging
) duplicates
WHERE row_num > 1;

-- Step 5: Confirm if any duplicates need to be removed (Example: Oda)
SELECT *
FROM layoff_staging
WHERE company = 'Oda';

-- Step 6: Find true duplicates based on multiple columns for better accuracy
SELECT *
FROM (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
        ROW_NUMBER() OVER (
            PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
        ) AS row_num
    FROM layoff_staging
) duplicates
WHERE row_num > 1;

-- Step 7: Use a CTE (Common Table Expression) to remove duplicates
WITH DELETE_CTE AS (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
        ROW_NUMBER() OVER (
            PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
        ) AS row_num
    FROM layoff_staging
)
DELETE FROM layoff_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
    FROM DELETE_CTE
) AND row_num > 1;

-- Step 8: Add a 'row_num' column for easy tracking and deletion of duplicates
ALTER TABLE layoff_staging ADD row_num INT;

-- Step 9: Create a new staging table ('layoff_staging2') and populate it with unique rows
CREATE TABLE layoff_staging2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT,
    percentage_laid_off TEXT,
    `date` TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT,
    row_num INT
);

-- Insert into the new staging table with ROW_NUMBER for identifying duplicates
INSERT INTO layoff_staging2
    (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num)
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num
FROM layoff_staging;

-- Step 10: Remove duplicate rows where 'row_num' >= 2
DELETE FROM layoff_staging2
WHERE row_num >= 2;

-- Step 11: Standardize Data
-- Handling missing and inconsistent values in the 'industry' column
-- Checking for null or empty values in 'industry'
SELECT DISTINCT industry
FROM layoff_staging2
ORDER BY industry;

SELECT *
FROM layoff_staging2
WHERE industry IS NULL OR industry = '';

-- Step 12: Set empty strings in 'industry' to NULL
UPDATE layoff_staging2
SET industry = NULL
WHERE industry = '';

-- Step 13: Populate null values in 'industry' using existing data for the same company
UPDATE layoff_staging2 t1
JOIN layoff_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Step 14: Standardize variations in industry values (e.g., 'Crypto' to a consistent value)
UPDATE layoff_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Step 15: Standardize inconsistent country names (e.g., 'United States.' to 'United States')
UPDATE layoff_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Step 16: Convert 'date' column to proper DATE data type
UPDATE layoff_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoff_staging2
MODIFY COLUMN `date` DATE;

-- Step 17: Handle Null Values
-- Review null values in key columns (total_laid_off, percentage_laid_off, funds_raised_millions) and decide on appropriate actions
SELECT *
FROM layoff_staging2
WHERE total_laid_off IS NULL;

-- Step 18: Remove rows where both 'total_laid_off' and 'percentage_laid_off' are null (unusable data)
DELETE FROM layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Step 19: Final Data Review and Cleanup
-- Drop the 'row_num' column after cleaning
ALTER TABLE layoff_staging2
DROP COLUMN row_num;

-- Final review of the cleaned dataset
SELECT * 
FROM layoff_staging2;
