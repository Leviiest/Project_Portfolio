-- Data Cleaning Project in MySQL 

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Introduction  | Data Cleaning on Worlds Layoffs dataset via MYSQL Workbench
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- | Step 1 | Create a staging dataset.
-- | Step 2 | Remove Duplicates.
-- | Step 3 | Standardize the Data.
-- | Step 4 | Deal with Null & Blank Valyes.
-- | Step 5 | Remove irrelevent rows and columns. 



-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Data Dictionary  | layoffs dataset 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------
-- |       Variable        |                            Description                                 |
-----------------------------------------------------------------------------------------------------
-- | company               | Name of the company.                                                   |
-- | location              | Location of the company.                                               |
-- | industry              | Field which a company operates.                                        |
-- | total_laid_off        | Number of people got layoff.                                           |
-- | percentage_laid_off   | Proportion of the workforce that has been terminated due to layoffs.   |
-- | date                  | The date, layoff happened.                                             |
-- | stage                 | Denotes the current financial or business stage of a company.          |
-- | country               | The country the company is located.                                    |
-- | funds_raised_millions | Total amount of capital raised by a company.                           |
-----------------------------------------------------------------------------------------------------



-- # Check the layoffs table
SELECT * 
FROM layoffs; 







-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--    Step 1     | Create a Staging Dataset. 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- | We want to create a copy of the raw layoffs dataset for peforming data cleaning. This can provid room for mistake if we accidently changed the data during ETL (Extract, Transform, Load) process.
-- | Performing validation checks in the staging dataset can helps in ensuring that the data meets the expected result.


-- # Create a staging table called: `layoffs_staging`.  (We'll be performing data cleaning on layoffs_staging)
CREATE TABLE layoffs_staging
LIKE layoffs;



-- # Insert the values from `layoffs` into `layoffs_staging` table
INSERT layoffs_staging
SELECT * 
FROM layoffs;



-- # Check the layoffs_staging table
SELECT *
FROM layoffs_staging;







-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--    Step 2     | Remove Duplicates 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- | For the layoffs table there aren't a clear Primary Key in the table, therefore I check are there any rows are excatly the same across all attributes/columns.
-- | Eliminating duplicates, ensures that each data point is unique, leading to more accurate results in statistical analysis and predictive modeling.



-- # This will generates a unique number for each row within a partition, starting at 1 for the first row in each partition. (if a row_count > 1 then it is a duplicate)
SELECT * , 
ROW_NUMBER()  OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_count
FROM layoffs_staging
ORDER BY company;



-- # Select all the duplicate rows in a Common Table Expression (CTE)
WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER()  OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_count
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_count > 1; 



-- # Check one of companies in the duplicated rows
SELECT *
FROM layoffs_staging
WHERE company = 'Cazoo';



-- # Creating a Stage 2 of layoffs_staging dataset (This is an empty table)
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
  `row_count` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



-- # Check the empty layoffs_staging2 table
SELECT *
FROM layoffs_staging2;



-- # INSERT values from layoffs_staging + the `row_count` into layoffs_staging2 table
INSERT INTO layoffs_staging2
SELECT * , 
ROW_NUMBER()  OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_count
FROM layoffs_staging;



-- # Select the duplicates in layoffs_staging2 table
SELECT *
FROM layoffs_staging2
WHERE row_count > 1;



-- # Delete the duplicates
DELETE 
FROM layoffs_staging2
WHERE row_count > 1;



-- # Re-Check if there are still any duplicates in the layoffs_staging2 table
SELECT *
FROM layoffs_staging2
WHERE row_count > 1;







-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--    Step 3     | Standardize the Data 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- | Find errors in the data   <--(This is what we're focusing on).
-- | Standardize the Data can also mean putting data on a scale(Z-score, Min-Max-scaling), to make easier comparisons & improve model performance.



-- | Step 3.1 |-- 
-- # Remove unwanted characters from the beginning and the end of a string. By default, it removes empty spaces.
SELECT company, TRIM(company)
FROM layoffs_staging2;



-- # UPDATE the company column with TRIM( )
UPDATE layoffs_staging2
SET company = TRIM(company);



-- | Step 3.2 | -- 
-- # Check types of industry are listeed in layoffs_staging2 table
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

		-- • There are two misspellings in the crypto industry. `Crypto Currency` & `CryptoCurrency`
		-- • Update them into `Crypto`



-- # Check the misspelled rows
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
ORDER BY industry DESC
LIMIT 10;



-- # Update the two misspelled crypto categories 
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';



-- | Step 3.3 | -- 
-- # Check unique countries listeed in layoffs_staging2 table
SELECT DISTINCT country, TRIM(country)
FROM layoffs_staging2
ORDER BY 1;

		-- • There is a misspelling in the `United States` with an extra '.' at the end



-- # PREVIEW THE Updated the country
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) AS TRIMMED_COL
FROM layoffs_staging2
ORDER BY 1;



-- # Update the country
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';



-- | Step 3.4 | -- 
-- # Convert `date` column from str to date-like str format 
SELECT date, STR_TO_DATE(date, '%m/%d/%Y')
FROM layoffs_staging2;



-- # Update the `date` column into the new formated `date` column (The datatype of the date column still a text)
UPDATE layoffs_staging2
SET date = STR_TO_DATE(date, '%m/%d/%Y');



-- # Change the `date` column from str to date datatype
ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE;







-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--    Step 4     | Deal with Null & Blank Values
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- | Null & Blank values can significantly impact the quality of analysis and the performance of machine learning models.
-- | To see if is possible to populate data using information from other columns & rows.




-- # Checking Null values in the ` total_laid_off ` & ` percentage_laid_off `column 
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;



-- # Checking Null and blank values in the `industry` column
SELECT *
FROM layoffs_staging2
WHERE industry = '' OR industry IS NULL;
		
        -- • Try to populate the NULL/blank values using informations from other columns.



-- # Populate potential blank values in `industry` column using `company` column
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';


# Joining table 1 & table 2 on `company` column, returning both the table 1 rows that has NULL value in `industry` and rows from table2 with values in `industry`.
SELECT t1.company, t1.industry AS industry_t1, t2.industry AS industry_t2
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company 
WHERE (t1.industry = '' OR t1.industry IS NULL) AND t2.industry IS NOT NULL; 



-- # Change the Blank values to Null values
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';



-- # Populate null `industry` values
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company 
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL; 







-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--    Step 5     | Remove irrelevent rows and columns 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- | The `row_count` do not contributing any meaningful for analysis, therefore we can drop the column



-- # Check rows with Null values in both `total_laid_off` & `percentage_laid_off`column 
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;



-- # Delete rows that has NULL values in both `total_laid_off` & `percentage_laid_off`.
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;



-- # Drop the `row_count` column
ALTER TABLE layoffs_staging2
DROP COLUMN row_count;

