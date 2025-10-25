-- Data Cleaning and Loading Cleaned Data into Silver Layer
SELECT * FROM bronze.crm_cust_info;

-- Displaying duplicates and Null values in cst_id column
SELECT
	cst_id,
    COUNT(*) AS Cnt
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING Cnt>1 OR cst_id IS NULL;

-- Removing Duplicate and NULL values in customer ID column
-- Using ROW NUMBER Function to filter only latest records for duplicate entries
SELECT *
FROM (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS ranking_by_row_number
	FROM bronze.crm_cust_info
) AS SubQuery1
WHERE ranking_by_row_number=1 AND cst_id IS NOT NULL; 


-- Cleaning FirstName and LastName
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
    CASE
		WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
        ELSE 'N/A'
    END AS cst_marital_status,
    CASE
		WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
        ELSE 'N/A'
    END AS cst_gndr,
    cst_create_date
FROM (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS ranking_by_row_number
	FROM bronze.crm_cust_info
) AS SubQuery1
WHERE ranking_by_row_number=1 AND cst_id IS NOT NULL; 


-- The above query involves cleaning all columns in customer info table
-- now we can load the data into silver layer
INSERT INTO silver.crm_cust_info(
	cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
    )
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
    CASE
		WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
        ELSE 'N/A'
    END AS cst_marital_status,
    CASE
		WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
        ELSE 'N/A'
    END AS cst_gndr,
    cst_create_date
FROM (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS ranking_by_row_number
	FROM bronze.crm_cust_info
) AS SubQuery1
WHERE ranking_by_row_number=1 AND cst_id IS NOT NULL; 

-- Checking and Validation
SELECT
	cst_id,
    COUNT(*) AS Cnt
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING Cnt>1 OR cst_id IS NULL;
