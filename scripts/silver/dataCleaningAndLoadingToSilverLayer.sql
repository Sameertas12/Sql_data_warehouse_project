-- Data Cleaning and Loading Cleaned Data into Silver Layer

-- -----------------------------------------------------------------------------------

-- crm_cust_info (Customer Info) Table
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

-- -----------------------------------------------------------------------------------

-- crm_prod_info (Product Info) Table
SELECT * FROM bronze.crm_prod_info;

-- Checking for Duplicate and Null values in prd_id column
SELECT 
	prd_id,
    COUNT(*) AS Cnt
FROM bronze.crm_prod_info
GROUP BY prd_id
HAVING Cnt>1 OR prd_id IS NULL;

-- According to Architecture, product key(prd_key) in product info table is combination of both
-- Sales product key(sls_prd_key) in Sales details Table and
-- id in px_cat_g1v2 table
SELECT * FROM bronze.crm_prod_info;
SELECT * FROM bronze.crm_sales_details;
SELECT * FROM bronze.erp_px_cat_g1v2;

-- So we should split the product key column in Product info table accordingly
SELECT
	*,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key
FROM bronze.crm_prod_info;

-- Checking for Extra spaces in product name column
SELECT prd_nm
FROM bronze.crm_prod_info
WHERE prd_nm != TRIM(prd_nm);
	
-- Checking for Null values and Negative Values in Product Cost column
SELECT prd_cost
FROM bronze.crm_prod_info
WHERE prd_cost<0 OR prd_cost IS NULL;

-- Replacing Null with 0 if exists
SELECT
	*,
    IFNULL(prd_cost, 0) AS prd_cost
FROM bronze.crm_prod_info;

-- Making prd_line column look more user Friendly
SELECT
	*,
    CASE
		WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
        WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
        ELSE 'N/A'
    END AS prd_line
FROM bronze.crm_prod_info;

-- Checking whether product end date is after product start date or not
SELECT *
FROM bronze.crm_prod_info
WHERE prd_end_dt < prd_start_dt;

-- We can see many dates are incorrect
-- There are multiple products(prd_nm) with same prd_key. The end date of a product is equal to 
-- start date of same product if exists. If a product exists only once it may not have end date.
-- We can achieve this using LEAD function
SELECT
	prd_id,
    prd_key,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
		DATE_SUB(
			LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt),
            INTERVAL 1 DAY
        ) AS DATE
	) AS prd_end_dt
FROM bronze.crm_prod_info;

-- Now Column wise problems are cleared and now we should merge all queries
SELECT
	prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    prd_nm,
    IFNULL(prd_cost, 0) AS prd_cost,
    CASE
		WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
        WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
        ELSE 'N/A'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
		DATE_SUB(
			LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt),
            INTERVAL 1 DAY
        ) AS DATE
	) AS prd_end_dt
FROM bronze.crm_prod_info;
    
-- One more thing before inserting values is that, we dont have cat_id column in silver layer of product table.
-- We should add that column before inserting values to avoid error.
-- AND We want this column after prd_id column, so we can specify it using AFTER keyword
ALTER TABLE silver.crm_prod_info
ADD COLUMN cat_id VARCHAR(50) AFTER prd_id;

ALTER TABLE silver.crm_prod_info
ADD COLUMN dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP;


-- Now we can Insert values into Product table in Silver layer using above query
INSERT INTO silver.crm_prod_info(
	prd_id,
    cat_id,
    prd_key,
    prd_nm,
	prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
	prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    prd_nm,
    IFNULL(prd_cost, 0) AS prd_cost,
    CASE
		WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
        WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
        ELSE 'N/A'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
		DATE_SUB(
			LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt),
            INTERVAL 1 DAY
        ) AS DATE
	) AS prd_end_dt
FROM bronze.crm_prod_info;

-- -----------------------------------------------------------------------------------
