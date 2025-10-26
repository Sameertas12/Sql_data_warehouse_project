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

-- crm_sales_details Table
SELECT * FROM bronze.crm_sales_details;

-- Checking for any Extra spaces in sls_ord_num column
SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- From Architecture we can observe that sls_prd_key is linked with prd_key of product info table
-- So lets check if there are any sls_prd_key in crm_sales_details table that are not in product info table
SELECT
	sls_ord_num,
    sls_prd_key,
    sls_quantity
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN(
	SELECT prd_key FROM silver.crm_prod_info
);
-- O/P: No such keys found

-- From Architecture we can observe that sls_cust_id is linked with cst_id of customer info table
-- So lets check if there are any ls_cust_id in crm_sales_details table that are not in customer info table
SELECT
	sls_ord_num,
    sls_cust_id,
    sls_quantity
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN(
	SELECT cst_id FROM silver.crm_cust_info
);
-- O/P: No such keys found

-- sls_order_dt, sls_ship_dt, sls_due_dt are in Number Format
-- Lets convert it into Appropriate Date Fromat
SELECT
	*,
    CASE
		WHEN sls_order_dt<=0 OR LENGTH(sls_order_dt)!=8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
    END AS sls_order_dt,
    CASE
		WHEN sls_ship_dt<=0 OR LENGTH(sls_ship_dt)!=8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
    END AS sls_ship_dt,
    CASE
		WHEN sls_due_dt<=0 OR LENGTH(sls_due_dt)!=8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
    END AS sls_due_dt
FROM bronze.crm_sales_details;

-- Checking if there are any NULL and Negative values in sls_sales, sls_quantity, sls_price columns
-- Also check if there are any sales where price*quantity != sales
SELECT * FROM bronze.crm_sales_details
WHERE sls_sales <=0 OR sls_sales IS NULL
	OR sls_quantity <=0 OR sls_quantity IS NULL
	OR sls_price <=0 OR sls_price IS NULL
    OR sls_sales != sls_price*sls_quantity;

-- Now Correct all the errors
SELECT
	*,
    CASE
		WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != ABS(sls_price)*sls_quantity 
        THEN ABS(sls_price)*sls_quantity
        ELSE sls_price
    END AS sls_sales,
    sls_quantity,
    CASE
		WHEN sls_price <=0 OR sls_price IS NULL OR sls_price != sls_sales/ABS(sls_quantity)
        THEN sls_sales/NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;

-- Merging all queries
SELECT
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE
		WHEN sls_order_dt<=0 OR LENGTH(sls_order_dt)!=8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
    END AS sls_order_dt,
    CASE
		WHEN sls_ship_dt<=0 OR LENGTH(sls_ship_dt)!=8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
    END AS sls_ship_dt,
    CASE
		WHEN sls_due_dt<=0 OR LENGTH(sls_due_dt)!=8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
    END AS sls_due_dt,
    CASE
		WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != ABS(sls_price)*sls_quantity 
        THEN ABS(sls_price)*sls_quantity
        ELSE sls_price
    END AS sls_sales,
    sls_quantity,
    CASE
		WHEN sls_price <=0 OR sls_price IS NULL OR sls_price != sls_sales/ABS(sls_quantity)
        THEN sls_sales/NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;

-- Before Inserting sls_order_dt, sls_ship_dt, sls_due_dt are INT type in silver layer
-- We should first change the datatype to DATE 
ALTER TABLE silver.crm_sales_details
MODIFY COLUMN sls_order_dt DATE,
MODIFY COLUMN sls_ship_dt DATE,
MODIFY COLUMN sls_due_dt DATE;

-- Now Insert this cleaned data into silver layer
INSERT INTO silver.crm_sales_details(
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE
		WHEN sls_order_dt<=0 OR LENGTH(sls_order_dt)!=8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
    END AS sls_order_dt,
    CASE
		WHEN sls_ship_dt<=0 OR LENGTH(sls_ship_dt)!=8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
    END AS sls_ship_dt,
    CASE
		WHEN sls_due_dt<=0 OR LENGTH(sls_due_dt)!=8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
    END AS sls_due_dt,
    CASE
		WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != ABS(sls_price)*sls_quantity 
        THEN ABS(sls_price)*sls_quantity
        ELSE sls_price
    END AS sls_sales,
    sls_quantity,
    CASE
		WHEN sls_price <=0 OR sls_price IS NULL OR sls_price != sls_sales/ABS(sls_quantity)
        THEN sls_sales/NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;

-- -----------------------------------------------------------------------------------

-- erp_cust_az12 Table
SELECT * FROM bronze.erp_cust_az12;

-- cid from erp_cust_az12 Table and cst_key from crm_cust_info are common columns 
-- but in cid we have NAS extra a prefix. we should fix this.
SELECT
	*,
    CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
        ELSE cid
    END as cid
FROM bronze.erp_cust_az12;

-- bdate column has dates which are from FUTURE i.e, after Today
-- and there are customers whose age is more than 100 years, i.e, bdate before 1925-01-01
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate > CURDATE() OR bdate<'1924-01-01';

SELECT
	CASE
		WHEN bdate > CURDATE() THEN NULL
        ELSE bdate
    END AS bdate
FROM bronze.erp_cust_az12;

-- Gender column is inappropriate. 
SELECT 
	DISTINCT gen,
    COUNT(*) AS Cnt
FROM bronze.erp_cust_az12
GROUP BY gen;

-- Lets fix it
-- SET SQL_SAFE_UPDATES = 0;
-- UPDATE bronze.erp_cust_az12
-- SET gen = CASE
--   WHEN REGEXP_REPLACE(UPPER(gen), '[^A-Z]', '') IN ('MALE','M') THEN 'Male'
--   WHEN REGEXP_REPLACE(UPPER(gen), '[^A-Z]', '') IN ('FEMALE','F') THEN 'Female'
--   ELSE 'N/A'
-- END;
-- SET SQL_SAFE_UPDATES = 1;

-- Now Merge the Queries
SELECT
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
        ELSE cid
    END as cid,
    CASE
		WHEN bdate > CURDATE() THEN NULL
        ELSE bdate
    END AS bdate,
    gen
FROM bronze.erp_cust_az12;

-- Now Insert the data into silver layer
INSERT INTO silver.erp_cust_az12()
SELECT
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
        ELSE cid
    END as cid,
    CASE
		WHEN bdate > CURDATE() THEN NULL
        ELSE bdate
    END AS bdate,
    gen
FROM bronze.erp_cust_az12;

-- -----------------------------------------------------------------------------------

-- erp_loc_a101 Table
SELECT * FROM bronze.erp_loc_a101;

-- In cid column '-' should be removed
SELECT
	cid,
    REPLACE(cid, '-', '') as cid
FROM bronze.erp_loc_a101;

-- Fixing Country column
-- SET SQL_SAFE_UPDATES = 0;
-- UPDATE bronze.erp_loc_a101
-- SET cntry = CASE
--   WHEN REGEXP_REPLACE(TRIM(cntry), '[^A-Z]', '') = 'DE' THEN 'Germany'
--   WHEN REGEXP_REPLACE(TRIM(cntry), '[^A-Z]', '') IN('US', 'USA') THEN 'United States'
--   WHEN REGEXP_REPLACE(TRIM(cntry), '[^A-Z]', '') = '' OR cntry IS NULL THEN 'N/A'
--   ELSE TRIM(cntry)
-- END;
-- SET SQL_SAFE_UPDATES = 1;

SELECT
	cntry1,
    COUNT(*) AS Cnt
FROM(
SELECT
	cntry,
    CASE
		WHEN cntry LIKE '%Germany%' THEN 'Germany'
        WHEN cntry LIKE '%States%' THEN 'United States'
        ELSE cntry
    END AS cntry1
FROM bronze.erp_loc_a101
) AS t
GROUP BY cntry1;

-- Merging Queries and Inserting into Silver Layer
INSERT INTO silver.erp_loc_a101()
SELECT
	REPLACE(cid, '-', '') as cid,
    CASE
		WHEN cntry LIKE '%Germany%' THEN 'Germany'
        WHEN cntry LIKE '%States%' THEN 'United States'
        ELSE cntry
    END AS cntry1
FROM bronze.erp_loc_a101;

-- -----------------------------------------------------------------------------------

-- erp_px_cat_g1v2
SELECT * FROM erp_px_cat_g1v2;

-- Checking for Extra Spaces in cat and subcat column
SELECT * FROM erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat);

-- Checking Distinct Values in maintenance column
SELECT maintenance, COUNT(*)
FROM erp_px_cat_g1v2
GROUP BY maintenance;

-- Fixing the Issue
SELECT maintenance, COUNT(*)
FROM(
SELECT
	CASE
		WHEN maintenance LIKE '%No%' THEN 'No'
        ELSE 'Yes'
    END AS maintenance
FROM erp_px_cat_g1v2
) AS Subquery1
GROUP BY maintenance;

INSERT INTO silver.erp_px_cat_g1v2()
SELECT
	id,
    cat,
    subcat,
    CASE
		WHEN maintenance LIKE '%No%' THEN 'No'
        ELSE 'Yes'
    END AS maintenance
FROM bronze.erp_px_cat_g1v2;
