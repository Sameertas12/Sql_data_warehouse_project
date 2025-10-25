
-- Allowing NULL Values in cust_id column
ALTER TABLE bronze.crm_cust_info
MODIFY cst_id INT NULL;

-- Temporarily changing sql mode
DESCRIBE bronze.crm_cust_info;
SELECT @@sql_mode;
SET SESSION sql_mode = '';

-- Loading Data in Tables
LOAD DATA INFILE "C:\\PROJECT 1\\source_crm\\cust_info.csv"
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
SET cst_id = NULLIF(cst_id, '');


LOAD DATA INFILE "C:\\PROJECT 1\\source_crm\\prd_info.csv"
INTO TABLE bronze.crm_prod_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE "C:\\PROJECT 1\\source_crm\\sales_details.csv"
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE "C:\\PROJECT 1\\source_erp\\CUST_AZ12.csv"
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE "C:\\PROJECT 1\\source_erp\\LOC_A101.csv"
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE "C:\\PROJECT 1\\source_erp\\PX_CAT_G1V2.csv"
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

