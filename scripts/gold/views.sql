-- -----------------------------------------------------------------------------------
-- Creating Star Schema (Fact Table and Dimension Tables)
-- -----------------------------------------------------------------------------------

-- Creating a VIEW of Dimension table for customers
CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE
		WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'NA')
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid;

-- Getting Data from View
SELECT * FROM gold.dim_customers;

-- Creating a VIEW of Dimension table for products
DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;

SELECT * FROM gold.dim_products;

-- Creating View for Fact Table
CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number,
    pr.product_key AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
	ON sd.sls_prd_key=pr.product_number
LEFT JOIN gold.dim_customers cu
	ON sd.sls_cust_id=cu.customer_id;
	
-- Getting Data from Fact Table View
SELECT * FROM gold.fact_sales;

-- -----------------------------------------------------------------------------------
-- Performing EDA and some Basic Analysis
-- -----------------------------------------------------------------------------------

-- Retrieve a list of unique countries from which customers originate
SELECT
	DISTINCT country AS Country
FROM gold.dim_customers
WHERE country!='N/A';

-- Retrieve a list of unique categories, sub-categories, and products
SELECT
	DISTINCT category,
    subcategory,
    product_name
FROM gold.dim_products
ORDER BY category, subcategory, product_name;

-- Determine the first and last order date and the total duration in months 2010-12-29   2014-01-28
SELECT
	MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) AS duration_in_months
FROM gold.fact_sales;

-- Find the youngest and oldest customer based on birthdate
SELECT
	MIN(birthdate) AS oldest_customer,
    TIMESTAMPDIFF(YEAR, MIN(birthdate), CURDATE()) AS oldest_customer_age,
    MAX(birthdate) AS youngest_customer,
    TIMESTAMPDIFF(YEAR, MAX(birthdate), CURDATE()) AS youngest_customer_age
FROM gold.dim_customers;

-- Find total sales
SELECT
	SUM(sales_amount) AS total_sales
FROM gold.fact_sales;

-- Find total items sold
SELECT
	SUM(quantity) AS total_items_sold
FROM gold.fact_sales;

-- Find average selling price
SELECT
	AVG(price) AS avg_selling_price
FROM gold.fact_sales;

-- Find total number of orders
SELECT
	COUNT(order_number) AS total_orders,
    COUNT(DISTINCT order_number) AS total_distinct_orders
FROM gold.fact_sales;

-- Find total number of products
SELECT
	COUNT(product_name) AS total_products
FROM gold.dim_products;

-- Find total number of customers
SELECT
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers;

-- Find total number of customers that has placed an order
SELECT
	COUNT(DISTINCT customer_key) AS customers_with_order_placed
FROM gold.dim_customers;

-- Generating a Report
SELECT "Total Sales" AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION
SELECT "Total Items Sold" AS measure_name, SUM(quantity) AS measure_value FROM gold.fact_sales
UNION
SELECT "Average Selling Price" AS measure_name, AVG(price) AS measure_value FROM gold.fact_sales
UNION
SELECT "Total No.of Orders" AS measure_name, COUNT(DISTINCT order_number) AS measure_value FROM gold.fact_sales
UNION
SELECT "Total No.of Products" AS measure_name, COUNT(product_name) AS measure_value FROM gold.dim_products
UNION
SELECT "Total No.of Customers" AS measure_name, COUNT(customer_key) AS measure_value FROM gold.dim_customers
UNION
SELECT "Total No.of Customers who placed an order" AS measure_name, COUNT(DISTINCT customer_key) AS measure_value FROM gold.dim_customers;
