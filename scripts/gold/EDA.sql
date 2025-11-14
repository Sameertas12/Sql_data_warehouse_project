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

-- Find total customers by countries
SELECT
	country,
    COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- Find total customers by gender
SELECT
	gender,
    COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC;

-- Find total products by category
SELECT
	category,
    COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC;

-- What is the Average costs in each category
SELECT
	category,
    AVG(cost) AS Average_cost
FROM gold.dim_products
GROUP BY category
ORDER BY Average_cost DESC;

-- What is the total revenue generated for each category
SELECT
	dp.category,
    SUM(fs.sales_amount) AS Total_revenue
FROM gold.dim_products dp
LEFT JOIN gold.fact_sales fs
ON dp.product_key=fs.product_key
GROUP BY dp.category
ORDER BY Total_revenue DESC;

-- Ranking Analysis
SELECT
    p.product_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC LIMIT 5;

-- Complex but Flexibly Ranking Using Window Functions
SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(f.sales_amount) AS total_revenue,
        RANK() OVER (ORDER BY SUM(f.sales_amount) DESC) AS rank_products
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
    GROUP BY p.product_name
) AS ranked_products
WHERE rank_products <= 5;

-- What are the 5 worst-performing products in terms of sales?
SELECT
    p.product_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue LIMIT 5;

-- Find the top 10 customers who have generated the highest revenue
SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_revenue DESC LIMIT 10;

-- The 3 customers with the fewest orders placed
SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_orders LIMIT 3;
