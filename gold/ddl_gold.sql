-- This SQL script creates three views for the gold layer: dim_products, fact_sales, and dim_customers. 
-- These views transform and join cleaned data from the silver layer to support analytics and reporting in a star schema format.

CREATE VIEW gold.dim_products AS
SELECT 
ROW_NUMBER() OVER(ORDER BY pn.prd_key , pn.prd_start_dt) as product_key,
pn.prd_id		AS product_id,
pn.prd_key		AS product_number, 
pn.prd_nm		AS product_name,   
pn.cat_id		AS category_id,	   
pc.cat			AS category,	   
pc.subcat		AS subcategory,	   
pc.maintenance,					   
pn.prd_cost		AS product_cost,   
pn.prd_line		AS product_line,
pn.prd_start_dt AS start_date
FROM 
silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc 
on pn.cat_id = pc.id
where prd_end_dt IS NULL;

CREATE VIEW gold.fact_sales AS
SELECT 
sd.sls_ord_num		AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt		AS order_date,
sd.sls_ship_date	AS shipping_date,
sd.sls_due_dt		AS due_date,
sd.sls_sales		AS sale_amount,
sd.sls_quantity		AS sales_quantity,
sd.sls_price		AS sales_price
FROM 
silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
on sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id;

CREATE VIEW gold.dim_customers AS
SELECT
ROW_NUMBER() OVER(ORDER BY cst_id) as customer_key,
ci.cst_id AS customer_id , 
ci.cst_key AS customer_number ,
ci.cst_firstname AS  first_name,
ci.cst_lastname AS last_name,
la.cntry as country,
ci.cst_marital_status AS martial_status,
CASE WHEN ci.cst_gndr != 'n/a' then ci.cst_gndr
	ELSE COALESCE(ca.gen , 'n/a')
END AS gender,
ci.cst_create_date AS create_date,
ca.bdate AS birthdate
FROM 
silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca  
on		ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la 
on		ci.cst_key = la.cid;
