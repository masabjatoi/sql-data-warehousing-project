/*
Data Warehouse Bronze Layer Table Creation Script

Purpose:
This script creates the initial bronze layer tables in a data warehouse schema.
The bronze layer stores raw, unprocessed data as it comes from source systems.

Tables Created:
1. bronze.crm_cust_info       - Customer data from CRM system
2. bronze.crm_prd_info        - Product data from CRM system
3. bronze.crm_sales_details   - Sales transaction details from CRM
4. bronze.erp_loc_a101        - Location data from ERP system
5. bronze.erp_cust_az12      - Customer demographic data from ERP
6. bronze.erp_px_cat_g1v2    - Product category data from ERP

Important Notes:
- Script DROPS existing tables before creation (destructive operation)
- Date formats are inconsistent across tables:
  * crm_cust_info.cst_create_date uses DATE type
  * crm_prd_info uses DATETIME for date ranges
  * crm_sales_details stores dates as INT (needs transformation)
- Column naming conventions vary between CRM and ERP sources
- No primary keys or constraints are defined (typical for bronze layer)

Execution Instructions:
1. Ensure database context is set to DataWarehouse
2. Verify you have permissions to create/drop tables in bronze schema
3. Run entire script to set up all tables
*/



USE DataWarehouse;

IF OBJECT_ID('bronze.crm_cust_info' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info
(
	cst_id				INT , 
	cst_key				NVARCHAR(50),
	cst_firstname		NVARCHAR(50),
	cst_lastname		NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr			NVARCHAR(50),
	cst_create_date		DATE

);

IF OBJECT_ID('bronze.crm_prd_info' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info 
(
	prd_id		 INT,
	prd_key		 NVARCHAR(50),
	prd_nm		 NVARCHAR(50),
	prd_cost	 INT,
	prd_line	 NVARCHAR(50),
	prd_start_dt DATETIME , 
	prd_end_dt	 DATETIME
);

IF OBJECT_ID('bronze.crm_sales_details' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details

(
	sls_ord_num    NVARCHAR(50),
	sls_prd_key	   NVARCHAR(50),
	sls_cust_id    INT,
	sls_order_dt   INT,
	sls_ship_dt    INT,
	sls_due_dt     INT,
	sls_sales      INT,
	sls_quantity   INT,
	sls_price      INT
);

IF OBJECT_ID('bronze.erp_loc_a101' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101
(
	cid   NVARCHAR(50),
	cntry  NVARCHAR(50),
);

IF OBJECT_ID('bronze.erp_cust_az12' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12
(
	cid		NVARCHAR(50),
	bdate	DATE,
	gen		NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_px_cat_g1v2' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 
(
	id			NVARCHAR(50),
	cat			NVARCHAR(50),
	subcat		NVARCHAR(50),
	maintenance NVARCHAR(50)
);




