/*
==================================================================================
    PROCEDURE: silver.load_silver
    PURPOSE  : Cleans and transforms data from the bronze layer to the silver layer.
    USAGE    : EXEC silver.load_silver;

----------------------------------------------------------------------------------
    📌 INSTRUCTIONS FOR USE:
    - Make sure the `bronze` layer tables are fully loaded before running this.
    - Execute `EXEC bronze.load_bronze` first if necessary.
    - Run this in a SQL Server environment that has access to `bronze` and `silver` schemas.

----------------------------------------------------------------------------------
    ⚠️ WARNINGS:
    - Ensure the destination `silver.*` tables already exist.
    - Review all transformation logic (e.g., date handling, gender normalization, etc.)
      before modifying or using on production data.
    - This procedure performs full truncation and reloading of each silver table.
    - Requires SQL Server 2016+ for `TRY_CAST` and `LEAD()` functions.
==================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver 
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;

    BEGIN TRY
        SET @batch_start = GETDATE();
        PRINT '==================================';
        PRINT 'Loading Silver Layer';
        PRINT '==================================';

        -------------------------------------------------------------------
        -- Table: silver.erp_loc_a101
        -------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting the data into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT  
            REPLACE(cid, '-', '') AS cid, 
            CASE 
                WHEN TRIM(cntry) IN ('DE') THEN 'Germany'
                WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '-------------------------';

        -------------------------------------------------------------------
        -- Table: silver.erp_cust_az12
        -------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting the data into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT 
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cid,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '-------------------------';

        -------------------------------------------------------------------
        -- Table: silver.crm_sales_details
        -------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting the data into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_date, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT 
            sls_ord_num, sls_prd_key, sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(CAST(sls_order_dt AS VARCHAR)) != 8 THEN NULL
                 ELSE TRY_CAST(CAST(sls_order_dt AS CHAR(8)) AS DATE) END,
            CASE WHEN sls_ship_dt = 0 OR LEN(CAST(sls_ship_dt AS VARCHAR)) != 8 THEN NULL
                 ELSE TRY_CAST(CAST(sls_ship_dt AS CHAR(8)) AS DATE) END,
            CASE WHEN sls_due_dt = 0 OR LEN(CAST(sls_due_dt AS VARCHAR)) != 8 THEN NULL
                 ELSE TRY_CAST(CAST(sls_due_dt AS CHAR(8)) AS DATE) END,
            CASE WHEN sls_sales IS NULL OR sls_sales < 0 OR sls_sales != sls_quantity * sls_price
                 THEN sls_quantity * ABS(sls_price) ELSE sls_sales END,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price < 0 THEN ABS(sls_sales / NULLIF(sls_quantity, 0))
                 ELSE sls_price END
        FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '-------------------------';

        -------------------------------------------------------------------
        -- Table: silver.erp_px_cat_g1v2
        -------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting the data into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance FROM bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '-------------------------';

        -------------------------------------------------------------------
        -- Table: silver.crm_prd_info
        -------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting the data into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost,
            prd_line, prd_start_dt, prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'R' THEN 'Road'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(
                DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (
                    PARTITION BY SUBSTRING(prd_key, 7, LEN(prd_key))
                    ORDER BY prd_start_dt
                )) AS DATE
            )
        FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '-------------------------';

        -------------------------------------------------------------------
        -- Table: silver.crm_cust_info
        -------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting the data into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_gndr, cst_material_status, cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                ELSE 'n/a'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_material_status)) = 'F' THEN 'Single'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
            FROM bronze.crm_cust_info
        ) T
        WHERE flag = 1;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' second';
        PRINT '-------------------------';

        -------------------------------------------------------------------
        -- Procedure End Summary
        -------------------------------------------------------------------
        SET @batch_end = GETDATE();
        PRINT 'Silver layer load completed successfully.';
        PRINT '-- Total duration: ' + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS NVARCHAR) + ' second';

    END TRY
    BEGIN CATCH
        PRINT '=======================================================';
        PRINT '❌ An error occurred while loading the silver layer.';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT '=======================================================';
    END CATCH
END;
