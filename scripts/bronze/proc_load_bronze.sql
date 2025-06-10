/*
Data Warehouse Bronze Layer Load Procedure

Purpose:
This stored procedure loads data from CSV files into the bronze layer tables.
It handles both CRM and ERP system data sources with error handling and performance tracking.

Process Flow:
1. Truncates target tables
2. Bulk inserts data from CSV files
3. Tracks and reports load durations for each table
4. Provides comprehensive error handling

Parameters: None

Source Files:
CRM Data:
- cust_info.csv → bronze.crm_cust_info
- prd_info.csv → bronze.crm_prd_info
- sales_details.csv → bronze.crm_sales_details

ERP Data:
- LOC_A101.csv → bronze.erp_loc_a101
- CUST_AZ12.csv → bronze.erp_cust_az12
- PX_CAT_G1V2.csv → bronze.erp_px_cat_g1v2

Important Notes:
1. File Paths:
   - Hardcoded to 'D:\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\'
   - Will need modification for different environments

2. CSV Format Assumptions:
   - First row contains headers (FIRSTROW = 2)
   - Comma delimited (FIELDTERMINATOR = ',')
   - Line endings with \n (ROWTERMINATOR = '\n')

3. Performance:
   - TABLOCK hint used for bulk insert operations
   - Individual table load times tracked
   - Total duration calculated

4. Error Handling:
   - Comprehensive TRY/CATCH block
   - Reports error message, number, and state

Execution:
EXEC bronze.load_bronze;

Dependencies:
1. All bronze layer tables must exist
2. SQL Server must have file system access to CSV locations
3. Appropriate permissions to truncate tables and bulk insert
*/




CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start DATETIME , @batch_end DATETIME 
BEGIN TRY
	SET @batch_start = GETDATE();
    PRINT '==================================';
    PRINT 'Loading Bronze Layer';
    PRINT '==================================';

    PRINT '-------------------------------------';
    PRINT 'Loading CRM Tables';
    PRINT '-------------------------------------';
    
	set @start_time = GETDATE()
    PRINT '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    PRINT '>> Inserting the data into : bronze.crm_cust_info';
    BULK INSERT bronze.crm_cust_info
    FROM 'D:\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
	set @end_time =  GETDATE();
	PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND , @start_time , @end_time) as NVARCHAR ) + ' second';
	PRINT '-------------------------'

	set @start_time = GETDATE()
    PRINT '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    PRINT '>> Inserting the data into : bronze.crm_prd_info';
    BULK INSERT bronze.crm_prd_info
    FROM 'D:\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
	SET @end_time =GETDATE();
	PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND , @start_time , @end_time) as NVARCHAR ) + ' second';
	PRINT '-------------------------'

	set @start_time = GETDATE()
    PRINT '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    PRINT '>> Inserting the data into : bronze.crm_sales_details';
    BULK INSERT bronze.crm_sales_details
    FROM 'D:\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );

	SET @end_time = GETDATE(); 
	PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND , @start_time , @end_time) as NVARCHAR ) + ' second';
	PRINT '-------------------------'

    PRINT '-------------------------------------';
    PRINT 'Loading ERP Tables';
    PRINT '-------------------------------------';
    
	set @start_time = GETDATE()
    PRINT '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    PRINT '>> Inserting the data into : bronze.erp_loc_a101';
    BULK INSERT bronze.erp_loc_a101
    FROM 'D:\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
	
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND , @start_time , @end_time) as NVARCHAR ) + ' second';
	PRINT '-------------------------';

	set @start_time = GETDATE()
    PRINT '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    PRINT '>> Inserting the data into : bronze.erp_cust_az12';
    BULK INSERT bronze.erp_cust_az12
    FROM 'D:\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );

	SET @end_time =  GETDATE();
	PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND , @start_time , @end_time) as NVARCHAR ) + ' second';
	PRINT '-------------------------';

	set @start_time = GETDATE()
    PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    PRINT '>> Inserting the data into : bronze.erp_px_cat_g1v2';
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'D:\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
	set @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND , @start_time , @end_time) as NVARCHAR ) + ' second';
	PRINT '-------------------------';
	
	SET @batch_end = GETDATE();
	PRINT 'Loading bronze layer is completed';
	PRINT '-- Total duration is : ' + CAST(DATEDIFF(second, @batch_start , @batch_end) as NVARCHAR ) +  ' Second'

END TRY
BEGIN CATCH
    PRINT '=======================================================';
    PRINT 'There is some error occured while loading bronze layer';
    PRINT 'The error message is ' + ERROR_MESSAGE();
    PRINT 'The error number is ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
    PRINT 'The error state is '+ CAST(ERROR_STATE() AS NVARCHAR(10));
    PRINT '=======================================================';
END CATCH
END


EXEC bronze.load_bronze;

