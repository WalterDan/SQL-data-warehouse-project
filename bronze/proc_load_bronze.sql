/*
===========================================================================
Stored Procedure: Load Bronze Layer (souce -> Bronze)
===========================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV file.
  It Performs the following actions:
  - Truncates the bronze table before loading the data.
  - Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

Parameters:
  None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC bronze.load_bronze;
===========================================================================
*/

-- create a stored procedure for this script because it is frequently used
CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY	-- handling errors
	SET @batch_start_time = GETDATE();
	-- Add Prints to track execution, debug issues, and understand its flow

		PRINT '==================================================';
		PRINT 'Loading the Bronze Layer';
		PRINT '==================================================';


		PRINT '.....................................................';
		PRINT 'Loading CRM Tables';
		PRINT '.....................................................';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info'
		-- making the table empty before loading
		TRUNCATE TABLE bronze.crm_cust_info
		-- Full-Load the data
		PRINT '>> Inserting Data Into: bronze.crm_cust_info'

		BULK INSERT  bronze.crm_cust_info
		FROM 'C:\Users\USER\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.CSV'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------------------------------------'



		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info'
		-- making the table empty before loading
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '>> Inserting Data Into: bronze.crm_prd_info'
		-- Full-Load the data
		BULK INSERT  bronze.crm_prd_info
		FROM 'C:\Users\USER\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.CSV'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------------------------------------'


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details'

		-- making the table empty before loading
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>> Inserting Data Into: bronze.crm_sales_details'
		-- Full-Load the data
		BULK INSERT  bronze.crm_sales_details
		FROM 'C:\Users\USER\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.CSV'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------------------------------------'



		PRINT '.....................................................';
		PRINT 'Loading ERP Tables';
		PRINT '.....................................................';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101'
		-- making the table empty before loading
		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101'
		-- Full-Load the data
		BULK INSERT  bronze.erp_loc_a101
		FROM 'C:\Users\USER\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------------------------------------'


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12'
		-- making the table empty before loading
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12'
		-- Full-Load the data
		BULK INSERT  bronze.erp_cust_az12
		FROM 'C:\Users\USER\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------------------------------------'


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
		-- making the table empty before loading
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2'
		-- Full-Load the data
		BULK INSERT  bronze.erp_px_cat_g1v2
		FROM 'C:\Users\USER\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------------------------------------'

		SET @batch_end_time = GETDATE();
		PRINT '==============================================='
		PRINT 'Loading BronzeLayer is Completed';
		PRINT '       _Total Load Duration:' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' second';
		PRINT '==============================================='

	END TRY
	BEGIN CATCH
		PRINT '============================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '============================================='
	END CATCH
END
