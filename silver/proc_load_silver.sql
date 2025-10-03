/*
************************************************************
Stored Procedure: LoadSilver Layer (Bronze -> Silver)
************************************************************
  Script Purpose:
      This stored procedure performs the ETL (Extract, Transform and Load)
      process to populate the 'silver' schema from the 'bronze' schema.
  Actions Performed:
    - Truncate silver table.
    - Insert transformed and cleaned data from the bronze into silver table.
  
  Parameters:
    None
    This stored procedure does not accept any parameters or return any values.
  
  Usage Example:
    EXEC silver.load_silver;

************************************************************
*/


EXEC silver.load_silver

	-- Complete insert into silver table


-- Using a stored procedure
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE  @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY

		SET @batch_start_time = GETDATE();
		PRINT '===============================================';
		PRINT 'Loading Silver Layer';
		PRINT '===============================================';

		PRINT'------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT'------------------------------------------------';
	

		-- ====================silver.crm_cust_info===============================

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>>> Inserting Data into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)

		-- Transformations
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,

		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'unknown'
		END cst_marital_status,

		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'unknown'
		END cst_gndr,
		cst_create_date

		FROM(
			select 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last     
			from bronze.crm_cust_info
			WHERE  cst_id is NOT NULL
		)t WHERE flag_last = 1
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>> -----------------------'


		-- ===========================silver.crm_prd_info==========================================

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>>> Inserting Data into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
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
		-- To extract part of the strings in the prd_key to match the cat_id in the bronze.erp_px_cat_g1v2 table
		-- in order to join the cat_id in cat_g1v2 and id prd_info, we have to change "-" to "_" to have matchin info
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,

		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'unknown'
		END AS prd_line,

		CAST (prd_start_dt AS DATE) AS prd_start_dt,
		CAST (
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ) -1 AS DATE
		) 
		AS prd_end_dt -- result: about 200 rows have lower end dates than start dates. so we have to fix that by
		-- making the start date of the next record for each product key the end date(minus 1 day) for the previous record

		from bronze.crm_prd_info

		SET @end_time = GETDATE()
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>> -----------------------'


		-- ==============================silver.crm_sales_details========================================
		
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>>> Inserting Data into: silver.crm_sales_details';
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

		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,

		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,

		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,


		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,

		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != (sls_quantity * ABS(sls_price))
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,

		sls_quantity,

		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0) 
			ELSE sls_price
		END AS sls_price

		FROM
		bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>> -----------------------'

		-- ===============================silver.erp_cust_az12=======================================

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>>> Inserting Data into: silver.erp_cust_az12';
		INSERT INTO  silver.erp_cust_az12(
		cid,
		bdate,
		gen
		) 
		-- clean and store bronze.erp_cust_az12 in the silver table
		select 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
		END AS cid,

		CASE WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,

		CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			ELSE 'unknown'
		END AS gen

		from bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>> -----------------------'

		-- =============================silver.erp_loc_a101=========================================

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>>> Inserting Data into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
		)
		-- clean and load erp_loc_a101
		select 
		REPLACE (cid, '-','') cid,
		CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
			 WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
			 WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'unknown'
			 ELSE TRIM(cntry)
		END AS cntry
		from bronze.erp_loc_a101
		SET @end_time= GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>> -----------------------'

		-- =============================silver.erp_px_cat_g1v2=========================================

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>>> Inserting Data into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2
		(
		id,
		cat,
		subcat,
		maintenance
		)
		-- clean and load erp_px_cat_g1v2 into the silver table
		SELECT 

		id,
		cat,
		subcat,
		maintenance

		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>> -----------------------';
		
		SET @batch_end_time =GETDATE();
		PRINT '=======================================';
		PRINT 'Loading Silver Layer is Completed';
		PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=======================================';
	END TRY

	BEGIN CATCH
	PRINT '======================================='
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '======================================='
	END CATCH
END
