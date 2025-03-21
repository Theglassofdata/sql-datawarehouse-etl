CREATE OR REPLACE PROCEDURE LOAD_TO_SILVER()
    RETURNS STRING
    LANGUAGE SQL
AS
$$
BEGIN

-- Load Customer Info (Latest Record Per Customer)
INSERT INTO MY_SQL_DATABASE.MY_SILVER_SCHEMA.crm_customer_info (
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
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'N/A'
    END,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'N/A'
    END,
    cst_create_date
FROM (
    SELECT *, 
           RANK() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM MY_SQL_DATABASE.MY_BRONZE_SCHEMA.crm_customer_info
    WHERE cst_id IS NOT NULL
)
WHERE flag_last = 1;

-- Load Product Info - FIXED to set NULL end dates for current products
INSERT INTO MY_SQL_DATABASE.MY_SILVER_SCHEMA.CRM_PRODUCT_INFO (
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
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
    SUBSTRING(prd_key, 7, LENGTH(prd_key) - 6),
    prd_nm,
    COALESCE(prd_cost, 0),
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'N/A'
    END,
    CAST(prd_start_dt AS DATE),
    -- Fixed calculation: Only set end date if there is a newer version
    CAST(DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY SUBSTRING(prd_key, 7, LENGTH(prd_key) - 6) ORDER BY prd_start_dt)) AS DATE)
FROM MY_SQL_DATABASE.MY_BRONZE_SCHEMA.CRM_PRODUCT_INFO;

-- Load Sales Details
INSERT INTO MY_SQL_DATABASE.MY_SILVER_SCHEMA.crm_sales_details (
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
        WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS STRING)) != 8 THEN NULL
        ELSE TO_DATE(CAST(sls_order_dt AS STRING), 'YYYYMMDD')
    END,
    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8 THEN NULL
        ELSE TO_DATE(CAST(sls_ship_dt AS STRING), 'YYYYMMDD')
    END,
    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS STRING)) != 8 THEN NULL
        ELSE TO_DATE(CAST(sls_due_dt AS STRING), 'YYYYMMDD')
    END,
    COALESCE(
        NULLIF(sls_sales, 0),
        sls_quantity * ABS(sls_price)
    ),
    sls_quantity,
    COALESCE(
        NULLIF(sls_price, 0), 
        NULLIF(sls_sales, 0) / NULLIF(sls_quantity, 0)
    )
FROM MY_SQL_DATABASE.MY_BRONZE_SCHEMA.crm_sales_details;

-- Load Customer Birthday Data
INSERT INTO MY_SQL_DATABASE.MY_SILVER_SCHEMA.ERP_CUSTOMER_BIRTHDAY(
    cid,
    bdate,
    gen
)
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)  -- Remove 'NAS' prefix if present
        ELSE cid
    END AS cid,
    CASE
        WHEN bdate > CURRENT_DATE THEN NULL  -- Set future birthdates to NULL
        ELSE bdate
    END AS bdate,
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen  -- Normalize gender values and handle unknown cases
FROM my_sql_database.my_bronze_schema.erp_customer_birthday;

-- Load Customer Location Data
INSERT INTO MY_SQL_DATABASE.MY_SILVER_SCHEMA.erp_customer_location (
    cid,
    cntry
)
SELECT 
    REPLACE(cid, '-', ''),
    CASE 
        WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'U.S.', 'U.S.A', 'UNITED STATES') THEN 'United States'
        WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
        WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'N/A'
        ELSE TRIM(cntry)
    END
FROM my_sql_database.my_bronze_schema.erp_customer_location;

-- Load Product Category Data
INSERT INTO MY_SQL_DATABASE.MY_SILVER_SCHEMA.erp_product_category (
    id,
    cat,
    subcat,
    maintenance
)
SELECT DISTINCT
    id,
    UPPER(TRIM(cat)),
    UPPER(TRIM(subcat)),
    TRIM(maintenance)
FROM my_sql_database.my_bronze_schema.erp_product_category;

-- Success message
RETURN 'DATA LOADED TO SILVER SUCCESSFULLY';

END;
$$;

-- Call the Procedure
CALL LOAD_TO_SILVER();