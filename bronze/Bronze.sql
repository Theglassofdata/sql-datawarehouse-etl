CREATE OR REPLACE PROCEDURE LOAD_BRONZE()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Grant permissions
    GRANT USAGE ON DATABASE MY_SQL_DATABASE TO ROLE ACCOUNTADMIN;
    GRANT USAGE ON SCHEMA MY_BRONZE_SCHEMA TO ROLE ACCOUNTADMIN;
    GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA MY_BRONZE_SCHEMA TO ROLE ACCOUNTADMIN;
    GRANT READ ON STAGE MY_PROJECT_STAGE TO ROLE ACCOUNTADMIN;
    GRANT WRITE ON STAGE MY_PROJECT_STAGE TO ROLE ACCOUNTADMIN;
    
    -- Step 1: Create or update file format
    CREATE OR REPLACE FILE FORMAT my_csv_format
        TYPE = 'CSV'
        SKIP_HEADER = 1
        FIELD_DELIMITER = ','
        TRIM_SPACE = TRUE
        NULL_IF = ('NULL', 'null', '')
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
    
    -- Step 2: Create tables if they don't exist
    -- CRM tables
    CREATE TABLE IF NOT EXISTS MY_SQL_DATABASE.MY_BRONZE_SCHEMA.crm_customer_info (
        cst_id FLOAT,
        cst_key VARCHAR(50),
        cst_firstname VARCHAR(100),
        cst_lastname VARCHAR(100),
        cst_marital_status VARCHAR(20),
        cst_gndr VARCHAR(10),
        cst_create_date VARCHAR(50)
    );
    
    CREATE TABLE IF NOT EXISTS MY_SQL_DATABASE.MY_BRONZE_SCHEMA.crm_product_info (
        prd_id INTEGER,
        prd_key VARCHAR(50),
        prd_nm VARCHAR(255),
        prd_cost FLOAT,
        prd_line VARCHAR(100),
        prd_start_dt DATE,
        prd_end_dt DATE
    );
    
    CREATE TABLE IF NOT EXISTS MY_SQL_DATABASE.MY_BRONZE_SCHEMA.crm_sales_details (
        sls_ord_num VARCHAR(50),
        sls_prd_key VARCHAR(50),
        sls_cust_id INTEGER,
        sls_order_dt INTEGER,
        sls_ship_dt INTEGER,
        sls_due_dt INTEGER,
        sls_sales FLOAT,
        sls_quantity INTEGER,
        sls_price FLOAT
    );
    
    -- ERP tables
    CREATE TABLE IF NOT EXISTS MY_SQL_DATABASE.MY_BRONZE_SCHEMA.erp_customer_birthday (
        CID VARCHAR(50),
        BDATE DATE,
        GEN VARCHAR(10)
    );
    
    CREATE TABLE IF NOT EXISTS MY_SQL_DATABASE.MY_BRONZE_SCHEMA.erp_customer_location (
        CID VARCHAR(50),
        CNTRY VARCHAR(100)
    );
    
    CREATE TABLE IF NOT EXISTS MY_SQL_DATABASE.MY_BRONZE_SCHEMA.erp_product_category (
        ID VARCHAR(50),
        CAT VARCHAR(100),
        SUBCAT VARCHAR(100),
        MAINTENANCE VARCHAR(100)
    );
    
    -- Step 3: Truncate tables to ensure clean loading
    TRUNCATE TABLE MY_SQL_DATABASE.MY_BRONZE_SCHEMA.crm_customer_info;
    TRUNCATE TABLE MY_SQL_DATABASE.MY_BRONZE_SCHEMA.crm_product_info;
    TRUNCATE TABLE MY_SQL_DATABASE.MY_BRONZE_SCHEMA.crm_sales_details;
    TRUNCATE TABLE MY_SQL_DATABASE.MY_BRONZE_SCHEMA.erp_customer_birthday;
    TRUNCATE TABLE MY_SQL_DATABASE.MY_BRONZE_SCHEMA.erp_customer_location;
    TRUNCATE TABLE MY_SQL_DATABASE.MY_BRONZE_SCHEMA.erp_product_category;
    
    -- Step 4: Load data into each table
    COPY INTO MY_SQL_DATABASE.MY_BRONZE_SCHEMA.crm_customer_info FROM @MY_PROJECT_STAGE/cust_info.csv 
        FILE_FORMAT = my_csv_format 
        ON_ERROR = 'CONTINUE';
    
    COPY INTO MY_SQL_DATABASE.MY_BRONZE_SCHEMA.crm_product_info FROM @MY_PROJECT_STAGE/prd_info.csv 
        FILE_FORMAT = my_csv_format 
        ON_ERROR = 'CONTINUE';
    
    COPY INTO MY_SQL_DATABASE.MY_BRONZE_SCHEMA.crm_sales_details FROM @MY_PROJECT_STAGE/sales_details.csv 
        FILE_FORMAT = my_csv_format 
        ON_ERROR = 'CONTINUE';
    
    COPY INTO MY_SQL_DATABASE.MY_BRONZE_SCHEMA.erp_customer_birthday FROM @MY_PROJECT_STAGE/CUST_AZ12.csv 
        FILE_FORMAT = my_csv_format 
        ON_ERROR = 'CONTINUE';
    
    COPY INTO MY_SQL_DATABASE.MY_BRONZE_SCHEMA.erp_customer_location FROM @MY_PROJECT_STAGE/LOC_A101.csv 
        FILE_FORMAT = my_csv_format 
        ON_ERROR = 'CONTINUE';
    
    COPY INTO MY_SQL_DATABASE.MY_BRONZE_SCHEMA.erp_product_category FROM @MY_PROJECT_STAGE/PX_CAT_G1V2.csv 
        FILE_FORMAT = my_csv_format 
        ON_ERROR = 'CONTINUE';
    
    -- Step 5: Return success message
    RETURN 'Data load complete';
END;
$$;