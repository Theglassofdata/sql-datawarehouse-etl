
    -- Create schema if it doesn't exist
    CREATE SCHEMA IF NOT EXISTS MY_SQL_DATABASE.MY_SILVER_SCHEMA;
    
    -- Grant permissions
    GRANT USAGE ON DATABASE MY_SQL_DATABASE TO ROLE ACCOUNTADMIN;
    GRANT USAGE ON SCHEMA MY_SQL_DATABASE.MY_SILVER_SCHEMA TO ROLE ACCOUNTADMIN;
    GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA MY_SQL_DATABASE.MY_SILVER_SCHEMA TO ROLE ACCOUNTADMIN;
    
    -- Step 2: Create tables if they don't exist (with explicit schema)
    -- CRM tables
    CREATE TABLE IF NOT EXISTS MY_SQL_DATABASE.MY_SILVER_SCHEMA.crm_customer_info (
        cst_id FLOAT,
        cst_key VARCHAR(50),
        cst_firstname VARCHAR(100),
        cst_lastname VARCHAR(100),
        cst_marital_status VARCHAR(20),
        cst_gndr VARCHAR(10),
        cst_create_date VARCHAR(50),
        dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL
    );
    
    CREATE TABLE IF NOT EXISTS MY_SQL_DATABASE.MY_SILVER_SCHEMA.crm_product_info (
        prd_id INTEGER,
        cat_id VARCHAR(50),
        prd_key VARCHAR(50),
        prd_nm VARCHAR(255),
        prd_cost FLOAT,
        prd_line VARCHAR(100),
        prd_start_dt DATE,
        prd_end_dt DATE,
        dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL
    );
    
    CREATE TABLE IF NOT EXISTS MY_SQL_DATABASE.MY_SILVER_SCHEMA.crm_sales_details (
        sls_ord_num VARCHAR(50),
        sls_prd_key VARCHAR(50),
        sls_cust_id INTEGER,
        sls_order_dt DATE,
        sls_ship_dt DATE,
        sls_due_dt DATE,
        sls_sales FLOAT,
        sls_quantity INTEGER,
        sls_price FLOAT
    );
        
    -- ERP tables
    CREATE TABLE IF NOT EXISTS MY_SQL_DATABASE.MY_SILVER_SCHEMA.erp_customer_birthday (
        CID VARCHAR(50),
        BDATE DATE,
        GEN VARCHAR(10)
    );
        
    CREATE TABLE IF NOT EXISTS MY_SQL_DATABASE.MY_SILVER_SCHEMA.erp_customer_location (
        CID VARCHAR(50),
        CNTRY VARCHAR(100)
    );
        
    CREATE TABLE IF NOT EXISTS MY_SQL_DATABASE.MY_SILVER_SCHEMA.erp_product_category (
        ID VARCHAR(50),
        CAT VARCHAR(100),
        SUBCAT VARCHAR(100),
        MAINTENANCE VARCHAR(100)
    );
    
    -- Truncate tables to ensure clean loading
        TRUNCATE TABLE MY_SQL_DATABASE.MY_SILVER_SCHEMA.crm_customer_info;
        TRUNCATE TABLE MY_SQL_DATABASE.MY_SILVER_SCHEMA.crm_product_info;
        TRUNCATE TABLE MY_SQL_DATABASE.MY_SILVER_SCHEMA.crm_sales_details;
        TRUNCATE TABLE MY_SQL_DATABASE.MY_SILVER_SCHEMA.erp_customer_birthday;
        TRUNCATE TABLE MY_SQL_DATABASE.MY_SILVER_SCHEMA.erp_customer_location;
        TRUNCATE TABLE MY_SQL_DATABASE.MY_SILVER_SCHEMA.erp_product_category;
