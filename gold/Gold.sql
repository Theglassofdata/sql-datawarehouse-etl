GRANT USAGE ON DATABASE MY_SQL_DATABASE TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA MY_SQL_DATABASE.MY_GOLD_SCHEMA TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA MY_GOLD_SCHEMA TO ROLE ACCOUNTADMIN;

CREATE SCHEMA IF NOT EXISTS MY_SQL_DATABASE.MY_GOLD_SCHEMA;


CREATE OR REPLACE VIEW MY_SQL_DATABASE.MY_GOLD_SCHEMA.GOLD_DIM_CUSTOMERS AS
SELECT 
row_number() over (order by ci.cst_id) as customer_key,
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as firstname,
    ci.cst_lastname as lastname,
            CASE WHEN ci.cst_gndr !='N/A' THEN ci.cst_gndr
            ELSE COALESCE(CA.GEN,'N/A') 
    END AS gender,
    la.cntry as country,
    ca.bdate as birth_date,
    ci.cst_marital_status as marital_status,
    ci.cst_create_date as create_date
FROM 
    my_sql_database.my_silver_schema.crm_customer_info ci
LEFT JOIN 
    my_sql_database.my_silver_schema.erp_customer_birthday ca
    ON ci.cst_key = ca.cid
LEFT JOIN 
    my_sql_database.my_silver_schema.erp_customer_location la
    ON ci.cst_key = la.cid;



CREATE OR REPLACE VIEW MY_SQL_DATABASE.MY_GOLD_SCHEMA.GOLD_DIM_PRODUCTS AS
SELECT 
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt,pn.prd_key) as product_key, 
    pn.prd_id AS PRODUCT_ID, 
    pn.prd_key AS PRODUCT_NUMBER,
    pn.prd_nm AS PRODUCT_NAME,
    pn.cat_id AS CATEGORY_ID,
    pc.subcat AS SUBCATEGORY,
    pn.prd_cost AS COST,
    pn.prd_line AS PRODUCT_LINE,
    pn.prd_start_dt AS START_DATE,
    pc.cat AS CATEGORY,
    pc.maintenance
from my_sql_database.my_silver_schema.crm_product_info pn
left join my_sql_database.my_silver_schema.erp_product_category pc
on pn.cat_id = pc.id
WHERE prd_end_dt is null;




CREATE OR REPLACE VIEW MY_SQL_DATABASE.MY_GOLD_SCHEMA.GOLD_DIM_SALES AS
SELECT    
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM MY_SQL_DATABASE.MY_SILVER_SCHEMA.CRM_SALES_DETAILS csd
LEFT JOIN MY_SQL_DATABASE.MY_GOLD_SCHEMA.GOLD_DIM_PRODUCTS gdp
ON csd.sls_prd_key = gdp.product_number
LEFT JOIN MY_SQL_DATABASE.MY_GOLD_SCHEMA.GOLD_DIM_CUSTOMERS gdc
on csd.sls_cust_id = gdc.customer_id;
