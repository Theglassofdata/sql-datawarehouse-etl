CREATE OR REPLACE PROCEDURE LOAD_BRONZE()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  // Results and logging variables
  var result_msg = '';
  var total_rows_loaded = 0;
  var batch_start_time = new Date();
  var file_counts = {};
  
  function log(message) {
    result_msg += message + '\n';
  }
  
  function getTimeElapsed(startTime) {
    var endTime = new Date();
    var elapsed = (endTime - startTime) / 1000; // in seconds
    return elapsed.toFixed(2);
  }
  
  function processBatch(tableName, fileName, description) {
    log('------------------------------------------------');
    log('Processing: ' + description);
    log('------------------------------------------------');
    
    // Start time for this batch
    var start_time = new Date();
    log('>> Start time: ' + start_time.toLocaleTimeString());
    
    // Truncate table
    log('>> Truncating table: ' + tableName);
    var truncate_stmt = snowflake.createStatement({
      sqlText: 'TRUNCATE TABLE ' + tableName
    });
    truncate_stmt.execute();
    
    // Load data
    log('>> Loading data from: ' + fileName);
    var copy_stmt = snowflake.createStatement({
      sqlText: `COPY INTO ${tableName}
                FROM @MY_PROJECT_STAGE/${fileName}
                FILE_FORMAT = my_csv_format
                FORCE = TRUE
                ON_ERROR = 'CONTINUE'`
    });
    
    var copy_result = copy_stmt.execute();
    var rows_loaded = 0;
    
    // Count rows loaded
    if (copy_result.next()) {
      rows_loaded = copy_result.getColumnValue(1);
      file_counts[tableName] = rows_loaded;
      total_rows_loaded += rows_loaded;
    }
    
    // Run count to verify
    var count_stmt = snowflake.createStatement({
      sqlText: `SELECT COUNT(*) FROM ${tableName}`
    });
    var count_result = count_stmt.execute();
    var row_count = 0;
    
    if (count_result.next()) {
      row_count = count_result.getColumnValue(1);
    }
    
    // Complete timing
    var end_time = new Date();
    var duration = getTimeElapsed(start_time);
    
    log('>> Rows loaded: ' + rows_loaded);
    log('>> Table row count: ' + row_count);
    log('>> End time: ' + end_time.toLocaleTimeString());
    log('>> Duration: ' + duration + ' seconds');
    log('------------------------------------------------\n');
    
    return rows_loaded;
  }

  try {
    // Setup - Create file format
    var create_format_stmt = snowflake.createStatement({
      sqlText: `CREATE OR REPLACE FILE FORMAT my_csv_format
                TYPE = 'CSV'
                SKIP_HEADER = 1
                FIELD_DELIMITER = ','
                TRIM_SPACE = TRUE
                NULL_IF = ('NULL', 'null', '')
                ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;`
    });
    create_format_stmt.execute();
    
    // Start the batch process
    log('==================================================');
    log('BRONZE LAYER DATA LOAD PROCESS STARTED');
    log('Start time: ' + batch_start_time.toLocaleString());
    log('==================================================\n');
    
    // Process CRM tables
    log('==================================================');
    log('LOADING CRM TABLES');
    log('==================================================');
    
    processBatch('crm_customer_info', 'cust_info.csv', 'Customer Information');
    processBatch('crm_product_info', 'prd_info.csv', 'Product Information');
    processBatch('crm_sales_details', 'sales_details.csv', 'Sales Transactions');
    
    // Process ERP tables
    log('==================================================');
    log('LOADING ERP TABLES');
    log('==================================================');
    
    processBatch('erp_customer_birthday', 'CUST_AZ12.csv', 'Customer Birthday Information');
    processBatch('erp_customer_location', 'LOC_A101.csv', 'Customer Location Information');
    processBatch('erp_product_category', 'PX_CAT_G1V2.csv', 'Product Category Information');
    
    // Batch completion
    var batch_end_time = new Date();
    var batch_duration = getTimeElapsed(batch_start_time);
    
    log('==================================================');
    log('BRONZE LAYER DATA LOAD PROCESS COMPLETED');
    log('End time: ' + batch_end_time.toLocaleString());
    log('Total duration: ' + batch_duration + ' seconds');
    log('Total rows loaded: ' + total_rows_loaded);
    log('==================================================');
    log('\nTable Summary:');
    
    for (var table in file_counts) {
      log('- ' + table + ': ' + file_counts[table] + ' rows');
    }
    
    return result_msg;
    
  } catch (err) {
    // Error handling
    var batch_error_time = new Date();
    
    log('==================================================');
    log('ERROR OCCURRED DURING BRONZE LAYER DATA LOAD');
    log('Error time: ' + batch_error_time.toLocaleString());
    log('Error message: ' + err.message);
    log('Error code: ' + err.code);
    log('Error state: ' + err.state);
    log('Error stack: ' + err.stackTraceTxt);
    log('==================================================');
    
    return result_msg;
  }
$$;

-- Execute the procedure
CALL LOAD_BRONZE();

