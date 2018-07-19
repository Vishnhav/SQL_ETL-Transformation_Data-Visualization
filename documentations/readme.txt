1. Connect to your local MySQL server via a client.

2. Open pentacho. unzip the folder and copy the contents to a location. 

3. The full path in my local system is as follows. (This is just for reference)	

	- C:\Users\vishn\Desktop\ \526\Assignments\week11
	
* NOTE * - Make sure you copy the folder with the name 'week11' as the ETL path is upto 'week11' folder. 

4. After connecting to your local MySQL server via a client and execute the ddl script below: (This contains the updated DDL scripts)
    documentations/ddl-scripts.sql


5. Launch Pentaho Kettle, go to “Edit” —-> “Edit the kettle.properties file”, and make necessary changes to the variables below:
    - ETL_USER_NAME
    - ETL_USER_PASS
    - ETL_HOME_DIR
    - DIM_DATE_END  : Set the value to 2020-12-31
    - DIM_DATE_START: Set the value to 2010-01-01


6. Open the job below in the main folder and execute it:
    main-job.kjb

4. Upon successful execution of the previous step, run the audit query below and verify that the row counts between the source and target match:

   /* Fact Granuality */
   SELECT 'source_db.gs_orders' AS table_name
         , COUNT(*) AS cnt_num 
      FROM source_db.gs_orders
     UNION ALL  
    SELECT 'datamart_db.fact_orders' AS table_name
         , COUNT(*) AS cnt_num 
      FROM datamart_db.fact_orders;

  /* State Information */
  SELECT 'source' AS table_name, COUNT(*)
    FROM stage_db.stg_state
   UNION ALL  
  SELECT 'target' AS table_name, COUNT(*)
    FROM datamart_db.dim_geography;        
    
  /* Order Date and Ship Date */      
  SELECT 'source_db.gs_orders', SUM(CONVERT(DATE_FORMAT(`Ship Date`, '%Y%m%d'), signed integer) - CONVERT(DATE_FORMAT(`Order Date`, '%Y%m%d'), signed integer)) AS total_date_diff
    FROM source_db.gs_orders
   UNION ALL
  SELECT 'target_db.gs_orders', SUM(ship_date_skey - order_date_skey) AS total_date_diff
    FROM datamart_db.fact_orders ;   
	
	
	----------------------------------------------------------------------------------------------------
	
	SELECT group_seq
     , MAX(IF(table_type = 'source', table_name,  NULL)) AS source
     , MAX(IF(table_type = 'target', table_name,  NULL)) AS target
     , SUM(IF(table_type = 'source', audit_value, NULL)) AS source_value
     , SUM(IF(table_type = 'target', audit_value, NULL)) AS target_value
     , IF (SUM(IF(table_type = 'source', audit_value, NULL)) = SUM(IF(table_type = 'target', audit_value, NULL)), 'PASS', 'FAIL') audit_result
  FROM
       (
        /* Fact Granuality */
        SELECT 1 AS group_seq, 'source' AS table_type, 'gs_orders count' AS table_name, COUNT(*) AS audit_value
          FROM source_db.gs_orders
         UNION ALL  
        SELECT 1 AS group_seq, 'target' AS table_type, 'fact_orders count' AS table_name, COUNT(*) AS audit_value
          FROM datamart_db.fact_orders

         UNION ALL        

         /* Order Date and Ship Date */      
        SELECT 2 AS group_seq, 'source' AS table_type, 'gs_orders data_diff' AS table_name, 
               SUM(CONVERT(DATE_FORMAT(`Ship Date`, '%Y%m%d'), SIGNED INTEGER) - CONVERT(DATE_FORMAT(`Order Date`, '%Y%m%d'), SIGNED INTEGER)) AS audit_value
          FROM source_db.gs_orders
         UNION ALL
        SELECT 2 AS group_seq, 'target' AS table_type, 'fact_orders date_diff' AS table_name, SUM(ship_date_skey - order_date_skey) AS audit_value
          FROM datamart_db.fact_orders 

         UNION ALL

         /* City Information */
        SELECT 3 AS group_seq, 'source' AS table_type, 'stg_city count' AS table_name, COUNT(*) AS audit_value
          FROM stage_db.stg_city
         UNION ALL  
        SELECT 3 AS group_seq, 'target' AS table_type, 'dim_geography count' AS table_name, COUNT(*) AS audit_value
          FROM datamart_db.dim_geography
       ) a 
 GROUP BY group_seq