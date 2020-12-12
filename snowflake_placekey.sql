create or replace procedure APPEND_PLACEKEYS(TBL_QUERY VARCHAR(100), TBL_MAPPING VARCHAR(100), 
                                             API_FUNCTION VARCHAR(100),TBL_OUTPUT VARCHAR(100),NO_OF_RECORDS FLOAT)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS $$
    // Copy the input table and add a primary key
    
    var cmd_seq1 = "create or replace sequence placekey_temp_seq1 start = 0 increment = 1;"
    var cmd_copytable = "create or replace table PLACEKEYS_APPENDED_TEMP like "+TBL_QUERY+";"
    var cmd_alter = "alter table PLACEKEYS_APPENDED_TEMP ADD COLUMN primary_key INT DEFAULT placekey_temp_seq1.nextval;"
    var cmd_insert = "insert into PLACEKEYS_APPENDED_TEMP select *, seq1.nextval FROM "+TBL_QUERY+";"
    
    var stmt_seq1 = snowflake.createStatement( {sqlText: cmd_seq1} );
    var stmt_copytable = snowflake.createStatement( {sqlText: cmd_copytable} );
    var stmt_alter = snowflake.createStatement( {sqlText: cmd_alter} );
    var stmt_insert = snowflake.createStatement( {sqlText: cmd_insert} );
    
    
    var result_seq1 = stmt_seq1.execute();
    result_seq1.next();
    var result_copytable = stmt_copytable.execute();
    result_copytable.next();
    var result_alter = stmt_alter.execute();
    result_alter.next()
    var result_insert = stmt_insert.execute();
    result_insert.next()
    
      
    // Column mapping
    
    var cmd_map = "select * from "+TBL_MAPPING+";"
    var stmt_map = snowflake.createStatement( {sqlText: cmd_map} );
    var result_map = stmt_map.execute();
    result_map.next(); 
    c_location_name = result_map.getColumnValue("LOCATION_NAME");
    c_street_address = result_map.getColumnValue("STREET_ADDRESS");
    c_city = result_map.getColumnValue("CITY");
    c_region = result_map.getColumnValue("REGION");
    c_postal_code = result_map.getColumnValue("POSTAL_CODE");
    c_latitude = result_map.getColumnValue("LATITUDE");
    c_longitude = result_map.getColumnValue("LONGITUDE");
    c_country_code = result_map.getColumnValue("ISO_COUNTRY_CODE");
    
       
    // Query the API
    
    var cmd_payload = "create or replace table "+TBL_OUTPUT+" (RESULT STRING,OUTPUT_ID INTEGER,PLACEKEY VARCHAR(1000),ERROR VARCHAR(100));"
    var stmt_payload = snowflake.createStatement( {sqlText: cmd_payload} );
    var result_payload = stmt_payload.execute();
    result_payload.next();
    
    //Loop part
    var cmd_min_key  = "select min(primary_key) from PLACEKEYS_APPENDED_TEMP;"
    var cmd_max_key  = "select max(primary_key) from PLACEKEYS_APPENDED_TEMP;"
    var stmt_min_key = snowflake.createStatement( {sqlText: cmd_min_key} );
    var stmt_max_key = snowflake.createStatement( {sqlText: cmd_max_key} );
    var result_min_key = stmt_min_key.execute();
    while(result_min_key.next()) {
        var idmin = result_min_key.getColumnValue(1)
    }
    var result_max_key = stmt_max_key.execute();
    while(result_max_key.next()) {
        var idmax = result_max_key.getColumnValue(1)
    }
    if (NO_OF_RECORDS ) {
        idmax = idmin+NO_OF_RECORDS;}
     var gap = idmax-idmin;
     var loops = Math.round(gap/1000)+1;
     for (var i = 0; i < loops; i++) {
        var cmd_api1 = "INSERT INTO "+TBL_OUTPUT+"(RESULT) "
        var cmd_api2 = "SELECT "+API_FUNCTION+"("+"a.*"+")"+ " as result from (";
        var cmd_api3 = "SELECT primary_key, "+c_location_name+", "+c_street_address+", "+c_city+", "+c_region+", "+c_postal_code+", "+
        c_latitude+","+c_longitude+", "+c_country_code+" FROM PLACEKEYS_APPENDED_TEMP WHERE PRIMARY_KEY "+">="+1000*i+
        " AND "+"PRIMARY_KEY "+"<"+1000*(i+1)+") AS a;";
        var cmd_api = cmd_api1 + cmd_api2 + cmd_api3;  
        var statementLoop = snowflake.createStatement( {sqlText: cmd_api} );
        var result_setLoop = statementLoop.execute();
        result_setLoop.next();
    }
    
    var cmd_out_upd1 = "UPDATE  "+TBL_OUTPUT+" SET OUTPUT_ID = REPLACE(SPLIT_PART(RESULT, ',', 1),'[',''),"
    var cmd_out_upd2 = "PLACEKEY = SPLIT_PART(RESULT, ',', 2),ERROR = REPLACE(SPLIT_PART(RESULT, ',', 3),']','');"
    var cmd_out_upd = cmd_out_upd1+cmd_out_upd2; 
    var stmt_out_upd = snowflake.createStatement( {sqlText: cmd_out_upd} );
    var result_out_upd = stmt_out_upd.execute();
    result_out_upd.next()
    var cmd_out_alter = "ALTER TABLE "+TBL_OUTPUT+" ADD PRIMARY KEY(OUTPUT_ID)";
    var stmt_out_alter = snowflake.createStatement( {sqlText: cmd_out_alter} );
    var result_out_alter = stmt_out_alter.execute();
    result_out_alter.next()
    var return_cmd = "select * from "+TBL_OUTPUT+" A JOIN PLACEKEYS_APPENDED_TEMP B ON A.OUTPUT_ID = B.primary_key;";
    
       
     // return "Done! Data stored in PLACEKEYS_APPENDED"
    return return_cmd
    
    // to do: expose the table name "api_payload" used throughout as an input to this function (so the user can specify the name of the temp table, preventing accidental overwrite)
    // to do: implement batching functionality, being careful that the code works for batches under 1,000, equal to 1,000
    // to do: expose the maximum number of rows to process as an input to this function
    // to do: split the array contained in api_payload(results) into three separate columns (id, placekey, and error message - may want to use the Flatten function below)
    // to do: join the table placekeys_appended to the table api_payload on id. The id column returned from the API (in api_layoad) exactly matches the id in placekeys_appended
$$
;   