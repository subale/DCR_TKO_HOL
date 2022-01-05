/////////////////////////////////////
// Data Clean Room Hands on Lab    //
// Script Name: Party2_Setup1      //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// Lab Author: Rachel Blum         //
// Lab Date: February 2022         //
/////////////////////////////////////



//Party 1: UFA43389
//Party 2: MNA66380

/* party2 account setup */

//Replace my username rblum with your user name
//Replace my Party1 account UFA43389 with your Party1 account
//Replace my Party2 account MNA66380 with your Party2 account



--create role
USE ROLE securityadmin;
CREATE OR REPLACE ROLE party2;
GRANT ROLE party2 TO ROLE sysadmin;
GRANT ROLE party2 TO USER rblum;

--grant privileges
USE ROLE accountadmin;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE party2;
GRANT CREATE SHARE ON ACCOUNT TO ROLE party2;
GRANT IMPORT SHARE ON ACCOUNT TO ROLE party2;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE party2;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE party2;

//Mount Sample Data Database
CREATE OR REPLACE DATABASE "SAMPLE_DATA" FROM SHARE SFC_SAMPLES."SAMPLE_DATA";
GRANT IMPORTED PRIVILEGES ON DATABASE "SAMPLE_DATA" TO ROLE "ACCOUNTADMIN";
GRANT IMPORTED PRIVILEGES ON DATABASE "SAMPLE_DATA" TO ROLE "PARTY2";

USE ROLE party2;

/* source object creation */
CREATE OR REPLACE WAREHOUSE party2wh warehouse_size=xsmall;

/* source object creation */

CREATE OR REPLACE DATABASE party2;
CREATE OR REPLACE SCHEMA party2.source;
CREATE OR REPLACE TABLE party2.source.customers 
AS
SELECT c_custkey customer_id, c_name customer_name, c_address customer_address, c_phone phone, c_custkey || '@email.net' email, 
    CASE WHEN charindex('t',customer_address)>0 THEN 'Tulsa' 
            WHEN charindex('s',customer_address)>0 THEN 'Seattle' 
            WHEN charindex('g',customer_address)>0 THEN 'Guayaquil'
            WHEN charindex('p',customer_address)>0 THEN 'Pasco'
            ELSE 'Walla Walla'
          END city,
    CASE WHEN charindex('t',customer_address)>0 THEN 'OK' 
            WHEN charindex('s',customer_address)>0 THEN 'WA' 
            WHEN charindex('g',customer_address)>0 THEN 'EC-G'
            WHEN charindex('p',customer_address)>0 THEN 'WA'
            ELSE 'WA'
          END state
FROM sample_data.tpch_sf1.customer
WHERE c_custkey between 9021 and 9070
;

/* clean room object creation */
--databases and schemas
CREATE OR REPLACE DATABASE clean_room;
CREATE OR REPLACE SCHEMA dcr_internal;
CREATE OR REPLACE SCHEMA dcr_shared;

/* internal objects */
--approved queries
CREATE OR REPLACE TABLE clean_room.dcr_internal.approved_query_requests
(
  query_name VARCHAR,
  query_text VARCHAR
);

--query templates (master)
CREATE OR REPLACE TABLE clean_room.dcr_shared.query_templates
(
  query_template_name VARCHAR,
  query_template_text VARCHAR
);

//TRUNCATE TABLE clean_room.dcr_shared.query_templates;
INSERT INTO clean_room.dcr_shared.query_templates
VALUES ('2_party_customer_overlap', $$SELECT @select_cols, COUNT(party2.customer_id) cnt_customers FROM party1_source.source.customers party1 INNER JOIN party2.source.customers at(timestamp=>'@attimestamp'::timestamp_tz) party2 ON party1.customer_id = party2.customer_id WHERE @filter AND exists (SELECT table_name FROM party2.information_schema.tables WHERE table_schema = 'SOURCE' AND table_name = 'CUSTOMERS' AND table_type = 'BASE TABLE') GROUP BY @group_by_cols HAVING COUNT(party2.customer_id) >= @threshold;$$);
INSERT INTO clean_room.dcr_shared.query_templates
VALUES ('3_party_customer_overlap', $$SELECT @select_cols, COUNT(party2.customer_id) cnt_customers FROM party1_source.source.customers party1 INNER JOIN party2.source.customers at(timestamp=>'@attimestamp'::timestamp_tz) party2 ON party1.customer_id = party2.customer_id INNER JOIN party3_source.source.customers party3 ON party2.customer_id = party3.customer_id WHERE @filter AND exists (SELECT table_name FROM party2.information_schema.tables WHERE table_schema = 'SOURCE' AND table_name = 'CUSTOMERS' AND table_type = 'BASE TABLE') GROUP BY @group_by_cols HAVING COUNT(party2.customer_id) >= @threshold;$$);

--row access policy
CREATE OR REPLACE ROW ACCESS POLICY party2.source.dcr_rap AS (customer_name varchar, customer_address varchar, phone varchar, email varchar) returns boolean ->
    current_role() IN ('ACCOUNTADMIN','PARTY2')
      or exists  (select query_text from clean_room.dcr_internal.approved_query_requests where query_text=current_statement() or query_text=sha2(current_statement()));

--apply row access policy
alter table party2.source.customers add row access policy party2.source.dcr_rap on (customer_name, customer_address, phone, email);




--validate query procedure
-- check select/filter against available_values
-- validate query restrictions value (having clause)?
-- generate query text as CTAS
-- store query text in approved table
-- return approved query text to requesting party in request_status table OR return error

CREATE OR REPLACE PROCEDURE clean_room.dcr_internal.validate_query(account_name VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$

try {
  //INSERT NEW REQUESTS INTO THEIR SECURE QUERY STATUS

  //Get variables
  var account_name = ACCOUNT_NAME.toUpperCase();

  //Get request
  var statement_requests = "SELECT * FROM clean_room.dcr_internal.new_requests_all;";


  //create temp table to store latest requests
  var create_temp_table_sql = "CREATE OR REPLACE TEMPORARY TABLE clean_room.dcr_internal.requests_temp( \
                                request_id VARCHAR, select_column_list VARCHAR, filters VARCHAR, at_timestamp VARCHAR, \
                                target_table_name VARCHAR, query_template_name VARCHAR, current_flag NUMBER);";
  var create_temp_table_statement = snowflake.createStatement( {sqlText: create_temp_table_sql} );
  var create_temp_table_results = create_temp_table_statement.execute();

  //insert request into temp table, resetting the stream
  var insert_temp_table_sql = "INSERT INTO clean_room.dcr_internal.requests_temp \
                                " + statement_requests;
  var insert_temp_table_statement = snowflake.createStatement( {sqlText: insert_temp_table_sql} );
  var insert_temp_table_results = insert_temp_table_statement.execute();

  //select fields from temp table
  var requests_sql = "SELECT request_id, select_column_list, filters, at_timestamp::string, target_table_name, query_template_name, current_flag \
                      FROM clean_room.dcr_internal.requests_temp;";
  var requests_statement = snowflake.createStatement( {sqlText: requests_sql} );
  var result_v = requests_statement.execute();

  while (result_v.next()) {

    var request_id = result_v.getColumnValue(1);
    var select_parse = result_v.getColumnValue(2);
    var where_parse =  result_v.getColumnValue(3);
    var at_timestamp = result_v.getColumnValue(4);
    var target_table_name = result_v.getColumnValue(5);
    var query_template_name = result_v.getColumnValue(6);

    //clean fields

    var select_val = "'" + select_parse + "'";
    var select_fields =  select_val;

    var where_val = "'" + where_parse.replace(/'/g,':') + "'";
    var where_fields = where_val;

    if (!at_timestamp) {
      //TODO error
    }
  
//TODO figure out how to know which select cols / filters are a part of this accounts source db.
// require alias as listed in available values table (TODO for available values)

    //validate fields put an if query is valid proceed, if not, update status with ERROR at least one of the fields or values specified is not valid
    var statement_validate = "SELECT LISTAGG(CASE WHEN v.field_group IS NULL \
                                              THEN CONCAT(value, ' - Field not available') \
                                              ELSE 'Valid' \
                                            END ,',') AS error_field \
                              FROM (SELECT UPPER(TRIM(value)) value FROM TABLE(SPLIT_TO_TABLE(UPPER(" + select_val + "),',')) AS selectp \
                                    WHERE UPPER(TRIM(selectp.value)) LIKE UPPER('party2.%') \
                                    UNION \
                                    SELECT UPPER(TRIM(value)) value \
                                    FROM TABLE(SPLIT_TO_TABLE(UPPER(" + where_val + "),' ')) AS wherep \
                                    WHERE UPPER(TRIM(wherep.value)) LIKE 'party2.%' AND UPPER(wherep.value) NOT LIKE '%:%' \
                                  ) a \
                                LEFT OUTER JOIN clean_room.dcr_shared.available_values v \
                                    ON UPPER(TRIM(a.value)) = UPPER(CONCAT(TRIM(v.field_group),'.', TRIM(v.field_name))) \
                              WHERE CASE WHEN v.field_group IS NULL \
                                      THEN CONCAT(value, ' - Field not available') \
                                      ELSE 'Valid' \
                                    END <> 'Valid';";

    var statement_l = snowflake.createStatement( {sqlText: statement_validate} );
    var result_l = statement_l.execute();

    while (result_l.next()) {

      var error = result_l.getColumnValue(1);

      if (error != '') {

        //invalid request - insert error record and end runner
        var insert_record = "INSERT INTO clean_room.dcr_shared.request_status \
                            (request_id, request_status, target_table_name, query_text, request_status_ts, comments, account_name) \
                            VALUES \
                            ( '" + request_id + "', \
                              'ERROR',  \
                              '" + target_table_name + "',\
                              NULL, \
                              CURRENT_TIMESTAMP(),\
                              '" + error + "', \
                              '" + account_name + "');";

        var statement_i = snowflake.createStatement( {sqlText: insert_record} );
        var result_i = statement_i.execute();

    } else {

      //build query from template
      //TODO use shared version if not main provider account
      var query_text_sql = "SELECT query_template_text FROM clean_room.dcr_shared.query_templates WHERE UPPER(query_template_name) = '" + query_template_name.toUpperCase() + "';";
      var query_text_statement = snowflake.createStatement( {sqlText: query_text_sql} );
      var query_text_result = query_text_statement.execute();

      while (query_text_result.next()) {
          var query_text = query_text_result.getColumnValue(1);
      }

      //if query_text is empty, report error *bad template name* else build the query
      if (!query_text) {
          //invalid request - insert error record and end 
          error = 'Invalid query template';
          var insert_record = "INSERT INTO clean_room.dcr_shared.request_status \
                              (request_id, request_status, target_table_name, query_text, request_status_ts, comments, account_name) \
                              VALUES \
                              ( '" + request_id + "', \
                                'ERROR',  \
                                '" + target_table_name + "',\
                                NULL, \
                                CURRENT_TIMESTAMP(),\
                                '" + error + "', \
                                '" + account_name + "');";

          var statement_i = snowflake.createStatement( {sqlText: insert_record} );
          var result_i = statement_i.execute();

      } else {
          //TODO make this configurable
          var threshold = 3;

          var approved_query_text = "CREATE OR REPLACE TABLE " + target_table_name + " AS " + query_text;

          approved_query_text = approved_query_text.replace(/@select_cols/g, select_parse);
          approved_query_text = approved_query_text.replace(/@filter/g, where_parse);
          approved_query_text = approved_query_text.replace(/@group_by_cols/g, select_parse);
          approved_query_text = approved_query_text.replace(/@threshold/g, threshold);
          approved_query_text = approved_query_text.replace(/@attimestamp/g, at_timestamp);


          //insert approved query into approved statements table
          var approved_query_text_sql = "INSERT INTO clean_room.dcr_internal.approved_query_requests (query_name, query_text) \
                                          VALUES ('" + query_template_name + "', " + String.fromCharCode(13, 36, 36) + approved_query_text + String.fromCharCode(13, 36, 36) + ");";
          var approved_query_text_statement = snowflake.createStatement( {sqlText: approved_query_text_sql} );
          var approved_query_text_result = approved_query_text_statement.execute();

      }

      var insert_approved_sql = "INSERT INTO clean_room.dcr_shared.request_status \
                            (request_id, request_status, target_table_name, query_text, request_status_ts, comments, account_name) \
                            VALUES \
                            ( '" + request_id + "', \
                              'APPROVED',  \
                              '" + target_table_name + "',\
                              " + String.fromCharCode(13, 36, 36) + approved_query_text + String.fromCharCode(13, 36, 36) + ", \
                              CURRENT_TIMESTAMP(),\
                              'APPROVED', \
                              '" + account_name + "');";
      var insert_approved_statement = snowflake.createStatement( {sqlText: insert_approved_sql} );
      var insert_approved_result = insert_approved_statement.execute();
    }
  }
}
}
catch (err) {
  var result = "Failed: Code: " + err.code + "\n  State: " + err.state;
  result += "\n  Message: " + err.message;
  result += "\nStack Trace:\n" + err.stackTraceTxt;
  return result;
}
return "Success!";
$$
;




--"
--generate query request procedure
-- insert new request into QUERY_REQUESTS table
-- poll REQUEST_STATUS shared table(s) for request id
-- run generated CTAS statement

CREATE OR REPLACE PROCEDURE clean_room.dcr_internal.generate_query_request(target_table_name VARCHAR,query_template_name VARCHAR,select_column_list VARCHAR,filters VARCHAR, parties VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$

try {
    //current date
    const date = Date.now();
    var currentDate = null;

    //get parameters
    var select_column_list = SELECT_COLUMN_LIST;
    var filters = FILTERS;
    var target_table_name = TARGET_TABLE_NAME;
    var query_template_name = QUERY_TEMPLATE_NAME;
    var at_timestamp = "CURRENT_TIMESTAMP()::string";
   
    var parties = PARTIES;

    var call_generate_results_sql = "";
    var insert_request_sql = "";
    var query_status_sql = "";
    var get_last_query_id_sql = "";
    var abort_query_sql = "";

    var timeout = 300000; //milliseconds - 5 min timeout

    //set result id
    var set_request_id_sql = "SET requestid = (SELECT UUID_STRING());";                 
    var set_request_id_statement = snowflake.createStatement( {sqlText: set_request_id_sql} );
    var set_request_id_result = set_request_id_statement.execute();

    //insert new request
    insert_request_sql = "INSERT INTO clean_room.dcr_shared.query_requests \
                          (request_id, target_table_name, query_template_name, select_column_list, filters, at_timestamp, request_ts) \
                          VALUES \
                          ( \
                            $requestid, \
                            \$\$" + target_table_name + "\$\$, \
                            \$\$" + query_template_name + "\$\$, \
                            \$\$" + select_column_list + "\$\$, \
                            \$\$" + filters + "\$\$, \
                            " + at_timestamp + ", \
                            CURRENT_TIMESTAMP() \
                          );";

    var insert_request_statement = snowflake.createStatement( {sqlText: insert_request_sql} );
    insert_request_statement.execute();

    //call validate procedure to ensure CTAS is built as expected
    validate_request_sql = "CALL clean_room.dcr_internal.validate_query(SELECT CURRENT_ACCOUNT());";

    var validate_request_statement = snowflake.createStatement( {sqlText: validate_request_sql} );
    var validate_request_result = validate_request_statement.execute();

    validate_request_result.next();
    var validate_request = validate_request_result.getColumnValue(1);

    if (validate_request.startsWith("Failed")) {
      result = "Validation failed.";
    } else {

      //capture CTAS statement for comparison
      ctas_sql = "SELECT query_text FROM clean_room.dcr_shared.request_status WHERE request_id = $requestid;";

      var ctas_statement = snowflake.createStatement( {sqlText: ctas_sql} );
      var ctas_result = ctas_statement.execute();

      ctas_result.next();
      var ctas_sql = ctas_result.getColumnValue(1);


      //TODO: determine which partners are involved in request and poll each partys request_status table.
      //      grab the party list from the parties parameter
      var parties_filter_arr = parties.split(',');
      var parties_count = parties_filter_arr.length;
      var parties_filter = "";

      parties_filter_arr.forEach(trimQuoteFunction);
      parties_filter = parties_filter.slice(0,-1);
  
      function trimQuoteFunction(item, index) {
        parties_filter += "'" + item.trim() + "',";
      }
      
      //poll the request_status view until request is complete, times out after 300 seconds (5 mins)
      query_status_sql = "SELECT request_status, query_text, comments, party FROM clean_room.dcr_internal.request_status_all WHERE request_status IN ('APPROVED', 'ERROR') AND request_id = $requestid AND party IN (" + parties_filter + ") ORDER BY request_status_ts DESC;";
      var query_status_statement = snowflake.createStatement( {sqlText: query_status_sql} );
      
      //poll request_status view until request is complete, or timeout is reached
      do {
          currentDate = Date.now();
          var query_status_results =  query_status_statement.execute();
      } while ((query_status_statement.getRowCount() != parties_count) && (currentDate - date < timeout));

      while (currentDate - date >= timeout){
          return "ERROR:  Query timed out";
      }

      //unset requestid QUESTION - when session ends, this gets reset anyways?
      var unset_result_id_sql = "UNSET requestid;";                 
      var unset_result_id_statement = snowflake.createStatement( {sqlText: unset_result_id_sql} );
      unset_result_id_statement.execute();
      
      //get results_table from query_status_results
      var results_count = 0;
      var approved_count = 0;

      while (query_status_results.next()) {
        results_count +=1;
        var status = query_status_results.getColumnValue(1);
        var query_text = query_status_results.getColumnValue(2);
        var comments = query_status_results.getColumnValue(3);

        if (status == "APPROVED") {
          if (ctas_sql == query_text) {
            approved_count += 1;
          } else {
              return "ERROR: the generated SQL statements do not match between parties. " + comments;
          }
        } else {
            return "ERROR: statement not approved. " + comments;
        }
      }

      if (approved_count == parties_count) {
          set_schema_sql = "USE SCHEMA clean_room.dcr_internal;";
          set_schema_statement = snowflake.createStatement( {sqlText: set_schema_sql} );
          set_schema_statement.execute(); 
      
          create_target_table_sql = query_text;
          var create_target_table_statement = snowflake.createStatement( {sqlText: create_target_table_sql} );
          create_target_table_statement.execute();    
      
          return "The results of your query are available: " + "clean_room.dcr_internal.".toUpperCase() + target_table_name.toUpperCase();
      } else {
          return "ERROR: statement not approved. " + comments;
      }

    }
} catch (err) {
    var result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
    result += "\n  Message: " + err.message;
    result += "\nStack Trace:\n" + err.stackTraceTxt;
    return result;
}

$$;


/* shared objects */

--query requests table
CREATE OR REPLACE TABLE clean_room.dcr_shared.query_requests
(
  request_id VARCHAR,
  target_table_name VARCHAR,
  query_template_name VARCHAR,
  select_column_list VARCHAR,
  filters VARCHAR,
  at_timestamp VARCHAR,
  request_ts TIMESTAMP_NTZ
);

ALTER TABLE clean_room.dcr_shared.query_requests
SET CHANGE_TRACKING = TRUE   
    DATA_RETENTION_TIME_IN_DAYS = 14;

--request status table
CREATE OR REPLACE TABLE clean_room.dcr_shared.request_status
(
  request_id VARCHAR
  ,request_status VARCHAR
  ,target_table_name VARCHAR
  ,query_text VARCHAR
  ,request_status_ts TIMESTAMP_NTZ
  ,comments VARCHAR
  ,account_name VARCHAR
);

--available values table
CREATE OR REPLACE TABLE clean_room.dcr_shared.available_values
(
  field_group VARCHAR,
  field_name VARCHAR,
  field_values VARCHAR
);

INSERT INTO clean_room.dcr_shared.available_values
VALUES ('PARTY2','CUSTOMER_NAME','');
INSERT INTO clean_room.dcr_shared.available_values
VALUES ('PARTY2','CUSTOMER_ADDRESS','');
INSERT INTO clean_room.dcr_shared.available_values
VALUES ('PARTY2','PHONE','');
INSERT INTO clean_room.dcr_shared.available_values
VALUES ('PARTY2','EMAIL','');
INSERT INTO clean_room.dcr_shared.available_values
VALUES ('PARTY2','CITY','');
INSERT INTO clean_room.dcr_shared.available_values
VALUES ('PARTY2','STATE','');

/* outbound share */
--create share objects
CREATE OR REPLACE SHARE party2_dcr;
CREATE OR REPLACE SHARE party2_source;

--grant object privileges to share
GRANT USAGE ON DATABASE clean_room TO SHARE party2_dcr;
GRANT USAGE ON SCHEMA clean_room.dcr_shared TO SHARE party2_dcr;
GRANT SELECT ON TABLE clean_room.dcr_shared.available_values TO SHARE party2_dcr;
GRANT SELECT ON TABLE clean_room.dcr_shared.query_requests TO SHARE party2_dcr;
GRANT SELECT ON TABLE clean_room.dcr_shared.request_status TO SHARE party2_dcr;
GRANT SELECT ON TABLE clean_room.dcr_shared.query_templates TO SHARE party2_dcr;

--grant object privileges to share
GRANT USAGE ON DATABASE party2 TO SHARE party2_source;
GRANT USAGE ON SCHEMA party2.source TO SHARE party2_source;
GRANT SELECT ON TABLE party2.source.customers TO SHARE party2_source;

--add accounts to share>> Add accounts Party 1 and Party 3
ALTER SHARE party2_dcr ADD ACCOUNTS = UFA43389;
ALTER SHARE party2_source ADD ACCOUNTS = UFA43389;

