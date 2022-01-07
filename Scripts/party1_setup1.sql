/////////////////////////////////////
// Data Clean Room Hands on Lab    //
// Script Name: Party1_Setup1      //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// Lab Author: Rachel Blum         //
// Lab Date: February 2022         //
/////////////////////////////////////

//set these variables 
set (myusername, party1account, party2account) = ('rblum','UFA43389','MNA66380');


--create role
USE ROLE securityadmin;
CREATE OR REPLACE ROLE party1;
GRANT ROLE party1 TO ROLE sysadmin;
GRANT ROLE party1 TO USER identifier($myusername);

--grant privileges
USE ROLE accountadmin;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE party1;
GRANT CREATE SHARE ON ACCOUNT TO ROLE party1;
GRANT IMPORT SHARE ON ACCOUNT TO ROLE party1;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE party1;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE party1;

//Mount Sample Data Database
CREATE DATABASE if not exists "SNOWFLAKE_SAMPLE_DATA" FROM SHARE SFC_SAMPLES."SAMPLE_DATA";
GRANT IMPORTED PRIVILEGES ON DATABASE "SNOWFLAKE_SAMPLE_DATA" TO ROLE "ACCOUNTADMIN";
GRANT IMPORTED PRIVILEGES ON DATABASE "SNOWFLAKE_SAMPLE_DATA" TO ROLE "PARTY1";

USE ROLE party1;
/* source object creation */
CREATE OR REPLACE WAREHOUSE party1wh warehouse_size=xsmall;
CREATE OR REPLACE DATABASE party1;
CREATE OR REPLACE SCHEMA party1.source;

CREATE OR REPLACE TABLE party1.source.customers 
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
FROM snowflake_sample_data.tpch_sf1.customer
WHERE c_custkey between 9001 and 9050
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



--row access policy
CREATE OR REPLACE ROW ACCESS POLICY party1.source.dcr_rap AS (customer_name varchar, customer_address varchar, phone varchar, email varchar) returns boolean ->
    current_role() IN ('ACCOUNTADMIN','PARTY1')
      or exists  (select query_text from clean_room.dcr_internal.approved_query_requests where query_text=current_statement() or query_text=sha2(current_statement()));

--apply row access policy
alter table party1.source.customers add row access policy party1.source.dcr_rap on (customer_name, customer_address, phone, email);
--alter table party1.source.customers drop row access policy party1.source.dcr_rap;



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
                                    WHERE UPPER(TRIM(selectp.value)) LIKE UPPER('party1.%') \
                                    UNION \
                                    SELECT UPPER(TRIM(value)) value \
                                    FROM TABLE(SPLIT_TO_TABLE(UPPER(" + where_val + "),' ')) AS wherep \
                                    WHERE UPPER(TRIM(wherep.value)) LIKE 'party1.%' AND UPPER(wherep.value) NOT LIKE '%:%' \
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
      var query_text_sql = "SELECT query_template_text FROM clean_room_party2.dcr_shared.query_templates WHERE UPPER(query_template_name) = '" + query_template_name.toUpperCase() + "';";
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
VALUES ('PARTY1','CUSTOMER_NAME','');
INSERT INTO clean_room.dcr_shared.available_values
VALUES ('PARTY1','CUSTOMER_ADDRESS','');
INSERT INTO clean_room.dcr_shared.available_values
VALUES ('PARTY1','PHONE','');
INSERT INTO clean_room.dcr_shared.available_values
VALUES ('PARTY1','EMAIL','');
INSERT INTO clean_room.dcr_shared.available_values
VALUES ('PARTY1','CITY','');
INSERT INTO clean_room.dcr_shared.available_values
VALUES ('PARTY1','STATE','');


/* outbound share */
--create share objects
CREATE OR REPLACE SHARE party1_dcr;
CREATE OR REPLACE SHARE party1_source;


--grant object privileges to share
GRANT USAGE ON DATABASE clean_room TO SHARE party1_dcr;
GRANT USAGE ON SCHEMA clean_room.dcr_shared TO SHARE party1_dcr;
GRANT SELECT ON TABLE clean_room.dcr_shared.available_values TO SHARE party1_dcr;
GRANT SELECT ON TABLE clean_room.dcr_shared.query_requests TO SHARE party1_dcr;
GRANT SELECT ON TABLE clean_room.dcr_shared.request_status TO SHARE party1_dcr;

--grant object privileges to share
GRANT USAGE ON DATABASE party1 TO SHARE party1_source;
GRANT USAGE ON SCHEMA party1.source TO SHARE party1_source;
GRANT SELECT ON TABLE party1.source.customers TO SHARE party1_source;

--add accounts to shares >> Change to your  Accounts  adding in remove share restrictions in case BC to Enterprise
use role accountadmin;
ALTER SHARE PARTY1_DCR ADD ACCOUNTS = identifier($party2account) SHARE_RESTRICTIONS=false;
ALTER SHARE PARTY1_SOURCE ADD ACCOUNTS = identifier($party2account) SHARE_RESTRICTIONS=false;


