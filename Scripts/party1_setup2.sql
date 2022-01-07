/////////////////////////////////////
// Data Clean Room Hands on Lab    //
// Script Name: Party1_Setup2      //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// Lab Author: Rachel Blum         //
// Lab Date: February 2022         //
/////////////////////////////////////

//set these variables 
set (myusername, party1account, party2account) = ('rblum','UFA43389','MNA66380');


/* add inbound share(s) */
use role accountadmin;
set shareparty2dcr = concat($party2account,'.party2_dcr');
set shareparty2source = concat($party2account,'.party2_source');

--create databases and grant privileges >> Change to your party 2 and 3 accounts
CREATE OR REPLACE DATABASE clean_room_party2 FROM SHARE identifier($shareparty2dcr);
GRANT IMPORTED PRIVILEGES ON DATABASE clean_room_party2 TO ROLE party1;

CREATE OR REPLACE DATABASE party2_source FROM SHARE identifier($shareparty2source);
GRANT IMPORTED PRIVILEGES ON DATABASE party2_source TO ROLE party1;

use role party1;

--create stream on each shared query requests table
CREATE OR REPLACE STREAM clean_room.dcr_internal.party2_new_requests
ON TABLE clean_room_party2.dcr_shared.query_requests
  APPEND_ONLY = TRUE 
  DATA_RETENTION_TIME_IN_DAYS = 14;
  

CREATE OR REPLACE STREAM clean_room.dcr_internal.new_requests
ON TABLE clean_room.dcr_shared.query_requests
  APPEND_ONLY = TRUE 
  DATA_RETENTION_TIME_IN_DAYS = 14;

--create view to pull data from each stream
--will need to be updated each time a new party is added to the clean room
CREATE OR REPLACE VIEW clean_room.dcr_internal.new_requests_all
AS
SELECT * FROM
  (SELECT * FROM 
    (SELECT request_id, 
        select_column_list, 
        filters, 
        at_timestamp, 
        target_table_name, 
        query_template_name, 
        RANK() OVER (PARTITION BY request_id ORDER BY request_ts DESC) AS current_flag 
      FROM clean_room.dcr_internal.new_requests 
      WHERE METADATA$ACTION = 'INSERT' 
      ) a 
  WHERE a.current_flag = 1)
UNION
  (SELECT * FROM 
    (SELECT request_id, 
        select_column_list, 
        filters, 
        at_timestamp, 
        target_table_name, 
        query_template_name, 
        RANK() OVER (PARTITION BY request_id ORDER BY request_ts DESC) AS current_flag 
      FROM clean_room.dcr_internal.party2_new_requests 
      WHERE METADATA$ACTION = 'INSERT' 
      ) a 
  WHERE a.current_flag = 1)
;

--create view for polling across all parties
--will need to be updated each time a new party is added to the clean room

CREATE OR REPLACE VIEW clean_room.dcr_internal.request_status_all
AS
SELECT
  'party1' party
  ,request_id 
  ,request_status 
  ,target_table_name 
  ,query_text 
  ,request_status_ts 
  ,comments 
  ,account_name 
FROM clean_room.dcr_shared.request_status
UNION
SELECT
  'party2' party
  ,request_id 
  ,request_status 
  ,target_table_name 
  ,query_text 
  ,request_status_ts 
  ,comments 
  ,account_name 
FROM clean_room_party2.dcr_shared.request_status
;





