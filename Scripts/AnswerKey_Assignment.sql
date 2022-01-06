//Add a task to call clean room procedure every minute
create or replace task clean_room.dcr_internal.execute_query
  WAREHOUSE = party1wh
  SCHEDULE = '1 minute'
AS
call clean_room.dcr_internal.validate_query('mna66380');

ALTER TASK clean_room.dcr_internal.execute_query RESUME;

//Stop this task from running as it has no condition to check for a new request
ALTER TASK clean_room.dcr_internal.execute_query SUSPEND;  
  
  
//Add a task to call clean room procedure every minute if there is a new request 
create or replace task clean_room.dcr_internal.execute_query_newrequest
    WAREHOUSE = party1wh
  SCHEDULE = '1 minute'
  WHEN SYSTEM$STREAM_HAS_DATA('clean_room.dcr_internal.party2_new_requests')
AS
call clean_room.dcr_internal.validate_query('mna66380');

ALTER TASK clean_room.dcr_internal.execute_query_newrequest RESUME;

//see your tasks
show tasks;

//watch your task history
select * from table(information_schema.task_history());
