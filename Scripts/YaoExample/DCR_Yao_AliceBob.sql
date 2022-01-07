// Create demo environment:
use role accountadmin;
////create warehouse
create or replace warehouse masking_wh  warehouse_size = 'X-Small';
////create roles & grant access to current user
create or replace role "ALICE1";
grant role "ALICE1" to role"SYSADMIN";
create or replace  ROLE "BOB1";
grant role "BOB1" TO ROLE "SYSADMIN";
////replace {USERNAME} 
grant role  "ALICE1" TO USER rblum;
grant role  "BOB1" TO USER rblum;
////create database & schemas, grant access
create or replace database alice1;
create or replace  schema alice1.data;
create or replace database bob1;
create or replace  schema bob1.data;
grant ownership on database alice1 to role alice1;
grant ownership on schema alice1.data to role alice1;
grant ownership on database bob1 to role bob1;
grant ownership on schema bob1.data to role bob1;
////grant usage on warehouse
grant usage on warehouse masking_wh to role alice1;
grant usage on warehouse masking_wh to role bob1;


// Alice puts her wealth number in her table
use role alice1;
create or replace table alice1.data.my_wealth as select 1110000 as wealth;



// Bob puts his  wealth number in his table
use role bob1;
create or replace table bob1.data.my_wealth as select 1250000 as wealth;

//Bob creates a table for allowed statements = this table will hold SQL statements that Bob will allow Alice to run against Bob’s data
create or replace table bob1.data.allowed_statements ( statement varchar(20000) );  

//Bob creates a row access policy to protect his table
--row access policy
CREATE OR REPLACE ROW ACCESS POLICY bob1.data.num_mask AS (wealth integer) returns boolean ->
    current_role() IN ('ACCOUNTADMIN','BOB1')
      or exists  (select statement from  bob1.data.allowed_statements where statement=current_statement() );
    
//Bob applies this row access policy to his wealth table
--apply row access policy
alter table bob1.data.my_wealth add row access policy bob1.data.num_mask on (wealth);

//Bob inserts an allowed statement into his allowed statements table
insert into bob1.data.allowed_statements (statement)
values ('select case
when bob.wealth > alice.wealth then \'bob is richer\'
when bob.wealth = alice.wealth then \'neither is richer\'
else \'alice is richer\' end
from bob1.data.my_wealth bob, 
alice1.data.my_wealth alice
where exists (select table_name from alice1.information_schema.tables where table_schema = \'DATA\' and table_name = \'MY_WEALTH\' and table_type = \'BASE TABLE\');');


//Bob grants usage on table to Alice
grant usage on database bob1 to role alice1;
grant usage on schema bob1.data to role alice1;
grant select on table bob1.data.my_wealth to role alice1;

// Shifting to Alice’s side:

use role alice1;

//Alice asks a question that is not allowed
select * from bob1.data.my_wealth;


//Alice asks a question that is allowed     
select case
when bob.wealth > alice.wealth then 'bob is richer'
when bob.wealth = alice.wealth then 'neither is richer'
else 'alice is richer' end
from bob1.data.my_wealth bob, 
alice1.data.my_wealth alice
where exists (select table_name from alice1.information_schema.tables where table_schema = 'DATA' and table_name = 'MY_WEALTH' and table_type = 'BASE TABLE');


////Clean
//use role accountadmin;
//drop database if exists bob1;
//drop database if exists alice1;
//drop role if exists bob1;
//drop role if exists alice1;
//drop warehouse if exists masking_wh;