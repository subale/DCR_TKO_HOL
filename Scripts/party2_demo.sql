/////////////////////////////////////
// Data Clean Room Hands on Lab    //
// Script Name: Party2_Demo        //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// Lab Author: Rachel Blum         //
// Lab Date: February 2022         //
/////////////////////////////////////


use role party2;
use warehouse PARTY2WH;


//DEMO - Step 1:  Generate the request 
call clean_room.dcr_internal.generate_query_request('customer_results','2_party_customer_overlap',$$party2.city$$,$$party1.email like '%.net'$$,'party1,party2');

//DEMO - Step 2:  Run Validate the Query in Party 1

//DEMO - Step 3: check the table
select * from clean_room.dcr_internal.customer_results;