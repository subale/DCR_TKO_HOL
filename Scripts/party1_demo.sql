/////////////////////////////////////
// Data Clean Room Hands on Lab    //
// Script Name: Party1_Demo        //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// Lab Author: Rachel Blum         //
// Lab Date: February 2022         //
/////////////////////////////////////

//set these variables 
set (myusername, party1account, party2account) = ('rblum','UFA43389','MNA66380');

//DEMO
--call validate query request procedure demo with party2 account locator
call clean_room.dcr_internal.validate_query(identifier($party2account));
