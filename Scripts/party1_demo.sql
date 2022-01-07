/////////////////////////////////////
// Data Clean Room Hands on Lab    //
// Script Name: Party1_Demo        //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// Lab Author: Rachel Blum         //
// Lab Date: February 2022         //
/////////////////////////////////////

use role party1;
use warehouse party1wh;

//set these variables 
set (myusername, party1account, party2account) = ('rblum','UFA43389','MNA66380');

//DEMO
--call validate query request procedure demo with party2 account locator
call clean_room.dcr_internal.validate_query($party2account);
