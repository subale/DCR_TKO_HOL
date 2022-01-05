/////////////////////////////////////
// Data Clean Room Hands on Lab    //
// Script Name: Party1_Demo        //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// Lab Author: Rachel Blum         //
// Lab Date: February 2022         //
/////////////////////////////////////

--call validate query request procedure to test with party1 account locator
call clean_room.dcr_internal.validate_query('UFA43389');

//DEMO
--call validate query request procedure demo with party2 account locator
call clean_room.dcr_internal.validate_query('mna66380');
