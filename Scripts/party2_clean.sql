/////////////////////////////////////
// Data Clean Room Hands on Lab    //
// Script Name: Party2_Clean       //
// Script Author: Michael Rainey   //
// Script Date: Dec 2020           //    
// Lab Author: Rachel Blum         //
// Lab Date: January 2021          //
/////////////////////////////////////

use role accountadmin;
use warehouse party2wh;

drop share PARTY2_SOURCE;
drop share PARTY2_DCR;
drop database CLEAN_ROOM;
drop database CLEAN_ROOM_PARTY1;
drop database PARTY2;
drop database PARTY1_SOURCE; 
drop role party2;
drop warehouse PARTY2WH;