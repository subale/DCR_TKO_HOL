/////////////////////////////////////
// Data Clean Room Hands on Lab    //
// Script Name: Party1_Clean       //
// Script Author: Michael Rainey   //
// Script Date: Dec 2020           //    
// Lab Author: Rachel Blum         //
// Lab Date: January 2021          //
/////////////////////////////////////


use role accountadmin;
use warehouse party1wh;

drop share PARTY1_SOURCE;
drop share PARTY1_DCR;
drop database CLEAN_ROOM;
drop database CLEAN_ROOM_PARTY2;
drop database PARTY1;
drop database PARTY2_SOURCE; 
drop role party1;
drop warehouse PARTY1WH;