/////////////////////////////////////
// Data Clean Room Hands on Lab    //
// Script Name: Party1_Clean       //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// Lab Author: Rachel Blum         //
// Lab Date: February 2022         //
/////////////////////////////////////


use role accountadmin;


drop share if exists PARTY1_SOURCE;
drop share if exists PARTY1_DCR;
drop database if exists CLEAN_ROOM;
drop database if exists CLEAN_ROOM_PARTY2;
drop database if exists PARTY1;
drop database if exists PARTY2_SOURCE; 
drop role if exists party1;
drop warehouse if exists PARTY1WH;