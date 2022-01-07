/////////////////////////////////////
// Data Clean Room Hands on Lab    //
// Script Name: Party2_Clean       //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //
// Lab Author: Rachel Blum         //
// Lab Date: February 2022         //
/////////////////////////////////////



use role accountadmin;


drop share if exists PARTY2_SOURCE;
drop share if exists PARTY2_DCR;
drop database if exists CLEAN_ROOM;
drop database if exists CLEAN_ROOM_PARTY1;
drop database if exists PARTY2;
drop database if exists PARTY1_SOURCE; 
drop role if exists party2;
drop warehouse if exists PARTY2WH;