CREATE DATABASE data_vendor_project;
USE data_vendor_project;


-- check table

CREATE TABLE check_table (
    InventoryId VARCHAR(255),
    Store INT,
    City VARCHAR(255),
    Brand INT,
    Description TEXT,
    Size VARCHAR(255),
    onHand INT,
    Price DOUBLE,
    startDate VARCHAR(20)   
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/begin_inventory.csv'
INTO TABLE check_table
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(InventoryId, Store, City, Brand, Description, Size, onHand, Price, startDate);


select count(*) from check_table;
