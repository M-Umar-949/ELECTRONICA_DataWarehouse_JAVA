-- Create the Customer dimension table
create database  IF NOT EXISTS ELECTRONICA_DW;

drop table if exists Product;
drop table if exists Customer;
drop table if exists Date_D;
drop table if exists Sales;
drop table if exists Store;
drop table if exists Supplier;

use ELECTRONICA_DW;
-- DROP database ELECTRONICA_DW;
create table Customer (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    CustomerID INT,
    CustomerName VARCHAR(255),
    Gender VARCHAR(10)
);

-- Create the Supplier dimension table
create table Supplier (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    SupplierID INT,
    SupplierName VARCHAR(255)
);

-- Create the Store dimension table
create table Store (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    StoreID INT,
    StoreName VARCHAR(255)
);

-- Create the Product dimension table
create table Product (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    ProductID INT,
    ProductName VARCHAR(255),
    ProductPrice DECIMAL(10, 2),
    SupplierID INT,
    StoreID INT
);

create table Date_D (
    DateID INT AUTO_INCREMENT primary key,
    FullDate DATETIME,  
    DateNorm DATE,      -
    Month INT,
    Quarter INT,
    Year INT,
    Weekend BOOLEAN
);
-- Create the Sales fact table
create table Sales (
    OrderID INT PRIMARY KEY,
    ProductID INT,
    CustomerID INT,
    SupplierID int,
    QuantityOrdered INT,
    DateID INT,
    StoreID INT,
    foreign key (ProductID) references Product(ID),
    foreign key (DateID) references Date_D(DateID),
    foreign key (CustomerID) references Customer(ID),
    foreign key (SupplierID) references Supplier(ID),
    foreign key (StoreID) references Store(ID)
);




-- Disable foreign key checks temporarily
-- SET foreign_key_checks = 0;
-- Truncate tables
-- TRUNCATE TABLE Sales;
-- TRUNCATE TABLE Customer;
-- TRUNCATE TABLE Product;
-- TRUNCATE TABLE Store;
-- TRUNCATE TABLE Supplier;
-- TRUNCATE TABLE Date_D;
-- Enable foreign key checks back
-- SET foreign_key_checks = 1; 

Alter table Product
drop column ProductPrice ;



