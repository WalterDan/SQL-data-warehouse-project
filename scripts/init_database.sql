/*
=================================
Create Database abd Schemas
================================
Script Purpose:
  This script creates a new database named 'DataWarehouse'. 
  Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'

*/


use master;
-- Create DataBase
CREATE DATABASE DataWarehouse;

use DataWarehouse;

-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
