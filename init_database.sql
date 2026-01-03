/*

=============================================================================
Create	Database and Schema
=============================================================================
Script Purpose:
	This script creates	a new database named 'College_DataWarehouse' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the scrips sets up three schemas
	within the database: 'bronze', 'silver', and 'gold'.
WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted. Proceed with caution
	and ensure you have proper backups before running this script.
*/

-- Create Database 'DataWarehouse'

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='College_DataWarehouse')
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE College_DataWarehouse;
END;
GO

--Create DataWarehouse database
CREATE DATABASE College_DataWarehouse;
GO

USE College_DataWarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
