/*
===========================================================================================================
Store Procedure: Load Bronze Layer	(Source -> Bronze)
===========================================================================================================
Script Purpose:
	This stored procedure loads data into the 'bronze' schema from external CSV files.
	It performs	the following actions:
	- Truncate the bronze tables before loading data.
	- Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

Parameters:
	None.
   This stored procedure does not accept any parameters or return any values.

Usage Example"
	EXEC bronze.load_bronze
============================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
SET @batch_start_time= GETDATE() 
PRINT '=====================================================================';
PRINT 'Loading Bronze Layer';
PRINT '=====================================================================';

PRINT '---------------------------------------------------------------------';
PRINT 'Loading bronze.advising_interactions';
PRINT '---------------------------------------------------------------------';

SET @start_time=GETDATE();

PRINT '>> Truncating Table: bronze.advising_interactions';
TRUNCATE TABLE bronze.advising_interactions;
PRINT '>> Inserting Data Into: bronze.advising_interactions';

BULK INSERT bronze.advising_interactions
FROM 'C:\Users\sebas\OneDrive\Documents\NEC MASTERS\Projects Portfolio\Projects Portfolio\Data Engineer\Higher Ed Project\Datasets\advising_interactions.csv'
WITH (FIRSTROW=2, 
	  FIELDTERMINATOR=',', 
	  ROWTERMINATOR='0x0a', 
	  TABLOCK, 
	  CODEPAGE='65001');

SET @end_time=GETDATE() 
PRINT '>> Load Duration: '+CAST (DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' seconds.'
PRINT '---------------------------------------------------------------------'

SET @start_time=GETDATE();

PRINT '>> Truncating Table: bronze.assessment_results';
TRUNCATE TABLE bronze.assessment_results;
PRINT '>> Inserting Data Into: bronze.assessment_results';

BULK INSERT bronze.assessment_results
FROM 'C:\Users\sebas\OneDrive\Documents\NEC MASTERS\Projects Portfolio\Projects Portfolio\Data Engineer\Higher Ed Project\Datasets\assessment_results.csv'
WITH (FIRSTROW=2, 
	  FIELDTERMINATOR=',', 
	  ROWTERMINATOR='0x0a', 
	  TABLOCK, 
	  CODEPAGE='65001');

SET @end_time=GETDATE() 
PRINT '>> Load Duration: '+CAST (DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' seconds.'
PRINT '---------------------------------------------------------------------'

SET @start_time=GETDATE();

PRINT '>> Truncating Table: bronze.course';
TRUNCATE TABLE bronze.course;
PRINT '>> Inserting Data Into: bronze.course';

BULK INSERT bronze.course
FROM 'C:\Users\sebas\OneDrive\Documents\NEC MASTERS\Projects Portfolio\Projects Portfolio\Data Engineer\Higher Ed Project\Datasets\courses.csv'
WITH (FIRSTROW=2, 
	  FIELDTERMINATOR=',', 
	  ROWTERMINATOR='0x0a', 
	  TABLOCK, 
	  CODEPAGE='65001');

SET @end_time=GETDATE() 
PRINT '>> Load Duration: '+CAST (DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' seconds.'
PRINT '---------------------------------------------------------------------'


SET @start_time=GETDATE();

PRINT '>> Truncating Table: bronze.course_grades';
TRUNCATE TABLE bronze.course_grades;
PRINT '>> Inserting Data Into: bronze.course_grades';

BULK INSERT bronze.course_grades
FROM 'C:\Users\sebas\OneDrive\Documents\NEC MASTERS\Projects Portfolio\Projects Portfolio\Data Engineer\Higher Ed Project\Datasets\course_grades.csv'
WITH (FIRSTROW=2, 
	  FIELDTERMINATOR=',', 
	  ROWTERMINATOR='0x0a', 
	  TABLOCK, 
	  CODEPAGE='65001');

SET @end_time=GETDATE() 
PRINT '>> Load Duration: '+CAST (DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' seconds.'
PRINT '---------------------------------------------------------------------'


SET @start_time=GETDATE();

PRINT '>> Truncating Table: bronze.enrollments';
TRUNCATE TABLE bronze.enrollments;
PRINT '>> Inserting Data Into: bronze.enrollments';

BULK INSERT bronze.enrollments
FROM 'C:\Users\sebas\OneDrive\Documents\NEC MASTERS\Projects Portfolio\Projects Portfolio\Data Engineer\Higher Ed Project\Datasets\enrollments.csv'
WITH (FIRSTROW=2, 
	  FIELDTERMINATOR=',', 
	  ROWTERMINATOR='0x0a', 
	  TABLOCK, 
	  CODEPAGE='65001');

SET @end_time=GETDATE() 
PRINT '>> Load Duration: '+CAST (DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' seconds.'
PRINT '---------------------------------------------------------------------'


SET @start_time=GETDATE();

PRINT '>> Truncating Table: bronze.program_learning_outcomes';
TRUNCATE TABLE bronze.program_learning_outcomes;
PRINT '>> Inserting Data Into: bronze.program_learning_outcomes';

BULK INSERT bronze.program_learning_outcomes
FROM 'C:\Users\sebas\OneDrive\Documents\NEC MASTERS\Projects Portfolio\Projects Portfolio\Data Engineer\Higher Ed Project\Datasets\program_learning_outcomes.csv'
WITH (FIRSTROW=2, 
	  FIELDTERMINATOR=',', 
	  ROWTERMINATOR='0x0a', 
	  TABLOCK, 
	  CODEPAGE='65001');

SET @end_time=GETDATE() 
PRINT '>> Load Duration: '+CAST (DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' seconds.'
PRINT '---------------------------------------------------------------------'


SET @start_time=GETDATE();

PRINT '>> Truncating Table: bronze.programs';
TRUNCATE TABLE bronze.programs;
PRINT '>> Inserting Data Into: bronze.programs';

BULK INSERT bronze.programs
FROM 'C:\Users\sebas\OneDrive\Documents\NEC MASTERS\Projects Portfolio\Projects Portfolio\Data Engineer\Higher Ed Project\Datasets\programs.csv'
WITH (FIRSTROW=2, 
	  FIELDTERMINATOR=',', 
	  ROWTERMINATOR='0x0a', 
	  TABLOCK, 
	  CODEPAGE='65001');

SET @end_time=GETDATE() 
PRINT '>> Load Duration: '+CAST (DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' seconds.'
PRINT '---------------------------------------------------------------------'


SET @start_time=GETDATE();

PRINT '>> Truncating Table: bronze.staff';
TRUNCATE TABLE bronze.staff;
PRINT '>> Inserting Data Into: bronze.staff';

BULK INSERT bronze.staff
FROM 'C:\Users\sebas\OneDrive\Documents\NEC MASTERS\Projects Portfolio\Projects Portfolio\Data Engineer\Higher Ed Project\Datasets\staff.csv'
WITH (FIRSTROW=2, 
	  FIELDTERMINATOR=',', 
	  ROWTERMINATOR='0x0a', 
	  TABLOCK, 
	  CODEPAGE='65001');

SET @end_time=GETDATE() 
PRINT '>> Load Duration: '+CAST (DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' seconds.'
PRINT '---------------------------------------------------------------------'


SET @start_time=GETDATE();

PRINT '>> Truncating Table: bronze.students';
TRUNCATE TABLE bronze.students;
PRINT '>> Inserting Data Into: bronze.students';

BULK INSERT bronze.students
FROM 'C:\Users\sebas\OneDrive\Documents\NEC MASTERS\Projects Portfolio\Projects Portfolio\Data Engineer\Higher Ed Project\Datasets\students.csv'
WITH (FIRSTROW=2, 
	  FIELDTERMINATOR=',', 
	  ROWTERMINATOR='0x0a', 
	  TABLOCK, 
	  CODEPAGE='65001');

SET @end_time=GETDATE() 
PRINT '>> Load Duration: '+CAST (DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' seconds.'
PRINT '---------------------------------------------------------------------'


SET @start_time=GETDATE();

PRINT '>> Truncating Table: bronze.survey';
TRUNCATE TABLE bronze.survey;
PRINT '>> Inserting Data Into: bronze.survey';

BULK INSERT bronze.survey
FROM 'C:\Users\sebas\OneDrive\Documents\NEC MASTERS\Projects Portfolio\Projects Portfolio\Data Engineer\Higher Ed Project\Datasets\surveys.csv'
WITH (FIRSTROW=2, 
	  FIELDTERMINATOR=',', 
	  ROWTERMINATOR='0x0a', 
	  TABLOCK, 
	  CODEPAGE='65001');

SET @end_time=GETDATE() 
PRINT '>> Load Duration: '+CAST (DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' seconds.'
PRINT '---------------------------------------------------------------------'


SET @start_time=GETDATE();

PRINT '>> Truncating Table: bronze.terms';
TRUNCATE TABLE bronze.terms;
PRINT '>> Inserting Data Into: bronze.terms';

BULK INSERT bronze.terms
FROM 'C:\Users\sebas\OneDrive\Documents\NEC MASTERS\Projects Portfolio\Projects Portfolio\Data Engineer\Higher Ed Project\Datasets\terms.csv'
WITH (FIRSTROW=2, 
	  FIELDTERMINATOR=',', 
	  ROWTERMINATOR='0x0a', 
	  TABLOCK, 
	  CODEPAGE='65001');

SET @end_time=GETDATE() 
PRINT '>> Load Duration: '+CAST (DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' seconds.'
PRINT '---------------------------------------------------------------------'

	END TRY
	BEGIN CATCH
		PRINT '==================================================='
		PRINT 'ERROR OCURRED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST( ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST( ERROR_STATE() AS NVARCHAR)
		PRINT '==================================================='

	END CATCH
END;
