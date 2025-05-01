USE DataWarehouse
GO

  /*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
--EXEC  bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY

		--bronze.Patients
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Patients';
				TRUNCATE TABLE  bronze.Patients;
		PRINT '>> Inserting Data Into:  bronze.Patients';
		BULK INSERT bronze.Patients 
		FROM  'D:\BTL HQTCSDL\data1k\data1k\process_csv\patients.csv'
		WITH (
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIRSTROW = 2
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.Doctors
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Doctors';
				TRUNCATE TABLE  bronze.Doctors;
		PRINT '>> Inserting Data Into:  bronze.Doctors';
		BULK INSERT bronze.Doctors
		FROM  'D:\BTL HQTCSDL\data1k\data1k\process_csv\doctors.csv'
		WITH (
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIRSTROW = 2
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.Appointments
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Appointments';
				TRUNCATE TABLE  bronze.Appointments;
		PRINT '>> Inserting Data Into:  bronze.Appointments';
		BULK INSERT bronze.Appointments
		FROM  'D:\BTL HQTCSDL\data1k\data1k\process_csv\appointments.csv'
		WITH (
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIRSTROW = 2
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.VitalSigns
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.VitalSigns';
				TRUNCATE TABLE  bronze.VitalSigns;
		PRINT '>> Inserting Data Into:  bronze.VitalSigns';
		BULK INSERT bronze.VitalSigns
		FROM  'D:\BTL HQTCSDL\data1k\data1k\process_csv\vital_signs.csv'
		WITH (
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIRSTROW = 2
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.Diseases
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Diseases';
				TRUNCATE TABLE  bronze.Diseases;
		PRINT '>> Inserting Data Into:  bronze.Diseases';
		BULK INSERT bronze.Diseases
		FROM  'D:\BTL HQTCSDL\data1k\data1k\process_csv\diseases.csv'
		WITH (
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIRSTROW = 2
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.Treatments
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Treatments';
				TRUNCATE TABLE  bronze.Treatments;
		PRINT '>> Inserting Data Into:  bronze.Treatments';
		BULK INSERT bronze.Treatments
		FROM  'D:\BTL HQTCSDL\data1k\data1k\process_csv\treatments.csv'
		WITH (
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIRSTROW = 2
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.HospitalFees
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.HospitalFees';
				TRUNCATE TABLE  bronze.HospitalFees;
		PRINT '>> Inserting Data Into:  bronze.HospitalFees';
		BULK INSERT bronze.HospitalFees 
		FROM  'D:\BTL HQTCSDL\data1k\data1k\process_csv\hospital_fee.csv'
		WITH (
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIRSTROW = 2
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.Prescriptions
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Prescriptions';
				TRUNCATE TABLE  bronze.Prescriptions;
		PRINT '>> Inserting Data Into:  bronze.Prescriptions';
		BULK INSERT bronze.Prescriptions 
		FROM  'D:\BTL HQTCSDL\data1k\data1k\process_csv\prescriptions.csv'
		WITH (
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIRSTROW = 2
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.Lab_results
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Lab_results';
				TRUNCATE TABLE bronze.Lab_results;
		PRINT '>> Inserting Data Into:  bronze.Lab_results';
		BULK INSERT bronze.Lab_results
		FROM  'D:\BTL HQTCSDL\data1k\data1k\process_csv\lab_results.csv'
		WITH (
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIRSTROW = 2
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

	END TRY
	BEGIN CATCH
			PRINT '=========================================='
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
			PRINT '=========================================='
	END CATCH
END
