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

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY

		--bronze.Patients
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Patients';
		TRUNCATE TABLE  bronze.Patients;
		PRINT '>> Inserting Data Into:  bronze.Patients';
		--BULK INSERT bronze.Patients 
		--FROM  'E:\UNIVERSITY SUBJECTS\PTIT subjects\Database management system\BTL\dataset\patients.csv'
		--WITH (
		--	FORMAT = 'CSV',
		--	CODEPAGE = '65001',
		--	FIRSTROW = 2
		--);
		INSERT INTO bronze.Patients (
			patient_id, name, gender, dob, address, phone, email
		)
		SELECT id, address, dob, email, gender, name, phone
		FROM hms_hqt.dbo.patient
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.Doctors
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Doctors';
		TRUNCATE TABLE  bronze.Doctors;
		PRINT '>> Inserting Data Into:  bronze.Doctors';
		--BULK INSERT bronze.Doctors
		--FROM  'E:\UNIVERSITY SUBJECTS\PTIT subjects\Database management system\BTL\dataset\doctors.csv'
		--WITH (
		--	FORMAT = 'CSV',
		--	CODEPAGE = '65001',
		--	FIRSTROW = 2
		--);
		INSERT INTO bronze.Doctors (
			doctor_id, name, last_name, specialization, phone, email
		)
		SELECT id, name, last_name, specialization, phone, email
		FROM hms_hqt.dbo.doctor AS DO;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.Appointments
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Appointments';
		TRUNCATE TABLE  bronze.Appointments;
		PRINT '>> Inserting Data Into:  bronze.Appointments';
		--BULK INSERT bronze.Appointments
		--FROM  'E:\UNIVERSITY SUBJECTS\PTIT subjects\Database management system\BTL\dataset\appointments.csv'
		--WITH (
		--	FORMAT = 'CSV',
		--	CODEPAGE = '65001',
		--	FIRSTROW = 2
		--);
		INSERT INTO bronze.Appointments (
			appointment_id, patient_id, doctor_id, appointment_date, start_time, end_time, reason, status, phone, location, appointment_type
		)
		SELECT id, patient_id, doctor_id, appointment_date, start_time, end_time, reason, status, phone, location, appointment_type
		FROM hms_hqt.dbo.appointment
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.VitalSigns
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.VitalSigns';
				TRUNCATE TABLE  bronze.VitalSigns;
		PRINT '>> Inserting Data Into:  bronze.VitalSigns';
		--BULK INSERT bronze.VitalSigns
		--FROM  'E:\UNIVERSITY SUBJECTS\PTIT subjects\Database management system\BTL\dataset\vital_signs.csv'
		--WITH (
		--	FORMAT = 'CSV',
		--	CODEPAGE = '65001',
		--	FIRSTROW = 2
		--);
		INSERT INTO bronze.VitalSigns (
			vital_id, patient_id, measurement_date, blood_pressure, heart_rate, respiratory_rate, temperature, oxygen_saturation, blood_sugar
		)
		SELECT id, patient_id, measurement_date, blood_pressure, heart_rate, respiratory_rate, temperature, oxygen_saturation, blood_sugar
		FROM hms_hqt.dbo.vital_sign
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.Diseases
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Diseases';
				TRUNCATE TABLE  bronze.Diseases;
		PRINT '>> Inserting Data Into:  bronze.Diseases';
		--BULK INSERT bronze.Diseases
		--FROM  'E:\UNIVERSITY SUBJECTS\PTIT subjects\Database management system\BTL\dataset\diseases.csv'
		--WITH (
		--	FORMAT = 'CSV',
		--	CODEPAGE = '65001',
		--	FIRSTROW = 2
		--);
		INSERT INTO bronze.Diseases (
			disease_id, patient_id, disease_name, diagnosis_date
		)
		SELECT id, patient_id, disease_name, diagnosis_date
		FROM hms_hqt.dbo.disease
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.Treatments
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Treatments';
		TRUNCATE TABLE  bronze.Treatments;
		PRINT '>> Inserting Data Into:  bronze.Treatments';
		--BULK INSERT bronze.Treatments
		--FROM  'E:\UNIVERSITY SUBJECTS\PTIT subjects\Database management system\BTL\dataset\treatments.csv'
		--WITH (
		--	FORMAT = 'CSV',
		--	CODEPAGE = '65001',
		--	FIRSTROW = 2
		--);
		INSERT INTO bronze.Treatments(
			treatment_id, patient_id, doctor_id, disease_id, treatment_description, treatment_date
		)
		SELECT id, patient_id, doctor_id, disease_id, treatment_description, treatment_date
		FROM hms_hqt.dbo.treatment
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.HospitalFees
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.HospitalFees';
				TRUNCATE TABLE  bronze.HospitalFees;
		PRINT '>> Inserting Data Into:  bronze.HospitalFees';
		--BULK INSERT bronze.HospitalFees 
		--FROM  'E:\UNIVERSITY SUBJECTS\PTIT subjects\Database management system\BTL\dataset\hospital_fee.csv'
		--WITH (
		--	FORMAT = 'CSV',
		--	CODEPAGE = '65001',
		--	FIRSTROW = 2
		--);
		INSERT INTO bronze.HospitalFees (
			fee_id, appointment_id, patient_id, service_type, description, amount, fee_date, phone
		)
		SELECT id, appointment_id, patient_id, service_type, description, amount, fee_date, phone
		FROM hms_hqt.dbo.hospital_fee
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.Prescriptions
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Prescriptions';
				TRUNCATE TABLE  bronze.Prescriptions;
		PRINT '>> Inserting Data Into:  bronze.Prescriptions';
		--BULK INSERT bronze.Prescriptions 
		--FROM  'E:\UNIVERSITY SUBJECTS\PTIT subjects\Database management system\BTL\dataset\prescriptions.csv'
		--WITH (
		--	FORMAT = 'CSV',
		--	CODEPAGE = '65001',
		--	FIRSTROW = 2
		--);
		INSERT INTO bronze.Prescriptions (
			prescription_id, appointment_id, doctor_id, patient_id, medicine_name, form, dosage_mg, instruction, duration_days, note
		)
		SELECT id, appointment_id, doctor_id, patient_id, medicine_name, form, dosage_mg, instruction, duration_days, note
		FROM hms_hqt.dbo.prescription
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -----------------------------------------------';

		--bronze.Lab_results
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.Lab_results';
		TRUNCATE TABLE bronze.Labresults;
		PRINT '>> Inserting Data Into: bronze.Lab_results';
		BULK INSERT bronze.LabResults
		FROM 'E:\UNIVERSITY SUBJECTS\PTIT subjects\Database management system\BTL\dataset\lab_results.csv'
		WITH (
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIRSTROW = 2
		);
		--INSERT INTO bronze.LabResults (
		
		--)
		--SELECT 
		--FROM hms_hqt.dbo
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
