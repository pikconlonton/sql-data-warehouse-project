/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

			--table Appointment
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.Appointments';
			TRUNCATE TABLE silver.Appointments;
			PRINT '>> Inserting Data Into: silver.Appointments';
			INSERT INTO silver.Appointments (
				appointment_id,
				patient_id,
				doctor_id,
				appointment_date,
				start_time,
				end_time,
				reason,
				status,
				phone,
				location,
				appointment_type
			)
				SELECT 
					appointment_id,
					patient_id,
					doctor_id,
					CASE 
						WHEN start_time <> appointment_date THEN start_time
						ELSE appointment_date
					END AS appointment_date,
					start_time,
					end_time,
					CASE
						WHEN reason IS NUll THEN 'n/a'
						ELSE TRIM(reason)
					END AS reason,
					TRIM(status),
					CASE 
						WHEN SUBSTRING(phone, 1, 1) = '+' THEN '0' + REPLACE(REPLACE(SUBSTRING(TRIM(phone), 4, LEN(TRIM(phone))),'-',''), ' ', '')
						WHEN SUBSTRING(phone, 1, 1) = '(' THEN REPLACE(REPLACE(REPLACE(REPLACE(TRIM(phone),'-',''), ' ', ''), '(', ''), ')', '')
						ELSE REPLACE(REPLACE(TRIM(phone),'-',''), ' ', '')
					END AS phone,
					TRIM(location),
					CASE
						WHEN appointment_type IS NUll THEN 'n/a'
						ELSE TRIM(appointment_type)
					END AS reason
				FROM bronze.Appointments
				ORDER BY appointment_date ASC
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> --------';

			--table Diseases
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.Diseases';
			TRUNCATE TABLE silver.Diseases;
			PRINT '>> Inserting Data Into: silver.Diseases';
			INSERT INTO silver.Diseases (
				disease_id,
				patient_id,
				disease_name,
				diagnosis_date
			)
				SELECT
					disease_id,
					patient_id,
					disease_name,
					CASE
						WHEN diagnosis_date IS NUll THEN GETDATE()
						ELSE diagnosis_date
					END AS diagnosis_date
				FROM bronze.Diseases
				ORDER BY diagnosis_date ASC
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> --------';

			--table doctor
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.Doctors';
			TRUNCATE TABLE silver.Doctors;
			PRINT '>> Inserting Data Into: silver.Doctors';
			INSERT INTO silver.Doctors(
				doctor_id,
				name,
				last_name,
				--gender,
				specialization,
				phone,
				email
			)
				SELECT 
					doctor_id,
					CASE	
						WHEN LOWER(LEFT(TRIM(name), CHARINDEX(' ', TRIM(name))-1)) IN ('chị', 'anh', 'bà', 'ông', 'cô', 'chú') THEN TRIM(SUBSTRING(TRIM(name), CHARINDEX(' ', TRIM(name))+1, 100))
						WHEN LOWER(LEFT(TRIM(name), CHARINDEX(' ', TRIM(name), CHARINDEX(' ', TRIM(name))+1))) IN ('quý ông', 'quý bà', 'quý cô') THEN TRIM(SUBSTRING(TRIM(name), CHARINDEX(' ', TRIM(name), CHARINDEX(' ', TRIM(name))+1)+1, 100))
						ELSE TRIM(name)
					END AS name,
					last_name,
					--CASE 
					--	WHEN TRIM(LOWER(gender)) IN ('male', 'm') THEN 'Nam'
					--	WHEN TRIM(LOWER(gender)) IN ('female', 'f') THEN 'Nữ'
					--	WHEN TRIM(LOWER(gender)) IS NULL THEN 'n/a'
					--	ELSE TRIM(gender)
					--END AS gender,
					specialization,
					CASE 
						WHEN SUBSTRING(phone, 1, 1) = '+' THEN '0' + REPLACE(REPLACE(SUBSTRING(TRIM(phone), 4, LEN(TRIM(phone))),'-',''), ' ', '')
						WHEN SUBSTRING(phone, 1, 1) = '(' THEN REPLACE(REPLACE(REPLACE(REPLACE(TRIM(phone),'-',''), ' ', ''), '(', ''), ')', '')
						ELSE REPLACE(REPLACE(TRIM(phone),'-',''), ' ', '')
					END AS phone,
					email
				FROM bronze.Doctors
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> --------';

			--table HospitalFee
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.HospitalFees';
			TRUNCATE TABLE silver.HospitalFees;
			PRINT '>> Inserting Data Into: silver.HospitalFees';
			INSERT INTO silver.HospitalFees(
				fee_id,
				appointment_id,
				patient_id,
				service_type,
				description,
				amount,
				phone,
				fee_date
			)
				SELECT 
				fee_id,
				appointment_id,
				patient_id,
				TRIM(service_type),
				TRIM(description),
				ROUND(CAST(amount AS FLOAT), 0),
				CASE 
						WHEN SUBSTRING(phone, 1, 1) = '+' THEN '0' + REPLACE(REPLACE(SUBSTRING(TRIM(phone), 4, LEN(TRIM(phone))),'-',''), ' ', '')
						WHEN SUBSTRING(phone, 1, 1) = '(' THEN REPLACE(REPLACE(REPLACE(REPLACE(TRIM(phone),'-',''), ' ', ''), '(', ''), ')', '')
						ELSE REPLACE(REPLACE(TRIM(phone),'-',''), ' ', '')
					END AS phone,
				fee_date
				FROM bronze.HospitalFees
				ORDER BY fee_date ASC
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> --------';

			--table patient
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.Patients';
			TRUNCATE TABLE silver.Patients;
			PRINT '>> Inserting Data Into: silver.Patients';
			INSERT INTO silver.Patients(
				patient_id,
				name,
				gender,
				dob,
				address,
				phone,
				email
			)
				SELECT 
					patient_id,
					CASE	
						WHEN LOWER(LEFT(TRIM(name), CHARINDEX(' ', TRIM(name))-1)) IN ('chị', 'anh', 'bà', 'ông', 'cô', 'chú') THEN TRIM(SUBSTRING(TRIM(name), CHARINDEX(' ', TRIM(name))+1, 100))
						WHEN LOWER(LEFT(TRIM(name), CHARINDEX(' ', TRIM(name), CHARINDEX(' ', TRIM(name))+1))) IN ('quý ông', 'quý bà', 'quý cô') THEN TRIM(SUBSTRING(TRIM(name), CHARINDEX(' ', TRIM(name), CHARINDEX(' ', TRIM(name))+1)+1, 100))
						ELSE TRIM(name)
					END AS name,
					CASE 
						WHEN TRIM(LOWER(gender)) IN ('male', 'm') THEN 'Nam'
						WHEN TRIM(LOWER(gender)) IN ('female', 'f') THEN 'Nữ'
						WHEN TRIM(LOWER(gender)) IS NULL THEN 'n/a'
						ELSE TRIM(gender)
					END AS gender,
					CAST(dob AS DATE),
					TRIM(address),
					CASE 
						WHEN SUBSTRING(phone, 1, 1) = '+' THEN '0' + REPLACE(REPLACE(SUBSTRING(TRIM(phone), 4, LEN(TRIM(phone))),'-',''), ' ', '')
						WHEN SUBSTRING(phone, 1, 1) = '(' THEN REPLACE(REPLACE(REPLACE(REPLACE(TRIM(phone),'-',''), ' ', ''), '(', ''), ')', '')
						ELSE REPLACE(REPLACE(TRIM(phone),'-',''), ' ', '')
					END AS phone,
					email
				FROM bronze.Patients
				ORDER BY dob ASC
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> --------';

			--table Prescription
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.Prescriptions';
			TRUNCATE TABLE silver.Prescriptions;
			PRINT '>> Inserting Data Into: silver.Prescriptions';
			INSERT INTO silver.Prescriptions(
				prescription_id,
				appointment_id,
				doctor_id,
				patient_id,
				medicine_name,
				form,
				dosage_mg,
				instruction,
				duration_days,
				note
			)
				SELECT
					prescription_id,
					appointment_id,
					doctor_id,
					patient_id,
					TRIM(medicine_name),
					TRIM(form),
					dosage_mg,
					TRIM(instruction),
					duration_days,
					TRIM(note)
				FROM bronze.Prescriptions
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> --------';

			--table Treatment
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.Treatments';
			TRUNCATE TABLE silver.Treatments;
			PRINT '>> Inserting Data Into: silver.Treatments';
			INSERT INTO silver.Treatments(
				treatment_id,
				patient_id,
				doctor_id,
				disease_id,
				treatment_description,
				treatment_date
			)
				SELECT
					treatment_id,
					patient_id,
					doctor_id,
					disease_id,
					CASE
						WHEN TRIM(treatment_description) IS NULL THEN 'n/a'
						ELSE TRIM(treatment_description)
					END AS treatment_description,
					treatment_date
				FROM bronze.Treatments
				ORDER BY treatment_date DESC
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> --------';

			--table vitalSign
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.VitalSigns';
			TRUNCATE TABLE silver.VitalSigns;
			PRINT '>> Inserting Data Into: silver.VitalSigns';
			INSERT INTO silver.VitalSigns(
				vital_id,
				patient_id,
				measurement_date,
				blood_pressure,
				heart_rate,
				respiratory_rate,
				temperature,
				oxygen_saturation,
				blood_sugar
			)
				SELECT 
					CASE
						WHEN vital_id IS NULL THEN 'n/a'
						ELSE vital_id
					END AS vital_id,
					CASE
						WHEN patient_id IS NULL THEN 'n/a'
						ELSE patient_id
					END AS patient_id,
					CASE
						WHEN measurement_date IS NULL THEN 'n/a'
						ELSE measurement_date
					END AS measurement_date,
					CASE
						WHEN TRIM(blood_pressure) IS NULL THEN 'n/a'
						WHEN CAST(LEFT(TRIM(blood_pressure), CHARINDEX('/', TRIM(blood_pressure)) - 1) AS FLOAT) < 90
						OR CAST(TRIM(SUBSTRING(TRIM(blood_pressure), CHARINDEX('/', TRIM(blood_pressure)) + 1, 100)) AS FLOAT) > 180
						THEN 'n/a'
						ELSE TRIM(blood_pressure)
					END AS blood_pressure,
					CASE
						WHEN heart_rate IS NULL THEN '-1'
						WHEN heart_rate < 50 OR heart_rate > 120 THEN '-1'
						ELSE heart_rate
					END AS heart_rate,
					CASE
						WHEN respiratory_rate IS NULL THEN '-1'
						WHEN respiratory_rate < 10 OR respiratory_rate > 30 THEN '-1'
						ELSE respiratory_rate
					END AS respiratory_rate,
					CASE
						WHEN temperature IS NULL THEN '-1'
						WHEN temperature < 30 OR temperature > 43 THEN '-1'
						ELSE temperature
					END AS temperature,
					CASE
						WHEN oxygen_saturation IS NULL THEN '-1'
						WHEN oxygen_saturation < 85 OR oxygen_saturation > 105 THEN '-1'
						ELSE oxygen_saturation
					END AS oxygen_saturation,
					CASE
						WHEN blood_sugar IS NULL THEN 'n/a'
						WHEN blood_sugar < 65 OR blood_sugar > 255 THEN '-1'
						ELSE blood_sugar
					END AS blood_sugar
				FROM bronze.VitalSigns
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> --------';

			--table LabResult
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.LabResults';
			TRUNCATE TABLE silver.LabResults;
			PRINT '>> Inserting Data Into: silver.LabResults';
			INSERT INTO silver.LabResults(
				lab_result_id,
				appointment_id,
				patient_id,
				test_type,
				parameter,
				value,
				unit,
				normal_range,
				interpretation,
				test_date
			)
				SELECT
					lab_result_id,
					appointment_id,
					patient_id,
					CASE
						WHEN TRIM(test_type) IS NULL THEN 'n/a'
						ELSE TRIM(test_type)
					END AS test_type,
					CASE
						WHEN TRIM(parameter) IS NULL THEN 'n/a'
						ELSE TRIM(parameter)
					END AS test_type,
					CASE
						WHEN TRIM(value) IS NULL THEN 'n/a'
						ELSE TRIM(value)
					END AS value,
					CASE
						WHEN TRIM(unit) IS NULL THEN 'n/a'
						ELSE TRIM(unit)
					END AS unit,
					CASE
						WHEN TRIM(normal_range) IS NULL THEN 'n/a'
						ELSE TRIM(normal_range)
					END AS normal_range,
					CASE
						WHEN TRIM(interpretation) IS NULL THEN 'n/a'
						ELSE TRIM(interpretation)
					END AS interpretation,
					CASE
						WHEN test_date IS NULL THEN GETDATE()
						ELSE test_date
					END AS test_date
				FROM bronze.LabResults
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> --------';
					
		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
