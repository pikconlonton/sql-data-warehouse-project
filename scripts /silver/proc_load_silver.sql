USE DataWarehouse
GO
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
						WHEN appointment_date IS NULL THEN 'n/a'
						ELSE appointment_date
					END AS appointment_date,
					CASE
						WHEN start_time IS NULL THEN 'n/a'
						ELSE start_time
					END AS start_time,
					CASE
						WHEN end_time IS NULL THEN 'n/a'
						ELSE end_time
					END AS end_time,
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
				FROM bronze.Appointments AS src
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
					CASE
						WHEN disease_name IS NUll THEN 'Undiscovered disease'
						ELSE disease_name
					END AS disease_name,
					CASE
						WHEN diagnosis_date IS NUll THEN GETDATE()
						ELSE diagnosis_date
					END AS diagnosis_date
				FROM bronze.Diseases AS src
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
						WHEN name IS NULL THEN 'n/a'
						WHEN LOWER(LEFT(TRIM(name), CHARINDEX(' ', TRIM(name))-1)) IN ('chị', 'anh', 'bà', 'ông', 'cô', 'chú') THEN TRIM(SUBSTRING(TRIM(name), CHARINDEX(' ', TRIM(name))+1, 100))
						WHEN LOWER(LEFT(TRIM(name), CHARINDEX(' ', TRIM(name), CHARINDEX(' ', TRIM(name))+1))) IN ('quý ông', 'quý bà', 'quý cô') THEN TRIM(SUBSTRING(TRIM(name), CHARINDEX(' ', TRIM(name), CHARINDEX(' ', TRIM(name))+1)+1, 100))
						ELSE TRIM(name)
					END AS name,
					CASE
						WHEN last_name IS NULL THEN 'n/a'
						ELSE last_name
					END AS last_name,
					--CASE 
					--	WHEN TRIM(LOWER(gender)) IN ('male', 'm') THEN 'Nam'
					--	WHEN TRIM(LOWER(gender)) IN ('female', 'f') THEN 'Nữ'
					--	WHEN TRIM(LOWER(gender)) IS NULL THEN 'n/a'
					--	ELSE TRIM(gender)
					--END AS gender,
					CASE
						WHEN specialization IS NULL THEN 'n/a'
						ELSE specialization
					END AS specialization,
					CASE 
						WHEN SUBSTRING(phone, 1, 1) = '+' THEN '0' + REPLACE(REPLACE(SUBSTRING(TRIM(phone), 4, LEN(TRIM(phone))),'-',''), ' ', '')
						WHEN SUBSTRING(phone, 1, 1) = '(' THEN REPLACE(REPLACE(REPLACE(REPLACE(TRIM(phone),'-',''), ' ', ''), '(', ''), ')', '')
						ELSE REPLACE(REPLACE(TRIM(phone),'-',''), ' ', '')
					END AS phone,
					CASE
						WHEN TRIM(email) IS NULL THEN 'n\a'
						WHEN TRIM(email) LIKE '%[^a-zA-Z0-9@.]%' OR TRIM(email) NOT LIKE '%@%.%' THEN 'n/a'
						ELSE TRIM(email)					
					END AS email
				FROM bronze.Doctors AS src
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
				CASE
					WHEN TRIM(service_type) IS NULL OR LEN(TRIM(service_type)) = 0THEN 'n/a'
					ELSE TRIM(service_type)
				END AS service_type,
				CASE
					WHEN TRIM(description) IS NULL OR LEN(TRIM(description)) = 0THEN 'n/a'
					ELSE TRIM(description)
				END AS description,
				CASE 
					WHEN amount IS NULL THEN 0
					ELSE amount
				END AS amount,
				CASE 
					WHEN SUBSTRING(phone, 1, 1) = '+' THEN '0' + REPLACE(REPLACE(SUBSTRING(TRIM(phone), 4, LEN(TRIM(phone))),'-',''), ' ', '')
					WHEN SUBSTRING(phone, 1, 1) = '(' THEN REPLACE(REPLACE(REPLACE(REPLACE(TRIM(phone),'-',''), ' ', ''), '(', ''), ')', '')
					ELSE REPLACE(REPLACE(TRIM(phone),'-',''), ' ', '')
				END AS phone,
				CASE
					WHEN fee_date IS NULL THEN 'n/a'
					ELSE fee_date
				END AS fee_date
				FROM bronze.HospitalFees AS src
			SET @end_time = GETDATE()
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
						WHEN TRIM(LOWER(gender)) IN ('male', 'm', 'nam') THEN 'Nam'
						WHEN TRIM(LOWER(gender)) IN ('female', 'f', 'nữ', 'nu') THEN 'Nữ'
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
					CASE
						WHEN TRIM(email) IS NULL THEN 'n\a'
						WHEN TRIM(email) LIKE '%[^a-zA-Z0-9@.]%' OR TRIM(email) NOT LIKE '%@%.%' THEN 'n/a'
						ELSE TRIM(email)					
					END AS email
				FROM bronze.Patients AS src
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
					CASE
						WHEN TRIM(medicine_name) IS NULL OR LEN(TRIM(medicine_name)) = 0 THEN 'n/a'
						ELSE TRIM(medicine_name)
					END AS medicine_name,
					CASE
						WHEN TRIM(form) IS NULL OR LEN(TRIM(form)) = 0 THEN 'n/a'
						ELSE TRIM(form)
					END AS form,
					CASE
						WHEN dosage_mg IS NULL THEN 'n/a'
						ELSE dosage_mg
					END AS dosage_mg,
					CASE
						WHEN TRIM(instruction) IS NULL OR LEN(TRIM(instruction)) = 0 THEN 'n/a'
						ELSE TRIM(instruction)
					END AS instruction,
					CASE
						WHEN duration_days IS NULL THEN 0
						ELSE duration_days
					END AS duration_days,
					CASE
						WHEN TRIM(note) IS NULL OR LEN(TRIM(note)) = 0 THEN 'n/a'
						ELSE TRIM(note)
					END AS note
				FROM bronze.Prescriptions AS src
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
					CASE
						WHEN treatment_date IS NULL THEN 'n/a'
						ELSE treatment_date
					END AS treatment_date
				FROM bronze.Treatments AS src
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
						WHEN heart_rate IS NULL THEN 70
						WHEN heart_rate < 50 OR heart_rate > 120 THEN 70
						ELSE heart_rate
					END AS heart_rate,
					CASE
						WHEN respiratory_rate IS NULL THEN 20
						WHEN respiratory_rate < 10 OR respiratory_rate > 30 THEN 20
						ELSE respiratory_rate
					END AS respiratory_rate,
					CASE
						WHEN temperature IS NULL THEN 37
						WHEN temperature < 30 OR temperature > 43 THEN 37
						ELSE temperature
					END AS temperature,
					CASE
						WHEN oxygen_saturation IS NULL THEN 95
						WHEN oxygen_saturation < 85 OR oxygen_saturation > 105 THEN 95
						ELSE oxygen_saturation
					END AS oxygen_saturation,
					CASE
						WHEN blood_sugar IS NULL THEN 175
						WHEN blood_sugar < 65 OR blood_sugar > 255 THEN 160
						ELSE blood_sugar
					END AS blood_sugar
				FROM bronze.VitalSigns AS src
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
