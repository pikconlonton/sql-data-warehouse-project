/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

USE DataWarehouse;
GO

IF OBJECT_ID('silver.VitalSigns', 'U') IS NOT NULL
    DROP TABLE silver.VitalSigns;
GO
CREATE TABLE silver.VitalSigns (
    vital_id INT,
	patient_id INT,
	measurement_date DATE,
	blood_pressure VARCHAR(255),
	heart_rate INT,
	respiratory_rate INT,
	temperature FLOAT,
	oxygen_saturation INT,
	blood_sugar INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.Treatments', 'U') IS NOT NULL
    DROP TABLE silver.Treatments;
GO
CREATE TABLE silver.Treatments (
	treatment_id INT,
	patient_id INT,
	doctor_id INT,
	disease_id INT,
	treatment_description NVARCHAR(MAX),
	treatment_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.Prescriptions', 'U') IS NOT NULL
    DROP TABLE silver.Prescriptions;
GO
CREATE TABLE silver.Prescriptions (
    prescription_id INT,
	appointment_id INT,
	doctor_id INT,
	patient_id INT,
	medicine_name NVARCHAR(100),
	form NVARCHAR(50),
	dosage_mg INT,
	instruction NVARCHAR(255),
	duration_days INT,
	note NVARCHAR(255),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.Patients', 'U') IS NOT NULL
    DROP TABLE silver.Patients;
GO
CREATE TABLE silver.Patients (
    patient_id INT,
	name NVARCHAR(255),
	gender NVARCHAR(255),
	dob DATE,
	address NVARCHAR(255),
	phone NVARCHAR(255),
	email VARCHAR(255),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.LabResults', 'U') IS NOT NULL
    DROP TABLE silver.LabResults;
GO
CREATE TABLE silver.LabResults (
    lab_result_id INT,
	appointment_id INT,
	patient_id INT,
	test_type NVARCHAR(255),
	parameter NVARCHAR(255),
	value NVARCHAR(255),
	unit NVARCHAR(255),
	normal_range NVARCHAR(255),
	interpretation NVARCHAR(255),
	test_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.HospitalFees', 'U') IS NOT NULL
    DROP TABLE silver.HospitalFees;
GO
CREATE TABLE silver.HospitalFees (
    fee_id INT,
	appointment_id INT,
	patient_id INT,
	service_type NVARCHAR(255),
	description NVARCHAR(255),
	amount DECIMAL(10,2),
	fee_date DATE,
	phone NVARCHAR(255),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.Doctors', 'U') IS NOT NULL
    DROP TABLE silver.Doctors;
GO
CREATE TABLE silver.Doctors (
    doctor_id INT,
	name NVARCHAR(255),
	last_name NVARCHAR(255),
	specialization NVARCHAR(255),
	phone NVARCHAR(255),
	email VARCHAR(255),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.Diseases', 'U') IS NOT NULL
    DROP TABLE silver.Diseases;
GO
CREATE TABLE silver.Diseases (
    disease_id INT,
	patient_id INT,
	disease_name NVARCHAR(255),
	diagnosis_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.Appointments', 'U') IS NOT NULL
    DROP TABLE silver.Appointments;
GO
CREATE TABLE silver.Appointments (
    appointment_id INT,
	patient_id INT,
	doctor_id INT,
	appointment_date DATETIME,
	start_time DATETIME,
	end_time DATETIME,
	reason NVARCHAR(255),
	status NVARCHAR(255),
	phone NVARCHAR(255),
	location NVARCHAR(255),
	appointment_type NVARCHAR(255),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
