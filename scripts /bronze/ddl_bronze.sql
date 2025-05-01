USE DataWarehouse 
GO

/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
  
-- Bảng Patients (Bệnh nhân)
IF OBJECT_ID('bronze.Patients', 'U') IS NOT NULL
    DROP TABLE bronze.Patients ;
GO
CREATE TABLE bronze.Patients (
    patient_id INT ,
    --first_name NVARCHAR(100),
    --last_name NVARCHAR(100),
	name NVARCHAR(100),
    gender NVARCHAR(10),
	dob NVARCHAR(100),
    address NVARCHAR(MAX),
	phone NVARCHAR(255),
	email VARCHAR(100)
    --date_registered DATETIME DEFAULT GETDATE()
);
GO

-- Bảng Doctors (Bác sĩ)
IF OBJECT_ID('bronze.Doctors', 'U') IS NOT NULL
    DROP TABLE bronze.Doctors ;
GO
CREATE TABLE bronze.Doctors (
    doctor_id INT,
    name NVARCHAR(100),
    last_name NVARCHAR(100),
    specialization NVARCHAR(100),
    phone NVARCHAR(255),
	email VARCHAR(225),
    --date_joined DATETIME DEFAULT GETDATE()
);
GO

-- Bảng Appointments (Cuộc hẹn)
IF OBJECT_ID('bronze.Appointments', 'U') IS NOT NULL
    DROP TABLE bronze.Appointments ;
GO
CREATE TABLE bronze.Appointments (
    appointment_id INT ,
    patient_id INT,
    doctor_id INT,
    appointment_date DATETIME,
	start_time DATETIME,
	end_time DATETIME,
    reason NVARCHAR(100),
    status NVARCHAR(50),
	phone NVARCHAR(100),
	location NVARCHAR(100),
	appointment_type NVARCHAR(100),

);
GO

-- Bảng VitalSigns (Dấu hiệu sinh tồn)
IF OBJECT_ID('bronze.VitalSigns', 'U') IS NOT NULL
    DROP TABLE bronze.VitalSigns;
GO
CREATE TABLE bronze.VitalSigns (
    vital_id INT PRIMARY KEY,
    patient_id INT,
    measurement_date DATE,
    blood_pressure VARCHAR(10),         -- Ví dụ: '171/118'
    heart_rate INT,
    respiratory_rate INT,
    temperature FLOAT,
    oxygen_saturation INT,
    blood_sugar INT
);
GO

-- Bảng Diseases (Bệnh án)
IF OBJECT_ID('bronze.Diseases', 'U') IS NOT NULL
    DROP TABLE bronze.Diseases ;
GO
CREATE TABLE bronze.Diseases (
    disease_id INT ,
    patient_id INT,
    disease_name NVARCHAR(255),
    diagnosis_date DATE,
    --diagnosis_details NVARCHAR(MAX)
);
GO

-- Bảng Treatments (Điều trị)
IF OBJECT_ID('bronze.Treatments', 'U') IS NOT NULL
    DROP TABLE bronze.Treatments ;
GO
CREATE TABLE bronze.Treatments (
    treatment_id INT ,
    patient_id INT,
    doctor_id INT,
	disease_id INT,
	treatment_description NVARCHAR(MAX),
    treatment_date DATE,
    
);
GO
-- Bảng HospitalFees (Viện phí) Bang lol loi
IF OBJECT_ID('bronze.HospitalFees', 'U') IS NOT NULL
    DROP TABLE bronze.HospitalFees ;
GO
CREATE TABLE bronze.HospitalFees (
      fee_id INT PRIMARY KEY,
    appointment_id INT,
    patient_id INT,
    service_type NVARCHAR(100),
    description NVARCHAR(255),
    amount DECIMAL(10,2),
    fee_date DATE,
    phone NVARCHAR(20)
);
GO

-- Bảng Prescriptions (Đơn thuốc)
IF OBJECT_ID('bronze.Prescriptions', 'U') IS NOT NULL
    DROP TABLE bronze.Prescriptions ;
GO
CREATE TABLE bronze.Prescriptions (
      prescription_id INT PRIMARY KEY,
    appointment_id INT,
    doctor_id INT,
    patient_id INT,
    medicine_name NVARCHAR(100),
    form NVARCHAR(50),
    dosage_mg INT,
    instruction NVARCHAR(255),
    duration_days INT,
    note NVARCHAR(255)
);
GO
