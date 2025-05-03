/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_patients
-- =============================================================================

IF OBJECT_ID('gold.dim_patients', 'V') IS NOT NULL
    DROP VIEW gold.dim_patients;
GO

CREATE VIEW gold.dim_patients AS
SELECT
    ROW_NUMBER() OVER (ORDER BY p.dwh_create_date) AS patient_key, -- Surrogate key
	p.patient_id,
	p.name as patient_name,
	p.gender,
	p.dob as birthday,
	p.address,
	p.phone as patient_phone_number,
	p.email as patient_email
FROM silver.Patients p
GO
--select * from gold.dim_patients

-- =============================================================================
-- Create Dimension: gold.dim_doctors
-- =============================================================================
--select * from silver.Doctors
IF OBJECT_ID('gold.dim_doctors', 'V') IS NOT NULL
    DROP VIEW gold.dim_doctors;
GO

CREATE VIEW gold.dim_doctors AS
SELECT
    ROW_NUMBER() OVER (ORDER BY d.dwh_create_date) AS doctor_key, -- Surrogate key
	d.doctor_id,
	d.name as doctor_name,
	d.last_name as doctor_lastname,
	d.specialization,
	d.phone as doctor_phone_number,
	d.email as doctor_email
FROM silver.Doctors as d
GO
--select * from gold.dim_doctors

-- ========================================================================================
-- Create Dimension And Fact: silver.Diseases tách thành gold.dim_diseases, gold.fact_diagnois
-- ========================================================================================

----------------------------gold.dim_diseases---------------------------------------------
IF OBJECT_ID('gold.dim_diseases', 'V') IS NOT NULL
    DROP VIEW gold.dim_diseases;
GO
CREATE VIEW gold.dim_diseases AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY D.disease_name) AS disease_key,
    D.disease_name
FROM silver.Diseases D
GROUP BY D.disease_name;
GO
--select * from gold.dim_diseases

----------------------------gold.fact_diagnois---------------------------------------------
IF OBJECT_ID('gold.fact_diagnois', 'V') IS NOT NULL
    DROP VIEW gold.fact_diagnois;
GO
CREATE VIEW gold.fact_diagnois AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY D.diagnosis_date) AS diagnosis_key,
	P.patient_key,
    DE.disease_key,
	D.disease_id AS diagnois_id,
	D.diagnosis_date
FROM silver.Diseases D
LEFT JOIN gold.dim_diseases AS DE
ON DE.disease_name = D.disease_name
LEFT JOIN gold.dim_patients AS P
ON P.patient_id = D.patient_id
GO
--select * from gold.fact_diagnois

-- ========================================================================================
-- Create Fact: gold.fact_appointments
-- ========================================================================================
--select * from silver.Appointments
IF OBJECT_ID('gold.fact_appointments', 'V') IS NOT NULL
    DROP VIEW  gold.fact_appointments;
GO
CREATE VIEW  gold.fact_appointments AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY A.start_time) AS appointment_key,
	P.patient_key,
	D.doctor_key,
	A.appointment_id,
	A.reason,
	A.status,
	A.phone AS patient_phone_number,
	A.location,
	A.appointment_type,
	A.appointment_date,
	A.start_time,
	A.end_time
FROM silver.Appointments A
LEFT JOIN gold.dim_patients AS P
ON P.patient_id = A.patient_id
LEFT JOIN gold.dim_doctors AS D
ON A.doctor_id = D.doctor_id
GO
--select * from gold.fact_appointments

-- ========================================================================================
-- Create Fact: gold.fact_hospital_fees
-- ========================================================================================
IF OBJECT_ID('gold.fact_hospital_fees', 'V') IS NOT NULL
    DROP VIEW  gold.fact_hospital_fees;
GO
CREATE VIEW  gold.fact_hospital_fees AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY hf.fee_date) AS hospital_fee_key,
	A.appointment_key,
	P.patient_key,
	hf.fee_id,
	hf.service_type,
	hf.amount,
	hf.description,
	hf.fee_date
FROM silver.HospitalFees as hf
LEFT JOIN gold.dim_patients AS P
ON P.patient_id = hf.patient_id
LEFT JOIN gold.fact_appointments AS A
ON A.appointment_id = hf.appointment_id
GO
--select * from gold.fact_hospital_fees

-- ========================================================================================
-- Create Fact: gold.fact_treatments
-- ========================================================================================
--select * from silver.Treatments
IF OBJECT_ID('gold.fact_treatments', 'V') IS NOT NULL
    DROP VIEW  gold.fact_treatments;
GO
CREATE VIEW  gold.fact_treatments AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY T.treatment_date) AS treatment_key,
	P.patient_key,
	DT.doctor_key,
	D.diagnosis_key,
	T.treatment_id,
	T.treatment_description,
	T.treatment_date
FROM silver.Treatments as T
LEFT JOIN gold.dim_patients AS P
ON P.patient_id = T.patient_id
LEFT JOIN gold.fact_diagnois AS D
ON D.diagnois_id = T.disease_id
LEFT JOIN gold.dim_doctors AS DT
ON DT.doctor_id = T.doctor_id
GO
--select * from gold.fact_treatments


-- ========================================================================================
-- Create Fact: gold.fact_prescriptions
-- ========================================================================================
--select * from silver.Prescriptions
IF OBJECT_ID('gold.fact_prescriptions', 'V') IS NOT NULL
    DROP VIEW  gold.fact_prescriptions;
GO
CREATE VIEW  gold.fact_prescriptions AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY PR.dwh_create_date) AS prescription_key,
	p.patient_key,
	DT.doctor_key,
	F.appointment_key,
	PR.prescription_id,
	PR.medicine_name,
	PR.form,
	PR.dosage_mg,
	PR.instruction,
	PR.duration_days,
	PR.note
FROM silver.Prescriptions as PR
LEFT JOIN gold.dim_patients AS P
ON P.patient_id = PR.patient_id
LEFT JOIN gold.dim_doctors AS DT
ON DT.doctor_id = PR.doctor_id
LEFT JOIN gold.fact_appointments AS F
ON F.appointment_id = PR.appointment_id
GO
--select * from gold.fact_prescriptions


-- ========================================================================================
-- Create Fact: gold.fact_vital_sign
-- ========================================================================================
--select * from silver.VitalSigns
IF OBJECT_ID('gold.fact_vital_signs', 'V') IS NOT NULL
    DROP VIEW  gold.fact_vital_signs;
GO
CREATE VIEW  gold.fact_vital_signs AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY VT.measurement_date,VT.blood_pressure,VT.heart_rate,VT.respiratory_rate,VT.oxygen_saturation) AS vital_sign_key,
	p.patient_key,
	VT.blood_pressure,
	VT.heart_rate,VT.respiratory_rate,
	VT.oxygen_saturation,
	VT.blood_sugar,
	VT.measurement_date
FROM silver.VitalSigns as VT
LEFT JOIN gold.dim_patients AS P
ON P.patient_id = VT.patient_id
GO
--select * from gold.fact_vital_signs

-- ========================================================================================
-- Create Fact: gold.fact_lab_results
-- ========================================================================================
--select * from silver.LabResults
IF OBJECT_ID('gold.fact_lab_results', 'V') IS NOT NULL
    DROP VIEW  gold.fact_lab_results;
GO
CREATE VIEW  gold.fact_lab_results AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY LR.test_date) AS lab_result_key,
	p.patient_key,
	A.appointment_key,
	LR.test_type,
	LR.parameter,
	LR.value,
	LR.unit,
	LR.normal_range,
	LR.interpretation,
	LR.test_date
FROM silver.LabResults as LR
LEFT JOIN gold.dim_patients AS P
ON P.patient_id = LR.patient_id
LEFT JOIN gold.fact_appointments AS A
ON A.appointment_id = LR.appointment_id
GO
--select * from gold.fact_lab_results

PRINT '================================================';
PRINT 'Created Gold Layer';
PRINT '================================================';
