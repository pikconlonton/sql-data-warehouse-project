USE DataWarehouse;
GO

--EXEC gold.load_gold;

CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME; 
	DECLARE @sql NVARCHAR(MAX);
    BEGIN TRY
		SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Gold Layer';
        PRINT '================================================';

		--dim_patients
		IF OBJECT_ID('gold.dim_patients', 'V') IS NOT NULL
		BEGIN
			SET @sql = N'DROP VIEW gold.dim_patients;';
			EXEC sp_executesql @sql;
		END
	
		SET @sql = N'
		CREATE VIEW gold.dim_patients AS
		SELECT
			ROW_NUMBER() OVER (ORDER BY p.dwh_create_date) AS patient_key,
			p.patient_id,
			p.name as patient_name,
			p.gender,
			p.dob as birthday,
			p.address,
			p.phone as patient_phone_number,
			p.email as patient_email
		FROM silver.Patients p;';
		EXEC sp_executesql @sql;
		
		--dim_doctors
		IF OBJECT_ID('gold.dim_doctors' ,'V') IS NOT NULL
		BEGIN
			SET @sql = N'DROP VIEW gold.dim_doctors;';
			EXEC sp_executesql @sql;
		END

		SET @sql = N'
		CREATE VIEW gold.dim_doctors AS
		SELECT
			ROW_NUMBER() OVER (ORDER BY d.dwh_create_date) AS doctor_key, -- Surrogate key
			d.doctor_id,
			d.name as doctor_name,
			d.last_name as doctor_lastname,
			d.specialization,
			d.phone as doctor_phone_number,
			d.email as doctor_email
		FROM silver.Doctors as d;';
		EXEC sp_executesql @sql;;

		--dim_disease
		IF OBJECT_ID('gold.dim_diseases', 'V') IS NOT NULL
		BEGIN
			SET @sql = N'DROP VIEW gold.dim_diseases;';
			EXEC sp_executesql @sql;
		END
		SET @sql = N'
		CREATE VIEW gold.dim_diseases AS
		SELECT 
			ROW_NUMBER() OVER (ORDER BY D.disease_name) AS disease_key,
			D.disease_name
		FROM silver.Diseases D
		GROUP BY D.disease_name;';
		EXEC sp_executesql @sql;

		--fact_diagnosis
		IF OBJECT_ID('gold.fact_diagnois', 'V') IS NOT NULL
		BEGIN
			SET @sql = N'DROP VIEW gold.fact_diagnois;';
			EXEC sp_executesql @sql;
		END
		SET @sql = N'
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
		ON P.patient_id = D.patient_id;';
		EXEC sp_executesql @sql;

		--fact_appoinments
		IF OBJECT_ID('gold.fact_appointments', 'V') IS NOT NULL
		BEGIN
			SET @sql = 'DROP VIEW  gold.fact_appointments;';
			EXEC sp_executesql @sql;
		END
		SET @sql = N'
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
		ON A.doctor_id = D.doctor_id;';
		EXEC sp_executesql @sql;
		
		--fact_hospital_fees
		IF OBJECT_ID('gold.fact_hospital_fees', 'V') IS NOT NULL
		BEGIN	
			SET @sql = 'DROP VIEW  gold.fact_hospital_fees;';
			EXEC sp_executesql @sql;
		END
		SET @sql = N'
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
		ON A.appointment_id = hf.appointment_id;';
		EXEC sp_executesql @sql;
		
		--fact_treatments
		IF OBJECT_ID('gold.fact_treatments', 'V') IS NOT NULL
		BEGIN
			SET @sql = N'DROP VIEW  gold.fact_treatments;';
			EXEC sp_executesql @sql;
		END
		SET @sql = N'
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
		ON DT.doctor_id = T.doctor_id;';
		EXEC sp_executesql @sql;

		--fact_prescriptions
		IF OBJECT_ID('gold.fact_prescriptions', 'V') IS NOT NULL
		BEGIN
			SET @sql = 'DROP VIEW  gold.fact_prescriptions;';
			EXEC sp_executesql @sql;
		END
		SET @sql = N'
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
		ON F.appointment_id = PR.appointment_id;';
		EXEC sp_executesql @sql;
		
		--fact_vital_signs
		IF OBJECT_ID('gold.fact_vital_signs', 'V') IS NOT NULL
		BEGIN	
			SET @sql = 'DROP VIEW  gold.fact_vital_signs;';
			EXEC sp_executesql @sql;
		END
		SET @sql = N'
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
		ON P.patient_id = VT.patient_id;';
		EXEC sp_executesql @sql;
		
		--fact_lab_results
		IF OBJECT_ID('gold.fact_lab_results', 'V') IS NOT NULL
		BEGIN
			SET @sql = N'DROP VIEW  gold.fact_lab_results;';
			EXEC sp_executesql @sql;
		END
		SET @sql = N'
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
		ON A.appointment_id = LR.appointment_id;';
		EXEC sp_executesql @sql;

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Gold Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING GOLD LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
