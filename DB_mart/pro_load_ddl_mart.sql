USE DB_mart;
GO

--EXAMPLE: EXEC load_Db_mart
--EXEC load_Db_mart;

-- CREATE LOADING PROCEDURE of DB_mart
CREATE OR ALTER PROCEDURE load_Db_mart AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading load_Db_mart';
        PRINT '================================================';

			-----------------------------------1. patient_age_groups-----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: patient_age_groups';
			TRUNCATE TABLE patient_age_groups;
			PRINT '>> Inserting Data Into: patient_age_groups';
			INSERT INTO patient_age_groups
			SELECT 
				CASE 
					WHEN age BETWEEN 0 AND 18 THEN '0-18'
					WHEN age BETWEEN 19 AND 35 THEN '19-35'
					WHEN age BETWEEN 36 AND 50 THEN '36-50'
					WHEN age BETWEEN 51 AND 65 THEN '51-65'
					ELSE '65+'
				END AS age_group,
				COUNT(*) AS total_patients
			FROM (
				SELECT DATEDIFF(YEAR, birthday, GETDATE()) - 
					   CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, birthday, GETDATE()), birthday) > GETDATE() THEN 1 ELSE 0 END AS age
				FROM DataWarehouse.gold.dim_patients
				WHERE birthday IS NOT NULL
			) AS derived
			GROUP BY 
				CASE 
					WHEN age BETWEEN 0 AND 18 THEN '0-18'
					WHEN age BETWEEN 19 AND 35 THEN '19-35'
					WHEN age BETWEEN 36 AND 50 THEN '36-50'
					WHEN age BETWEEN 51 AND 65 THEN '51-65'
					ELSE '65+'
				END;
			--select * from patient_age_groups

			-----------------------------------2. patient_gender_stats-----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: patient_gender_stats';
			TRUNCATE TABLE patient_gender_stats;
			PRINT '>> Inserting Data Into: patient_gender_stats';
			INSERT INTO patient_gender_stats
			SELECT 
				CASE 
					WHEN gender IN (N'Nam', N'Male', N'M') THEN N'Nam'
					WHEN gender IN (N'Nữ', N'Female', N'F') THEN N'Nữ'
					ELSE N'Không xác định'
				END AS gender_group,
				COUNT(*) AS total_patients
			FROM DataWarehouse.gold.dim_patients
			GROUP BY 
				CASE 
					WHEN gender IN (N'Nam', N'Male', N'M') THEN N'Nam'
					WHEN gender IN (N'Nữ', N'Female', N'F') THEN N'Nữ'
					ELSE N'Không xác định'
				END;

			--select * from patient_gender_stats

			-----------------------------------3. appointments_by_calendar_month-----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: appointments_by_calendar_month';
			TRUNCATE TABLE appointments_by_calendar_month;
			PRINT '>> Inserting Data Into: appointments_by_calendar_month';

			INSERT INTO appointments_by_calendar_month
			SELECT MONTH(appointment_date), COUNT(*)
			FROM DataWarehouse.gold.fact_appointments
			GROUP BY MONTH(appointment_date);
			--select * from appointments_by_calendar_month

			-----------------------------------4.appointments_by_status-----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: appointments_by_status';
			TRUNCATE TABLE appointments_by_status;
			PRINT '>> Inserting Data Into: appointments_by_status';

			INSERT INTO appointments_by_status
			SELECT status, COUNT(*)
			FROM DataWarehouse.gold.fact_appointments
			GROUP BY status;
			--select * from appointments_by_status

			-----------------------------------5. top5_doctors_by_appointments (top_doctors_by_appointments docter decreasing) -----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: top5_doctors_by_appointments';
			TRUNCATE TABLE top5_doctors_by_appointments;
			PRINT '>> Inserting Data Into: top5_doctors_by_appointments';

			INSERT INTO top5_doctors_by_appointments
			SELECT TOP 5 d.doctor_key, d.doctor_name, d.doctor_lastname, d.specialization, COUNT(*)
			FROM DataWarehouse.gold.fact_appointments fa
			JOIN DataWarehouse.gold.dim_doctors d ON fa.doctor_key = d.doctor_key
			GROUP BY d.doctor_key, d.doctor_name, d.doctor_lastname, d.specialization
			ORDER BY COUNT(*) DESC;
			--select * from top5_doctors_by_appointments

			-----------------------------------6. common_diseases-----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: common_diseases';
			TRUNCATE TABLE common_diseases;
			PRINT '>> Inserting Data Into: common_diseases';

			INSERT INTO common_diseases
			SELECT d.disease_name, COUNT(*)
			FROM DataWarehouse.gold.fact_diagnois fd
			JOIN DataWarehouse.gold.dim_diseases d ON fd.disease_key = d.disease_key
			GROUP BY d.disease_name;
			--select * from common_diseases

			-----------------------------------7. diseases_by_age_group-----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: diseases_by_age_group';
			TRUNCATE TABLE diseases_by_age_group;
			PRINT '>> Inserting Data Into: diseases_by_age_group';

			INSERT INTO diseases_by_age_group
			SELECT 
				d.disease_name,
				CASE 
					WHEN DATEDIFF(YEAR, p.birthday, GETDATE()) <= 18 THEN '0-18'
					WHEN DATEDIFF(YEAR, p.birthday, GETDATE()) BETWEEN 19 AND 35 THEN '19-35'
					WHEN DATEDIFF(YEAR, p.birthday, GETDATE()) BETWEEN 36 AND 50 THEN '36-50'
					WHEN DATEDIFF(YEAR, p.birthday, GETDATE()) BETWEEN 51 AND 65 THEN '51-65'
					ELSE '65+'
				END AS age_group,
				COUNT(*) AS total_cases
			FROM DataWarehouse.gold.fact_diagnois fd
			JOIN DataWarehouse.gold.dim_diseases d ON fd.disease_key = d.disease_key
			JOIN DataWarehouse.gold.dim_patients p ON fd.patient_key = p.patient_key
			GROUP BY 
				d.disease_name,
				CASE 
					WHEN DATEDIFF(YEAR, p.birthday, GETDATE()) <= 18 THEN '0-18'
					WHEN DATEDIFF(YEAR, p.birthday, GETDATE()) BETWEEN 19 AND 35 THEN '19-35'
					WHEN DATEDIFF(YEAR, p.birthday, GETDATE()) BETWEEN 36 AND 50 THEN '36-50'
					WHEN DATEDIFF(YEAR, p.birthday, GETDATE()) BETWEEN 51 AND 65 THEN '51-65'
					ELSE '65+'
				END;
			--select * from diseases_by_age_group


			-----------------------------------8. disease_by_month-----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: disease_by_month';
			TRUNCATE TABLE disease_by_month;
			PRINT '>> Inserting Data Into: disease_by_month';

			INSERT INTO disease_by_month
			SELECT 
				MONTH(fd.diagnosis_date),
				d.disease_name,
				COUNT(*)
			FROM DataWarehouse.gold.fact_diagnois fd
			JOIN DataWarehouse.gold.dim_diseases d ON fd.disease_key = d.disease_key
			GROUP BY MONTH(fd.diagnosis_date), d.disease_name;
			--select * from disease_by_month

			-----------------------------------9. disease_by_gender-----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: disease_by_gender';
			TRUNCATE TABLE disease_by_gender;
			PRINT '>> Inserting Data Into: disease_by_gender';

			INSERT INTO disease_by_gender
			SELECT 
				p.gender,
				d.disease_name,
				COUNT(*)
			FROM DataWarehouse.gold.fact_diagnois fd
			JOIN DataWarehouse.gold.dim_diseases d ON fd.disease_key = d.disease_key
			JOIN DataWarehouse.gold.dim_patients p ON fd.patient_key = p.patient_key
			GROUP BY p.gender, d.disease_name;
			/*
			select * from disease_by_gender
			where gender = N'Nữ'
			*/
			-----------------------------------10. cardiovascular_risk (risk of diabetes)-----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: cardiovascular_risk';
			TRUNCATE TABLE cardiovascular_risk;
			PRINT '>> Inserting Data Into: cardiovascular_risk';

			INSERT INTO cardiovascular_risk
			SELECT 
				p.patient_key,
				p.patient_name,
				CASE WHEN TRY_CAST(LEFT(vs.blood_pressure, CHARINDEX('/', vs.blood_pressure) - 1) AS INT) > 140 THEN 1 ELSE 0 END,
				CASE WHEN vs.heart_rate > 100 THEN 1 ELSE 0 END,
				CASE WHEN vs.respiratory_rate > 20 THEN 1 ELSE 0 END,
				CASE WHEN vs.oxygen_saturation < 95 THEN 1 ELSE 0 END,
				CASE WHEN vs.blood_sugar > 126 THEN 1 ELSE 0 END,
				(CASE WHEN TRY_CAST(LEFT(vs.blood_pressure, CHARINDEX('/', vs.blood_pressure) - 1) AS INT) > 140 THEN 1 ELSE 0 END
				 + CASE WHEN vs.heart_rate > 100 THEN 1 ELSE 0 END
				 + CASE WHEN vs.respiratory_rate > 20 THEN 1 ELSE 0 END
				 + CASE WHEN vs.oxygen_saturation < 95 THEN 1 ELSE 0 END
				 + CASE WHEN vs.blood_sugar > 126 THEN 1 ELSE 0 END)
			FROM DataWarehouse.gold.dim_patients p
			LEFT JOIN DataWarehouse.gold.fact_vital_signs vs ON p.patient_key = vs.patient_key;
			--select * from cardiovascular_risk

			-----------------------------------11. appointments_by_reason-----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: appointments_by_reason';
			TRUNCATE TABLE appointments_by_reason;
			PRINT '>> Inserting Data Into: appointments_by_reason';

			INSERT INTO appointments_by_reason
			SELECT reason, COUNT(*)
			FROM DataWarehouse.gold.fact_appointments
			GROUP BY reason;
			--select * from appointments_by_reason

			-----------------------------------12. appointments_by_time -----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: appointments_by_time';
			TRUNCATE TABLE appointments_by_time;
			PRINT '>> Inserting Data Into: appointments_by_time';

			INSERT INTO appointments_by_time
			SELECT FORMAT(start_time, 'HH:mm'), COUNT(*)
			FROM DataWarehouse.gold.fact_appointments
			GROUP BY FORMAT(start_time, 'HH:mm');
			--select * from appointments_by_time

			-----------------------------------13. appointments_by_location -----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: appointments_by_location';
			TRUNCATE TABLE appointments_by_location;
			PRINT '>> Inserting Data Into: appointments_by_location';

			INSERT INTO appointments_by_location
			SELECT location, COUNT(*)
			FROM DataWarehouse.gold.fact_appointments
			GROUP BY location;
			--select * from appointments_by_location

			-----------------------------------14. appointments_by_day_of_week -----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: appointments_by_day_of_week';
			TRUNCATE TABLE appointments_by_day_of_week;
			PRINT '>> Inserting Data Into: appointments_by_day_of_week';

			INSERT INTO appointments_by_day_of_week
			SELECT DATENAME(WEEKDAY, appointment_date), COUNT(*)
			FROM DataWarehouse.gold.fact_appointments
			GROUP BY DATENAME(WEEKDAY, appointment_date);
			--select * from appointments_by_day_of_week

			----------------------------------- 15. revenue_by_month -----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: revenue_by_month';
			TRUNCATE TABLE revenue_by_month;
			PRINT '>> Inserting Data Into: revenue_by_month';

			INSERT INTO revenue_by_month
			SELECT YEAR(fee_date), MONTH(fee_date), SUM(amount)
			FROM DataWarehouse.gold.fact_hospital_fees
			GROUP BY YEAR(fee_date), MONTH(fee_date);
			--select * from revenue_by_month

			----------------------------------- 16. revenue_by_service_type -----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: revenue_by_service_type';
			TRUNCATE TABLE revenue_by_service_type;
			PRINT '>> Inserting Data Into: revenue_by_service_type';

			INSERT INTO revenue_by_service_type
			SELECT service_type, SUM(amount)
			FROM DataWarehouse.gold.fact_hospital_fees
			GROUP BY service_type;
			--select * from revenue_by_service_type

			----------------------------------- 17. revenue_count_by_cost_range -----------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: revenue_count_by_cost_range';
			TRUNCATE TABLE revenue_count_by_cost_range;
			PRINT '>> Inserting Data Into: revenue_count_by_cost_range';

			INSERT INTO revenue_count_by_cost_range
			SELECT 
				CASE 
					WHEN amount < 1000000 THEN '<1000000'
					WHEN amount BETWEEN 1000000 AND 3000000 THEN '1000000-3000000'
					WHEN amount BETWEEN 3000000 AND 5000000 THEN '3000000-5000000'
					WHEN amount BETWEEN 5000000 AND 10000000 THEN '5000000-10000000'
					ELSE '>10000000'
				END,
				COUNT(*)
			FROM DataWarehouse.gold.fact_hospital_fees
			GROUP BY 
				CASE 
					WHEN amount < 1000000 THEN '<1000000'
					WHEN amount BETWEEN 1000000 AND 3000000 THEN '1000000-3000000'
					WHEN amount BETWEEN 3000000 AND 5000000 THEN '3000000-5000000'
					WHEN amount BETWEEN 5000000 AND 10000000 THEN '5000000-10000000'
					ELSE '>10000000'
				END;
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> --------';
					
		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading DB_mart is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING DB_mart'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
