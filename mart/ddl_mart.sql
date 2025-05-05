Use DataWarehouse
go

-----------------------------PHÂN TÍCH TỔNG QUAN BỆNH VIỆN ---------------------------------------------
-----------------------bệnh nhân theo tuổi-----------------------------
IF OBJECT_ID('mart.patient_age_groups', 'V') IS NOT NULL
    DROP VIEW mart.patient_age_groups;
GO

CREATE VIEW mart.patient_age_groups AS
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
    SELECT 
        DATEDIFF(YEAR, dp.birthday, GETDATE()) 
            - CASE 
                WHEN DATEADD(YEAR, DATEDIFF(YEAR, dp.birthday, GETDATE()), dp.birthday) > GETDATE() 
                THEN 1 
                ELSE 0 
              END AS age
    FROM gold.dim_patients dp
    WHERE dp.birthday IS NOT NULL
) AS derived
GROUP BY 
    CASE 
        WHEN age BETWEEN 0 AND 18 THEN '0-18'
        WHEN age BETWEEN 19 AND 35 THEN '19-35'
        WHEN age BETWEEN 36 AND 50 THEN '36-50'
        WHEN age BETWEEN 51 AND 65 THEN '51-65'
        ELSE '65+'
    END;
GO

--SELECT * FROM mart.patient_age_groups ORDER BY age_group;


-----------------------bệnh nhân theo giới tính-----------------------------
IF OBJECT_ID('mart.patient_gender_stats', 'V') IS NOT NULL
    DROP VIEW mart.patient_gender_stats;
GO

CREATE VIEW mart.patient_gender_stats AS
SELECT 
    CASE 
        WHEN dp.gender IN (N'Nam', N'Male', N'M') THEN N'Nam'
        WHEN dp.gender IN (N'Nữ', N'Female', N'F') THEN N'Nữ'
        ELSE N'Không xác định'
    END AS gender_group,
    COUNT(*) AS total_patients
FROM gold.dim_patients dp
GROUP BY 
    CASE 
        WHEN dp.gender IN (N'Nam', N'Male', N'M') THEN N'Nam'
        WHEN dp.gender IN (N'Nữ', N'Female', N'F') THEN N'Nữ'
        ELSE N'Không xác định'
    END;
GO
--SELECT * FROM mart.patient_gender_stats;

--------------------------cuộc họp theo tháng---------------------------
IF OBJECT_ID('mart.appointments_by_calendar_month', 'V') IS NOT NULL
    DROP VIEW mart.appointments_by_calendar_month;
GO

CREATE VIEW mart.appointments_by_calendar_month AS
SELECT 
    MONTH(appointment_date) AS calendar_month,
    COUNT(*) AS total_appointments
FROM gold.fact_appointments
GROUP BY MONTH(appointment_date);
GO

--SELECT * 
--FROM mart.appointments_by_calendar_month
--ORDER BY calendar_month;

--------------------------cuộc họp theo trạng thái ---------------------------
IF OBJECT_ID('mart.appointments_by_status', 'V') IS NOT NULL
    DROP VIEW mart.appointments_by_status;
GO

CREATE VIEW mart.appointments_by_status AS
SELECT 
    status,
    COUNT(*) AS total_appointments
FROM gold.fact_appointments
GROUP BY status;
GO
SELECT * FROM mart.appointments_by_status;

--------------------------top 5 bác sĩ có cuộc hẹn nhiều nhất ---------------------------
IF OBJECT_ID('mart.top5_doctors_by_appointments', 'V') IS NOT NULL
    DROP VIEW mart.top5_doctors_by_appointments;
GO
CREATE VIEW mart.top5_doctors_by_appointments AS
SELECT TOP 5
    d.doctor_key,
    d.doctor_name,
    d.doctor_lastname,
    d.specialization,
    COUNT(*) AS total_appointments
FROM gold.fact_appointments fa
JOIN gold.dim_doctors d ON fa.doctor_key = d.doctor_key
GROUP BY d.doctor_key, d.doctor_name, d.doctor_lastname, d.specialization
ORDER BY total_appointments DESC;
GO
SELECT * FROM mart.top5_doctors_by_appointments;


-----------------------------PHÂN TÍCH BỆNH LÝ---------------------------------------------
--------------------------top bệnh lý phổ biến nhất ---------------------------
IF OBJECT_ID('mart.common_diseases', 'V') IS NOT NULL
    DROP VIEW mart.common_diseases;
GO

CREATE VIEW mart.common_diseases AS
SELECT 
    d.disease_name,
    COUNT(*) AS total_cases
FROM gold.fact_diagnois fd
JOIN gold.dim_diseases d ON fd.disease_key = d.disease_key
GROUP BY d.disease_name;
GO

--SELECT TOP 10 *
--FROM mart.common_diseases
--ORDER BY total_cases DESC;

--------------------------bệnh lý theo nhóm tuổi ---------------------------
IF OBJECT_ID('mart.diseases_by_age_group', 'V') IS NOT NULL
    DROP VIEW mart.diseases_by_age_group;
GO

CREATE VIEW mart.diseases_by_age_group AS
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
FROM gold.fact_diagnois fd
JOIN gold.dim_diseases d ON fd.disease_key = d.disease_key
JOIN gold.dim_patients p ON fd.patient_key = p.patient_key
GROUP BY 
    d.disease_name,
    CASE 
        WHEN DATEDIFF(YEAR, p.birthday, GETDATE()) <= 18 THEN '0-18'
        WHEN DATEDIFF(YEAR, p.birthday, GETDATE()) BETWEEN 19 AND 35 THEN '19-35'
        WHEN DATEDIFF(YEAR, p.birthday, GETDATE()) BETWEEN 36 AND 50 THEN '36-50'
        WHEN DATEDIFF(YEAR, p.birthday, GETDATE()) BETWEEN 51 AND 65 THEN '51-65'
        ELSE '65+'
    END;
GO
/*
SELECT 
    disease_name,
    total_cases
FROM mart.diseases_by_age_group
WHERE age_group = '0-18'
ORDER BY total_cases DESC;
*/

--------------------------xu hướng bệnh lý theo thời gian (theo tháng) ---------------------------
IF OBJECT_ID('mart.disease_by_month', 'V') IS NOT NULL
    DROP VIEW mart.disease_by_month;
GO

CREATE VIEW mart.disease_by_month AS
SELECT 
    MONTH(fd.diagnosis_date) AS month,
    dd.disease_name,
    COUNT(*) AS total_cases
FROM gold.fact_diagnois fd
JOIN gold.dim_diseases dd ON fd.disease_key = dd.disease_key
GROUP BY MONTH(fd.diagnosis_date), dd.disease_name;
GO
/*
SELECT * FROM mart.disease_by_month
where month = 12
ORDER BY month, total_cases DESC;
*/

--------------------------xu hướng bệnh lý theo giới tính ---------------------------
IF OBJECT_ID('mart.disease_by_gender', 'V') IS NOT NULL
    DROP VIEW mart.disease_by_gender;
GO

CREATE VIEW mart.disease_by_gender AS
SELECT 
    dp.gender,
    dd.disease_name,
    COUNT(*) AS total_cases
FROM gold.fact_diagnois fd
JOIN gold.dim_diseases dd ON fd.disease_key = dd.disease_key
JOIN gold.dim_patients dp ON fd.patient_key = dp.patient_key
GROUP BY dp.gender, dd.disease_name;
GO
SELECT * 
FROM mart.disease_by_gender
ORDER BY gender, total_cases DESC;

--------------------------xu hướng bệnh lý theo giới tính (CẦN PHẢI TRAO ĐỔI THÊM) ---------------------------
IF OBJECT_ID('mart.cardiovascular_risk', 'V') IS NOT NULL
    DROP VIEW mart.cardiovascular_risk;
GO

CREATE VIEW mart.cardiovascular_risk AS
SELECT 
    P.patient_key,
    P.patient_name,
    
    -- Tính tỷ lệ tăng huyết áp: (Huyết áp > 140/90)
    CASE 
        WHEN CAST(SUBSTRING(VS.blood_pressure, 1, CHARINDEX('/', VS.blood_pressure) - 1) AS INT) > 140 THEN 1
        ELSE 0
    END AS high_blood_pressure,
    
    -- Tính tỷ lệ nhịp tim cao: (Nhịp tim > 100)
    CASE 
        WHEN VS.heart_rate > 100 THEN 1
        ELSE 0
    END AS high_heart_rate,

    -- Tính tỷ lệ khó thở: (Tần số hô hấp > 20)
    CASE 
        WHEN VS.respiratory_rate > 20 THEN 1
        ELSE 0
    END AS high_respiratory_rate,

    -- Tính tỷ lệ oxy trong máu thấp: (Oxygen saturation < 95%)
    CASE 
        WHEN VS.oxygen_saturation < 95 THEN 1
        ELSE 0
    END AS low_oxygen_saturation,

    -- Tính tỷ lệ đường huyết cao: (Đường huyết > 126 mg/dL)
    CASE 
        WHEN VS.blood_sugar > 126 THEN 1
        ELSE 0
    END AS high_blood_sugar,
    
    -- Tổng số yếu tố rủi ro
    (CASE 
        WHEN CAST(SUBSTRING(VS.blood_pressure, 1, CHARINDEX('/', VS.blood_pressure) - 1) AS INT) > 140 THEN 1
        ELSE 0
    END + 
    CASE 
        WHEN VS.heart_rate > 100 THEN 1
        ELSE 0
    END + 
    CASE 
        WHEN VS.respiratory_rate > 20 THEN 1
        ELSE 0
    END + 
    CASE 
        WHEN VS.oxygen_saturation < 95 THEN 1
        ELSE 0
    END + 
    CASE 
        WHEN VS.blood_sugar > 126 THEN 1
        ELSE 0
    END) AS total_risk_factors

FROM gold.dim_patients P
LEFT JOIN gold.fact_vital_signs VS
    ON P.patient_key = VS.patient_key;
GO
/*
SELECT *
FROM mart.cardiovascular_risk;
*/

---------------------------------PHÂN TÍCH ĐẶT LỊCH VÀ CUỘC HẸN----------------------------

--------------------------thống kê cuộc hẹn theo lý do(reason) ---------------------------
IF OBJECT_ID('mart.appointments_by_reason', 'V') IS NOT NULL
    DROP VIEW mart.appointments_by_reason;
GO

CREATE VIEW mart.appointments_by_reason AS
SELECT 
    A.reason, 
    COUNT(A.appointment_key) AS total_appointments 
FROM 
    gold.fact_appointments A
GROUP BY 
    A.reason;
GO
/*
SELECT * 
FROM mart.appointments_by_reason
ORDER BY total_appointments DESC;
*/
--------------------------thống kê cuộc hẹn theo thời gian ---------------------------
IF OBJECT_ID('mart.appointments_by_time', 'V') IS NOT NULL
    DROP VIEW mart.appointments_by_time;
GO

CREATE VIEW mart.appointments_by_time AS
SELECT 
    FORMAT(A.start_time, 'HH:mm') AS appointment_time,  -- Định dạng giờ: phút
    COUNT(A.appointment_key) AS total_appointments       -- Tổng số cuộc hẹn theo thời gian
FROM 
    gold.fact_appointments A
GROUP BY 
    FORMAT(A.start_time, 'HH:mm');  
GO

/*
SELECT * 
FROM mart.appointments_by_time
ORDER BY appointment_time;

*/
--------------------------thống kê cuộc hẹn theo địa điểm ---------------------------
IF OBJECT_ID('mart.appointments_by_location', 'V') IS NOT NULL
    DROP VIEW mart.appointments_by_location;
GO

CREATE VIEW mart.appointments_by_location AS
SELECT 
    A.location,                               -- Địa điểm của cuộc hẹn
    COUNT(A.appointment_key) AS total_appointments  -- Tổng số cuộc hẹn tại mỗi địa điểm
FROM 
    gold.fact_appointments A
GROUP BY 
    A.location;                                -- Nhóm theo địa điểm
GO
/*
SELECT * 
FROM mart.appointments_by_location
ORDER BY total_appointments DESC;  
*/

--------------------------thống kê cuộc hẹn theo thứ trong tuần ---------------------------
IF OBJECT_ID('mart.appointments_by_day_of_week', 'V') IS NOT NULL
    DROP VIEW mart.appointments_by_day_of_week;
GO

CREATE VIEW mart.appointments_by_day_of_week AS
SELECT 
    DATENAME(WEEKDAY, A.appointment_date) AS day_of_week,  -- Trích xuất ngày trong tuần
    COUNT(A.appointment_key) AS total_appointments         -- Tổng số cuộc hẹn theo thứ
FROM 
    gold.fact_appointments A
GROUP BY 
    DATENAME(WEEKDAY, A.appointment_date);  -- Nhóm theo ngày trong tuần
GO

/*
SELECT * 
FROM mart.appointments_by_day_of_week
ORDER BY 
    CASE 
        WHEN day_of_week = 'Monday' THEN 1
        WHEN day_of_week = 'Tuesday' THEN 2
        WHEN day_of_week = 'Wednesday' THEN 3
        WHEN day_of_week = 'Thursday' THEN 4
        WHEN day_of_week = 'Friday' THEN 5
        WHEN day_of_week = 'Saturday' THEN 6
        WHEN day_of_week = 'Sunday' THEN 7
    END;
*/

--------------------------PHÂN TÍCH CHI PHÍ VÀ THANH TOÁN ---------------------------

--------------------------thống kê doanh thu theo tháng của năm ---------------------------
IF OBJECT_ID('mart.revenue_by_month', 'V') IS NOT NULL
    DROP VIEW mart.revenue_by_month;
GO

CREATE VIEW mart.revenue_by_month AS
SELECT 
    YEAR(hf.fee_date) AS year,                               -- Năm
    MONTH(hf.fee_date) AS month,                             -- Tháng
    SUM(hf.amount) AS total_revenue                          -- Tổng doanh thu
FROM 
    gold.fact_hospital_fees hf
GROUP BY 
    YEAR(hf.fee_date), MONTH(hf.fee_date);                  -- Nhóm theo năm và tháng
GO

/*
SELECT 
    year, 
    month, 
    total_revenue
FROM 
    mart.revenue_by_month
ORDER BY 
    year, month;
*/

--------------------------thống kê doanh thu theo dịch vụ ---------------------------
IF OBJECT_ID('mart.revenue_by_service_type', 'V') IS NOT NULL
    DROP VIEW mart.revenue_by_service_type;
GO

CREATE VIEW mart.revenue_by_service_type AS
SELECT 
    hf.service_type, 
    SUM(hf.amount) AS total_revenue
FROM 
    gold.fact_hospital_fees hf
GROUP BY 
    hf.service_type
GO
--SELECT * FROM mart.revenue_by_service_type;

--------------------------thống kê doanh thu theo tháng của năm ---------------------------
IF OBJECT_ID('mart.revenue_count_by_cost_range', 'V') IS NOT NULL
    DROP VIEW mart.revenue_count_by_cost_range;
GO

CREATE VIEW mart.revenue_count_by_cost_range AS
SELECT 
    CASE 
        WHEN hf.amount < 1000000 THEN '<1000000'
        WHEN hf.amount BETWEEN 1000000 AND 3000000 THEN '1000000-3000000'
        WHEN hf.amount BETWEEN 3000000 AND 5000000 THEN '3000000-5000000'
        WHEN hf.amount BETWEEN 5000000 AND 10000000 THEN '5000000-10000000'
        WHEN hf.amount > 10000000 THEN '>10000000'
    END AS cost_range,
    COUNT(*) AS frequency
FROM 
    gold.fact_hospital_fees hf
GROUP BY 
    CASE 
        WHEN hf.amount < 1000000 THEN '<1000000'
        WHEN hf.amount BETWEEN 1000000 AND 3000000 THEN '1000000-3000000'
        WHEN hf.amount BETWEEN 3000000 AND 5000000 THEN '3000000-5000000'
        WHEN hf.amount BETWEEN 5000000 AND 10000000 THEN '5000000-10000000'
        WHEN hf.amount > 10000000 THEN '>10000000'
    END
GO
--SELECT * FROM mart.revenue_count_by_cost_range;
