-- Script tạo toàn bộ các bảng trong DB_mart tương ứng với các views mart trong DataWarehouse
USE DB_mart;
GO

-- 1. patient_age_groups
IF OBJECT_ID('patient_age_groups', 'U') IS NOT NULL DROP TABLE patient_age_groups;
CREATE TABLE patient_age_groups (
    age_group NVARCHAR(20),
    total_patients INT
);

-- 2. patient_gender_stats
IF OBJECT_ID('patient_gender_stats', 'U') IS NOT NULL DROP TABLE patient_gender_stats;
CREATE TABLE patient_gender_stats (
    gender_group NVARCHAR(20),
    total_patients INT
);

-- 3. appointments_by_calendar_month
IF OBJECT_ID('appointments_by_calendar_month', 'U') IS NOT NULL DROP TABLE appointments_by_calendar_month;
CREATE TABLE appointments_by_calendar_month (
    calendar_month INT,
    total_appointments INT
);

-- 4. appointments_by_status
IF OBJECT_ID('appointments_by_status', 'U') IS NOT NULL DROP TABLE appointments_by_status;
CREATE TABLE appointments_by_status (
    status NVARCHAR(50),
    total_appointments INT
);

-- 5. top5_doctors_by_appointments
IF OBJECT_ID('top5_doctors_by_appointments', 'U') IS NOT NULL DROP TABLE top5_doctors_by_appointments;
CREATE TABLE top5_doctors_by_appointments (
    doctor_key INT,
    doctor_name NVARCHAR(100),
    doctor_lastname NVARCHAR(100),
    specialization NVARCHAR(100),
    total_appointments INT
);

-- 6. common_diseases
IF OBJECT_ID('common_diseases', 'U') IS NOT NULL DROP TABLE common_diseases;
CREATE TABLE common_diseases (
    disease_name NVARCHAR(100),
    total_cases INT
);

-- 7. diseases_by_age_group
IF OBJECT_ID('diseases_by_age_group', 'U') IS NOT NULL DROP TABLE diseases_by_age_group;
CREATE TABLE diseases_by_age_group (
    disease_name NVARCHAR(100),
    age_group NVARCHAR(20),
    total_cases INT
);

-- 8. disease_by_month
IF OBJECT_ID('disease_by_month', 'U') IS NOT NULL DROP TABLE disease_by_month;
CREATE TABLE disease_by_month (
    month INT,
    disease_name NVARCHAR(100),
    total_cases INT
);

-- 9. disease_by_gender
IF OBJECT_ID('disease_by_gender', 'U') IS NOT NULL DROP TABLE disease_by_gender;
CREATE TABLE disease_by_gender (
    gender NVARCHAR(20),
    disease_name NVARCHAR(100),
    total_cases INT
);

-- 10. cardiovascular_risk
IF OBJECT_ID('cardiovascular_risk', 'U') IS NOT NULL DROP TABLE cardiovascular_risk;
CREATE TABLE cardiovascular_risk (
    patient_key INT,
    patient_name NVARCHAR(100),
    high_blood_pressure BIT,
    high_heart_rate BIT,
    high_respiratory_rate BIT,
    low_oxygen_saturation BIT,
    high_blood_sugar BIT,
    total_risk_factors INT
);

-- 11. appointments_by_reason
IF OBJECT_ID('appointments_by_reason', 'U') IS NOT NULL DROP TABLE appointments_by_reason;
CREATE TABLE appointments_by_reason (
    reason NVARCHAR(255),
    total_appointments INT
);

-- 12. appointments_by_time
IF OBJECT_ID('appointments_by_time', 'U') IS NOT NULL DROP TABLE appointments_by_time;
CREATE TABLE appointments_by_time (
    appointment_time NVARCHAR(10),
    total_appointments INT
);

-- 13. appointments_by_location
IF OBJECT_ID('appointments_by_location', 'U') IS NOT NULL DROP TABLE appointments_by_location;
CREATE TABLE appointments_by_location (
    location NVARCHAR(255),
    total_appointments INT
);

-- 14. appointments_by_day_of_week
IF OBJECT_ID('appointments_by_day_of_week', 'U') IS NOT NULL DROP TABLE appointments_by_day_of_week;
CREATE TABLE appointments_by_day_of_week (
    day_of_week NVARCHAR(20),
    total_appointments INT
);

-- 15. revenue_by_month
IF OBJECT_ID('revenue_by_month', 'U') IS NOT NULL DROP TABLE revenue_by_month;
CREATE TABLE revenue_by_month (
    year INT,
    month INT,
    total_revenue MONEY
);

-- 16. revenue_by_service_type
IF OBJECT_ID('revenue_by_service_type', 'U') IS NOT NULL DROP TABLE revenue_by_service_type;
CREATE TABLE revenue_by_service_type (
    service_type NVARCHAR(100),
    total_revenue MONEY
);

-- 17. revenue_count_by_cost_range
IF OBJECT_ID('revenue_count_by_cost_range', 'U') IS NOT NULL DROP TABLE revenue_count_by_cost_range;
CREATE TABLE revenue_count_by_cost_range (
    cost_range NVARCHAR(50),
    frequency INT
);
