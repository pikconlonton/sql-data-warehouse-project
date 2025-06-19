--create procedure delay and run load mart job
CREATE PROCEDURE sp_DelayAndRunJob
AS
BEGIN
    WAITFOR DELAY '00:00:10';
    EXEC msdb.dbo.sp_start_job @job_name = 'Job_load_data_into_datawarehouse2';
END  
