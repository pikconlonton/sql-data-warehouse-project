USE DataWarehouse
--create job load data from web database to ETL 
EXEC msdb.dbo.sp_add_job
	@job_name = N'Job_load_data_into_datawarehouse',
	@enabled = 1,
	@description = N'Job load data from web database to data warehouse';

--add steps to job
EXEC msdb.dbo.sp_add_jobstep
	@job_name = N'Job_load_data_into_datawarehouse',
	@step_name = N'Step 1 - load from web database into bronze layer',
	@subsystem = N'TSQL',
	@command = N'EXEC DataWarehouse.bronze.load_bronze',
	@database_name = N'DataWarehouse',
	@on_success_action = 1;

EXEC msdb.dbo.sp_add_jobstep
	@job_name = N'Job_load_data_into_datawarehouse',
	@step_name = N'Step 2 - load from bronze layer into silver layer',
	@subsystem = N'TSQL',
	@command = N'EXEC DataWarehouse.silver.load_silver',
	@database_name = N'DataWarehouse',
	@on_success_action = 1;

EXEC msdb.dbo.sp_add_jobstep
	@job_name = N'Job_load_data_into_datawarehouse',
	@step_name = N'Step 3 - load from silver layer into gold layer',
	@subsystem = N'TSQL',
	@command = N'EXEC DataWarehouse.gold.load_gold',
	@database_name = N'DataWarehouse',
	@on_success_action = 1;

--create schedule
EXEC msdb.dbo.sp_add_schedule
	@schedule_name = N'load everyday',
	@freq_type = 4,
	@freq_interval = 1,
	@freq_subday_type = 4,
	@freq_subday_interval = 1,
	@active_start_time = 010000;

--attach schedule to job
EXEC msdb.dbo.sp_attach_schedule 
	@job_name = N'Job_load_data_into_datawarehouse',
	@schedule_name = N'load everyday';

--add job to sql server agent
EXEC msdb.dbo.sp_add_jobserver
	@job_name = N'Job_load_data_into_datawarehouse';



USE DB_mart
--create job load data ETL to db_mart
EXEC msdb.dbo.sp_add_job
	@job_name = N'Job_load_data_into_datawarehouse2',
	@enabled = 1,
	@description = N'Job load data from web database to data warehouse';

--add step to job
EXEC msdb.dbo.sp_add_jobstep
	@job_name = N'Job_load_data_into_datawarehouse',
	@step_name = N'load from gold layer into mart',
	@subsystem = N'TSQL',
	@command = N'EXEC DB_mart.load_Db_mart',
	@database_name = N'DB_mart',
	@on_success_action = 1;

--add job to sql server agent
EXEC msdb.dbo.sp_add_jobserver
	@job_name = N'Job_load_data_into_datawarehouse2';
