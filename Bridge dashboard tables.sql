SELECT TOP 5 *  FROM nvdev.wbr_user_details_hist wud
SELECT TOP 5 *  FROM nvdev.wbr_week_cal wwc

SELECT TOP 5 * FROM "nvdev"."date_dim"

SELECT TOP 5 * FROM nvads.dim_employee_history

SELECT TOP 5 * FROM nvads.ads_workflows_categorical_mappings

SELECT TOP 5 *  FROM nvrpt.fact_da_availability_daily
SELECT TOP 5 * FROM nvads.da_mapping_table

SELECT TOP 5 * FROM Nvdev.wbr_analyst_dim
SELECT TOP 5 * FROM nvdev.wbr_week_cal
where start_date > '2020-1-1' and business_days <5
