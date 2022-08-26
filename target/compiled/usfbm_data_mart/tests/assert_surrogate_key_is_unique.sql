
--{ config(severity='error') }}

with validation as  (
SELECT  count(*) AS total_count , count(DISTINCT DIM_DATE_PK ) AS surr_count
FROM CDW_DEV.SANDBOX_W846026.DIM_DATE
)
select * from validation where  total_count <> surr_count