DELIMITER $$
CREATE DEFINER=`admin`@`%` PROCEDURE `updateDimensions`()
BEGIN
Set Foreign_Key_Checks = 0;

-- Populate dim_date
Insert into mortality_mart.dim_date (date_year, date_string, date_tens)
 select distinct year, year,
Case 
When year >= 1960  AND year < 1970 Then 1960 -- fill with the decade
When year >= 1970  AND year < 1980 Then 1970
When year >= 1980  AND year < 1990 Then 1980
When year >= 1990  AND year < 2000 Then 1990
When year >= 2000  AND year < 2010 Then 2000
When year >= 2010  AND year < 2020 Then 2010
When year >= 2020  AND year < 2030 Then 2020 End as date_tens
FROM Mortality.Indicator_per_country a;

-- add dummy record to avoid null in the joins
INSERT INTO mortality_mart.dim_date(date_year,date_string,date_tens) VALUES (-1, 'NO DATE AVAILABLE', 'NO DATE AVAILABLE');

-- Populate dim_country
-- The Indicator '1' is the under 5 child mortality, and we have data from 1960,
-- for the other indicator we are able to populate the income_group starting in 1987,
-- so for those records < 1987 the country will have the income_name set to 'NO INCOME AVAILABLE'

insert into mortality_mart.dim_country (country_id, country_name, region_name, date_year, income_name)
SELECT distinct cty.country_id, cty.country_name, reg.region_name, ipc.year,  
case when table_income.income_name is null then 'NO INCOME AVAILABLE' ELSE table_income.income_name END income_name 
FROM Mortality.Country cty
LEFT JOIN Mortality.Region reg on cty.region_id = reg.region_id
LEFT JOIN Mortality.Indicator_per_country ipc on cty.country_id = ipc.country_id and ipc.indicator_id = 1
LEFT JOIN 
(SELECT igc.country_id, ing.income_name, igy.year 
FROM Mortality.Income_group_country igc
INNER JOIN Mortality.Income_group_year igy on igc.income_group_year_id = igy.income_group_year_id
INNER JOIN Mortality.Income_group ing on igy.income_group_id = ing.income_group_id) table_income
ON ipc.year = table_income.year and cty.country_id = table_income.country_id;

-- Populate dim_reason
-- ETL
Insert into mortality_mart.dim_reason (reason_id, reason_desc,date_year)
select distinct b.reason_id, a.reason_name, c.year
   from  Mortality.Death_reason as a
   left join Mortality.Indicator_per_country_per_reason as b on b.reason_id = a.death_reason_id
   left join Mortality.Indicator_per_country as c on c.indicator_per_country_id = b.indicator_per_country_id;

Set Foreign_Key_Checks = 1;
END$$
DELIMITER ;
