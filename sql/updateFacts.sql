DELIMITER $$
CREATE DEFINER=`admin`@`%` PROCEDURE `updateFacts`()
BEGIN
Set Foreign_Key_Checks = 0;

-- Populate 1st fact table where it will be 
-- 2 records per each country and year
-- corresponding to the values for the 
-- under5 child mortality rate and
-- Gross National Income (GNI)
-- (in almost all cases there will be 2 records per country and year, some countries 
-- will only have one of them, depending if that year our datasources had the data)

Insert into  mortality_mart.fact_metrics ( 
indicator_name,
indicator_metric,
date_year,
country_key
) 
Select 
  h.indicator_name, a.value, a.year, f.country_key
from Mortality.Indicator_per_country as a
left join Mortality.Indicator as h on h.indicator_id = a.indicator_id
left join Mortality.Country as e on a.country_id = e.country_id 
left join mortality_mart.dim_country as f on f.country_id = e.country_id AND f.date_year = a.year;

-- Populate 2nd fact table
-- Here we will store 13 records per each record in our 1st fact table
-- corresponding to the values of each death reason during each year.
Insert into  mortality_mart.fact_metric_death_reasons ( 
  metric_id,
  reason_key,
  death_reason_metric,
  date_year
) 
Select distinct f.metric_id, d.reason_key, b.value_per_reason, f.date_year
from Mortality.Indicator_per_country a
inner join Mortality.Indicator_per_country_per_reason b on a.indicator_per_country_id = b.indicator_per_country_id  -- INNER JOIN because it only applies to indicator_id = 1
inner join Mortality.Country c on a.country_id = c.country_id
inner join mortality_mart.dim_reason d on d.reason_id = b.reason_id and d.date_year = a.year
inner join mortality_mart.dim_country e on c.country_id = e.country_id and e.date_year = a.year
inner join mortality_mart.fact_metrics as f on e.country_key = f.country_key and e.date_year = f.date_year
where f.indicator_name = 'Mortality rate, under-5 (per 1,000 live births)' ;

Set Foreign_Key_Checks = 1;
END$$
DELIMITER ;
