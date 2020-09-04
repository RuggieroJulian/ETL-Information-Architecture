DELIMITER $$
CREATE DEFINER=`admin`@`%` PROCEDURE `create_schema_datamart`()
BEGIN

-- dim_date
CREATE TABLE `dim_date`(
  `date_year` int(11) NOT NULL,
  `date_string` varchar(100) DEFAULT NULL,
  `date_tens` varchar(100) NOT NULL,
  PRIMARY KEY (`date_year`)
);

-- Dim_country
CREATE TABLE `dim_country` (
  `country_key` int(11) NOT NULL AUTO_INCREMENT,
  `country_id` varchar(100) NOT NULL, -- this is the actual PK from the OLTP table
  `country_name` varchar(255) DEFAULT NULL,
  `region_name` varchar(255) DEFAULT NULL,
  `date_year` int(11) DEFAULT NULL,
  `income_name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`country_key`),
  KEY `date_year` (`date_year`),
  CONSTRAINT `dim_country_ibfk_1` FOREIGN KEY (`date_year`) REFERENCES `dim_date` (`date_year`)
) ;

-- Dim_reason
CREATE TABLE `dim_reason` (
  `reason_key` int(11) NOT NULL AUTO_INCREMENT,
  `reason_id` varchar(100) NOT NULL, -- this is the actual PK from the OLTP table
  `reason_desc` varchar(255) DEFAULT NULL,
  `date_year` int(11) DEFAULT NULL,
  PRIMARY KEY (`reason_key`),
  KEY `date_year` (`date_year`),
  CONSTRAINT `dim_reason_ibfk_1` FOREIGN KEY (`date_year`) REFERENCES `dim_date` (`date_year`)
);

-- fact_metrics   
CREATE TABLE `fact_metrics` (
  `metric_id` int(11) NOT NULL AUTO_INCREMENT,
  `indicator_name` varchar(255) DEFAULT NULL,
  `indicator_metric` decimal(10,2) DEFAULT NULL,
  `date_year` int(11) DEFAULT NULL,
  `country_key` int(11) DEFAULT NULL,
  PRIMARY KEY (`metric_id`),
  KEY `country_key` (`country_key`),
  KEY `date_year` (`date_year`),
  CONSTRAINT `fact_metrics_ibfk_2` FOREIGN KEY (`country_key`) REFERENCES `dim_country` (`country_key`),
  CONSTRAINT `fact_metrics_ibfk_3` FOREIGN KEY (`date_year`) REFERENCES `dim_date` (`date_year`)
);

-- fact_metric_death_reasons
CREATE TABLE `fact_metric_death_reasons` (
  `death_metric_id` int(11) NOT NULL AUTO_INCREMENT,
  `metric_id` int(11) DEFAULT NULL,
  `reason_key` int(11) DEFAULT NULL,
  `death_reason_metric` decimal(10,2) DEFAULT NULL,
  `date_year` int(11) DEFAULT NULL,
  PRIMARY KEY (`death_metric_id`),
  KEY `date_year` (`date_year`),
  KEY `reason_key` (`reason_key`),
  KEY `metric_id` (`metric_id`),
  CONSTRAINT `fact_metric_death_reasons_ibfk_1` FOREIGN KEY (`date_year`) REFERENCES `dim_date` (`date_year`),
  CONSTRAINT `fact_metric_death_reasons_ibfk_2` FOREIGN KEY (`reason_key`) REFERENCES `dim_reason` (`reason_key`),
  CONSTRAINT `fact_metric_death_reasons_ibfk_3` FOREIGN KEY (`metric_id`) REFERENCES `fact_metrics` (`metric_id`)
) ;

END$$
DELIMITER ;
