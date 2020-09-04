#import libraries and rds_config to access RDS instance
import pymysql
import sys
import rds_config
import logging

#create connection to RDS
#variables for RDS connection
rds_host=rds_config.db_host
name=rds_config.db_username
password=rds_config.db_password
db_name=rds_config.db_name
logger=logging.getLogger()
logger.setLevel(logging.INFO)
try:
    conn=pymysql.connect(host=rds_host, user=name, passwd=password, port=3306, db=db_name, connect_timeout=5)
    cursor = conn.cursor()
except pymysql.MySQLError as e:
    logger.error("Error: could not connect to database")
    logger.error(e)
    sys.exit()
logger.info("Success: connection completed")

def lambda_handler(event, context):
    """
    - Run SP to perform ETL from Staging schema to Datawarehouse
    """

    #call SP to perform ETL and load Dimensions
    cursor.callproc('updateDimensions')

    #call SP to perform ETL and load Fact Tables
    cursor.callproc('updateFacts')
    
    #close cursor
    close()

def close():
    """Commit and close connection"""
    conn.commit()
    cursor.close()

