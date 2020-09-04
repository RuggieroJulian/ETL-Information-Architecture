#get WDI API wrapper
import world_bank_data as wb

#pandas
import pandas as pd

# libraries to write to s3 bucket
import s3fs

#read data from s3 and save to mysql RDS staging instance

#import boto3 library and boto_config with the credentials to read from s3
import boto3
import boto_config
from io import StringIO

#import libraries and rds_config to access RDS instance
import pymysql
import sys
import rds_config
import logging
import pandas as pd

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

#connection to s3 bucket
client = boto3.client('s3', aws_access_key_id=boto_config.key_id,
    aws_secret_access_key=boto_config.secret_key)

#define bucket name
bucket_name='finalprojectgroup4'

def lambda_handler(event, context):
    """
    - call function to get files from the s3 bucket 
    - call function to save the file to the corresponding table in RDS
    """
    #get file 'region' and save to RDS
    object_key = 'WDI/Region.csv'
    regions=read_file_from_s3(object_key)
    #insert_regions(regions)

    #get file 'income_group' and save to RDS
    object_key = 'WDI/Income_group.csv'
    income_group=read_file_from_s3(object_key)
    #insert_income_group(income_group)

    #get file 'country' and save to RDS
    object_key = 'WDI/Country.csv'
    countries=read_file_from_s3(object_key)
    countries = countries.astype(object).where(pd.notnull(countries), None)
    #insert_countries(countries)

    #get file 'indicator' and save to RDS
    object_key = 'WDI/Indicator.csv'
    indicators=read_file_from_s3(object_key)
    #insert_indicators(indicators)

    #get death reasons from WHO folder in s3 and save 
    #to RDS table 'death_reason'
    response = client.list_objects_v2(Bucket=bucket_name)
    reasons=[]
    for content in response['Contents']:
        if (content['Key'][:3] == 'WHO'):
            if (content['Key'][4:] != ''):
                reasons.append(content['Key'][4:].replace('.csv', ''))

    #insert_death_reasons(reasons)

    #get file 'under_5_per_country' and save to RDS
    object_key = 'WDI/Under5_per_country.csv'
    under5=read_file_from_s3(object_key)
    #insert_indicator_per_country(under5)

    #get file 'gni_per_country' and save to RDS
    object_key = 'WDI/Gni_per_country.csv'
    gni=read_file_from_s3(object_key)
    gni = gni.astype(object).where(pd.notnull(gni), None)
    #insert_indicator_per_country(gni)

    #get death reasons files from WHO folder in s3 and save 
    #to RDS table 'indicator_per_country_per_reason'
    response = client.list_objects_v2(Bucket=bucket_name)

    #the next loop will run 13 times, one per each death reason file
    counter=1
    for content in response['Contents']:

        #check if file is under WHO folder
        if (content['Key'][:3] == 'WHO'):

            #get file name
            file_name=content['Key'][4:]
            if (file_name != ''):

                #read death reason file from s3
                df=read_file_from_s3(content['Key'])
                df.rename(columns={'Unnamed: 0': "name"}, inplace=True)

                #get the corresponding value from the 0-4 years old column
                df=df.loc[3:,['name','2017.2','2016.2','2015.2','2014.2','2013.2','2012.2','2011.2',
                                            '2010.2','2009.2','2008.2','2007.2','2006.2','2005.2',
                                            '2004.2','2003.2','2002.2','2001.2','2000.2']]
                melted = pd.melt(df, ['name'])
                melted.rename(columns={'variable': "year", 'value':'value_per_reason'}, inplace=True)
                melted.year=melted.year.astype(float).astype(int)

                #next step is to merge with the WDI indicator,
                #but several country names are different, and as this will be the key
                #we need to update before merging 
                melted.replace({'Bahamas': 'Bahamas, The', 
                'Bolivia (Plurinational State of)': 'Bolivia',
                'Congo': 'Congo, Rep.',
                "CÃ´te d'Ivoire": "Cote d'Ivoire",
                'Czechia': 'Czech Republic',
                "Democratic People's Republic of Korea": "Korea, Dem. People’s Rep.",
                'Democratic Republic of the Congo': 'Congo, Dem. Rep.',
                'Egypt': 'Egypt, Arab Rep.',
                'Gambia': 'Gambia, The',
                'Iran (Islamic Republic of)': 'Iran, Islamic Rep.',
                'Kyrgyzstan': 'Kyrgyz Republic',
                "Lao People's Democratic Republic": 'Lao PDR',
                'Micronesia (Federated States of)': 'Micronesia, Fed. Sts.',
                'Republic of Korea': 'Korea, Rep.',
                'Republic of Moldova': 'Moldova',
                'Republic of North Macedonia': 'North Macedonia',
                'Saint Kitts and Nevis': 'St. Kitts and Nevis',
                'Saint Lucia': 'St. Lucia',
                'Saint Vincent and the Grenadines': 'St. Vincent and the Grenadines',
                'Slovakia': 'Slovak Republic',
                'United Kingdom of Great Britain and Northern Ireland': 'United Kingdom',
                'United Republic of Tanzania': 'Tanzania',
                'United States of America': 'United States',
                'Venezuela (Bolivarian Republic of)': 'Venezuela, RB',}, inplace=True)

                #change type to allow merging, and add an index, which will be the FK
                under5.year=under5.year.astype(float).astype(int)
                under5.rename(columns={'Unnamed: 0': "index"}, inplace=True)
                under5['indicator_per_country_id']=under5['index']+1

                #merge melted WHO death reason file with WDI with under5 mortality rate
                new_df = pd.merge(melted, under5,  how='inner', on=['name','year'])

                #assign the counter to the reason_id column (will be from 1 to 13)
                new_df['reason_id']=counter
                counter+=1
            
                #prepare bulk insert
                columns=['value_per_reason','reason_id','indicator_per_country_id']
                cols = "`,`".join([str(i) for i in columns])

                #insert records in RDS
                #insert_indicators_country_per_reason(new_df[['value_per_reason','reason_id','indicator_per_country_id']], cols)

    #get file 'income_boundaries' and save to RDS
    object_key = 'WDI/Income_boundaries.csv'
    boundaries=read_file_from_s3(object_key)
    boundaries['income_group_id']=0
    boundaries.loc[boundaries.income == 'Low income (L)', 'income_group_id'] = 2
    boundaries.loc[boundaries.income == 'Lower middle income (LM)', 'income_group_id'] = 3
    boundaries.loc[boundaries.income == 'Upper middle income (UM)', 'income_group_id'] = 4
    boundaries.loc[boundaries.income == 'High income (H)', 'income_group_id'] = 1

    #insert_income_group_year(boundaries)

    #call SP to assign each country the corresponding
    #income level per year based on that year's GNI value.
    #RDS table 'income_group_country'
    cursor.callproc('fill_income_group_country')
    
    #close cursor
    close()

def close():
    conn.commit()
    cursor.close()

def read_file_from_s3(filename):
    """Function to read files from S3 bucket"""
    object_key = filename
    csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_string))
    return df
    
def insert_regions(regions):
    """Gets the region name and inserts into RDS table Regions"""
    for i, row in regions.iterrows():
        sql = "INSERT INTO Region (region_name) values (%s)"
        parameters=(row['region'])
        cursor.execute(sql, parameters)
    conn.commit()

def insert_income_group(income_group):
    """Takes a dataframe and inserts income_group table"""
    for i, row in income_group.iterrows():
        cursor = conn.cursor()
        sql = "INSERT INTO Income_group (income_name) values (%s)"
        parameters=(row['incomeLevel'])
        cursor.execute(sql, parameters)
    conn.commit()

def insert_countries(countries):
    """Takes a dataframe and inserts into Country table"""
    for i, row in countries.iterrows():
        sql = "INSERT INTO Country (country_name, iso2code, iso3Code, region_id) values (%s, %s, %s, %s)"
        parameters=(row['name'], row['iso2Code'], row['iso3Code'], row['index_region'])
        cursor.execute(sql, parameters)
    conn.commit()

def insert_indicators(indicators):
    """Takes a dataframe and inserts into Indicator table"""
    for i, row in indicators.iterrows():
        cursor = conn.cursor()
        sql = "INSERT INTO Indicator (indicator_name, world_bank_ind_id, source_organization, source_note) values (%s, %s, %s, %s)"
        parameters=(row['name'], row['id'], row['sourceOrganization'], row['sourceNote'])
        cursor.execute(sql, parameters)
    conn.commit()

def insert_death_reasons(reasons):
    """Takes a dataframe and inserts into Death_reason table"""
    for reason in reasons:
        cursor = conn.cursor()
        sql = "INSERT INTO Death_reason (reason_name) values (%s)"
        parameters=(reason)
        cursor.execute(sql, parameters)
    conn.commit()

def insert_indicator_per_country(indicator_per_country):
    """Takes a dataframe and inserts into Indicator_per_country table"""
    for i, row in indicator_per_country.iterrows():
        cursor = conn.cursor()
        sql = "INSERT INTO Indicator_per_country (value, year, indicator_id, country_id) values (%s, %s, %s, %s)"
        parameters=(row['value'], row['year'], row['indicator'], row['index_country'])
        cursor.execute(sql, parameters)
    conn.commit()
    cursor.close()

def insert_indicators_country_per_reason(df, cols):
    """Takes a dataframe and inserts into Indicator_per_country_per_reason table"""
    for i, row in df.iterrows():
        cursor = conn.cursor()
        sql = "INSERT INTO `Indicator_per_country_per_reason` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql, tuple(row))
    conn.commit()
    cursor.close()

def insert_income_group_year(income_boundaries):
    """Takes a dataframe and inserts into Income_group_year table with the lower and upper bounds for the GNI"""
    for i, row in income_boundaries.iterrows():
        cursor = conn.cursor()
        sql = "INSERT INTO Income_group_year (income_group_id, year, lower_bound, upper_bound, income_group_name) values (%s, %s, %s, %s, %s)"
        parameters=(row['income_group_id'], row['year'], row['lower_bound'], row['upper_bound'], row['income'])
        cursor.execute(sql, parameters)
    conn.commit()
    cursor.close()