# libraries for web scraping
import selenium
from bs4 import BeautifulSoup
from selenium import webdriver
from time import sleep
import pandas as pd
import boto_config

# libraries to write to s3 bucket
import s3fs

def web_driver_to_s3(path):
    """This function receives a path corresponding to a World Health Organization url and use chrome browser
    to get to that location. Inside the website there is a table and a link called 'CSV table', the function 
    gets the href of the html 'a' element and loads the content in a dataframe.
    Then calls a function save_to_s3() to store the dataframe as a csv in s3 bucket"""
    
    #enter the url path that needs to be accessed by webdriver
    browser.get(path)
    
    #the table with the data is inside nested iframes, so need get into both of them
    browser.switch_to.frame('content_iframe')
    browser.switch_to.frame('passthrough')
    
    #locate the CSV table link
    element=browser.find_element_by_xpath("/html/body/div/div/div[1]/div[2]/div[2]/a[4]")
    
    #get the href element of that link
    href=element.get_attribute('href')
    
    #use the href to load the table into a dataframe
    df=pd.read_csv(href)
    
    #get death reason as file name, located in 1st row and 1st column of the dataframe
    #also replace the / since the first file is for HIV/AIDS, to avoid creating a new folder in s3
    filename=df.iloc[1,1].strip().replace('/', '')
    
    #define folder to save in s3 bucket
    folder='WHO'
    
    #call function to save to s3, send: folder name, filename, dataframe
    save_to_s3(folder, filename, df)

def save_to_s3(folder, filename, data):
    """Funtion that saves a dataframe as a csv file in a s3 bucket, receives 3 parameters:
    folder: corresponds to the folder in s3 where it will save the csv
    filename: the filename for the file
    data: the dataframe that will turn into a csv"""
    
    #prepare location
    location = 's3:/finalprojectgroup4/'+folder+'/'
    
    #prepare filname
    filenames3 = "%s%s.csv"%(location,filename)
    
    #encodes file as binary
    byte_encoded_df = data.to_csv(None).encode() 
    s3 = s3fs.S3FileSystem(anon=False, key=boto_config.key_id, secret=boto_config.secret_key)
    with s3.open(filenames3, 'wb') as file:
        
        #writes byte-encoded file to s3 location
        file.write(byte_encoded_df) 

    #print success message
    print("Successfull uploaded file to location:"+str(filenames3))

#get the webdriver for google chrome
browser = webdriver.Chrome("C:/Program Files (x86)/Microsoft Visual Studio/Shared/Python/chromedriver.exe")

#Define an initial path to WHO website corresponding to:
#Mortality and global health estimates > Child mortality > Causes of child death > Rate of deaths by cause
#First url navigates to child mortality death rates originated by HIV AIDS.

#define initial path
initial_path='https://apps.who.int/gho/data/node.main.ChildMortCTRY2002015?lang=en'

#call function to navigate to url, get table and save to s3
web_driver_to_s3(initial_path)

#go back to main html, outside of the iframes
browser.switch_to.default_content()

#there are 13 death reasons, we already loaded 1 to s3, 12 are remaining
#to load the rest of the 12 csv files, we need to get the unordered (bulleted) list element
ul=browser.find_element_by_xpath("/html/body/div[1]/div/div[2]/div[3]/table/tbody/tr[1]/td[1]/div/div/ul")

#get the 12 list elements of that unordered list
li=ul.find_elements_by_tag_name("li")

#create list to store the 12 url
new_paths=[]

#for loop to get the href element of the remaining list items
for list_item in li:
    a_element=list_item.find_elements_by_tag_name("a")
    new_paths.append(a_element[0].get_attribute('href'))

#get to each of the 12 remaining url, get the table and save to s3
for item in new_paths:
    web_driver_to_s3(item)
