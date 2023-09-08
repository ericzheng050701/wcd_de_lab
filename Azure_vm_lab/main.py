#!/usr/bin python3
"""
This is the main file for the py_cloud project. It can be used at any situation
"""
import requests
import json
import pandas as pd
from collections import ChainMap
from dotenv import load_dotenv
import os
import subprocess


def read_api(url):
    """
    Reads the API and returns the response
    """
    response = requests.get(url)
    return response.json()

# main function
if __name__=='__main__':
    url = 'https://www.themuse.com/api/public/jobs?page=50'

    # read the API
    print('Reading the API...')
    data=read_api(url)
    print('API Reading Done!')

    # the company name
    print('Building the dataframe...')
    company_list = [data['results'][i]['company']['name'] for i in range(len(data['results']))]
    company_name = {'company':company_list}

    # the locations
    location_list = [data['results'][i]['locations'][0]['name'] for i in range(len(data['results']))]
    location_name = {'locations':location_list}
    
    # the job name
    job_list = [data['results'][i]['name'] for i in range(len(data['results']))]
    job_name = {'job':job_list}

    # the job type
    job_type_list = [data['results'][i]['type'] for i in range(len(data['results']))]
    job_type = {'job_type':job_type_list}

    # the publication date
    publication_date_list = [data['results'][i]['publication_date'] for i in range(len(data['results']))]
    publication_date = {'publication_date':publication_date_list}

    # merge the dictionaries with ChainMap and dict "from collections import ChainMap"
    data = dict(ChainMap(company_name, location_name, job_name, job_type, publication_date))
    df=pd.DataFrame.from_dict(data)

    # Cut publication date to date
    df['publication_date'] = df['publication_date'].str[:10]

    # split location to city and country and drop the location column
    df['city'] = df['locations'].str.split(',').str[0]
    df['country'] = df['locations'].str.split(',').str[1]
    df.drop('locations', axis=1, inplace=True)

    # save the dataframe to a csv file locally first
    df.to_csv('jobs2.csv', index=False)
    print('datafrme saved to local')

    # use linux command to upload file to S3
    subprocess.run(['azcopy','copy','jobs2.csv','https://delecturedemo.blob.core.windows.net/de-lecture-demo-container?sp=racwdl&st=2023-09-08T18:09:47Z&se=2025-11-09T03:09:47Z&spr=https&sv=2022-11-02&sr=c&sig=0xvvZjr4z2WYH0LAFXNWJIP9hCCWnABf1o%2Byeb1Xe3o%3D','--recursive=true'])
  
    # Success.
    print('File uploading Done!')
