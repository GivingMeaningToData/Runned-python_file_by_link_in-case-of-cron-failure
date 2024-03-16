#!/usr/bin/env python
# coding: utf-8

# In[1]:


import gspread
import sys
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

start_time = datetime.now()

import datetime
current_date = datetime.date.today()
yesterday = current_date - datetime.timedelta(days=1)
yesterday_month = yesterday.month

# define the scope
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
# add credentials to the account
creds = ServiceAccountCredentials.from_json_keyfile_name('/home/ubuntu/GK-DM/Crons/syed-reporting-83428b4082ce.json',
                                                               scope) 
# authorize the clientsheet 
client = gspread.authorize(creds)
# print("done")
Curr="https://docs.google.com/spreadsheets/d/19S3VHNKyUqH5zSrZ6wtalpFqMvY45mzDR4H0pL53R1s/edit#gid=0" #7Day

sheet = client.open_by_url(Curr)
wks = sheet.worksheet('Raw')

#getting record
rc = wks.get_all_records()

#converting dict to data frame
leadsmain = pd.DataFrame.from_dict(rc)

leadsmain=leadsmain.dropna(subset=['leadId'])
leadsmain = leadsmain[leadsmain['leadId'].astype(str).str.strip() != '']
leadsmain.rename(columns={'Enq_id':'X'},inplace=True)
# leadsmain.tail()

leadsmain['Enq_Date']=pd.to_datetime(leadsmain['Enq_Date']).dt.date
max_date_in_leadsdata=leadsmain['Enq_Date'].max()
max_date_in_leadsdata


# In[34]:


# max_date_in_leadsdata=max_date_in_leadsdata - datetime.timedelta(days=1)
# max_date_in_leadsdata


# In[2]:


if(max_date_in_leadsdata<yesterday):
    print("No Data found")
    import os
    from nbconvert.preprocessors import ExecutePreprocessor
    from nbformat import read, write
    from IPython.display import display, HTML

    def execute_py_script(script_path):
        try:
            exec(open(script_path).read())
            print(f"Python script '{script_path}' executed successfully.")
        except Exception as e:
            print(f"Error executing Python script '{script_path}': {str(e)}")

    def execute_ipynb_notebook(notebook_path):
        try:
            with open(notebook_path, 'r', encoding='utf-8') as f:
                nb = read(f, as_version=4)

            ep = ExecutePreprocessor(timeout=-1, kernel_name='python3')
            ep.preprocess(nb, {'metadata': {'path': os.path.dirname(notebook_path)}})

            with open(notebook_path, 'w', encoding='utf-8') as f:
                write(nb, f)

            print(f"Jupyter Notebook '{notebook_path}' executed successfully.")
        except Exception as e:
            print(f"Error executing Jupyter Notebook '{notebook_path}': {str(e)}")

    # Example usage:
    py_script_path = "/home/ubuntu/GK-DM/Crons/MARKETING_DAILY_REPORT_Test_Newupdated_GK.py"
#     ipynb_notebook_path = "/home/ubuntu/GK-DM/Crons/MARKETING_DAILY_REPORT_Test_Newupdated_GK.ipynb"

    # Execute Python script
    print("Running..........")
    execute_py_script(py_script_path)
    print("Done......!!")

    # Execute Jupyter Notebook
#     execute_ipynb_notebook(ipynb_notebook_path)


