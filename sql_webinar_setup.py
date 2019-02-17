#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 01 08:59:20 2019

@author: mona

Setting up the database for the webinar, "Spend Less Time in Spreadsheets with SQL"
Webinar can be viewed at https://www.youtube.com/watch?v=ZfWRdQwbd4Q
"""

import pandas as pd
import urllib.request, json

#%%
# Load the json from NYC Open Data

url = urllib.request.urlopen("https://data.cityofnewyork.us/resource/kpav-sd4t.json")
jobs_json = json.loads(url.read().decode())

#%%
# Set up the Dataframe
jobs = pd.DataFrame(jobs_json)

# Convert columns to numeric
cols = ['__of_positions', 'job_id', 'salary_range_from', 'salary_range_to']
jobs[cols] = jobs[cols].apply(pd.to_numeric, errors = 'coerce', axis = 1)

# Convert columns to date
cols2 = ['post_until', 'posting_date']
jobs[cols2] = jobs[cols2].apply(pd.to_datetime)

print(jobs.info)
print(jobs.dtypes)

#%%
# Set up the database
# I'm using a SQLite database for this example

from sqlalchemy import create_engine

engine = create_engine('sqlite:///Documents/nyc_jobs.db', echo=False)
jobs.to_sql('jobs_all', con = engine) # Convert jobs dataframe to sql & add to database

engine.execute("SELECT * FROM jobs_all LIMIT 5;").fetchall() 

#%%
# Create and check the title table
engine.execute("""
               CREATE TABLE title AS
               SELECT DISTINCT title_code_no, civil_service_title, job_category
               FROM jobs_all;
               """)

engine.execute("SELECT * FROM title LIMIT 5;").fetchall()

# Create and check the agency table
engine.execute("""
               CREATE TABLE agency AS
               SELECT DISTINCT agency, division_work_unit, work_location AS address
               FROM jobs_all;
               """)

engine.execute("SELECT * FROM agency LIMIT 5;").fetchall()

# Create and check the jobs table
engine.execute("""
               CREATE TABLE jobs AS
               SELECT DISTINCT 
                   job_id, 
                   business_title, 
                   title_code_no AS title_code,
                   agency, 
                   work_location, 
                   __of_positions AS positions, 
                   posting_type,
                   full_time_part_time_indicator AS ftpt,
                   level,
                   hours_shift,
                   posting_date,
                   post_until,
                   salary_range_from AS salary_from,
                   salary_range_to AS salary_to,
                   salary_frequency,
                   job_description,
                   minimum_qual_requirements AS qualifications,
                   preferred_skills,
                   residency_requirement,
                   additional_information,
                   to_apply
              FROM jobs_all;
               """)

engine.execute("SELECT * FROM jobs LIMIT 5;").fetchall()

# Drop the original messy jobs_all table
engine.execute("DROP TABLE jobs_all;")
engine.execute("SELECT * FROM jobs_all;").fetchall() # Throws an error, so table was successfully dropped