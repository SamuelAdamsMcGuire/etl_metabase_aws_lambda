import os
import datetime
import config
import logging
import pandas as pd
from sqlalchemy import create_engine

# set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s- %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='log.log', filemode='w'
)

#read in data from DataSF
df = pd.read_csv('https://data.sfgov.org/api/views/wg3w-h783/rows.csv?accessType=DOWNLOAD',parse_dates = ['Incident Datetime', 'Incident Date'], index_col=False)
logging.info(df)

#preprocessing the dataframe
df_crime = df[['Incident Datetime','Incident Date', 'Incident ID', 'Incident Category', 
               'Incident Description', 'Latitude', 'Longitude', 'Police District', 'Analysis Neighborhood']]
df_crime = df_crime[df_crime['Longitude'].notna()]
df_crime = df_crime.rename(columns={'Incident Datetime':'datetime','Incident Date':'date', 'Incident ID':'id', 'Incident Category':'category', 
               'Incident Description':'description', 'Latitude':'latitude', 'Longitude':'longitude',
                         'Police District':'pd_distirct', 'Analysis Neighborhood':'neighborhood'})

#define connect to postgresql via config file, see attached example 
URL = config.conn_postgres
engine = create_engine(URL)

#send dataframe to postgresql
df_crime.to_sql('sf_crime', engine, if_exists='replace', index=False, method='multi',chunksize=1000) 

logging.info('Your database has been updated')
