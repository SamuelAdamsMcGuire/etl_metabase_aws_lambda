import pandas as pd
from sqlalchemy import create_engine

def lambda_handler(event, context):
    print('The crime program has started')

    #read in data from DataSF
    df = pd.read_csv('https://data.sfgov.org/api/views/wg3w-h783/rows.csv?accessType=DOWNLOAD',parse_dates = ['Incident Datetime', 'Incident Date'], index_col=False)
    print('data recieved')

    #preprocessing the dataframe
    df_crime = df[['Incident Datetime','Incident Date', 'Incident ID', 'Incident Category', 
                'Incident Description', 'Latitude', 'Longitude', 'Police District', 'Analysis Neighborhood']]
    df_crime = df_crime[df_crime['Longitude'].notna()]
    df_crime = df_crime.rename(columns={'Incident Datetime':'datetime','Incident Date':'date', 'Incident ID':'id', 'Incident Category':'category', 
                'Incident Description':'description', 'Latitude':'latitude', 'Longitude':'longitude',
                            'Police District':'pd_distirct', 'Analysis Neighborhood':'neighborhood'})

    #define connect to postgresql via config file, enter data from your postgresql server
    URL = 'postgresql://username:password@ip_address:5432/crime'
    engine = create_engine(URL)

    #send dataframe to postgresql
    df_crime.to_sql('sf_crime', engine, if_exists='replace', index=False, method='multi',chunksize=1000) 

    print('Your database has been updated')
    