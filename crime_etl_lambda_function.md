### Creating AWS lambda function for crime data ETL

1.  Must wrap program into lamba_handler function:

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
    
    #define connect to postgresql via config file, see attached example 
    URL = 'postgresql://postgres:postgres@34.125.36.4:5432/crime'
    engine = create_engine(URL)
    
    #send dataframe to postgresql
    df_crime.to_sql('sf_crime', engine, if_exists='replace', index=False, method='multi',chunksize=1000) 
    
    print('Your database has been updated')

2. Use built in aws data wrangling layer for access to pandas
3. Make custom layer for sqlalchemy and psycopg2:
   - create a folder named `python`
   - pip install the needed libraries into that folder using target argument:
     - `pip install aws-psycopg2 sqlalchemy -t python/`
   - zip the contents of the `python`folder:
     - ` zip -r sql_lambda.zip python/`
4. Upload the zip file to aws lamba layers
5. Add custom layer to funtion
6. Run test of lambda function 

### Set up Lambda function to run automatically every day at 4:05 am

1. Once the lambda function has been tested and it works go to AWS EventBridge
2. Create a rule using the `cron expression` option with the input `5 4 * * ? *`
3. Select your target as the lambda function that was made and create the rule. 
4. Now the lambda function should be executed daily at 4:05 am



```sql
select count(*)/count(distinct(date)) as crimes, 
date_part('hour', datetime) as hour_of_day 
from sf_crime 
group by hour_of_day;
```

