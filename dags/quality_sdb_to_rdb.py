# Import necessary libraries
import psycopg2
from datetime import timedelta, date
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from psycopg2.extras import execute_values
import numpy as np

# Establish connection to the staging database
connection_1 = psycopg2.connect(host='3.108.122.116', database='staging_db', user='myusername', password='mypassword', port='5432')
cursor_1 = connection_1.cursor()

# Establish connection to the reporting database
connection_2 = psycopg2.connect(host='3.108.122.116', database='reporting_db', user='myusername', password='mypassword', port='5432')
cursor_2 = connection_2.cursor()

# Define a list of column names
column_list=["date", "agent_name", "emp_id", "call_date", "overall_score", "scorable", "scored", "fatal", "tenure", "month", "week", "qa_name", "tl_name", "fatal_reason", "greetings1_binary_count", "greeting2_binary_count", "authentication1_binary_count", "authentication2_binary_count", "issue_identification1_binary_count", "issue_identification2_binary_count", "probing1_binary_count", "probing2_binary_count", "call_etiquette1_binary_count", "call_etiquette2_binary_count", "call_etiquette3_binary_count", "call_etiquette4_binary_count", "call_etiquette5_binary_count", "call_etiquette6_binary_count", "empathy_and_sympathy1_binary_count", "empathy_and_sympathy1_binary1_count", "ownership1_binary_count", "resolution1_binary_count", "resolution2_binary_count", "resolution3_binary_count", "resolution4_binary_count", "resolution5_binary_count", "resolution6_binary_count", "resolution7_binary_count", "tagging_and_documentation_binary_count", "summarization1_binary_count", "summarization2_binary_count", "summarization3_binary_count", "closing__binary_count", "zero_tolerance1_binary_count", "zero_tolerance2_binary_count", "zero_tolerance3_binary_count", "zero_tolerance4_binary_count", "zero_tolerance5_binary_count", "compliance1_binary_count", "compliance2_binary_count", "compliance3_binary_count", "compliance4_binary_count", "role_fit", "pkt_scores","manager","sr__manager","grid","tni_assessment_score","bq_scores","team_lead","team_lead_name","manager_name"]
# # Set the start and end dates as the previous day
# startdate = date.today() - timedelta(days=1)
# enddate = startdate

'''
The quality_rdb_function performs the following tasks:

Retrieves the count of records in the quality_rdb table and stores it in count_quality_temp.
Retrieves the count of records in the quality_sdb table and stores it in count_quality_main.
Checks if the counts of quality_rdb and quality_sdb are equal. If they are equal, it prints "No need to run" and exits the function.
If the counts are different, it enters the loop and performs the following steps:
Retrieves the date range of new records in the quality_sdb table.
Deletes the existing records in the quality_rdb table for the current date.
Retrieves the data from the temp_quality_sdb table.
Inserts the data into the quality_rdb table.
Drops the temp_quality_sdb table.
Closes the cursors and connections.
Prints "Need to run" if the function executed any code.
'''
def quality_rdb_function():
    # Query to get the count of records in quality_rdb
    query = '''SELECT COUNT(*) FROM quality_rdb'''
    cursor_2.execute(query)
    count_quality_temp = cursor_2.fetchone()[0]
    # Query to get the count of records in quality_sdb
    query = '''SELECT COUNT(*) FROM quality_sdb'''
    cursor_1.execute(query)
    count_quality_main = cursor_1.fetchone()[0]   
    if count_quality_main == count_quality_temp:
        # No need to run if the counts are equal
        print('No need to run')
    else:
        print('Need to run')   
        # Retrieve the dates for which data needs to be transferred
        query = f'''select date,count(*) from quality_sdb group by date order by date;'''
        cursor_1.execute(query)
        column_names = [desc[0] for desc in cursor_1.description]
        df1 = pd.DataFrame(cursor_1.fetchall(), columns=column_names)
        query = f'''select date,count(*) from quality_rdb group by date order by date;'''
        cursor_2.execute(query)
        column_names = [desc[0] for desc in cursor_2.description]
        df2 = pd.DataFrame(cursor_2.fetchall(), columns=column_names)
        # Merge the two DataFrames on the `date` column
        df_merged = df1.merge(df2, how='left', on='date')

        # Select the rows where the count is not matching
        df_diff = df_merged[df_merged['count_x'] != df_merged['count_y']]

        # Get the dates from the DataFrame
        dates = df_diff['date'].tolist()
        startdate = (dates[0])
        enddate = (dates[-1])    
        print(startdate,enddate)   
        # Loop through the date range
        for x in pd.date_range(startdate, enddate):
            print('Time to run')
            # Delete records from quality_rdb for the current date
            query = f'''DELETE FROM quality_rdb WHERE date = '{x.date()}';'''
            cursor_2.execute(query)  
            connection_2.commit()
            query=f'''SELECT get_temp_table_quality('{x.date()}');'''
            cursor_1.execute(query)
            final_columns= ','.join(['"{0}"'.format(item) for item in column_list])              
            # Get data from temp_quality_sdb
            query = f'''SELECT {final_columns} FROM temp_quality_sdb;'''
            cursor_1.execute(query)
            column_names = [desc[0] for desc in cursor_1.description]
            df = pd.DataFrame(cursor_1.fetchall(), columns=column_names) 
            df = df.replace({np.nan: None})            
            # Insert data into quality_rdb
            for i, row in df.iterrows():
                value = row.values
                try:
                    query = '''INSERT INTO quality_rdb VALUES (%s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s);'''  # Insert appropriate number of placeholders
                    cursor_2.execute(cursor_2.mogrify(query, tuple(value)))
                    connection_2.commit()        
                except Exception as e:
                    print(e)            
            # Drop temp_quality_sdb table
            query = f'''DROP TABLE temp_quality_sdb;'''
            cursor_1.execute(query)
            connection_1.commit()  
    # Close cursors and connections
    cursor_1.close()
    cursor_2.close()
    connection_1.close()
    connection_2.close()


'''
The function checks the current day using the date.today() function.
If the current day is the first day of the month (today.day == 1), the code block inside the if statement is executed.
Inside the code block, a query is executed using cursor_2 to select and perform operations on the abcd_categorie_table().
The connection is committed to save the changes made by the query.
Additional code specific to the first day of the month can be added in the designated area.
If the current day is not the first day of the month, a message is printed indicating that there is no need to run the code.
Finally, the cursor and connection are closed to clean up resources.
'''
def run_if_first_day():
    today = date.today()
    if today.day == 1:
        # Run your code here
        print("Today is the first day of the month. Running...")        
        # Execute the query using cursor_2
        query='''select abcd_categorie_table()'''
        cursor_2.execute(query)
        connection_2.commit()        
        # Add your code to be executed on the first day  
    else:
        print("Today is not the first day of the month. No need to run.")    
    # Close the cursor and connection
    cursor_2.close()
    connection_2.close()

    

'''The default_args dictionary defines default configuration parameters for an Airflow task or DAG. Here's a breakdown of the dictionary and its purpose:
    'owner': 'airflow': Specifies the owner of the task or DAG. This is typically used for identification and categorization purposes.
    'depends_on_past': False: Indicates whether the task or DAG depends on the successful completion of its past instances. Setting it to False means the task or DAG does not depend on past instances.
    'retries': 1: Specifies the number of times the task or DAG can be retried in case of a failure. In this case, the task or DAG will be retried once.
    'retry_delay': timedelta(minutes=10): Defines the delay between retries. In this case, the retry delay is set to 10 minutes using the timedelta function.
'''
default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'retries': 1,
   'retry_delay':timedelta(minutes=10)
} 
   
'''
The DAG is scheduled to run daily at 2:30 AM (schedule_interval='0 1 * * *') (UTC).
 It has a start date of March 24, 2023, and does not have an end date specified. 
 The catchup parameter is set to False, meaning that Airflow will only schedule and execute tasks for future runs and not catch up on missed runs.
''' 

with DAG(default_args=default_args,
         dag_id='quality_sdb_to_rdb',
         schedule_interval="45 7 * * *",
         start_date=pendulum.datetime(year=2023, month=3, day=24,tz='Asia/Kolkata'),
         end_date=None,
         catchup=False
         )as dag:
    quality_sdb_to_quality_rdb_ = PythonOperator(
    task_id="quality_sdb_to_quality_rdb",
    python_callable=quality_rdb_function
    )
    abcd_categorie_table_creation_ = PythonOperator(
    task_id="abcd_categorie_table_creation",
    python_callable=run_if_first_day
    )


'''
The quality_sdb_to_quality_rdb_ task is set as the dependency of the abcd_categorie_table_creation_ task using the >> operator,
indicating that abcd_categorie_table_creation_ should run after quality_sdb_to_quality_rdb_ completes.
'''
quality_sdb_to_quality_rdb_>>abcd_categorie_table_creation_