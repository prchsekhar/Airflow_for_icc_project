import psycopg2
from datetime import datetime, timedelta, date
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import psycopg2.extensions

# Establish connection to the 'landing_db' database
connection1 = psycopg2.connect(
    host='3.108.122.116',
    database='landing_db',
    user='myusername',
    password='mypassword',
    port='5432'
)
cursor1 = connection1.cursor()

# Establish connection to the 'staging_db' database
connection2 = psycopg2.connect(
    host='3.108.122.116',
    database='staging_db',
    user='myusername',
    password='mypassword',
    port='5432'
)
cursor2 = connection2.cursor()

# Set the start and end dates as the previous day
startdate = date.today() - timedelta(days=1)
enddate = date.today() - timedelta(days=1)


csat_list=['date','user_id','call_date','phone_number','ivr_flow','user_full_name','lead_id']
csat_final_column=['date','user_id' ,'call_date','phone_number','survey_rating','user_full_name','lead_id']
exportcall_list=['date','user','full_name','call_date','status','campaign_id','query_typelevel_1','call_transferred_to_survey_ivr','emp_id','phone_number','cust_talk_sec','customer_hold_time','wrap_up_time','queue_seconds','query_type_level_2','subquery_type_level_2','lead_id','customer_number']
agent_column_list=['date','agent_id','emp_name','call_attended','emp_id','login_duration','total_talk_time','total_break','total_wrap_time','total_hold_time' ,'bio' ,'lunch' ,'tea','total_idle_time']

'''

The function ivr_flow_int takes a sequence of values as input and returns the first integer value found in the sequence. 
It iterates over each item in the sequence and attempts to convert it to an integer. If a valid integer is found, it is returned as the result. 
If no integer is found, it returns the default value 'NoResponse'. 
The purpose of this function is to extract the first integer from a sequence of values, prioritizing the earliest occurrence of an integer.
'''
def ivr_flow_int(value):
    # Initialize the default value as 'NoResponse'
    first_int = 'NoResponse'
    # Iterate over each item in the value sequence
    for item in value:
        try:
            # Check if the item can be converted to an integer
            if isinstance(int(item), int):
                # Update the first_int variable if the item is an integer
                first_int = item
                break
        except:
            # If conversion to integer raises an exception, continue to the next item
            pass
    # Return the first integer found, or 'NoResponse' if no integer is found
    return first_int


'''
The function count_function takes three parameters: table_name_ldb, table_name_sdb, and date. 
It executes two SQL queries to count the number of rows in the specified tables (table_name_ldb and table_name_sdb) for a given date. 
It uses the cursor1 and cursor2 objects to execute the queries on the respective database connections. 
The function then retrieves the count values from the query results and returns them as a tuple (count_data_ldb, count_data_sdb).
'''
def count_function(table_name_ldb, table_name_sdb, date):
    # Construct the SQL query for the landing database table
    query = f'''select count(*) from {table_name_ldb} where date='{date.date()}';'''
    # Execute the query on the landing database
    cursor1.execute(query)
    # Fetch the count value from the query result
    count_data_ldb = cursor1.fetchone()[0]
    # Construct the SQL query for the staging database table
    query = f'''select count(*) from {table_name_sdb} where date='{date.date()}';'''
    # Execute the query on the staging database
    cursor2.execute(query)
    # Fetch the count value from the query result
    count_data_sdb = cursor2.fetchone()[0]    
    # Return the count values for the landing and staging databases
    return count_data_ldb, count_data_sdb


'''
It iterates over a range of dates.
It checks if the count of rows in the landing database (agent_info_ldb) is zero. If so, it prints "no need to run."
It checks if the count of rows in the landing and staging databases (agent_info_ldb and agent_info_sdb) is equal. If so, it prints "no need to run."
If the counts differ, it deletes existing rows in the staging database (agent_info_sdb) for the current date.
It fetches the column names and data from the landing database for the current date and creates a DataFrame (df).
It converts the 'date' column to the desired date format and converts the 'emp_name' column to uppercase.
It converts time-related columns to timedelta format.
It inserts each row from the DataFrame into the staging database.
If an exception occurs during the insertion process, it rolls back the changes, prints the problematic values, and the error message.
It prints the current date after completing the operations for that date.
'''
def agent_ldb_to_sdb():
    # Iterate over each date in the specified date range
    for date in pd.date_range(startdate, enddate):
        # Retrieve the count data for the landing and staging databases
        count_data_ldb, count_data_sdb = count_function('agent_info_ldb', 'agent_info_sdb', date)   
        # Check if there are no rows in the landing database
        if count_data_ldb == 0:
            print('no need to run')     
        # Check if the counts in the landing and staging databases are equal
        elif count_data_ldb == count_data_sdb:
            print('no need to run')       
        else:
            print('time to run')           
            # Delete existing rows from the staging database for the current date
            query = f'''delete from agent_info_sdb where date='{date.date()}';'''
            cursor2.execute(query)
            connection2.commit()            
            # Fetch the column names and data from the landing database for the current date
            final_columns = ','.join(['"{0}"'.format(item) for item in agent_column_list])
            query = f'''select {final_columns} from agent_info_ldb where date='{date.date()}';'''
            cursor1.execute(query)
            column_names = [desc[0] for desc in cursor1.description]
            df = pd.DataFrame(cursor1.fetchall(), columns=column_names) 
            # Convert 'date' column to datetime format and uppercase 'emp_name'
            df['date'] = pd.to_datetime(df['date'])
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            df['emp_name'] = df['emp_name'].apply(str.upper)            
            # Convert time-related columns to timedelta format
            for i in range(5, len(agent_column_list)):
                df[f'{agent_column_list[i]}'] = df[f'{agent_column_list[i]}'].apply(lambda x: datetime.combine(datetime.min, x) - datetime.min if x is not None else None)
                df[f'{agent_column_list[i]}'] = df[f'{agent_column_list[i]}'].apply(lambda x: pd.to_timedelta(x).total_seconds() if x is not None else None)            
            # Insert rows into the staging database for each row in the DataFrame
            for i, row in df.iterrows():
                value = row.to_list()
                try:
                    query = f'''INSERT INTO agent_info_sdb ({final_columns}) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
                    cursor2.execute(cursor2.mogrify(query, tuple(value)))
                    connection2.commit()
                except Exception as e:
                    cursor2.execute("ROLLBACK")
                    connection2.commit()
                    print(value)
                    print(e)      
        # Print the current date
        print(date.date())
    cursor1.close()
    cursor2.close()
    connection1.close()
    connection2.close()    

'''
It iterates over a range of dates.
It checks if the count of rows in the landing database (daily_exportcall_ldb) is zero. If so, it prints "no need to run."
It checks if the counts of rows in the landing and staging databases (daily_exportcall_ldb and exportcall_sdb) are equal. If so, it prints "no need to run."
If the counts differ, it deletes existing rows in the staging database (exportcall_sdb) for the current date.
It fetches the column names and data from the landing database for the current date and creates a DataFrame (df).
It converts the 'date' column to the desired date format and converts the 'full_name' column to uppercase.
It inserts each row from the DataFrame into the staging database.
If an exception occurs during the insertion process, it prints the error message, rolls back the changes, and continues.
It executes an update function on the staging database (exportcall_sdb).
It prints the current date after completing the operations for that date.
'''        
def exportcall_ldb_to_sdb():
    # Iterate over each date in the specified date range
    for date in pd.date_range(startdate, enddate):
        # Retrieve the count data for the landing and staging databases
        count_data_ldb, count_data_sdb = count_function('daily_exportcall_ldb', 'exportcall_sdb', date)        
        # Check if there are no rows in the landing database
        if count_data_ldb == 0:
            print('no need to run')        
        # Check if the counts in the landing and staging databases are equal
        elif count_data_ldb == count_data_sdb:
            print('no need to run')        
        else:
            print('time to run')            
            # Delete existing rows from the staging database for the current date
            query = f'''delete from exportcall_sdb where date='{date.date()}';'''
            cursor2.execute(query)
            connection2.commit()            
            # Fetch the column names and data from the landing database for the current date
            final_columns = ','.join(['"{0}"'.format(item) for item in exportcall_list])
            query = f'''select {final_columns} from daily_exportcall_ldb where date='{date.date()}';'''
            cursor1.execute(query)
            column_names = [desc[0] for desc in cursor1.description]
            df = pd.DataFrame(cursor1.fetchall(), columns=column_names)             
            # Convert 'date' column to datetime format and uppercase 'full_name'
            df['date'] = pd.to_datetime(df['date'])
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            df['full_name'] = df['full_name'].apply(str.upper)            
            # Insert rows into the staging database for each row in the DataFrame
            for i, row in df.iterrows():
                value = row.values
                try:
                    query = f'''INSERT INTO exportcall_sdb ({final_columns}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
                    cursor2.execute(cursor2.mogrify(query, tuple(value)))
                    connection2.commit()
                except Exception as e:
                    print(e)
                    cursor2.execute("ROLLBACK")
                    connection2.commit()            
            # Execute an update function on the staging database
            query = f'''select update_repeat_column_produtivity('{date.date()}','{date.date() - timedelta(days=2)}','{date.date() - timedelta(days=6)}')'''
            cursor2.execute(query)
            connection2.commit()        
        # Print the current date
        print(date.date())
    cursor1.close()
    cursor2.close()
    connection1.close()
    connection2.close()



'''
It iterates over a range of dates.
It checks if the count of rows in the landing database (csat_ldb) is zero. If so, it prints "no need to run."
It checks if the counts of rows in the landing and staging databases (csat_ldb and csat_info_sdb) are equal. If so, it prints "no need to run."
If the counts differ, it deletes existing rows in the staging database (csat_info_sdb) for the current date.
It fetches the column names and data from the landing database for the current date and creates a DataFrame (df).
It converts the 'date' column to the desired date format.
It processes the 'ivr_flow' column by stripping brackets, splitting values, and applying the ivr_flow_int function to determine the survey rating.
It inserts rows into the staging database (csat_info_sdb) for the selected columns in the DataFrame.
If an exception occurs during insertion, it prints the error message, rolls back the changes, and continues.
It prints the current date after completing the operations for that date.

'''
def csat_ldb_to_sdb():
    # Iterate over each date in the specified date range
    for date in pd.date_range(startdate, enddate):
        # Retrieve the count data for the landing and staging databases
        count_data_ldb, count_data_sdb = count_function('csat_ldb', 'csat_info_sdb', date)       
        # Check if there are no rows in the landing database
        if count_data_ldb == 0:
            print('no need to run')        
        # Check if the counts in the landing and staging databases are equal
        elif count_data_ldb == count_data_sdb:
            print('no need to run')        
        else:
            print('time to run')           
            # Delete existing rows from the staging database for the current date
            query = f'''delete from csat_info_sdb where date='{date.date()}';'''
            cursor2.execute(query)
            connection2.commit()            
            # Fetch the column names and data from the landing database for the current date
            final_columns = ','.join(['"{0}"'.format(item) for item in csat_list])
            query = f'''select {final_columns} from csat_ldb where date='{date.date()}';'''
            cursor1.execute(query)
            column_names = [desc[0] for desc in cursor1.description]
            df = pd.DataFrame(cursor1.fetchall(), columns=column_names)             
            # Convert 'date' column to datetime format
            df['date'] = pd.to_datetime(df['date'])
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')  
            df['user_full_name'] = df['user_full_name'].apply(str.upper)            
            # Process 'ivr_flow' column by stripping brackets, splitting values, and applying 'ivr_flow_int' function
            df['ivr_flow'] = df['ivr_flow'].apply(lambda x: x.strip('[]').split(','))
            df['survey_rating'] = df['ivr_flow'].apply(lambda x: ivr_flow_int(x))
            df['survey_rating'] = df['survey_rating'].str.replace(" ", "")            
            final_columns_db = ','.join(['"{0}"'.format(item) for item in csat_final_column])
            # Insert rows into the staging database for selected columns in the DataFrame
            for _, row in df.loc[:, csat_final_column].iterrows():
                value = row.values
                try:
                    query = f'''INSERT INTO csat_info_sdb (%s) VALUES %s;''' % (final_columns_db, tuple(value))
                    cursor2.execute(query)
                    connection2.commit()
                except Exception as e:
                    cursor2.execute("ROLLBACK")
                    connection2.commit()
                    print(e)            
            # Print the current date
            print(date.date())
    cursor1.close()
    cursor2.close()
    connection1.close()
    connection2.close()



forecast_sdb_list=['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', 'Total', 'date']


'''
It iterates over a range of dates.
It deletes existing rows from the staging database (forecast_sdb) for the current date.
It fetches the column names and data from the landing database (forecast) for the current date and creates a DataFrame (df).
It converts the 'date' column to the desired date format.
It inserts rows into the staging database for all columns in the DataFrame.
If an exception occurs during insertion, it prints the error message, rolls back the changes, and continues.
It prints the current date after completing the operations for that date.
'''
def forecast_ldb_to_sdb():
    # Iterate over each date in the specified date range
    for date in pd.date_range(startdate, enddate):
        # Delete existing rows from the staging database for the current date
        query = f'''delete from forecast_sdb where date='{date.date()}';'''
        cursor2.execute(query)
        connection2.commit()        
        # Fetch the column names and data from the landing database for the current date
        final_columns = ','.join(['"{0}"'.format(item) for item in forecast_sdb_list])
        query = f'''select {final_columns} from forecast where date='{date.date()}';'''
        cursor1.execute(query)
        column_names = [desc[0] for desc in cursor1.description]
        df = pd.DataFrame(cursor1.fetchall(), columns=column_names)       
        # Convert 'date' column to datetime format
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')        
        # Insert rows into the staging database for all columns in the DataFrame
        for _, row in df.iterrows():
            value = row.values
            try:
                query = f'''INSERT INTO forecast_sdb ({final_columns}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
                cursor2.execute(cursor2.mogrify(query, tuple(value)))
                connection2.commit()
            except Exception as e:
                cursor2.execute("ROLLBACK")
                connection2.commit()
                print(e)      
        # Print the current date
        print(date.date())
    cursor1.close()
    cursor2.close()
    connection1.close()
    connection2.close()

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
   'retry_delay': timedelta(minutes=10)
}


'''
The DAG is scheduled to run daily at 2:00 AM (schedule_interval='0 2 * * *') (UTC).
 It has a start date of March 03, 2023, and does not have an end date specified. 
 The catchup parameter is set to False, meaning that Airflow will only schedule and execute tasks for future runs and not catch up on missed runs.
'''    
with DAG(default_args=default_args,
         dag_id='ldb_to_sdb',
         schedule_interval='0 7 * * * ',
         start_date=pendulum.datetime(year=2023, month=3, day=16, tz='Asia/Kolkata'),
         end_date=None,
         catchup=False) as dag:

    # Task 1: Ldb_to_daily_agent_sdb
    Ldb_to_daily_agent_sdb_ = PythonOperator(
        task_id="Ldb_to_daily_agent_sdb",
        python_callable=agent_ldb_to_sdb,
        dag=dag
    )

    # Task 2: Ldb_to_daily_exportcall_sdb
    Ldb_to_daily_exportcall_sdb_ = PythonOperator(
        task_id="Ldb_to_daily_exportcall_sdb",
        python_callable=exportcall_ldb_to_sdb,
        dag=dag
    )

    # Task 3: Ldb_to_daily_csat_sdb
    Ldb_to_daily_csat_sdb_ = PythonOperator(
        task_id="Ldb_to_daily_csat_sdb",
        python_callable=csat_ldb_to_sdb,
        dag=dag
    )

    # Task 4: Ldb_to_daily_forecast_sdb
    Ldb_to_daily_forecast_sdb_ = PythonOperator(
        task_id="Ldb_to_daily_forecast_sdb",
        python_callable=forecast_ldb_to_sdb,
        dag=dag
    )

    # Define the task dependencies
    Ldb_to_daily_agent_sdb_ >> Ldb_to_daily_exportcall_sdb_ >> Ldb_to_daily_csat_sdb_ >> Ldb_to_daily_forecast_sdb_



