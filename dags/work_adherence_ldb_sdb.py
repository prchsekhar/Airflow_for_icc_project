import psycopg2
import pandas as pd
import numpy as np
import re
import datetime  
from datetime import date, timedelta
from psycopg2.extras import execute_values
from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

connection_source = psycopg2.connect(host='3.108.122.116',database='landing_db', user='myusername', password='mypassword',port='5432')
cursor_source = connection_source.cursor()
connection_dest=psycopg2.connect(host='3.108.122.116',database='staging_db', user='myusername', password='mypassword',port='5432')
cursor_dest = connection_dest.cursor()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

'''
#Retrieve the count of records in the source table '"agent_roster"'.
#Check if the counts of records in '"agent_roster"' match on a date basis with the '"agent_roster_sdb"' table. If they match, there is no need to run the process, and the function exits.
#If the counts do not match, proceed with the following steps:
#Get the mismatched dates by comparing the counts on a date basis between 'agent_roster' and '"agent_roster_sdb"'.
#Delete the existing data in '"agent_roster_sdb"' for the mismatched dates.
#Truncate the temporary table "temp_agent_roster_sdb".
#Select the data from 'agent_roster' for the mismatched dates.
#Clean and preprocess the selected columns.
#Convert the DataFrame to a list of values.
#Insert the values into '"agent_roster_sdb"' and "temp_agent_roster_sdb" tables using the execute_values() method.
#If the insertion process is successful, commit the changes and print a success message. Otherwise, print any encountered error.
#The code essentially checks if there are any new or modified records in the "agent_roster" table and updates the "agent_roster_sdb" table accordingly.
'''

import datetime

column_list_agent_roster_sdb = ['employeeid', 'employeename', 'current_status', 'shift', 'shifts_timings', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'wo', 'ph', 'leaves', 'total_planned_', 'date', 'team_lead']

def agent_roster_sdb_function():
    query = '''select date from agent_roster group by date;'''
    cursor_source.execute(query)
    ldb_date_datadb_date_data = cursor_source.fetchall()
    ldb_date_data = [datetime.datetime.strptime(date[0], '%Y-%m-%d').date() for date in ldb_date_datadb_date_data]

    query = '''select date from agent_roster_sdb group by date;'''
    cursor_dest.execute(query)
    sdb_date_data = cursor_dest.fetchall()
    sdb_date_data = [date[0] for date in sdb_date_data]

    not_matched_dates = list(set(sdb_date_data) ^ set(ldb_date_data))
    not_matched_dates = [date for date in not_matched_dates if date != 'None']

    if not_matched_dates == []:
        print('No need to run')
    else:
        for range_date in not_matched_dates:
            query_delete = f'''DELETE FROM agent_roster_sdb WHERE date = '{range_date}'; '''
            cursor_dest.execute(query_delete)

            final_columns = ','.join(['"{0}"'.format(item) for item in column_list_agent_roster_sdb])

            query = f'''SELECT {final_columns} FROM agent_roster WHERE date = '{range_date}';'''
            cursor_source.execute(query)
            column_names = [desc[0] for desc in cursor_source.description]

            df = pd.DataFrame(cursor_source.fetchall(), columns=column_names)

            values = df.values.tolist()

            try:
                query = f"INSERT INTO agent_roster_sdb ({final_columns}) VALUES %s;"
                psycopg2.extras.execute_values(cursor_dest, query, values)
                connection_dest.commit()
                print(f'Successfully uploaded for date: {range_date}')
            except Exception as error:
                print(error)

    print('Upload process completed')



# def agent_roster_sdb_function():
#     # Get count of records in the source table 'agent_roster'
#     query = '''SELECT COUNT(*) FROM agent_roster;'''
#     cursor_source.execute(query)
#     count_data = cursor_source.fetchone()[0]
#     print(count_data)

#     # Get count of records in the destination table 'agent_roster_sdb'
#     query = '''SELECT COUNT(*) FROM agent_roster_sdb;'''
#     cursor_dest.execute(query)
#     count_table_data = cursor_dest.fetchone()[0]
#     print(count_table_data)

#     # Check if counts match, indicating no need to run the process
#     if count_table_data == count_data:
#         print('No need to run')
#     else:
#         print('Time to run')
        
#         # Truncate the temporary table 'temp_agent_roster_sdb'
#         query = '''TRUNCATE TABLE agent_roster_sdb ;'''
#         cursor_dest.execute(query)
#         connection_dest.commit()

#         # Joining column names with commas and enclosing them in double quotes
#         final_columns = ','.join(['"{0}"'.format(item) for item in column_list_agent_roster_sdb])

#         # Selecting data from 'agent_roster' starting from the offset of count_table_data
#         query = f'''SELECT {final_columns} FROM agent_roster OFFSET {count_table_data};'''
#         cursor_source.execute(query)
#         column_names = [desc[0] for desc in cursor_source.description]

#         # Creating a DataFrame with fetched data and assigning column names
#         df = pd.DataFrame(cursor_source.fetchall(), columns=column_names)

#         # Clean and preprocess the 'employeename' column
#         # df['employeename'] = df['employeename'].apply(lambda x: x.replace('\xa0', ''))
#         df['employeeid'] = df['employeeid'].apply(lambda x: re.sub(r'[^\x00-\x7F]+', '', str(x)))

#         # df['employeename'] = df['employeename'].str.replace(r'[\u00A0\u202F]', '', regex=True).str.strip()
#         df['employeeid'] = df['employeeid'].str.strip()
#         df['employeename'] = df['employeename'].str.upper()
#         df['employeeid'] = df['employeeid'].str.replace(' ', '')


#         # Convert DataFrame to a list of values
#         values = df.values.tolist()

#         try:
#             # Insert values into 'agent_roster_sdb' and 'temp_agent_roster_sdb' tables
#             query = f"INSERT INTO agent_roster_sdb ({final_columns}) VALUES %s;"
#             psycopg2.extras.execute_values(cursor_dest, query, values)
#             query = f"INSERT INTO temp_agent_roster_sdb ({final_columns}) VALUES %s;"
#             psycopg2.extras.execute_values(cursor_dest, query, values)
#             connection_dest.commit()
#             print('sucessfully_uploaded')
#         except Exception as error:
#             print(error)

# agent_roster_sdb_function()




column_list_login_logout_sdb=['user_name','user_id','first_login_date','last_logout_date','campaign_id','date']

def login_logout_sdb_function():
    query = '''SELECT date, COUNT(*) FROM login_logout WHERE campaign_id = 'INBOUND' GROUP BY date;'''
    cursor_source.execute(query)
    ldb_date_datadb_date_data = cursor_source.fetchall()
    ldb_date_data = [datetime.datetime.strptime(date[0], '%Y-%m-%d').date() for date in ldb_date_datadb_date_data]

    query = '''select date from login_logout_sdb group by date;'''
    cursor_dest.execute(query)
    sdb_date_data = cursor_dest.fetchall()
    sdb_date_data = [date[0] for date in sdb_date_data]

    not_matched_dates = list(set(sdb_date_data) ^ set(ldb_date_data))
    not_matched_dates = [date for date in not_matched_dates if date != 'None']

    if not_matched_dates == []:
        print('No need to run')
    else:
        for range_date in not_matched_dates:
            query_delete = f'''DELETE FROM login_logout_sdb WHERE date = '{range_date}'; '''
            cursor_dest.execute(query_delete)

            final_columns = ','.join(['"{0}"'.format(item) for item in column_list_login_logout_sdb])

            query = f'''SELECT {final_columns} FROM login_logout WHERE date = '{range_date}' and campaign_id = 'INBOUND';'''
            cursor_source.execute(query)
            column_names = [desc[0] for desc in cursor_source.description]

            df = pd.DataFrame(cursor_source.fetchall(), columns=column_names)
            df['date'] = pd.to_datetime(df['date'])
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            df['user_id'] = df['user_id'].str.strip()

            # Convert 'user_name' column values to uppercase
            df['user_name'] = df['user_name'].str.upper()

            values = df.values.tolist()

            try:
                query = f"INSERT INTO login_logout_sdb ({final_columns}) VALUES %s;"
                psycopg2.extras.execute_values(cursor_dest, query, values)
                connection_dest.commit()
                print(f'Successfully uploaded for date: {range_date}')
            except Exception as error:
                print(error)

    print('Upload process completed')




# def login_logout_sdb_function():
#     # Get count of records in the source table 'login_logout' with campaign_id = 'INBOUND'
#     query = '''SELECT COUNT(*) FROM login_logout WHERE campaign_id = 'INBOUND';'''
#     cursor_source.execute(query)
#     count_data = cursor_source.fetchone()[0]
#     print(count_data)

#     # Check if counts match on a date basis, indicating no need to run the process
#     query = '''SELECT date, COUNT(*) FROM login_logout WHERE campaign_id = 'INBOUND' GROUP BY date;'''
#     cursor_source.execute(query)
#     counts_data = cursor_source.fetchall()

#     query = '''SELECT date, COUNT(*) FROM login_logout_sdb GROUP BY date;'''
#     cursor_dest.execute(query)
#     counts_table_data = cursor_dest.fetchall()

#     # Convert the data to sets for comparison
#     set_counts_data = set(counts_data)
#     set_counts_table_data = set(counts_table_data)

#     if set_counts_table_data == set_counts_data:
#         print('No need to run')
#         return
#     else:
#         print('Time to run')

#         # Get the mismatched dates by comparing the counts on a date basis
#         mismatched_dates = []
#         for date_count in counts_data:
#             date = date_count[0]
#             count = date_count[1]
#             print(date)
#             print(count)
#             found = False
#             for table_count in counts_table_data:
#                 if table_count[0] == date and table_count[1] == count:
#                     found = True
#                     break
#             if not found:
#                 mismatched_dates.append(date)

#         # Delete the existing data in 'login_logout_sdb' for the mismatched dates
#         for date in mismatched_dates:
#             query_delete = f'''DELETE FROM login_logout_sdb WHERE date = '{date}' AND campaign_id = 'INBOUND';'''
#             cursor_dest.execute(query_delete)
#             connection_dest.commit()

#         # Select and upload the data for the mismatched dates
#         for date in mismatched_dates:
#             # Joining column names with commas and enclosing them in double quotes
#             final_columns = ','.join(['"{0}"'.format(item) for item in column_list_login_logout_sdb])

#             # Selecting data from 'login_logout' for the specific date and campaign_id = 'INBOUND'
#             query = f'''SELECT {final_columns} FROM login_logout WHERE campaign_id = 'INBOUND' AND date = '{date}';'''
#             cursor_source.execute(query)
#             column_names = [desc[0] for desc in cursor_source.description]

#             # Creating a DataFrame with fetched data and assigning column names
#             df = pd.DataFrame(cursor_source.fetchall(), columns=column_names)

#             # Clean and preprocess the columns

#             # Clean and preprocess the 'date' column
#             df['date'] = pd.to_datetime(df['date'])
#             df['date'] = df['date'].dt.strftime('%Y-%m-%d')
#             df['user_id'] = df['user_id'].str.strip()

#             # Convert 'user_name' column values to uppercase
#             df['user_name'] = df['user_name'].str.upper()

#             # Convert DataFrame to a list of tuples
#             values = df.values.tolist()

#             try:
#                 # Insert values into 'login_logout_sdb' for the specific date and campaign_id = 'INBOUND'
#                 query = f"INSERT INTO login_logout_sdb ({final_columns}) VALUES %s;"
#                 psycopg2.extras.execute_values(cursor_dest, query, values)
#                 connection_dest.commit()
#                 print(f'Successfully uploaded for date: {date}')
#             except Exception as error:
#                 print(error)

#     print('Upload process completed')




emp_details_columns=[ 'sl_no','lob',  'employeeid', 'employeename', 'sip', 'gender', 'team_lead',  'doj', 'current_status', 'designation',  'role_fit', 'grid','manager','sr__manager']


def emp_details_sdb_function():
    
    final_columns= ','.join(['"{0}"'.format(item) for item in emp_details_columns])
    query=f'''truncate table  emp_details_sdb;'''
    cursor_dest.execute(query)
    query=f'''select {final_columns} from emp_details ;'''
    cursor_source.execute(query)
    column_names= [desc[0] for desc in cursor_source.description]
    df=pd.DataFrame(cursor_source.fetchall(), columns= column_names)
    df['employeeid'] = df['employeeid'].str.strip() 
    df['employeename'] = df['employeename'].apply(str.upper)
    df = df.replace({np.nan:None})
    value=df.values
    value=list(map(tuple, value))
    a=str(value).strip("[]")
    query=f'''INSERT INTO emp_details_sdb (%s) VALUES %s;'''%(final_columns,a)
    cursor_dest.execute(query)
    connection_dest.commit()
    query = '''SELECT updating_tl_and_manager_names();'''
    cursor_dest.execute(query)
    connection_dest.commit()

    print('sucessfully_uploaded')
# emp_details_sdb_function()


# column_list_emp_details_sdb=['employeeid','sip','employeename','current_status']
# def emp_details_sdb_function():
#     query='''select count(*) from emp_details;'''
#     cursor_source.execute(query)
#     count_data=cursor_source.fetchone()[0]
#     print(count_data)
#     query='''select count(*) from emp_details_sdb;'''
#     cursor_dest.execute(query)
#     count_table_data=cursor_dest.fetchone()[0]
#     print(count_table_data)
#     if count_table_data==count_data:
#         print('no need to run')
#     else:
#         print('time to run')
#         final_columns= ','.join(['"{0}"'.format(item) for item in column_list_emp_details_sdb])
#         query=f'''select {final_columns} from emp_details OFFSET {count_table_data};'''
#         cursor_source.execute(query)
#         column_names= [desc[0] for desc in cursor_source.description]
#         df=pd.DataFrame(cursor_source.fetchall(), columns= column_names) 
#         # df['date'] = pd.to_datetime(df['date'])
#         # df['date'] = df['date'].dt.strftime('%Y-%m-%d')
#         df['employeename'] = df['employeename'].apply(str.upper)
#         value=df.values
#         value=list(map(tuple, value))
#         a=str(value).strip("[]")
#         query=f'''INSERT INTO emp_details_sdb (%s) VALUES %s;'''%(final_columns,a)
#         cursor_dest.execute(query)
#         connection_dest.commit()

# emp_details_sdb_function()

column_list_attrition_sdb=['employeeid','employeename','supervisor','attrited_from_','date','attrition_type','reasons','category_','resons','ielevate_updated','lob']
def attrition_tracker_sdb_function():
    # Get count of records in the source table 'login_logout' with campaign_id = 'INBOUND'
    query = '''SELECT COUNT(*) FROM attrition_tracker where lob='FHPL';'''
    cursor_source.execute(query)
    count_data = cursor_source.fetchone()[0]
    print(count_data)

    # Get count of records in the destination table 'login_logout_sdb'
    query = '''SELECT COUNT(*) FROM attrition_tracker_sdb;'''
    cursor_dest.execute(query)
    count_table_data = cursor_dest.fetchone()[0]
    print(count_table_data)

    # Check if counts match, indicating no need to run the process
    if count_table_data == count_data:
        print('No need to run')
    else:
        print('Time to run')

        # Joining column names with commas and enclosing them in double quotes
        final_columns = ','.join(['"{0}"'.format(item) for item in column_list_attrition_sdb])

        # Selecting data from 'login_logout' with campaign_id = 'INBOUND' starting from the offset of count_table_data
        query = f'''SELECT {final_columns} FROM attrition_tracker where lob='FHPL' OFFSET {count_table_data} ;'''
        cursor_source.execute(query)
        column_names = [desc[0] for desc in cursor_source.description]

        # Creating a DataFrame with fetched data and assigning column names
        df = pd.DataFrame(cursor_source.fetchall(), columns=column_names)

        # Clean and preprocess the 'date' column
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')

        # Convert 'user_name' column values to uppercase
        df['employeename'] = df['employeename'].apply(str.upper)

        # Get the values from the DataFrame and convert them to a list of tuples
        value = df.values
        value = list(map(tuple, value))

        # Convert the list of tuples to a string representation without square brackets
        a = str(value).strip("[]")

        # Construct the SQL query to insert data into 'login_logout_sdb' table
        query = f'''INSERT INTO attrition_tracker_sdb (%s) VALUES %s;''' % (final_columns, a)

        # Execute the query on the destination cursor
        cursor_dest.execute(query)

        # Commit the changes to the destination connection
        connection_dest.commit()


# attrition_tracker_sdb_function()
with DAG(default_args=default_args,
         dag_id='work_adherence_ldb_sdb',
         schedule_interval="0 */6 * * *",
         start_date=pendulum.datetime(year=2023, month=3, day=24,tz='Asia/Kolkata'),
         end_date=None,
         catchup=False
         )as dag:
        agent_roster_ldb_sdb = PythonOperator(
        task_id="agent_roster_ldb_sdb",
        python_callable=agent_roster_sdb_function
        )

        login_logout_ldb_sdb = PythonOperator(
        task_id="login_logout_ldb_sdb",
        python_callable=login_logout_sdb_function
        )
        attrition_tracker_ldb_sdb = PythonOperator(
        task_id="attrition_tracker_ldb_sdb",
        python_callable=attrition_tracker_sdb_function
        )
        emp_details_ldb_sbd=PythonOperator(
        task_id="emp_details_ldb_sbd",
        python_callable=emp_details_sdb_function
        )