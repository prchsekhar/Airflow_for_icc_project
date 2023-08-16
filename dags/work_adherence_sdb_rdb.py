import psycopg2
from datetime import timedelta,date,datetime
import pandas as pd
import datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from psycopg2.extras import execute_values
import numpy as np


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

connection_1=psycopg2.connect(host='3.108.122.116',database='staging_db', user='myusername', password='mypassword',port='5432')
cursor_1 = connection_1.cursor()
connection_2=psycopg2.connect(host='3.108.122.116',database='reporting_db', user='myusername', password='mypassword',port='5432')
cursor_2 = connection_2.cursor()






#----------------------------------------TL Wise-------------------------

columns_list_1=[ 'first_login_date', 'last_logout_date', 'employeeid', 'employeename','shift', 'shifts_timings', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'wo', 'ph', 'leaves', 'total_planned_', 'login_details', 'logout_details', 'attendance_present', 'log_date','sip','late_minutes','bio','lunch','tea','week_number','date','team_lead','non_rosted_count','roster_count','manager','tl','sr__manager','emp_deatail_id','user_name','total_working_hours','total_break_hours','schedule_adherence_percentage','total_hours','emp_id','login_adherence_percentage'] 

def work_adherence_rdb_testing():
    today = datetime.date.today()
    validation_date = datetime.date(today.year, 5, 5)  # May 5th of the current year
    
    query = '''select log_date from work_adherence_rdb_tl group by log_date;'''
    cursor_2.execute(query)
    rdb_date_data = cursor_2.fetchall()
    rdb_date_data = [str(date[0]) for date in rdb_date_data if date[0] is not None and date[0] > validation_date]
    
    query = '''select date from login_logout_sdb group by date;'''
    cursor_1.execute(query)
    sdb_date_data = cursor_1.fetchall()
    sdb_date_data = [str(date[0]) for date in sdb_date_data if date[0] is not None and date[0] > validation_date]
    
    not_matched_dates = list(set(rdb_date_data) ^ set(sdb_date_data))
    not_matched_dates = [date for date in not_matched_dates if date != 'None']
    
    if not_matched_dates == []:
        print('No need to run')
    else:
        for range_date in not_matched_dates:
            query_delete = f'''DELETE FROM work_adherence_rdb_tl WHERE update_date = '{range_date}'; '''
            cursor_2.execute(query_delete)
            
            # Joining column names with commas and enclosing them in double quotes
            final_columns = ','.join(['"{0}"'.format(item) for item in columns_list_1])
            date_obj = datetime.datetime.strptime(range_date, '%Y-%m-%d')
            
            # Convert the datetime object to a different date format
            formatted_date = date_obj.strftime('%d-%m-%Y')  # Change the format as needed
            print(range_date)
            print(date_obj.strftime('%a').lower())
            print(int(date_obj.strftime('%W')))
            
            # Execute a query to get work adherence data based on range_date and col_name
            query = f'''SELECT get_work_function('{range_date}', '{date_obj.strftime('%a').lower()}', '{int(date_obj.strftime('%W'))}')'''
            cursor_1.execute(query)
            connection_1.commit()
            
            # Execute a query to select data from the temporary table 'tmp'
            query = f'''SELECT {final_columns} FROM tmp'''
            cursor_1.execute(query)
            column_names = [desc[0] for desc in cursor_1.description]
            
            # Create a DataFrame with fetched data and assign column names
            df = pd.DataFrame(cursor_1.fetchall(), columns=column_names)
            df['update_date'] = pd.to_datetime(range_date).strftime('%Y-%m-%d')
            
            # Clean and preprocess the 'log_date' column
            df['log_date'] = pd.to_datetime(df['log_date'], errors='coerce')
            df['log_date'] = df['log_date'].dt.strftime('%Y-%m-%d')
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
            # Convert DataFrame to a list of values
            df = df.replace({np.nan: None})
            values = df.values.tolist()
            
            print(df.shape)
            
            for value in values:
                try:
                    value = [v if v is not None else 'NULL' for v in value]
                    # Insert values into 'work_adherence_rdb' table using execute_values
                    query = f'''INSERT INTO work_adherence_rdb_TL ({final_columns}, update_date) VALUES ({','.join(['%s'] * len(value))});'''
                    value = [None if v == 'NULL' else v for v in value]
                    cursor_2.execute(query, value)
                    query = f'''select * from work_adherence_rdb_TL where date='{range_date}';'''
                    cursor_2.execute(query)
                    # column_names = [desc[0] for desc in cursor_2.description]
                    # df = pd.DataFrame(cursor_2.fetchall(), columns=column_names)
                    # print(df.head())
                    
                    connection_2.commit()
                    # print('Successfully uploaded')
                except Exception as e:
                    print(e)
                    print(query, value)
            print('Successfully uploaded')
            
            # Drop the temporary table 'tmp'
            query = '''DROP TABLE tmp'''
            cursor_1.execute(query)
            connection_1.commit()



# columns_list=[ 'first_login_date', 'last_logout_date', 'employeeid', 'employeename','shift', 'shifts_timings', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'wo', 'ph', 'leaves', 'total_planned_', 'login_details', 'logout_details', 'attendance_present', 'log_date','sip','late_minutes','on_time','bio','lunch','tea','week_number','date','team_lead','non_rosted_count','roster_count','manager','tl','sr__manager','emp_deatail_id','user_name','total_working_hours','total_break_hours','schedule_adherence_percentage','total_hours','emp_id','category_','attrition_type','reasons','attrited_date','ielevate_updated'] 
 

# def work_adherence_rdb_TL():
#     # Iterate over the range of dates using pd.date_range()
#     for range_date in pd.date_range(start_date, end_date):
#         # Get count of records in 'work_adherence_rdb' for the current range_date
#         query = f'''SELECT COUNT(*) FROM work_adherence_rdb_TL WHERE log_date = '{range_date.date()}' and week_number='{week_integer}';'''
#         cursor_2.execute(query)
#         count_table_data = cursor_2.fetchone()[0]
#         print(count_table_data)

#         # Get count of records in 'login_logout_sdb' for the current range_date
#         query = f'''SELECT COUNT(*) FROM login_logout_sdb WHERE date = '{range_date.date()}' and week_number='{week_integer}';'''
#         cursor_1.execute(query)
#         count_sdb_data = cursor_1.fetchone()[0]
#         print(count_sdb_data)

#         if count_table_data == count_sdb_data:
#             # If count_table_data is not zero, no need to run the process
#             print('No need to run')
#         elif count_sdb_data == 0:
#             # If count_sdb_data is zero, no need to run the process
#             print('No need to run')
#         else:
#             # If both counts are valid, time to run the process
#             print('Time to run')

#             query_delete = f'''DELETE FROM work_adherence_rdb_tl WHERE update_date = '{range_date.date()}'; '''
#             cursor_2.execute(query_delete)

#             # Joining column names with commas and enclosing them in double quotes
#             final_columns = ','.join(['"{0}"'.format(item) for item in columns_list])

#             # Execute a query to get work adherence data based on range_date and col_name
#             query = f'''SELECT get_work_adherence_data('{range_date.date()}','{col_name}','{week_integer}')'''
#             cursor_1.execute(query)

#             # Execute a query to select data from the temporary table 'tmp'
#             query = f'''SELECT {final_columns} FROM tmp'''
#             cursor_1.execute(query)
#             column_names = [desc[0] for desc in cursor_1.description]

#             # Create a DataFrame with fetched data and assign column names
#             df = pd.DataFrame(cursor_1.fetchall(), columns=column_names)
#             # previous_day = date.today() - timedelta(days=1)
#             df['update_date'] = pd.to_datetime(range_date.date()).strftime('%Y-%m-%d')

#             # Clean and preprocess the 'log_date' column
#             df['log_date'] = pd.to_datetime(df['log_date'],errors='coerce')
#             df['log_date'] = df['log_date'].dt.strftime('%Y-%m-%d')
#             df['date'] = pd.to_datetime(df['date'],errors='coerce')
#             df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
#             # Convert DataFrame to a list of values
#             df=df.replace({np.nan:None})
#             values = df.values.tolist()

#             # print(df.shape)

#             for value in values:
#                 try:
#                     value = [v if v is not None else 'NULL' for v in value]
#                     # Insert values into 'work_adherence_rdb' table using execute_values
#                     query = f'''INSERT INTO work_adherence_rdb_TL ({final_columns}, update_date) VALUES ({','.join(['%s'] * len(value))});'''
#                     value = [None if v == 'NULL' else v for v in value]
#                     cursor_2.execute(query, value)
           
#                     # connection_2.commit()
#                     # print('Successfully uploaded')
#                 except Exception as e:
#                     print(e)
#                     print(query,value)
#                     break
#             connection_2.commit()
#             print('Successfully uploaded')
#             # Drop the temporary table 'tmp'
#             query = '''DROP TABLE tmp'''
#             cursor_1.execute(query)
#             connection_1.commit()
column_list_attrition_sdb=['employeeid','employeename','supervisor','attrited_from_','date','attrition_type','reasons','category_','resons','ielevate_updated','lob']
def attrition_tracker_rdb_function():
    query = '''SELECT date, COUNT(*) FROM attrition_tracker_sdb  GROUP BY date;'''
    cursor_1.execute(query)
    sdb_date_datadb_date_data = cursor_1.fetchall()
    sdb_date_data = [date[0] for date in sdb_date_datadb_date_data]

    query = '''select date,count(*) from attrition_tracker_rdb group by date;'''
    cursor_2.execute(query)
    rdb_date_data = cursor_2.fetchall()
    rdb_date_data = [date[0] for date in rdb_date_data]

    not_matched_dates = list(set(rdb_date_data) ^ set(sdb_date_data))
    not_matched_dates = [date for date in not_matched_dates if date != 'None']

    if not_matched_dates == []:
        print('No need to run')
    else:
        for range_date in not_matched_dates:
            cursor_2.execute("ROLLBACK")
            query_delete = f'''DELETE FROM attrition_tracker_rdb WHERE date = '{range_date}'; '''
            cursor_2.execute(query_delete)

            final_columns = ','.join(['"{0}"'.format(item) for item in column_list_attrition_sdb])

            query = f'''SELECT {final_columns} FROM attrition_tracker_sdb WHERE date = '{range_date}';'''
            cursor_1.execute(query)
            column_names = [desc[0] for desc in cursor_1.description]

            df = pd.DataFrame(cursor_1.fetchall(), columns=column_names)
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')

            # Convert 'user_name' column values to uppercase
            df['employeename'] = df['employeename'].apply(str.upper)

            # Get the values from the DataFrame as a list of tuples
            values = [tuple(row) for row in df.values]

            try:
                query = f'''INSERT INTO attrition_tracker_rdb ({final_columns}) VALUES ({','.join(['%s'] * len(column_names))});'''
                psycopg2.extras.execute_batch(cursor_2, query, values)
                connection_2.commit()
                print(f'Successfully uploaded for date: {range_date}')
            except Exception as error:
                print(error)

    print('Upload process completed')



    



#------------------------------------------------------------

with DAG(default_args=default_args,
         dag_id='work_adherence_sdb_to_rdb',
         schedule_interval="30 */6 * * *",
         start_date=pendulum.datetime(year=2023, month=3, day=24,tz='Asia/Kolkata'),
         end_date=None,
         catchup=False
         )as dag:
        attrition_tracker_rdb_task = PythonOperator(
        task_id='attrition_tracker_rdb_task',
        python_callable=attrition_tracker_rdb_function,
        dag=dag,
        )
        work_adherence_rdb_tl_task=PythonOperator(
        task_id='work_adherence_rdb_tl_task',
        python_callable=work_adherence_rdb_testing,
        dag=dag
        )

        # attrition_tracker_rdb_task = TriggerDagRunOperator(
        # task_id='attrition_tracker_rdb_task',
        # trigger_dag_id='attrition_tracker_rdb_function',
        # dag=dag,
        # )

