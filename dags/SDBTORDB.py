# Import necessary libraries
import psycopg2
from datetime import timedelta,date
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from psycopg2.extras import execute_values
import numpy as np

# Establish connection to the staging database
connection1=psycopg2.connect(host='3.108.122.116',database='staging_db', user='myusername', password='mypassword',port='5432')
cursor1 = connection1.cursor()

# Establish connection to the reporting database
connection2=psycopg2.connect(host='3.108.122.116',database='reporting_db', user='myusername', password='mypassword',port='5432')
cursor2 = connection2.cursor()

product_list=['date', 'agent_id', 'emp_name', 'call_attended', 'login_duration', 'total_talk_time', 'total_break', 'total_wrap_time', 'total_hold_time', 'bio', 'lunch', 'tea', 'total_idle_time', 'avg_time_hold_daily', 'avg_wrap_time_daily', 'avg_talk_time_daily', 'avg_handling_time_daily', 'utilization', 'occupancy', 'wrap_perc', 'hold_perc','taged','emp_id']
product_csat_list=['date', 'agent_id', 'emp_name', 'call_attended','emp_id','login_duration', 'total_talk_time', 'total_break', 'total_wrap_time', 'total_hold_time', 'bio', 'lunch', 'tea', 'total_idle_time', 'avg_time_hold_daily', 'avg_wrap_time_daily', 'avg_talk_time_daily', 'avg_handling_time_daily', 'utilization', 'occupancy', 'wrap_perc', 'hold_perc','taged','cumulative_score','avg_handling_cumulative_score','ftr','total_calls','survey_ivr','csat','dsat','neutral','response_count','great_service','csat_per','dsat_per', 'neutral_per', 'great_service_per','first_call_resolution','ivr_transfer','categorie']

# Set the start and end dates as the previous day
startdate=date.today() - timedelta(days=1)
enddate=startdate

'''
It iterates over each date in the specified date range using pd.date_range.
It converts the list of product columns into a string for SQL queries.
It deletes existing records from the target RDB for the current date.
It calls a function 'get_product_table' in the source SDB to retrieve data for the current date.
It retrieves data from the temporary product table in the source SDB.
It formats the 'date' column as a datetime object and converts it to the '%Y-%m-%d' format.
It replaces any NaN values in the DataFrame with None.
It iterates over the rows of the DataFrame and inserts the data into the target RDB.
It prints the current date being processed.
It calls an update function to calculate cumulative scores in the target RDB.
It drops the temporary product table in the source SDB.
'''
def product_sdb_to_rdb():
    # Iterate over each date in the range specified by 'startdate' and 'enddate'
    for x in pd.date_range(startdate, enddate):
        # Convert the list of product columns into a string for SQL query
        final_columns = ','.join(['"{0}"'.format(item) for item in product_list])
        # Delete existing records from the target relational database (RDB) for the current date
        query = f'''delete from product_rdb where date='{x.date()}';'''
        cursor2.execute(query)
        # Call a function 'get_product_table' in the source staging database (SDB) for the current date
        query = f'''SELECT get_product_table('{x.date()}');'''
        cursor1.execute(query)
        # Retrieve data from the temporary product table in the SDB
        query = f'''SELECT {final_columns} from temp_product;'''
        cursor1.execute(query)
        column_names = [desc[0] for desc in cursor1.description]
        df = pd.DataFrame(cursor1.fetchall(), columns=column_names)
        # Convert the 'date' column to a datetime object and format it as '%Y-%m-%d'
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        # Replace any NaN values in the DataFrame with None
        df = df.replace({np.nan: None})
        # Iterate over the rows of the DataFrame and insert the data into the target RDB
        for i, row in df.iterrows():
            value = row.values
            try:
                query = f'''INSERT INTO product_rdb ({final_columns}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
                cursor2.execute(cursor2.mogrify(query, value))
                connection2.commit()
            except Exception as e:
                cursor2.execute("ROLLBACK")
                connection2.commit()
                print(e)
        # Print the current date being processed
        print(x.date())
        # Call an update function to calculate cumulative scores in the target RDB
        query = '''select update_cumulative_scores();'''
        cursor2.execute(query)
        connection2.commit()
        # Drop the temporary product table in the source SDB
        query = '''drop table temp_product;'''
        cursor1.execute(query)
        connection1.commit()
    cursor1.close()
    cursor2.close()
    connection1.close()
    connection2.close()


product_exportcall_list=['date', 'user', 'full_name', 'call_date', 'status', 'campaign_id', 'query_typelevel_1', 'ans', 'tagging', 'ftr', 'call_transferred_to_survey_ivr', 'survey_ivr', 'emp_id', 'extracted_time', 'extracted_interval', 'count', 'abandon', 'outbound', 'phone_number', 'cust_talk_sec', 'customer_hold_time', 'wrap_up_time', 'queue_seconds', 'sl', 'ht','query_type_level_2','subquery_type_level_2','lead_id','customer_number','csat','survey_feedback','quality_score','repeat_same_day','repeat_7days','repeat_3days']


'''
The function count_function takes three parameters: table_name_ldb, table_name_sdb, and date. 
It executes two SQL queries to count the number of rows in the specified tables (table_name_ldb and table_name_sdb) for a given date. 
It uses the cursor1 and cursor2 objects to execute the queries on the respective database connections. 
The function then retrieves the count values from the query results and returns them as a tuple (count_data_sdb, count_data_rdb).
'''
def count_function(table_name_sdb, table_name_rdb, date):
    # Construct the SQL query for the landing database quality_rdb_functiontable
    query = f'''select count(*) from {table_name_sdb} where date='{date.date()}';'''
    # Execute the query on the landing database
    cursor1.execute(query)
    # Fetch the count value from the query result
    count_data_sdb = cursor1.fetchone()[0]
    # Construct the SQL query for the staging database table
    query = f'''select count(*) from {table_name_rdb} where date='{date.date()}';'''
    # Execute the query on the staging database
    cursor2.execute(query)
    # Fetch the count value from the query result
    count_data_rdb = cursor2.fetchone()[0]    
    # Return the count values for the landing and staging databases
    return count_data_sdb, count_data_rdb

'''
It iterates over each date in the specified date range using pd.date_range.
It calls a count function to determine the counts of data in the source SDB and target RDB for the current date.
Based on the counts, it determines whether there is a need to run the data transfer process or not.
If the data transfer is required, it deletes existing records from the target RDB for the current date.
It retrieves data from the source SDB for the current date and stores it in a DataFrame.
The 'date' column is formatted as a datetime object and converted to the '%Y-%m-%d' format.
NaN values in the DataFrame are replaced with None.
It iterates over the rows of the DataFrame and inserts the data into the target RDB.
Any exceptions that occur during the insertion process are caught and rolled back.
The current date being processed is printed.
Finally, the cursors and connections to the databases are closed.
'''
def exportcall_sdb_to_rdb():
    # Iterate over each date in the range specified by 'startdate' and 'enddate'
    for date in pd.date_range(startdate, enddate):
        # Call a count function to get the counts of data in the source SDB and target RDB for the current date
        count_data_sdb, count_data_rdb = count_function('exportcall_sdb', 'exportcall_rdb', date)   
        # Check if there is no data in the source SDB for the current date
        if count_data_sdb == 0:
            print('no need to run')
        # Check if the counts of data in the source SDB and target RDB are equal for the current date
        elif count_data_sdb == count_data_rdb:
            print('no need to run')
        else:
            print('time to run')
            # Delete existing records from the target RDB for the current date
            query = f'''delete from exportcall_rdb where date='{date.date()}';'''
            cursor2.execute(query)
            connection2.commit() 
            query = f'''select process_insights('{date.date()}');'''
            cursor1.execute(query)
            connection1.commit()         
            # Convert the list of product_exportcall columns into a string for SQL query
            final_columns = ','.join(['"{0}"'.format(item) for item in product_exportcall_list])           
            # Retrieve data from the source SDB for the current date
            query = f'''select {final_columns} from temp_process;'''
            cursor1.execute(query)
            column_names = [desc[0] for desc in cursor1.description]
            df = pd.DataFrame(cursor1.fetchall(), columns=column_names)           
            # Convert the 'date' column to a datetime object and format it as '%Y-%m-%d'
            df['date'] = pd.to_datetime(df['date'])
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            # Replace any NaN values in the DataFrame with None
            df = df.replace({np.nan: None})          
            # Iterate over the rows of the DataFrame and insert the data into the target RDB
            for i, row in df.iterrows():
                value = row.values
                try:
                    query = f'''INSERT INTO exportcall_rdb ({final_columns}) VALUES (%s, %s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s);'''
                    cursor2.execute(cursor2.mogrify(query, tuple(value)))
                    connection2.commit()
                except Exception as e:
                    print(e)
                    cursor2.execute("ROLLBACK")
                    connection2.commit()      
        # Print the current date being processed
            query = '''drop table temp_process;'''
            cursor1.execute(query)
            connection1.commit()
            print(date.date())  
    # Close the cursors and connections to the databases
    cursor1.close()
    cursor2.close()
    connection1.close()
    connection2.close()

'''
It iterates over each date in the specified date range using pd.date_range.
It deletes existing records from the target RDB for the current date.
It retrieves CSAT data for the current date from the source SDB and stores it in a temporary table called 'temp_csat'.
The data from the temporary table is fetched into a DataFrame.
Any NaN values in the DataFrame are replaced with None.
It iterates over the rows of the DataFrame and inserts the data into the target RDB.
Any exceptions that occur during the insertion process are caught, rolled back, and printed.
The current date being processed is printed.
The temporary table 'temp_csat' is dropped.
Finally, the cursors and connections to the databases are closed.
'''
def csat_sdb_to_rdb():
    # Iterate over each date in the range specified by 'startdate' and 'enddate'
    for x in pd.date_range(startdate, enddate):
        # Delete existing records from the target RDB for the current date
        query = f'''delete from csat_rdb where date='{x.date()}';'''
        cursor2.execute(query)       
        # Retrieve CSAT data for the current date from the source SDB
        query = f'''SELECT get_csat_table('{x.date()}');'''
        cursor1.execute(query)      
        # Retrieve the data from the temporary table 'temp_csat'
        query = '''SELECT * from temp_csat;'''
        cursor1.execute(query)       
        # Extract column names from the cursor's description and create a DataFrame
        column_names = [desc[0] for desc in cursor1.description]
        df = pd.DataFrame(cursor1.fetchall(), columns=column_names)        
        # Replace any NaN values in the DataFrame with None
        df = df.replace({np.nan: None})        
        # Iterate over the rows of the DataFrame and insert the data into the target RDB
        for i, row in df.iterrows():
            value = row.values
            try:
                query = '''INSERT INTO csat_rdb ("date","user","full_name","ftr","total_calls","survey_ivr","csat","dsat","neutral","response_count","great_service") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
                cursor2.execute(cursor2.mogrify(query, value))
                connection2.commit()
            except Exception as e:
                cursor2.execute("ROLLBACK")
                connection2.commit()
                print(e)      
        # Print the current date being processed
        print(x.date())      
        # Drop the temporary table 'temp_csat'
        query = '''drop table temp_csat;'''
        cursor1.execute(query)
        connection1.commit()
    # Close the cursors and connections to the databases
    cursor1.close()
    cursor2.close()
    connection1.close()
    connection2.close()


'''
It iterates over each date in the specified date range using pd.date_range.
It joins the column names for the product_csat_list with quotes.
It deletes existing records from the target RDB for the current date.
It retrieves product CSAT data for the current date from the source RDB and stores it in a temporary table called 'temp_product_csat'.
The data from the temporary table is fetched into a DataFrame.
The 'date' column in the DataFrame is converted to a datetime object and formatted as '%Y-%m-%d'.
Any NaN values in the DataFrame are replaced with None.
It iterates over the rows of the DataFrame and inserts the data into the target RDB.
Any exceptions that occur during the insertion process are caught, rolled back, and printed.
The current date being processed is printed.
The temporary table 'temp_product_csat' is dropped.
Finally, the cursors and connections to the databases are closed.
'''
def product_csat_rdb():
    # Iterate over each date in the range specified by 'startdate' and 'enddate'
    for x in pd.date_range(startdate, enddate):
        # Join the column names for the product_csat_list with quotes
        final_column = ','.join(['"{0}"'.format(item) for item in product_csat_list])
        
        # Delete existing records from the target RDB for the current date
        query = f'''delete from product_csat_rdb where date='{x.date()}';'''
        cursor2.execute(query)     
        # Retrieve product CSAT data for the current date from the source RDB and store it in a temporary table 'temp_product_csat'
        query = f'''SELECT get_product_csat_table('{x.date()}');'''
        cursor2.execute(query)
        connection2.commit()     
        # Retrieve the data from the temporary table 'temp_product_csat'
        query = f'''SELECT {final_column} from temp_product_csat;'''
        cursor2.execute(query)      
        # Extract column names from the cursor's description and create a DataFrame
        column_names = [desc[0] for desc in cursor2.description]
        df = pd.DataFrame(cursor2.fetchall(), columns=column_names)      
        # Convert the 'date' column to a datetime object and format it as '%Y-%m-%d'
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')      
        # Replace any NaN values in the DataFrame with None
        df = df.replace({np.nan: None})      
        # Iterate over the rows of the DataFrame and insert the data into the target RDB
        for i, row in df.iterrows():
            value = row.values
            try:
                query = f'''INSERT INTO product_csat_rdb ({final_column}) VALUES ( %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
                cursor2.execute(cursor2.mogrify(query, tuple(value)))
                connection2.commit()
            except Exception as e:
                cursor2.execute("ROLLBACK")
                connection2.commit()
                print(e)     
        # Print the current date being processed
        print(x.date())     
        # Drop the temporary table 'temp_product_csat'
        query = '''drop table temp_product_csat;'''
        cursor2.execute(query)
        connection2.commit()
        query = f'''UPDATE product_csat_rdb SET team_lead=t3.team_lead,designation=t3.designation,role_fit=t3.role_fit,tenure=t3.days_category,manager=t3.manager,sr_manager=t3.sr__manager,grid=t3.grid from (select t1.emp_id,t2.team_lead,t2.designation,t2.role_fit,t2.manager,t2.sr__manager,t2.grid,(CASE
            WHEN (CURRENT_DATE - t2.doj)::integer <= 30 THEN '0-30 days'
            WHEN (CURRENT_DATE - t2.doj)::integer <= 60 THEN '30-60 days'
            WHEN (CURRENT_DATE - t2.doj)::integer <= 90 THEN '60-90 days'
            WHEN (CURRENT_DATE - t2.doj)::integer <= 120 THEN '90-120 days'
            WHEN (CURRENT_DATE - t2.doj)::integer <= 180 THEN '120-180 days'
            ELSE 'Above 180 days'
            END) AS days_category
            from product_csat_rdb t1 left join fhpl_emp_details_rdb t2 on t1.emp_id=t2.employeeid 
            where t1.date='{x.date()}') as t3 where product_csat_rdb.date='{x.date()}' and product_csat_rdb.emp_id=t3.emp_id;'''
        cursor2.execute(query)
        connection2.commit()
    # Close the cursors and connections to the databases
    cursor1.close()
    cursor2.close()
    connection1.close()
    connection2.close()

'''
Truncates the target RDB table 'fhpl_emp_details_rdb'.
Retrieves employee details from the source SDB table 'emp_details_sdb' and stores them in a DataFrame.
Replaces any NaN values in the DataFrame with None.
Iterates over the rows of the DataFrame and inserts the data into the target RDB 'fhpl_emp_details_rdb'.
Updates the 'team_lead', 'designation', 'role_fit', 'tenure', 'manager', 'sr_manager', and 'grid' columns in the target RDB 'product_csat_rdb' based on employee details.
Closes the cursors and connections to the databases.
'''
def csat_tl_fun():
    # Iterate over each date in the range specified by 'startdate' and 'enddate'
    for x in pd.date_range(startdate, enddate):
        # Truncate the 'fhpl_emp_details_rdb' table in the target RDB
        query = f'''truncate table fhpl_emp_details_rdb;'''
        cursor2.execute(query)      
        # Retrieve employee details from the source SDB
        query = '''SELECT "employeeid","employeename","team_lead","designation","role_fit","doj","manager","sr__manager","grid" from emp_details_sdb;'''
        cursor1.execute(query)       
        # Extract column names from the cursor's description and create a DataFrame
        column_names = [desc[0] for desc in cursor1.description]
        df = pd.DataFrame(cursor1.fetchall(), columns=column_names)       
        # Replace any NaN values in the DataFrame with None
        df = df.replace({np.nan: None})       
        # Iterate over the rows of the DataFrame and insert the data into the 'fhpl_emp_details_rdb' table in the target RDB
        for i, row in df.iterrows():
            value = row.values
            try:
                query = '''INSERT INTO fhpl_emp_details_rdb ("employeeid","employeename","team_lead","designation","role_fit","doj","manager","sr__manager","grid") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
                cursor2.execute(cursor2.mogrify(query, value))
                connection2.commit()
            except Exception as e:
                cursor2.execute("ROLLBACK")
                connection2.commit()
                print(e)      
    # Close the cursors and connections to the databases
    cursor1.close()
    cursor2.close()
    connection1.close()
    connection2.close()

'''
Deletes data from the 'shrinkage_sdb' and 'shrinkage_rdb' tables for each date in the specified range.
Retrieves shrinkage data for each date and stores it in 'shrinkage_sdb'.
Transfers the shrinkage data from 'shrinkage_sdb' to 'shrinkage_rdb'.
Deletes data from the 'interval_wise_rdb' table for each date.
Generates interval-wise data for each date and stores it in 'interval_wise_table'.
Transfers the interval-wise data from 'interval_wise_table' to 'interval_wise_rdb'.
Closes the cursors and connections.
'''
def shrinkage_function():
    # Iterate over each date in the range specified by 'startdate' and 'enddate'
    for x in pd.date_range(startdate, enddate):
        # Delete data from the 'shrinkage_sdb' table for the current date
        query = f'''delete from shrinkage_sdb where date='{x.date()}';'''
        cursor1.execute(query)
        connection1.commit()      
        # Delete data from the 'shrinkage_rdb' table for the current date
        query = f'''delete from shrinkage_rdb where date='{x.date()}';'''
        cursor2.execute(query)
        connection2.commit()      
        # Call a function to retrieve shrinkage data for the current date and store it in 'shrinkage_sdb'
        query = f'''SELECT public.get_shri_function('{x.date() - timedelta(days=1)}','{x.date()}', '{(x.date() - timedelta(days=1)).strftime('%a').lower()}', '{x.date().strftime('%a').lower()}');'''
        cursor1.execute(query)
        connection1.commit()       
        # Retrieve shrinkage data for the current date from 'shrinkage_sdb'
        query = f'''SELECT * from shrinkage_sdb where date='{x.date()}';'''
        cursor1.execute(query)     
        # Extract column names from the cursor's description and create a DataFrame
        column_names = [desc[0] for desc in cursor1.description]
        df = pd.DataFrame(cursor1.fetchall(), columns=column_names)        
        # Replace any NaN values in the DataFrame with None
        df = df.replace({np.nan: None})       
        # Iterate over the rows of the DataFrame and insert the data into the target RDB 'shrinkage_rdb'
        for i, row in df.iterrows():
            value = row.values
            try:
                query = '''INSERT INTO shrinkage_rdb VALUES (%s,%s,%s);'''
                cursor2.execute(cursor2.mogrify(query, value))
                connection2.commit()
            except Exception as e:
                cursor2.execute("ROLLBACK")
                connection2.commit()
                print(e)    
        print(x.date())     
        # Delete data from the 'interval_wise_rdb' table for the current date
        query = f'''delete from interval_wise_rdb where date ='{x.date()}';'''
        cursor2.execute(query)
        connection2.commit()      
        # Generate interval-wise data for the current date and store it in 'interval_wise_table'
        query = f'''SELECT interval_wise_table('{x.date()}')'''
        cursor1.execute(query)       
        # Retrieve interval-wise data from 'interval_wise_table'
        query = f'''SELECT date, "interval", roster_emp_count, extracted_interval, count, ans, sl, sl_per, abn, aht, asa, forecast,repeat_same_day,repeat_3days,repeat_7days from interval_wise_table;'''
        cursor1.execute(query)      
        # Extract column names from the cursor's description and create a DataFrame
        column_names = [desc[0] for desc in cursor1.description]
        df = pd.DataFrame(cursor1.fetchall(), columns=column_names)    
        # Replace any NaN values in the DataFrame with None
        df = df.replace({np.nan: None})      
        # Iterate over the rows of the DataFrame and insert the data into the target RDB 'interval_wise_rdb'
        for i, row in df.iterrows():
            value = row.values
            try:
                query = '''INSERT INTO interval_wise_rdb (date, "interval", roster_emp_count, extracted_interval, count, ans, sl, sl_per, abn, aht, asa, forecast,repeat_same_day,repeat_3days,repeat_7days) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s);'''
                cursor2.execute(cursor2.mogrify(query, value))
                connection2.commit()
            except Exception as e:
                cursor2.execute("ROLLBACK")
                connection2.commit()
                print(e)     
        print(x.date())  
    # Close cursors and connections
    cursor1.close()
    connection1.close()
    cursor2.close()
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
   'retry_delay':timedelta(minutes=10)
} 

with DAG(default_args=default_args,
         dag_id='sdb_to_rdb',
         schedule_interval="30 7 * * * ",
         start_date=pendulum.datetime(year=2023, month=3, day=16, tz='Asia/Kolkata'),
         end_date=None,
         catchup=False
         ) as dag:
    # Define a PythonOperator task for transferring data from 'product_sdb' to 'daily_agent_rdb'
    sdb_to_daily_product_rdb_ = PythonOperator(
        task_id="sdb_to_daily_product_rdb",
        python_callable=product_sdb_to_rdb,
        dag=dag,
    )
    
    # Define a PythonOperator task for transferring data from 'csat_sdb' to 'daily_csat_rdb'
    sdb_to_daily_csat_rdb_ = PythonOperator(
        task_id="sdb_to_daily_csat_rdb",
        python_callable=csat_sdb_to_rdb,
        dag=dag,
    )
    
    # Define a PythonOperator task for transferring data from 'product_csat_sdb' to 'product_csat_rdb'
    sdb_to_daily_product_csat_rdb_ = PythonOperator(
        task_id="sdb_to_daily_product_csat_rdb",
        python_callable=product_csat_rdb,
        dag=dag,
    )
    
    # Define a PythonOperator task for performing CSAT team lead function and updating 'product_csat_rdb'
    sdb_to_daily_csat_tl_fun_ = PythonOperator(
        task_id="sdb_to_daily_csat_tl_fun",
        python_callable=csat_tl_fun,
        dag=dag,
    )
    
    # Define a PythonOperator task for transferring data from 'exportcall_sdb' to 'product_exportcall_rdb'
    sdb_to_daily_product_exportcall_rdb_ = PythonOperator(
        task_id="sdb_to_daily_product_exportcall_rdb",
        python_callable=exportcall_sdb_to_rdb,
        dag=dag,
    )
    
    # Define a PythonOperator task for handling shrinkage data and transferring it to 'shrinkage_rdb'
    sdb_to_daily_shrinkage_ = PythonOperator(
        task_id="sdb_to_daily_shrinkage",
        python_callable=shrinkage_function,
        dag=dag,
    )

# Define the dependency chain by specifying the order of tasks
sdb_to_daily_product_rdb_ >> sdb_to_daily_csat_rdb_ >> sdb_to_daily_product_csat_rdb_ >> sdb_to_daily_csat_tl_fun_ >> sdb_to_daily_product_exportcall_rdb_ >> sdb_to_daily_shrinkage_
