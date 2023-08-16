# Import necessary libraries
import psycopg2
from datetime import timedelta,date,datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from psycopg2.extras import execute_values
import numpy as np

# Set the current month
current_month = datetime.now().month

# Establish connection to the landing database
connection1=psycopg2.connect(host='3.108.122.116',database='landing_db', user='myusername', password='mypassword',port='5432')
cursor1= connection1.cursor()

# Establish connection to the staging database
connection2=psycopg2.connect(host='3.108.122.116',database='staging_db', user='myusername', password='mypassword',port='5432')
cursor2 = connection2.cursor()

# Define a list of column names
column_list=['date','agent_name','agent_id','call_date','overall_score','scorable','scored','fatal','tenure','month','week','qa_name','tl_name','fatal_reason','greetings1_binary','greeting2_binary','authentication1_binary','authentication2_binary','issue_identification1_binary','issue_identification2_binary','probing1_binary','probing2_binary','call_etiquette1_binary','call_etiquette2_binary','call_etiquette3_binary','call_etiquette4_binary','call_etiquette5_binary','call_etiquette6_binary','empathy_and_sympathy1_binary','empathy_and_sympathy1_binary1','ownership1_binary','resolution1_binary','resolution2_binary','resolution3_binary','resolution4_binary','resolution5_binary','resolution6_binary','resolution7_binary','tagging_and_documentation_binary','summarization1_binary','summarization2_binary','summarization3_binary','closing__binary','zero_tolerance1_binary','zero_tolerance2_binary','zero_tolerance3_binary','zero_tolerance4_binary','zero_tolerance5_binary','compliance1_binary','compliance2_binary','compliance3_binary','compliance4_binary','customer_number']

'''
The function retrieves the count of records in both databases and compares them to determine if the transfer is necessary. 
If the counts match, it indicates that the data is already up to date, and the function terminates. 
Otherwise, it identifies the dates for which data needs to be transferred based on the difference in counts.
For each date, the function deletes existing records for that date in the staging database and selects the corresponding data from the landing database. 
The selected data undergoes transformations and cleaning operations, such as converting date formats, modifying the agent name, and stripping whitespace from agent IDs.
Finally, the transformed data is batch-inserted into the staging database using the execute_values() function, which efficiently handles multiple insertions. 
Any errors that occur during the insertion process are caught and printed.
'''
# Define a function to transfer data from the landing database to the staging database for quality table
# Define a function to transfer data from the landing database to the staging database for quality table
def quality_ldb_to_sdb():
    # Get the count of records in the landing database quality table
    query = '''select count(*) from quality;'''
    cursor1.execute(query)
    count_ldb_data = cursor1.fetchone()[0]
    print(count_ldb_data) 
    # Get the count of records in the staging database quality_sdb table
    query = '''select count(*) from quality_sdb;'''
    cursor2.execute(query)
    count_sdb_data = cursor2.fetchone()[0]
    print(count_sdb_data)  
    # Check if the counts match, indicating that the data is already up to date
    if count_sdb_data == count_ldb_data:
        print('no need to run')
    else:
        print('time to run')
        # Retrieve the dates for which data needs to be transferred
        query = f'''select date,count(*) from quality group by date order by date;'''
        cursor1.execute(query)
        column_names = [desc[0] for desc in cursor1.description]
        df1 = pd.DataFrame(cursor1.fetchall(), columns=column_names)
        query = f'''select date,count(*) from quality_sdb group by date order by date;'''
        cursor2.execute(query)
        column_names = [desc[0] for desc in cursor2.description]
        df2 = pd.DataFrame(cursor2.fetchall(), columns=column_names)
        # Merge the two DataFrames on the `date` column
        df_merged = df1.merge(df2, how='left', on='date')

        # Select the rows where the count is not matching
        df_diff = df_merged[df_merged['count_x'] != df_merged['count_y']]

        # Get the dates from the DataFrame
        dates = df_diff['date'].tolist()
        startdate = (dates[0])
        enddate = (dates[-1])    
        print(startdate,enddate)
        # Iterate over the date range and transfer data for each date
        for x in pd.date_range(startdate, enddate):
            # Delete existing records for the date in the staging database
            query = f'''delete from quality_sdb where date='{x.date()}';'''
            cursor2.execute(query)
            connection2.commit()         
            # Select data from the landing database for the date
            final_columns = ','.join(['"{0}"'.format(item) for item in column_list])
            query = f'''select {final_columns} from quality where date ='{x.date()}';'''
            cursor1.execute(query)
            column_names = [desc[0] for desc in cursor1.description]
            df = pd.DataFrame(cursor1.fetchall(), columns=column_names)          
            # Perform data transformations/cleaning on the DataFrame
            df['date'] = pd.to_datetime(df['date'])
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            df['agent_name'] = df['agent_name'].apply(str.upper)
            df['agent_name'] = df['agent_name'].str.replace(r'\\XA0', '', regex=True)
            df['agent_id'] = df['agent_id'].str.strip()           
            # Convert the DataFrame rows to tuples for batch insertion
            values = [tuple(x) for x in df.to_numpy()]            
            # Insert the data into the staging database using batch insertion
            try:
                query = f'''INSERT INTO quality_sdb ({final_columns}) VALUES %s;'''
                execute_values(cursor2, query, values)
                connection2.commit()
            except Exception as error:
                print(error)
            print(x.date())
    cursor1.close()
    cursor2.close()
    connection1.close()
    connection2.close()
# Define a list of column names
column_list_1=['date','Emp_Code_','Employee_Name_','Assessment_Scores'] 

'''
The function retrieves the count of records in both databases and compares them to determine if the transfer is necessary. 
If the counts match, it indicates that the data is already up to date, and the function terminates.Otherwise, it proceeds with the data transfer.
The function begins by truncating the temporary staging table (temp_datawisepkt_sdb) to clear any existing data. 
It then selects the remaining data from the landing database based on the difference in counts.
Data transformations are performed on the DataFrame, such as converting date formats and modifying the Employee Name. 
The DataFrame rows are converted to tuples for batch insertion.
The data is inserted into both the main staging table (datawisepkt_sdb) and the temporary staging table using batch insertion. 
Any errors that occur during the insertion process are caught and printed.
''' 
# Define a function to transfer data from the landing database to the staging database for datawisepkt table
def datawisepkt_ldb_to_sdb():
    # Get the count of records in the landing database datawisepkt table
    query = '''select count(*) from datawisepkt;'''
    cursor1.execute(query)
    count_ldb_data = cursor1.fetchone()[0]
    print(count_ldb_data)  
    # Get the count of records in the staging database datawisepkt_sdb table
    query = '''select count(*) from datawisepkt_sdb;'''
    cursor2.execute(query)
    count_sdb_data = cursor2.fetchone()[0]
    print(count_sdb_data)   
    # Check if the counts match, indicating that the data is already up to date
    if count_ldb_data == count_sdb_data:
        print('no need to run')
    else:
        print('time to run')
        query = f'''select date,count(*) from datawisepkt group by date order by date;'''
        cursor1.execute(query)
        column_names = [desc[0] for desc in cursor1.description]
        df1 = pd.DataFrame(cursor1.fetchall(), columns=column_names)
        query = f'''select date,count(*) from datawisepkt_sdb group by date order by date;'''
        cursor2.execute(query)
        column_names = [desc[0] for desc in cursor2.description]
        df2 = pd.DataFrame(cursor2.fetchall(), columns=column_names)
        # Merge the two DataFrames on the `date` column
        df_merged = df1.merge(df2, how='left', on='date')

        # Select the rows where the count is not matching
        df_diff = df_merged[df_merged['count_x'] != df_merged['count_y']]

        # Get the dates from the DataFrame
        dates = df_diff['date'].tolist()
        for date in  dates:
           
            # Truncate the temporary staging table to clear existing data
            query = f'''truncate temp_datawisepkt_sdb;'''
            cursor2.execute(query)
            query = f'''delete from datawisepkt_sdb where date='{date}';'''
            cursor2.execute(query)
            connection2.commit()      
            # Select the remaining data from the landing database based on the difference in counts
            final_columns = ','.join(['"{0}"'.format(item) for item in column_list_1])
            query = f'''select {final_columns} from datawisepkt where date='{date}';'''
            cursor1.execute(query)
            column_names = [desc[0] for desc in cursor1.description]
            df = pd.DataFrame(cursor1.fetchall(), columns=column_names)       
            # Perform data transformations on the DataFrame
            df['date'] = pd.to_datetime(df['date'])
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            df['Employee_Name_'] = df['Employee_Name_'].apply(str.upper)      
            # Convert the DataFrame rows to tuples for batch insertion
            values = [tuple(x) for x in df.to_numpy()]      
            try:
                # Insert the data into the staging database using batch insertion
                query = f'''INSERT INTO datawisepkt_sdb ({final_columns}) VALUES %s;'''
                execute_values(cursor2, query, values)          
                # Insert the data into the temporary staging table as well
                query = f'''INSERT INTO temp_datawisepkt_sdb ({final_columns}) VALUES %s;'''
                execute_values(cursor2, query, values)         
                connection2.commit()
            except Exception as error:
                print(error)
            print(date)
    cursor1.close()
    cursor2.close()
    connection1.close()
    connection2.close()
    
tni_column_list=['date','emp_id','name','status_','training_date_','training_start_time','training_end_time','same_day_assessment_score'] 

'''
The function retrieves the count of records in both databases for the current month and compares them to determine if the transfer is necessary. 
If the counts match, it indicates that the data is already up to date, and the function terminates. Otherwise, it proceeds with the data transfer.
The function begins by deleting the existing data for the current month from the staging database. 
It then selects the data for the current month from the landing database.
Data transformations are performed on the DataFrame, such as converting date formats and modifying the name. 
NaN values in the DataFrame are replaced with None.
The function iterates over the DataFrame rows and inserts the data row by row into the staging database. 
Any exceptions during the insertion process are caught and printed.
''' 

# Define a function to transfer data from the landing database to the staging database for tni_efficacy table
def tni_ldb_to_sdb():
    # Get the count of records in the landing database tni_efficacy table for the current month
    query = f'''select count(*) from tni_efficacy WHERE EXTRACT(MONTH FROM date) = {current_month};'''
    cursor1.execute(query)
    count_ldb_data = cursor1.fetchone()[0]
    print(count_ldb_data)
    # Get the count of records in the staging database tni_efficacy_sdb table for the current month
    query = f'''select count(*) from tni_efficacy_sdb WHERE EXTRACT(MONTH FROM date) = {current_month};'''
    cursor2.execute(query)
    count_sdb_data = cursor2.fetchone()[0]
    print(count_sdb_data)  
    # Check if the counts match, indicating that the data is already up to date
    if count_ldb_data == count_sdb_data:
        print('no need to run')
    else:
        print('time to run')      
        # Delete the existing data for the current month from the staging database
        query = f'''delete from tni_efficacy_sdb WHERE EXTRACT(MONTH FROM date) = {current_month};'''
        cursor2.execute(query)
        connection2.commit()     
        # Select the data for the current month from the landing database
        final_columns = ','.join(['"{0}"'.format(item) for item in tni_column_list])
        query = f'''SELECT {final_columns} FROM tni_efficacy WHERE EXTRACT(MONTH FROM date) = {current_month};'''
        cursor1.execute(query)
        column_names = [desc[0] for desc in cursor1.description]
        df = pd.DataFrame(cursor1.fetchall(), columns=column_names)   
        # Perform data transformations on the DataFrame
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        df['name'] = df['name'].apply(str.upper)
        df = df.replace({np.nan: None})    
        # Iterate over the DataFrame rows and insert data into the staging database row by row
        for i, row in df.iterrows():
            value = row.values
            try:
                query = '''INSERT INTO tni_efficacy_sdb  VALUES (%s,%s,%s,%s,%s,%s,%s,%s );'''
                cursor2.execute(cursor2.mogrify(query, value))
                connection2.commit()
            except Exception as error:
                print(error)
    cursor1.close()
    cursor2.close()
    connection1.close()
    connection2.close()

'''
The function queries the landing database to obtain the count of records for the current month (count_ldb_data) and prints it.
It then queries the staging database to obtain the count of records for the current month (count_sdb_data) and prints it.
It compares the counts of records between the landing and staging databases. If they match, it prints "no need to run." Otherwise, it proceeds to transfer the data.
It deletes existing records from the staging database for the current month.
It retrieves data from the landing database for the current month.
It formats the date_of_refresher column as a datetime object and converts it to the '%Y-%m-%d' format.
It converts the agent_name column to uppercase.
It replaces any NaN values in the DataFrame with None.
It iterates over the rows of the DataFrame and inserts the data into the staging database.
'''
 
def bq_ldb_to_sdb():
    # Query the landing database to get the count of records for the current month
    query = f'''select count(*) from bq_refresher WHERE EXTRACT(MONTH FROM date) = {current_month};;'''
    cursor1.execute(query)
    count_ldb_data = cursor1.fetchone()[0]
    print(count_ldb_data)
    # Query the staging database to get the count of records for the current month
    query = f'''select count(*) from bq_refresher_sdb WHERE EXTRACT(MONTH FROM date) = {current_month};;'''
    cursor2.execute(query)
    count_sdb_data = cursor2.fetchone()[0]
    print(count_sdb_data)
    # Check if the count of records in the landing database matches the count in the staging database
    if count_ldb_data == count_sdb_data:
        print('no need to run')
    else:
        print('time to run')
        # Delete existing records from the staging database for the current month
        query = f'''delete from bq_refresher_sdb WHERE EXTRACT(MONTH FROM date) = {current_month};'''
        cursor2.execute(query)
        connection2.commit()
        # Retrieve data from the landing database for the current month
        query = f'''select date_of_refresher, emp_id, agent_name, assessment_scores from bq_refresher 
                    WHERE EXTRACT(MONTH FROM date) = {current_month};'''
        cursor1.execute(query)
        column_names = [desc[0] for desc in cursor1.description]
        df = pd.DataFrame(cursor1.fetchall(), columns=column_names)
        # Convert date_of_refresher column to datetime format and format as '%Y-%m-%d'
        df['date_of_refresher'] = pd.to_datetime(df['date_of_refresher'])
        df['date_of_refresher'] = df['date_of_refresher'].dt.strftime('%Y-%m-%d')
        # Convert agent_name column to uppercase
        df['agent_name'] = df['agent_name'].apply(str.upper)
        # Replace NaN values with None
        df = df.replace({np.nan: None})
        # Iterate over rows of the DataFrame and insert data into the staging database
        for i, row in df.iterrows():
            value = row.values
            try:
                query = '''INSERT INTO bq_refresher_sdb  VALUES (%s, %s, %s, %s);'''
                cursor2.execute(cursor2.mogrify(query, value))
                connection2.commit()
            except Exception as error:
                print(error)
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
   'retry_delay':timedelta(minutes=10)
} 

# Define a DAG (Directed Acyclic Graph) for the data transfer process
with DAG(default_args=default_args,
         dag_id='quality_ldb_to_sdb',
         schedule_interval="0 7 * * *",
         start_date=pendulum.datetime(year=2023, month=3, day=24, tz='Asia/Kolkata'),
         end_date=None,
         catchup=False
         ) as dag:
    
    # Define a PythonOperator for transferring data from quality_ldb to quality_sdb
    quality_ldb_to_quality_sdb_ = PythonOperator(
        task_id="quality_ldb_to_quality_sdb",
        python_callable=quality_ldb_to_sdb
    )

    # Define a PythonOperator for transferring data from datawisepkt_ldb to datawisepkt_sdb
    datawisepkt_ldb_to_datawisepkt_sdb_ = PythonOperator(
        task_id="datawisepkt_ldb_to_datawisepkt_sdb",
        python_callable=datawisepkt_ldb_to_sdb
    )

    # Define a PythonOperator for transferring data from tni_ldb to tni_sdb
    tni_ldb_to_sdb_ = PythonOperator(
        task_id="tni_ldb_to_sdb",
        python_callable=tni_ldb_to_sdb
    )
    
    # Define a PythonOperator for transferring data from bq_ldb to bq_sdb
    bq_ldb_to_sdb_ = PythonOperator(
        task_id="bq_ldb_to_sdb",
        python_callable=bq_ldb_to_sdb
    )

    # Set the data transfer dependencies between the tasks
    quality_ldb_to_quality_sdb_ >> datawisepkt_ldb_to_datawisepkt_sdb_ >> tni_ldb_to_sdb_ >> bq_ldb_to_sdb_
