# requriments 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime,timedelta,date
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
import psycopg2
import requests
import pandas as pd
import json
import os
import pendulum
import operator as op
import re


# Establish connection to the 'landing_db' database
connection = psycopg2.connect(
    host='3.108.122.116',
    database='landing_db',
    user='myusername',
    password='mypassword',
    port='5432'
)
cursor = connection.cursor()

# assigning the date for the collecting the date
startdate=date.today() - timedelta(days=1)
enddate=startdate

# API's links what client given to us
# export_url it containes each call deatils daily wise
# agent_url it containes agent daily information 
# csat_url it containes coustmer rating deatils daily wise
export_url = 'http://14.140.2.27:9086/rightside/webservices/custom_exportcall_report'
agent_url='http://14.140.2.27:9086/rightside/webservices/custom_apr_report'
csat_url='http://14.140.2.27:9086/rightside/webservices/agent_feedback_ivr_report'


# freezed column list repectivey API's
freezed_column_list=['project_id','project_name','lead_entry_date','source','customer_name','call_date','status','user','full_name','campaign_id','list_id','phone_number','cust_talk_sec','lead_id','call_type','list_name','main_disposition','customer_hold_time','wrap_up_time','queue_seconds','customer_number','uic_id','ingroup_name','registered_number_of_the_customer','caller_name','uhid','corporate_name','claim_type','claim_no','claim_status','caller_type','query_typelevel_1','query_type_level_2','subquery_type_level_2','additional_comments','callbacks_required','call_transferred_to_survey_ivr','ingroup_list','resolution','mobilenumber','states','emailid','reasonfornotselectedifno','rightparty','campaignname','currentlocation','phonenumber','altnumber','selected','customername','folionumber','pannumber','casetype','typesubtype','paymentstatus','paymentstatuskfin','documentstatus','assignedteam','remarks',]
agent_column_list=['agent_id', 'process_name', 'call_attended', 'login_duration', 'total_talk_time', 'total_break', 'total_wrap_time', 'total_hold_time', 'breaks', 'emp_name', 'undefined', 'bio', 'lagged', 'login', 'lunch', 'manual', 'meet', 'qa', 'tea', 'traux', 'tlbrf', 'precal','total_idle_time']
csat_freezed_column_list=['lead_id' ,'call_date'  ,'phone_number' ,'user_id' ,'inbound_group' ,'ivr_flow','user_full_name']


tables = {'agent_info_ldb': 'agent','daily_exportcall_ldb': 'export','csat_ldb':'csat'}

json_inserted_rows_data = {}

def fetch_data_function(query):
    # Execute the given query using the 'cursor' object
    cursor.execute(query)
    # Fetch the first row from the query result and retrieve the first column value
    data = cursor.fetchone()[0]
    # Return the fetched data
    return data


def fetch_data_function_all(query):
    # Execute the given query using the 'cursor' object
    cursor.execute(query)
    # Fetch all rows from the query result
    data = cursor.fetchall()
    # Return the fetched data
    return data


def execute_function(query):
    # Execute the given query using the 'cursor' object
    cursor.execute(query)
    # Commit the changes to the database
    connection.commit()


def extracting_api_data(url, date):
    # Convert the 'date' parameter to a string in the format 'YYYY-MM-DD'
    required_date = str(date.date())
    # Create a payload JSON object with the 'from_date' and 'to_date' set to the 'required_date'
    payload = json.dumps({"from_date": required_date, "to_date": required_date})
    # Send a POST request to the 'url' with the payload
    response = requests.post(url, payload)
    # Extract the API response data from the JSON response
    API_response = response.json()['data']
    # Extract the API information (message) from the JSON response
    API_information = response.json()['message']
    # Extract the number of rows from the API information using list comprehension and integer conversion
    API_rows_Number = [int(i) for i in API_information.split() if i.isdigit()]
    # Return the API response, the list of API rows numbers, and the original 'date' parameter
    return API_response, API_rows_Number, date


def status_table_inserting(tablename, API_rows_Number):
    # Create a query to count the number of rows in the 'tablename' table for a specific 'startdate'
    table_row_count_query = f"select count(*) from {tablename} where date='{startdate}';"
    # Fetch the result of the row count query using the 'fetch_data_function'
    table_row_count = fetch_data_function(table_row_count_query)
    # Create an insert query for the 'status_ldb' table to insert the status information
    status_table_insert_query = f'''insert into status_ldb (table_name, json_rows, inserted_rows, db_name) 
                                    values('{tablename}', {API_rows_Number[0]}, {table_row_count}, 'landing_db');'''
    # Execute the status table insert query using the 'execute_function'
    execute_function(status_table_insert_query)


def time_error_function(value):
    # Create an empty list to store the converted time values
    time = []
    # Iterate over each value in the 'value' list
    for each_value in value:
        try:
            # Split the value by ':' and convert the resulting parts to integers
            hours, minutes, seconds = map(int, each_value.split(':'))
            # Calculate the total number of seconds from hours, minutes, and seconds
            total_seconds = (hours * 3600) + (minutes * 60) + seconds
            # Calculate the number of hours, minutes, and seconds from the total seconds
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            # Format the hours, minutes, and seconds as 'HH:MM:SS' and append to the 'time' list
            time.append('{:02d}:{:02d}:{:02d}'.format(hours, minutes, seconds))
        except (AttributeError, ValueError):
            # If an error occurs during the conversion, append the original value to the 'time' list
            time.append(each_value)
    # Return the list of converted time values
    return time


def error_function(value):
    # The following line replaces all occurrences of single quotes ('), 
    # with backticks (`) in each element of the 'value' list.
    a = [s.replace("'", "`") for s in value]
    # The next line replaces all occurrences of double quotes ("),
    # with single quotes (') in each element of the 'a' list.
    a1 = [s.replace('"', "'") for s in a]
    # The following line converts each element in the 'a1' list to a string.
    b1 = [str(r) for r in a1]
    # The next line converts the 'b1' list into a tuple.
    value1 = tuple(b1)
    # Finally, the 'value1' tuple is returned as the result of the function.
    return value1


def error_function_1(value):
    # Replace single quotes, square brackets, double quotes, and commas
    value1 = tuple(s.replace("'","").replace('"',"'") for s in value)
    return value1


'''The agent_api_to_ldb function processes API data for agents and inserts it into the agent_info_ldb table. 
It iterates over each agent in the API response, modifies the agent data if it has breaks, 
constructs the necessary SQL queries for insertion, executes the queries, and updates the status table.
'''
def agent_api_to_ldb(table_name, date):
    # Call the 'extracting_api_data' function to retrieve API response, API rows number, and modified 'date' value
    API_response, API_rows_Number, date = extracting_api_data(agent_url, date)
    # Initialize a day counter variable
    day_counter = 0
    # Iterate over each agent in the API response
    for each_agent in API_response:
        day_counter = day_counter + 1
        # Check if the agent has breaks
        if each_agent["breaks"]:
            # Create a dictionary with lowercase keys for agent breaks information
            agent_info_dict = {k.lower(): v for k, v in each_agent["breaks"].items()}
            # Remove the 'breaks' key from the agent dictionary and update it with the modified breaks information
            each_agent.update({"breaks": '0'})
            each_agent.update(agent_info_dict)
            # Get the columns that exist in both 'each_agent' and 'agent_column_list'
            columns = [i for i in each_agent.keys() if op.countOf(agent_column_list, i) > 0]
            # Create a string of column names to be used in the INSERT query
            final_column = ','.join(['"{0}"'.format(item) for item in columns])
            # Create a tuple of values for the INSERT query
            value = tuple([str(r) for r in (list(map(each_agent.get, columns)))])
        try:
            # Insert the agent data into the 'agent_info_ldb' table
            agent_data_insert_query = "insert into agent_info_ldb (%s) values %s;" % (final_column, value)
            execute_function(agent_data_insert_query)
            # Update the 'date' column in 'agent_info_ldb' where it is null
            date_update_query = "update agent_info_ldb set date = '%s' where date is null;" % (date.date())
            execute_function(date_update_query)
        except Exception as e:
            # If an exception occurs during the insert query, rollback the transaction
            execute_function("ROLLBACK")
            # Convert the 'value' to a valid time format using 'time_error_function'
            time = time_error_function(value)
            # Insert the modified agent data with time values into 'agent_info_ldb' table
            agent_data_insert_query = "insert into agent_info_ldb (%s) values %s;" % (final_column, tuple(time))
            execute_function(agent_data_insert_query)
            # Update the 'date' column in 'agent_info_ldb' where it is null
            date_update_query = "update agent_info_ldb set date = '%s' where date is null;" % (date.date())
            execute_function(date_update_query)
    # Print the 'date' and 'day_counter' values
    print(date, day_counter)
    # Call the 'status_table_inserting' function to insert status information into 'status_ldb' table
    status_table_inserting(table_name, API_rows_Number)


'''The export_api_to_ldb function processes API data for export calls and inserts it into the daily_exportcall_ldb table.
It iterates over each row in the API response, 
constructs the necessary SQL queries for insertion, executes the queries, and updates the status table.
'''
def export_api_to_ldb(table_name, date):
    # Retrieve API response, API rows number, and modified date value
    API_response, API_rows_Number, date = extracting_api_data(export_url, date)
    # Initialize a counter for the number of rows processed
    rows_counter = 0
    # Iterate over each row in the API response
    for index, each_row in enumerate(API_response):
        # Increment the rows counter
        rows_counter += 1
        # Create a dictionary with lowercase keys for each row
        export_call_dict = {k.lower(): v for k, v in each_row.items()}
        # Get the common columns between export_call_dict and freezed_column_list
        columns = [i for i in export_call_dict.keys() if op.countOf(freezed_column_list, i) > 0]
        # Construct the column names string for the INSERT query
        final_columns = ','.join(['"{0}"'.format(item) for item in columns])
        # Create a tuple of values for the INSERT query
        value = tuple([str(r) for r in (list(map(export_call_dict.get, columns))) ])
        try:
            # Insert the row data into the daily_exportcall_ldb table
            insert_query = '''insert into daily_exportcall_ldb (%s) values %s;''' % (final_columns, value)
            execute_function(insert_query)
            # Update the date column in daily_exportcall_ldb where it is null
            date_update_query = "update daily_exportcall_ldb set date = '%s' where date is null;" % (date.date())
            execute_function(date_update_query)
        except Exception as e:
            # If an exception occurs during the insert query, rollback the transaction
            execute_function("ROLLBACK")
            # Convert the value to a modified format using the error_function
            value1 = error_function(value)
            # Insert the modified row data into the daily_exportcall_ldb table
            insert_query = '''insert into daily_exportcall_ldb (%s) values %s;''' % (final_columns, value1)
            execute_function(insert_query)
            # Update the date column in daily_exportcall_ldb where it is null
            date_update_query = "update daily_exportcall_ldb set date = '%s' where date is null;" % (date.date())
            execute_function(date_update_query)
    # Print the date and rows_counter values
    print(date, rows_counter)
    # Call the status_table_inserting function to insert status information into the status_ldb table
    status_table_inserting(table_name, API_rows_Number)

'''The csat_api_to_ldb function processes API data for CSAT (Customer Satisfaction) and inserts it into the csat_ldb table. 
It retrieves the API response, iterates over each row, maps the row data to lowercase keys, selects relevant columns, 
constructs INSERT queries to insert the data into the table, handles exceptions, updates the date column, 
and finally inserts status information into the status_ldb table.
'''
def csat_api_to_ldb(table_name, date):
    # Retrieve API response, API rows number, and modified date value
    API_response, API_rows_Number, date = extracting_api_data(csat_url, date)
    # Initialize a counter for the number of rows processed
    rows_counter = 0
    # Iterate over each row in the API response
    for index, each_row in enumerate(API_response):
        # Increment the rows counter
        rows_counter += 1
        # Create a dictionary with lowercase keys for each row
        csat_dict = {k.lower(): v for k, v in each_row.items()}
        # Get the common columns between csat_dict and csat_freezed_column_list
        columns = [i for i in csat_dict.keys() if op.countOf(csat_freezed_column_list, i) > 0]
        # Construct the column names string for the INSERT query
        final_columns = ','.join(['"{0}"'.format(item) for item in columns])
        # Create a tuple of values for the INSERT query
        value = tuple([str(r) for r in (list(map(csat_dict.get, columns))) ])
        try:
            # Insert the row data into the csat_ldb table
            insert_query = '''insert into csat_ldb (%s) values %s;''' % (final_columns, value)
            execute_function(insert_query)
            # Update the date column in csat_ldb where it is null
            date_update_query = "update csat_ldb set date = '%s' where date is null;" % (date.date())
            execute_function(date_update_query)
        except Exception as e:
            # If an exception occurs during the insert query, rollback the transaction
            execute_function("ROLLBACK")
            # Convert the value to a modified format using error_function_1
            value1 = error_function_1(value)
            # Insert the modified row data into the csat_ldb table
            insert_query = '''insert into csat_ldb (%s) values %s;''' % (final_columns, value1)
            execute_function(insert_query)
            # Update the date column in csat_ldb where it is null
            date_update_query = "update csat_ldb set date = '%s' where date is null;" % (date.date())
            execute_function(date_update_query)
    # Print the date and rows_counter values
    print(date, rows_counter)
    # Call the status_table_inserting function to insert status information into the status_ldb table
    status_table_inserting(table_name, API_rows_Number)
# Dictionary mapping table names to corresponding API processing functions
tablename = {'agent_info_ldb': agent_api_to_ldb, 'daily_exportcall_ldb': export_api_to_ldb, 'csat_ldb': csat_api_to_ldb}

'''The final_database function retrieves the JSON rows count and inserted rows count from the status_ldb table for each table specified in the tables dictionary. 
It constructs queries to fetch the relevant data based on the table name and current date. The function uses the fetch_data_function to execute the queries and retrieve the results.
The retrieved counts are then stored in a dictionary json_inserted_rows_data with keys formatted using the table name prefixes. Finally, 
the function closes the cursor and returns the json_inserted_rows_data dictionary.
'''
def final_database():
    # Initialize an empty dictionary to store the counts
    json_inserted_rows_data = {}
    # Iterate over the items in the 'tables' dictionary
    for table_name, prefix in tables.items():
        # Construct a query to fetch the JSON rows count from the 'status_ldb' table
        json_data_query = f"SELECT json_rows FROM status_ldb WHERE table_name='{table_name}' AND timestamp::date='{date.today()}';"
        json_rows = fetch_data_function(json_data_query)
        # Construct a query to fetch the inserted rows count from the 'status_ldb' table
        inserted_data_query = f"SELECT inserted_rows FROM status_ldb WHERE table_name='{table_name}' AND timestamp::date='{date.today()}';"
        inserted_rows = fetch_data_function(inserted_data_query)
        # Store the counts in the dictionary with keys formatted using the table name prefixes
        json_inserted_rows_data[f'{prefix}_json_rows_count'] = json_rows
        json_inserted_rows_data[f'{prefix}_insert_rows_count'] = inserted_rows
    # Close the cursor
    cursor.close()
    # Return the dictionary containing the counts
    return json_inserted_rows_data

'''The validation function iterates over a date range and checks the existence of records for each date in specified tables.
If no records are found for a particular date, it calls the corresponding API function to insert data for that date and table. 
If records already exist, it deletes them from the table and then calls the API function to insert new data. 
The function helps validate and update database records based on the provided date range and table names.
'''
def validation():
    # Iterate over each date in the date range
    for date in pd.date_range(startdate, enddate):
        # Iterate over each table name and corresponding API function
        for table_name, api_fun in tablename.items():
            # Construct a query to count the number of records for the current date in the table
            count_query = f"SELECT COUNT(*) FROM {table_name} WHERE date='{date.date()}';"
            data_count = fetch_data_function(count_query)
            if data_count == 0:
                # If no records exist for the current date, call the API function to insert data
                api_fun(table_name, date)
            else:
                # If records exist for the current date, delete them from the table
                delete_query = f"DELETE FROM {table_name} WHERE date='{date.date()}';"
                execute_function(delete_query)
                # Call the API function to insert data for the current date
                api_fun(table_name, date)


'''The email_notification function sends an email notification containing the data obtained from the final_database function. 
It sets up the SMTP server, login credentials, sender and receiver email addresses, and creates a multipart message with both plain text and HTML versions of the email content.
The function retrieves the rows data from the final_database function, including the counts of API rows and inserted rows for different tables. 
It formats the email content with this data and sends the email using the configured SMTP server and login credentials.
Overall, the function facilitates the generation and sending of an email notification with relevant data to the specified receiver email address.
'''
def email_notification():
    smtp_server = "smtp.gmail.com"
    port = 587
    login = "prchs44@gmail.com"  # Replace with your login email address
    password = "byyjjhdevzgdzlue"  # Replace with your login password
    sender_email = "prchs44@gmail.com"
    receiver_email = "polamarasetti.rajachandrasekhar@3i-infotech.com"
    rows_data = final_database()
    message = MIMEMultipart("alternative")
    message["Subject"] = "ICC daily data insert API to SDB"
    message["From"] = sender_email
    message["To"] = receiver_email
    
    text = f"""Dear Team,
    Exportcall_report API rows: {rows_data['export_json_rows_count']}
    Exportcall_report inserted rows: {rows_data['export_insert_rows_count']}
    Agent_info API rows: {rows_data['agent_json_rows_count']}
    Agent_info inserted rows: {rows_data['agent_insert_rows_count']}
    csat_info API rows: {rows_data['csat_json_rows_count']}
    csat_info inserted rows: {rows_data['csat_insert_rows_count']}
    """
    
    html = f"""<html>
      <body>
        <p>Dear Team,</p>
        <p>Exportcall_report API rows: {rows_data['export_json_rows_count']}</p>
        <p>Exportcall_report inserted rows: {rows_data['export_insert_rows_count']}</p>
        <p>Agent_info API rows: {rows_data['agent_json_rows_count']}</p>
        <p>Agent_info inserted rows: {rows_data['agent_insert_rows_count']}</p>
        <p>csat_info API rows: {rows_data['csat_json_rows_count']}</p>
        <p>csat_info inserted rows: {rows_data['csat_insert_rows_count']}</p>
      </body>
    </html>
    """
    
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")
    message.attach(part1)
    message.attach(part2)
    
    with smtplib.SMTP(smtp_server, port) as server:
        server.starttls()
        server.login(login, password)
        server.sendmail(sender_email, receiver_email, message.as_string())
    
    # No need to call server.quit() when using 'with' statement

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
The DAG is scheduled to run daily at 1:00 AM (schedule_interval='0 1 * * *') (UTC).
 It has a start date of March 16, 2023, and does not have an end date specified. 
 The catchup parameter is set to False, meaning that Airflow will only schedule and execute tasks for future runs and not catch up on missed runs.
'''    
with DAG(default_args=default_args,
         dag_id='API_to_LDB',
         schedule_interval='0 6 * * * ',
         start_date=pendulum.datetime(year=2023, month=3, day=16,tz='Asia/Kolkata'),
         end_date=None,
         catchup=False
         )as dag:
    api_to_sdb_ = PythonOperator(
        task_id="API_to_daily_ldb",
        python_callable=validation,
        dag=dag,
    )
    email_notifications=PythonOperator(
        task_id="email_sender",
        python_callable=email_notification,
        dag=dag,
    )

'''
The api_to_sdb_ task is set as the dependency of the email_notifications task using the >> operator,
indicating that email_notifications should run after api_to_sdb_ completes.
'''

api_to_sdb_>>email_notifications