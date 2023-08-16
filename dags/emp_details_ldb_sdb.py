import psycopg2
import pandas as pd
import re
from datetime import date, timedelta
from psycopg2.extras import execute_values
from datetime import timedelta, datetime

connection_source = psycopg2.connect(host='3.108.122.116',database='landing_db', user='myusername', password='mypassword',port='5432')
cursor_source = connection_source.cursor()
connection_dest=psycopg2.connect(host='3.108.122.116',database='staging_db', user='myusername', password='mypassword',port='5432')
cursor_dest = connection_dest.cursor()




emp_details_columns=[ 'sl_no','lob',  'employeeid', 'employeename', 'sip', 'gender', 'team_lead',  'doj', 'current_status', 'designation',  'role_fit', 'grid']


def emp_details_sdb_function():
    # Function to perform the required operations

    # Joining the column names with commas and enclosing them in double quotes
    final_columns = ','.join(['"{0}"'.format(item) for item in emp_details_columns])

    # Constructing the SQL query to select data from 'emp_details' table
    query = f'''SELECT {final_columns} FROM emp_details WHERE lob='FHPL';'''
    
    # Executing the query on the source cursor
    cursor_source.execute(query)

    # Fetching the column names from the source cursor description
    column_names = [desc[0] for desc in cursor_source.description]

    # Creating a DataFrame with fetched data and assigning column names
    df = pd.DataFrame(cursor_source.fetchall(), columns=column_names)

    # Converting the 'employeename' column values to uppercase
    df['employeename'] = df['employeename'].apply(str.upper)

    # Getting the values from the DataFrame and converting them to a list of tuples
    value = df.values
    value = list(map(tuple, value))

    # Converting the list of tuples to a string representation without square brackets
    a = str(value).strip("[]")

    # Constructing the SQL query to insert data into 'emp_details_sdb' table
    query = f'''INSERT INTO emp_details_sdb (%s) VALUES %s;''' % (final_columns, a)
    
    # Executing the query on the destination cursor
    cursor_dest.execute(query)

    # Committing the changes to the destination connection
    connection_dest.commit()

# emp_details_sdb_function()