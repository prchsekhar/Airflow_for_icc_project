import psycopg2
import pandas as pd
from datetime import date, timedelta
from psycopg2.extras import execute_values
from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


connection_source = psycopg2.connect(host='3.108.122.116', database='staging_db', user='myusername', password='mypassword', port='5432')
cursor_source = connection_source.cursor()

connection_dest = psycopg2.connect(host='3.108.122.116', database='reporting_db', user='myusername', password='mypassword', port='5432')
cursor_dest = connection_dest.cursor()
# startdate=date.today()-timedelta(days=1)
# enddate=startdate
start_date=date.today()-timedelta(days=1)
end_date=start_date

column_list = ['agentname', 'csat_date','emp_code','user_id', 'csat', 'csat_per', 'dsat', 'dsat_per', 'neutral', 'neutral_per', 'response_count', 'great_score', 'great_score_percen', 'ftr', 'survey_ivr','first_call_resolution']
# target_date = date.today() - timedelta(days=1)#giving data day -1 
def csat_rdb_function():
    for range_date in pd.date_range(start_date,end_date):
        query = f'''SELECT COUNT(*) FROM csat_rdb where csat_date='{range_date.date()}';'''#getting count of csat file from reporting database
        cursor_dest.execute(query)
        count_table_data = cursor_dest.fetchone()[0]
        print(count_table_data)
        query = f'''SELECT COUNT(*) FROM csat_sdb where date='{range_date.date()}';'''#getting count of csat file from staging database
        cursor_source.execute(query)
        count_sdb_data = cursor_source.fetchone()[0]
        print(count_sdb_data)
        if count_table_data != 0:# if the count is greater than zero then print no need to run
            print('No need to run')
        elif count_sdb_data==0:
            print('No need to run')
        else:# if count  of reporting csat database is zero then time to run the below code.
            print('Time to run')
            query = f'''SELECT  get_csat_data('{range_date.date()}');'''
            #calling function from the postgres 
            cursor_source.execute(query)
            final_columns = ','.join(['"{0}"'.format(item) for item in column_list])#taking column names in double quotes.
            query = f"SELECT {final_columns} FROM temp_csat" #fetch column from temp_csat
            cursor_source.execute(query)
            column_names = [desc[0] for desc in cursor_source.description]
            df = pd.DataFrame(cursor_source.fetchall(), columns=column_names)
            df['csat_date'] = pd.to_datetime(df['csat_date'])
            df['csat_date'] = df['csat_date'].dt.strftime('%Y-%m-%d')
            df['agentname'] = df['agentname'].apply(str.upper)
            value = [tuple(x) for x in df.to_numpy()]
            try:
                
                query=f'''INSERT INTO csat_rdb ({final_columns}) VALUES %s;'''# inserting data into csat reporting DB 
                execute_values(cursor_dest,query,value)
                connection_dest.commit()
            except Exception as e:
                print(e)
            query=f'''drop table temp_csat;'''
            cursor_source.execute(query)
            connection_source.commit()

with DAG(default_args=default_args,
         dag_id='csat_sdb_to_rdb',
         schedule_interval="30 */6 * * *",
         start_date=pendulum.datetime(year=2023, month=3, day=24,tz='Asia/Kolkata'),
         end_date=None,
         catchup=False
         )as dag:
        csat_rdb_task = PythonOperator(
        task_id='csat_rdb_task',
        python_callable=csat_rdb_function,
        dag=dag,
        )
#         trigger_next_dag_csat_rdb_ = TriggerDagRunOperator(
#         task_id='trigger_next_dag_csat_rdb',
#         trigger_dag_id='csat_rdb_task',
#         dag=dag,
#         )
# csat_rdb_task>>trigger_next_dag_csat_rdb_


