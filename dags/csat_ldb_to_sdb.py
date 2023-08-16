import psycopg2
import pandas as pd
from datetime import date, timedelta
from psycopg2.extras import execute_values
from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

#---------------connections-------------------------#
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


column_list_csat_sdb=['date','user_id','emp_code','agentname','campaign_id','survey_rating','category','surveycount','tl','tenure','week']

def csat_sdb_function():
    query='''select count(*) from csat;'''
    cursor_source.execute(query)
    count_data=cursor_source.fetchone()[0]
    print(count_data)
    query='''select count(*) from csat_sdb;'''
    cursor_dest.execute(query)
    count_table_data=cursor_dest.fetchone()[0]
    print(count_table_data)
    if count_table_data==count_data:
        print('no need to run')
    else:
        print('time to run')
        final_columns= ','.join(['"{0}"'.format(item) for item in column_list_csat_sdb])
        query=f'''select {final_columns} from csat OFFSET {count_table_data};'''
        cursor_source.execute(query)
        column_names= [desc[0] for desc in cursor_source.description]
        df=pd.DataFrame(cursor_source.fetchall(), columns= column_names) 
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        df['agentname'] = df['agentname'].apply(str.upper)
        value=df.values
        value=list(map(tuple, value))
        a=str(value).strip("[]")
        query=f'''INSERT INTO csat_sdb (%s) VALUES %s;'''%(final_columns,a)
        cursor_dest.execute(query)
        connection_dest.commit()

    
column_list_ftr_sdb=['date','full_name','status','call_transferred_to_survey_ivr','ftr','survey_transfer','tl','emp_id','user___sip_id']  
def ftr_sdb_function():
    query='''select count(*) from ftr_ivr;'''
    cursor_source.execute(query)
    count_data=cursor_source.fetchone()[0]
    print(count_data)
    query='''select count(*) from ftr_ivr_sdb;'''
    cursor_dest.execute(query)
    count_table_data=cursor_dest.fetchone()[0]
    print(count_table_data)
    if count_table_data==count_data:
        print('no need to run')
    else:
        print('time to run')
        final_columns= ','.join(['"{0}"'.format(item) for item in column_list_ftr_sdb])
        query=f'''select {final_columns} from ftr_ivr OFFSET {count_table_data};'''
        cursor_source.execute(query)
        column_names= [desc[0] for desc in cursor_source.description]
        df=pd.DataFrame(cursor_source.fetchall(), columns= column_names) 
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        df['full_name'] = df['full_name'].apply(str.upper)
        value=df.values
        value=list(map(tuple, value))
        a=str(value).strip("[]")
        query=f'''INSERT INTO ftr_ivr_sdb (%s) VALUES %s;'''%(final_columns,a)
        cursor_dest.execute(query)
        connection_dest.commit()


with DAG(default_args=default_args,
         dag_id='csat_ldb_to_sdb',
         schedule_interval="0 */6 * * *",
         start_date=pendulum.datetime(year=2023, month=3, day=24,tz='Asia/Kolkata'),
         end_date=None,
         catchup=False
         )as dag:
        csat_ldb_to_csat_sdb_ = PythonOperator(
        task_id="csat_ldb_to_quality_sdb",
        python_callable=csat_sdb_function
        )

        ftr_ldb_to_ftr_sdb_ = PythonOperator(
        task_id="ftr_ldb_to_ftr_sdb",
        python_callable=ftr_sdb_function
        )

    # trigger_next_dag_csat_sdb_ = TriggerDagRunOperator(
    #     task_id='trigger_next_dag_csat_sdb',
    #     trigger_dag_id='csat_ldb_to_sdb',
    #     dag=dag,
    # )
csat_ldb_to_csat_sdb_>>ftr_ldb_to_ftr_sdb_

