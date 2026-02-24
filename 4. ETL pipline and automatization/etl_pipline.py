import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta, date

#ф-я для импорта из кликхауса
def ch_get_df(query):
    connection = {'host':'host', 
                  'database':'database', 
                  'user':'user', 
                  'password':'password'}
    return ph.read_clickhouse(query, connection=connection)

default_args = {
    'owner': 'shaikhutdinova_e', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 2, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками
    'email': 'e.shaihutdinova@mail.ru', # Почта для уведомлений 
    'start_date': datetime(2026, 1, 22) # Дата начала выполнения DAG
 
}

schedule_interval = '0 10 * * *' 
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=True)
def dag_shaikhutdinova_e():
    
    @task
    def extract_feed():
        q_feed="""
                SELECT 
                    toDate(time) as event_date, 
                    user_id, 
                    gender, 
                    age, 
                    os,
                    countIf(action='like') as likes,
                    countIf(action='view') as views
                FROM simulator_20251220.feed_actions
                WHERE toDate(time)= today() - INTERVAL 1 DAY
                GROUP BY event_date, user_id, gender, age, os
                ORDER BY event_date, user_id
                """
        df_feed = ch_get_df(q_feed)
        return df_feed
    
    @task
    def extract_messages():
        q_message="""
            SELECT
                dt as event_date,
                user_id,
                MAX(messages_out) AS messages_sent,
                MAX(people_out) AS users_sent,
                MAX(messages_in) AS messages_received,
                MAX(people_in) AS users_received
            FROM (
                SELECT
                    toDate(time) AS dt,
                    user_id,
                    COUNT(receiver_id) AS messages_out,
                    COUNT(DISTINCT receiver_id) AS people_out,
                    0 AS messages_in,
                    0 AS people_in
                FROM simulator_20251220.message_actions
                GROUP BY 
                        dt,
                        user_id

                UNION ALL

                SELECT
                    toDate(time) AS dt,
                    receiver_id AS user_id,
                    0 AS messages_out,
                    0 AS people_out,
                    COUNT(user_id) AS messages_in,
                    COUNT(DISTINCT user_id) AS people_in
                FROM simulator_20251220.message_actions
                GROUP BY
                    dt,
                    user_id
                ) AS temp_table
            WHERE dt = today() - INTERVAL 1 DAY
            GROUP BY
                event_date,
                user_id
            ORDER BY
                event_date ASC,
                user_id ASC
            """
                    
        df_messages = ch_get_df(q_message)
        
        q_message2= """
                     SELECT
                        distinct user_id,
                        os,
                        age, 
                        gender
                    FROM simulator_20251220.message_actions
            """
        df_messages2 = ch_get_df(q_message2)
        
        messages_merged=pd.merge(df_messages, df_messages2, on='user_id', how='left')
        return messages_merged

    @task
    def cube_data(df_feed, messages_merged):
        df_cube=pd.merge(df_feed, messages_merged, on = ['user_id', 'event_date', 'gender', 'age', 'os'], how = 'outer')
        metric_cols= ['likes', 'views', 'messages_sent', 'messages_received', 'users_sent', 'users_received']
        df_cube[metric_cols] = df_cube[metric_cols].fillna(0)
        return df_cube
        
    @task 
    def metric_gender(df_cube):
        metric_cols= ['likes', 'views', 'messages_sent', 'messages_received', 'users_sent', 'users_received']
        metrics_gender=df_cube.groupby('gender')[metric_cols].sum().reset_index()
            
        metrics_gender['dimension']='gender'
        metrics_gender=metrics_gender.rename(columns={'gender':'dimension_value'})
        return metrics_gender
        
    @task 
    def metric_os(df_cube):
        metric_cols= ['likes', 'views', 'messages_sent', 'messages_received', 'users_sent', 'users_received']
        metrics_os=df_cube.groupby('os')[metric_cols].sum().reset_index()
            
        metrics_os['dimension']='os'
        metrics_os=metrics_os.rename(columns={'os':'dimension_value'})
        return metrics_os
            
    @task 
    def metric_age(df_cube):
        bins = [0, 18, 25, 35, 45, 61, float('inf')]
        labels = ['<18', '18-24', '25-34', '35-44', '45-60', 'старше 60']
        df_cube['dimension_value'] = pd.cut(
                    df_cube['age'], 
                    bins=bins, 
                    labels=labels, 
                    right=False
                )
        metric_cols= ['likes', 'views', 'messages_sent', 'messages_received', 'users_sent', 'users_received']
        metrics_age=df_cube.groupby('dimension_value')[metric_cols].sum().reset_index()
        metrics_age['dimension']='age'
        return metrics_age
    
    @task
    def load_to_clickhouse(metrics_gender, metrics_os, metrics_age):
        df_result = pd.concat([metrics_gender, metrics_os, metrics_age], ignore_index=True)
        df_result['event_date']=date.today() - timedelta(days=1)
        
        metric_cols = ['views', 'likes', 'messages_sent', 'users_sent', 'messages_received', 'users_received']
        df_result[metric_cols] = df_result[metric_cols].astype(int)
        
        df_result = df_result[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_sent'
                         , 'users_sent', 'messages_received', 'users_received']]
        
        
        connection_test = {'host': 'host',
                      'database':'database',
                      'user':'user', 
                      'password':'password'
                     }
        
        table_name = 'shaikhutdinova_e_metrics_cube'
        
        create_query = f"""
        CREATE TABLE IF NOT EXISTS test.{table_name} (
            event_date Date,
            dimension String,
            dimension_value String,
            views UInt64,
            likes UInt64,
            messages_sent UInt64,
            users_sent UInt64,
            messages_received UInt64,
            users_received UInt64
        ) 
        ENGINE = MergeTree()
        ORDER BY event_date
        """
        ph.execute(create_query, connection=connection_test)               
      
        ph.to_clickhouse(df=df_result, table=table_name, index=False, connection=connection_test)
            



    extract_feed = extract_feed()
    extract_messages = extract_messages()
    cube_data = cube_data(extract_feed, extract_messages)

    metric_gender = metric_gender(cube_data)
    metric_os = metric_os(cube_data)
    metric_age = metric_age(cube_data)

    load_to_clickhouse=load_to_clickhouse(metric_gender, metric_os, metric_age)

dag_final = dag_shaikhutdinova_e()


