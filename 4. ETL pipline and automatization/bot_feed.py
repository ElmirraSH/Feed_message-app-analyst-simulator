import pandas as pd
import pandahouse as ph
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import io
from datetime import datetime, timedelta 

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

my_token = my_token 
chat_id = chat_id

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
    'start_date': datetime(2026, 1, 30) # Дата начала выполнения DAG
 
}

schedule_interval = '0 8 * * *' 
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=True)
def dag_shaikhutdinova_bot_news():
    
    @task
    def send_bot_metrics():
        q_metrics="""
        SELECT
            uniq(user_id) AS DAU, 
            countIf(action='view') as views,
            countIf(action='like') as likes,
            round(countIf(action='like')/countIf(action='view'),2) as CTR
        FROM simulator_20251220.feed_actions
        WHERE toDate(time) = yesterday()
        """
        yesterday = pd.Timestamp.today().date() - pd.Timedelta(days=1)
        q_metrics=ch_get_df(q_metrics)

        send_metrics=f""" Main metrics {yesterday}\t
              DAU  = {q_metrics.iloc[0,0]}\t
              views =  {q_metrics.iloc[0,1]}\t
              likes= {q_metrics.iloc[0,2]}\t
              CTR = {q_metrics.iloc[0,3]}
            """
        bot = telegram.Bot(token=my_token)
        bot.sendMessage(chat_id=chat_id, text=send_metrics)
        return "Metrics sent"
    
    @task
    def send_grafics():
        q_weekly="""
        SELECT
            toDate(time) as dt,
            uniq(user_id) AS DAU, 
            countIf(action='view') as views,
            countIf(action='like') as likes,
            round(countIf(action='like')/countIf(action='view'),4) as CTR
        FROM simulator_20251220.feed_actions
        WHERE toDate(time) >= yesterday()-interval 7 days and toDate(time)!= today()
        group by toDate(time)

        """
        q_weekly=ch_get_df(q_weekly)
        q_weekly=q_weekly.set_index('dt')
        plt.rcParams['figure.figsize'] = (10, 6)
        bot = telegram.Bot(token=my_token)
        for column in q_weekly.columns:
            sns.lineplot(data=q_weekly, x=q_weekly.index, y=column, marker='o')
            plt.title(f'Weekly {column} Dynamics')
            plt.xticks(rotation=45, ha='right') 

            plot_object = io.BytesIO()
            plt.savefig(plot_object, format='png') 
            plot_object.seek(0) 
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)

            plt.close()
        return 'Grafics sent'

    send_metrics = send_bot_metrics()
    send_grafics = send_grafics()
    
dag_final = dag_shaikhutdinova_bot_news()
