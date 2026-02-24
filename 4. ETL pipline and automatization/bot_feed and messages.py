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
def dag_shaikhutdinova_bot_services():
    
    @task
    def send_dau_metrics():
        bot = telegram.Bot(token=my_token)
        dau_both_s="""
        WITH cte as (
            SELECT  
                user_id,
                max(news_app) as news_app,
                max(message_app) as message_app,
                dt
            FROM (
                    SELECT 
                        user_id,
                        toDate(time) as dt,
                        1 as news_app, 
                        0 as message_app
                    FROM simulator_20251220.feed_actions
                    WHERE toDate(time) between yesterday() - interval 6 days and yesterday()

                    UNION ALL

                    SELECT 
                        user_id, 
                        toDate(time) as dt,
                        0 as news_app, 
                        1 as message_app
                    FROM simulator_20251220.message_actions
                    WHERE toDate(time) between yesterday() - interval 6 days and yesterday()
                    ) a
            GROUP BY user_id, dt)

        SELECT
            countIf(user_id, news_app = 1 and message_app=1) as news_and_message_users,
            countIf(user_id, news_app = 0 and message_app=1) as message_users,
            countIf(user_id, news_app = 1 and message_app=0) as news_users,
            dt
        FROM cte 
        GROUP BY dt
        ORDER BY dt """
        
        dau_df=ch_get_df(dau_both_s)
        dau_df = dau_df.set_index('dt')
        
        #by os
        q_os= """
        With cte as (Select 
            user_id, os,
            max(news_app) as news_app,
            max(message_app) as message_app
        from (
        SELECT user_id, os,
        1 as news_app, 
        0 as message_app
        from simulator_20251220.feed_actions
        WHERE toDate(time) = yesterday() 

        union all

        SELECT user_id, os,
        0 as news_app, 
        1 as message_app
        from simulator_20251220.message_actions
        WHERE toDate(time) = yesterday())
        a
        Group by user_id, os)

        Select countIf(user_id, news_app = 1 and message_app=1) as news_and_message_users,
        countIf(user_id, news_app = 0 and message_app=1) as message_users,
        countIf(user_id, news_app = 1 and message_app=0) as news_users,
        os
        from cte 
        group by os
        order by news_and_message_users desc
        """
        df_os=ch_get_df(q_os)
        
        top3_countries= """
        With cte as (Select 
            user_id, country,
            max(news_app) as news_app,
            max(message_app) as message_app,
            dt
        from (
        SELECT user_id, toDate(time) as dt, country,
        1 as news_app, 
        0 as message_app
        from simulator_20251220.feed_actions
        WHERE toDate(time) = yesterday() 

        union all

        SELECT user_id, toDate(time) as dt, country,
        0 as news_app, 
        1 as message_app
        from simulator_20251220.message_actions
        WHERE toDate(time) = yesterday())
        a
        Group by user_id, dt, country)

        Select countIf(user_id, news_app = 1 and message_app=1) as news_and_message_users,
        countIf(user_id, news_app = 0 and message_app=1) as message_users,
        countIf(user_id, news_app = 1 and message_app=0) as news_users,
        dt, country
        from cte 
        group by dt, country
        order by news_and_message_users desc
        limit 3
        """
        top_3_countries=ch_get_df(top3_countries)
        yesterday = (datetime.now() - timedelta(days=1)).date()
        yesterday_data = dau_df.iloc[-1]
        day_before_data = dau_df.iloc[-2]
        send_metrics= f""" 
        Main metrics {yesterday}
        DAU n&m  = {yesterday_data[0]}
            - изменение со вчера: {(yesterday_data[0]/day_before_data[0]-1)*100:.2f} %
            из них 
                🍎 {df_os.iloc[1,3]} {df_os.iloc[1,0]}
                🤖 {df_os.iloc[0,3]} {df_os.iloc[0,0]}
            География (топ-3): 
                {top_3_countries.iloc[0,4]} {top_3_countries.iloc[0,0]}
                {top_3_countries.iloc[1,4]} {top_3_countries.iloc[1,0]}
                {top_3_countries.iloc[2,4]} {top_3_countries.iloc[2,0]}                       

        DAU messages only= {yesterday_data[1]}\t
            - изменение со вчера: {(yesterday_data[1]/day_before_data[1]-1)*100:.2f} % 
        DAU news only =  {yesterday_data[2]}\t
            - изменение со вчера: {(yesterday_data[2]/day_before_data[2]-1)*100:.2f} % 
        """
        bot.sendMessage(chat_id=chat_id, text=send_metrics)
        
        cols_count = len(dau_df.columns)
        fig, axes = plt.subplots(1, cols_count, figsize=(15, 7)) 

        for i, column in enumerate(dau_df.columns):
            sns.lineplot(data=dau_df, x=dau_df.index, y=column, marker='o', ax=axes[i])


            axes[i].set_title(f'{column}')
            axes[i].tick_params(axis='x', rotation=45)
            axes[i].set_ylabel('')
            axes[i].set_xlabel('')

        plt.tight_layout() # Автоматически подправляет отступы между графиками

        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png')
        plot_object.seek(0)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    @task
    def age_group_send():
        bot = telegram.Bot(token=my_token)
        agegroup= """
        WITH cte as (
            SELECT  
                user_id,
                age,
                max(news_app) as news_app,
                max(message_app)  as message_app

            FROM (
                    SELECT 
                        user_id,
                        age,
                        1 as news_app, 
                        0 as message_app
                    FROM simulator_20251220.feed_actions
                    WHERE toDate(time) between yesterday() - interval 6 days and yesterday()

                    UNION ALL

                    SELECT 
                        user_id, 
                        age, 
                        0 as news_app, 
                        1 as message_app
                    FROM simulator_20251220.message_actions
                    WHERE toDate(time) between yesterday() - interval 6 days and yesterday()
                    ) a
         GROUP BY user_id, age )

        SELECT
            multiIf(age < 18, '<18', 
                    age < 25, '18-24', 
                    age < 35, '25-34', 
                    age < 45, '35-44', 
                    age < 65, '45-64', '65+') as age_group,
            countIf(user_id, news_app = 1 and message_app=1) as news_and_message_users,
            countIf(user_id, news_app = 0 and message_app=1) as message_users,
            countIf(user_id, news_app = 1 and message_app=0) as news_users
        FROM cte 
        GROUP BY age_group
        ORDER BY age_group
        """
       
        age_group_df= ch_get_df(agegroup)
        plt.figure(figsize= (12,7))
        sns.barplot(data=age_group_df, x='news_and_message_users', y='age_group', color='skyblue')

        plt.title('Уникальные пользователи обоих сервисов по возрасту за последнюю неделю')
        plt.ylabel('')
        plt.xlabel('')
        plot_object2 = io.BytesIO()
        plt.savefig(plot_object2, format='png')
        plot_object2.seek(0)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object2)

    send_part1 = send_dau_metrics()
    send_part2 = age_group_send()
    
dag_final = dag_shaikhutdinova_bot_services()
