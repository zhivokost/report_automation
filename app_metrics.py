'''
Нужно собрать отчет по работе всего приложения (новостная лента и мессенджер) как единого целого. 

Следует подумать над метриками и их динамикой. Должны быть графики или файлы, чтобы сделать его более наглядным и информативным. Отчет должен быть не просто набором графиков или текста, а помогать отвечать бизнесу на вопросы о работе всего приложения совокупно. В отчете обязательно должны присутствовать метрики приложения как единого целого, или можно отобразить метрики по каждой из частей приложения — по ленте новостей и по мессенджеру. 
'''

import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandahouse as ph

from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}

my_token = '7737779209:AAGQd5mQDZBtV0Gyg7zLwiVJWkReJk2GXIY'
bot = telegram.Bot(token=my_token)
chat_id = '-938659451'

default_args = {
    'owner': 'i.arkhincheev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 20)
}


@dag(default_args=default_args, schedule_interval='0 11 * * *', catchup=False)
def app_metrics_i_arkhincheev():
    
    
    @task()
    def extract_users_count():
        q = """
            SELECT 
                uniqExactIf(f.user_id, f.user_id <> 0 and m.user_id <> 0) as both,
                uniqExactIf(f.user_id, f.user_id <> 0 and m.user_id = 0) as only_feed,
                uniqExactIf(m.user_id, f.user_id = 0 and m.user_id <> 0) as only_messages,
                both + only_feed + only_messages as all_users,
                max(toDate(f.time)) as max_date
            FROM
                (SELECT time,user_id FROM simulator_20241120.feed_actions WHERE toDate(time) <= yesterday()) as f
            FULL OUTER JOIN
                (SELECT user_id FROM simulator_20241120.message_actions WHERE toDate(time) <= yesterday()) as m
            ON
                f.user_id = m.user_id
            """
        result = ph.read_clickhouse(q, connection=connection)
        return result
    
    
    @task()
    def make_and_send_msg_users_count(chat_id, users_count):
        msg = f"""
<tg-emoji emoji-id="5368324170671202286">👨‍👩‍👦‍👦</tg-emoji> Размер аудитории приложения на <b><u>{users_count.max_date[0].date().strftime('%d.%m.%Y')}</u>:</b>
    
<tg-emoji emoji-id="5368324170671202286">🔢</tg-emoji> Всего пользователей: <b>{users_count.all_users[0]:,}</b>
        
<tg-emoji emoji-id="5368324170671202286">1️⃣</tg-emoji> Обоих серисов: <b>{users_count.both[0]:,}</b> <i>({users_count.both[0] / users_count.all_users[0]:.0%})</i>    
<tg-emoji emoji-id="5368324170671202286">2️⃣</tg-emoji> Только ленты новостей: <b>{users_count.only_feed[0]:,}</b> <i>({users_count.only_feed[0] / users_count.all_users[0]:.0%})</i>    
<tg-emoji emoji-id="5368324170671202286">3️⃣</tg-emoji> Только мессенджера: <b>{users_count.only_messages[0]:,}</b> <i>({users_count.only_messages[0] / users_count.all_users[0]:.1%})</i>   
       """.replace(',', ' ')
        bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='HTML')
        context = get_current_context()
        ts = context['ts']
        print(f'---send message for {ts}---') 

    
    @task()
    def extract_demographic():
        q = """
            SELECT
                multiIf(age < 20, '0-19', age >= 20 and age < 25, '20-24', age >= 25 and age < 30, '25-29', age >= 30 and age < 35, '30-34', age >= 35 and age < 40, '35-39', '40+') as age,
                if(gender=0, 'male', 'female') as gender,
                os,
                country,
                city,
                uniqExact(user_id) as users
            FROM
                (
                    SELECT DISTINCT user_id,gender,age,os,country,city FROM simulator_20241120.message_actions
                    UNION ALL
                    SELECT DISTINCT user_id,gender,age,os,country,city FROM simulator_20241120.feed_actions
                )
            GROUP BY
                age,
                gender,
                os,
                country,
                city
            """
        result = ph.read_clickhouse(q, connection=connection)
        return result 
    
    
    @task()
    def draw_and_send_graphs_demographic(chat_id, demographic):
        plt.figure(figsize=(40,30))
        plt.suptitle(f'Распределение пользователей по категориям', ha='center', fontsize=40, y=0.93, x=0.45)
        axs1 = plt.subplot2grid(shape=(2,6), loc=(0,0), colspan=2)
        axs2 = plt.subplot2grid((2,6), (0,2), colspan=2)
        axs3 = plt.subplot2grid((2,6), (0,4), colspan=2)
        axs4 = plt.subplot2grid((2,6), (1,0), colspan=2)
        axs5 = plt.subplot2grid((2,6), (1,4), colspan=2)

        axs1.set_title('Пол', fontsize=20, pad=5)
        explode = (0, 0.07)
        axs1.pie(demographic.groupby('gender', as_index=False).agg({'users': 'sum'})['users'], labels =demographic.groupby('gender', as_index=False).agg({'users': 'sum'})['gender'], startangle=90, colors=['#BCA3AC', '#8F9491'], wedgeprops={'edgecolor': 'black'}, autopct='%.0f%%', explode=explode, shadow=True, textprops={'color':"black", 'fontsize': 20})         

        axs2.set_title('Возрастная категория', fontsize=20, pad=5)
        sns.barplot(data=demographic.groupby('age', as_index=False).agg({'users': 'sum'}), x='age', y='users', ax=axs2, color='#4374B3')
        axs2.set_ylabel('Кол-во пользователей', fontsize=15, labelpad=10, loc='bottom')
        axs2.set_xlabel('')
        axs2.tick_params(labelsize=15)
        for p in axs2.patches: # добавляем значения над столбцами
                axs2.annotate("%.0f" % p.get_height(), (p.get_x() + p.get_width() / 2., p.get_height()),
                                ha='center', va='top', fontsize=18, color='black', xytext=(0, 15),
                                textcoords='offset points')

        axs3.set_title('Операционная система', fontsize=20, pad=5)
        axs3.pie(demographic.groupby('os', as_index=False).agg({'users': 'sum'})['users'], labels=demographic.groupby('os', as_index=False).agg({'users': 'sum'})['os'], startangle=90, colors=['#6E44FF', '#B892FF'], wedgeprops={'edgecolor': 'black'}, autopct='%.0f%%', explode=explode, shadow=True, textprops={'color':"black", 'fontsize': 20}) 

        sns.barplot(data=demographic.groupby('country', as_index=False).agg({'users': 'sum'}).sort_values('users', ascending=False).head(15), x='users', y='country', ax=axs4, color='#A8DCD1', orient='h')
        axs4.set_ylabel('')
        axs4.set_xlabel('')
        axs4.set_title('Топ-15 стран', fontsize=20, pad=10)
        axs4.invert_xaxis()
        axs4.yaxis.tick_right()
        axs4.tick_params(labelsize=20)
        for p in axs4.patches:
            axs4.text(5+p.get_width(), p.get_y()+0.55*p.get_height(),
                         '{:,.0f}'.format(p.get_width()),
                         ha='right', va='center', fontsize=18, color='black')

        sns.barplot(data=demographic.groupby('city', as_index=False).agg({'users': 'sum'}).sort_values('users', ascending=False).head(15), x='users', y='city', ax=axs5, color='#8E9DCC', orient='h')
        axs5.set_ylabel('')
        axs5.set_xlabel('')
        axs5.set_title('Топ-15 городов', fontsize=20, pad=10)
        axs5.tick_params(labelsize=20)
        for p in axs5.patches:
            axs5.text(5+p.get_width(), p.get_y()+0.55*p.get_height(),
                         '{:,.0f}'.format(p.get_width()),
                         ha='left', va='center', fontsize=18, color='black')  

        plt.subplots_adjust(wspace = 0.3)
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'demographic.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        context = get_current_context()
        ts = context['ts']
        print(f'---send demographic photo for {ts}---')
        
        
    @task()
    def extract_new_users():
        q = """
            SELECT 
                toStartOfWeek(reg_date, 1) as reg_week, source, count(user_id) as new_users
            FROM
                (SELECT 
                    user_id, source, min(reg_date) as reg_date
                FROM 
                    (
                    SELECT user_id, source, min(toDate(time)) as reg_date FROM simulator_20241120.message_actions WHERE toStartOfWeek(toDate(time), 1) < toStartOfWeek(today(), 1) GROUP BY user_id, source
                    UNION ALL
                    SELECT user_id, source, min(toDate(time)) as reg_date FROM simulator_20241120.feed_actions WHERE toStartOfWeek(toDate(time), 1) < toStartOfWeek(today(), 1) GROUP BY user_id, source
                    )
                GROUP BY user_id, source)
            GROUP BY 
                reg_week,
                source
            ORDER BY
                reg_week
        """    
        result = ph.read_clickhouse(q, connection=connection)  
        return result
    
    
    @task()
    def draw_and_send_graphs_new_users(chat_id, new_users): 
        plt.figure(figsize=(20, 10))
        colors = ["#69b3a2", "#4374B3"]
        sns.set_palette(sns.color_palette(colors))

        plot = sns.lineplot(data=new_users, x=new_users.reg_week.dt.date, y='new_users', hue='source', color='darkviolet', linewidth=7, marker='o', markersize=10, markerfacecolor='orange')
        plt.title('Динамика привлечения новых пользователей в разрезе каналов привлечения', fontsize=20, pad=20, ha='center')
        plt.ylabel('Кол-во новых пользователей', fontsize=12, labelpad=10)
        plt.xlabel('Недельный таймфрейм', labelpad=10)
        for x,y in new_users[['reg_week','new_users']].values:
            plt.text(x,y-300,f'{y:.0f}',color='gray', ha='center', fontsize=10)
        plt.setp(plot.get_legend().get_texts(), fontsize='15') 
        plt.setp(plot.get_legend().get_title(), fontsize='15') 
        plt.setp(plot.get_legend().get_lines(), linewidth='10') 

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'new_users.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        context = get_current_context()
        ts = context['ts']
        print(f'---send new users photo for {ts}---')
    
    
    @task() 
    def extract_retention(source):
        q = f"""
            WITH users_dates AS
                (
                    SELECT DISTINCT user_id, toDate(time) as event_date FROM simulator_20241120.message_actions WHERE event_date between yesterday()-20 and yesterday() and source='{source}' 
                    UNION ALL
                    SELECT DISTINCT user_id, toDate(time) as event_date FROM simulator_20241120.feed_actions WHERE event_date between yesterday()-20 and yesterday() and source='{source}' 
                )


            SELECT 
                start_date, 
                n_days, 
                users / first_cohort as rr
            FROM
                (
                SELECT
                    start_dates.start_date as start_date,
                    users_dates.event_date - start_dates.start_date as n_days,
                    count(start_dates.user_id) as users,
                    first_value(users) OVER (PARTITION BY start_date ORDER BY n_days) as first_cohort
                FROM
                    (
                        SELECT 
                            user_id,
                            min(event_date) as start_date
                        FROM
                            users_dates
                        GROUP BY
                            user_id
                    ) start_dates
                INNER JOIN
                    users_dates
                ON users_dates.user_id = start_dates.user_id
                GROUP BY
                    start_date,
                    n_days
                )
            ORDER BY
                start_date,
                n_days
        """    
        result = ph.read_clickhouse(q, connection=connection)  
        return result
   

    @task()
    def draw_and_send_graphs_retention(chat_id, retention, source, source_text):
        retention = retention.pivot(index='start_date', columns='n_days', values='rr')
        retention.index = retention.index.strftime('%d.%m.%Y')
        
        plt.figure(figsize=(20, 10))
        plt.title(f'Удержание пользователей, привлеченных {source_text} за прошедшие 20 дней', fontsize = 14, pad=20, ha='center')

        sns.heatmap(retention, annot=True, fmt= '.0%',cmap='YlGnBu', vmin = 0.0 , vmax = 0.5)
        plt.ylabel('Когорты пользователей по дате регистрации')
        plt.xlabel('Кол-во дней с даты регистрации')

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = f'retention_{source}.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        context = get_current_context()
        ts = context['ts']
        print(f'---send retention_{source} photo for {ts}---')
        
        
    @task() 
    def extract_actions():
        q = """
            SELECT 
                toStartOfWeek(event_date, 1) as event_week, 
                SUM(actions) as actions 
            FROM
                (    
                    SELECT toDate(time) as event_date, count(user_id) as actions FROM simulator_20241120.message_actions WHERE toStartOfWeek(toDate(time), 1) < toStartOfWeek(today(), 1) GROUP BY event_date
                    UNION ALL
                    SELECT toDate(time) as event_date, count(user_id) as actions FROM simulator_20241120.feed_actions WHERE toStartOfWeek(toDate(time), 1) < toStartOfWeek(today(), 1) GROUP BY event_date
                )
            GROUP BY
                event_week
            ORDER BY 
                event_week
            """
        result = ph.read_clickhouse(q, connection=connection)  
        return result
    
    
    @task() 
    def extract_posts():
        q = """
            SELECT 
                toStartOfWeek(toDate(time), 1) as event_week, 
                uniqExact(post_id) as posts 
            FROM 
                simulator_20241120.feed_actions 
            WHERE 
                toStartOfWeek(toDate(time), 1) < toStartOfWeek(today(), 1)
            GROUP BY 
                event_week
            """
        result = ph.read_clickhouse(q, connection=connection)  
        return result
    
    
    @task()
    def draw_and_send_actions_and_posts(chat_id, actions, posts):
        fig, axs = plt.subplots(nrows=2)
        fig.set_figwidth(30)
        fig.set_figheight(20)
        plt.suptitle(f'Вовлеченность и частота публикаций', ha='center', fontsize=40, y=0.98, x=0.45)

        sns.barplot(data=actions, x=actions.event_week.dt.date, y='actions', ax=axs[0], color='#FE938C')
        axs[0].set_ylabel('Кол-во действий', fontsize=15, labelpad=10)
        axs[0].set_xlabel('Недельный таймфрейм', fontsize=15, labelpad=10)
        axs[0].tick_params(labelsize=12)
        axs[0].set_title('Динамика действий пользователей (просмотры, лайки, отправленные и полученные сообщения), млн.', fontsize=20, pad=10, ha='center')
        for p in axs[0].patches: 
                axs[0].annotate(f"{p.get_height() / 1000000:.1f} млн.", (p.get_x() + p.get_width() / 2., p.get_height()),  
                                ha='center', va='top', fontsize=15, color='black', xytext=(0, 15),
                                textcoords='offset points')

        sns.barplot(data=posts, x=posts.event_week.dt.date, y='posts', ax=axs[1], color='#EDD382')
        axs[1].set_ylabel('Кол-во постов', fontsize=15, labelpad=10)
        axs[1].set_xlabel('Недельный таймфрейм', fontsize=15, labelpad=10)
        axs[1].tick_params(labelsize=12)
        axs[1].set_title('Динамика публикаций (постов)', fontsize=20, pad=10, ha='center')
        for p in axs[1].patches: 
                axs[1].annotate(f"{p.get_height():.0f}", (p.get_x() + p.get_width() / 2., p.get_height()),
                                ha='center', va='top', fontsize=15, color='black', xytext=(0, 15),
                                textcoords='offset points')

        plt.subplots_adjust(hspace = 0.3)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'actions_and_posts.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        context = get_current_context()
        ts = context['ts']
        print(f'---send actions_and_posts photo for {ts}---')
    
    
    
    
    users_count = extract_users_count()
    msg_users_count = make_and_send_msg_users_count(chat_id=chat_id, users_count=users_count)
    demographic = extract_demographic()
    graphs_demographic = draw_and_send_graphs_demographic(chat_id=chat_id, demographic=demographic)
    new_users = extract_new_users()
    graphs_new_users = draw_and_send_graphs_new_users(chat_id=chat_id, new_users=new_users)
    retention_ads = extract_retention('ads')
    graphs_retention_ads = draw_and_send_graphs_retention(chat_id=chat_id, retention=retention_ads, source='ads', source_text='рекламными компаниями')
    retention_organic = extract_retention('organic')
    graphs_retention_ogranic = draw_and_send_graphs_retention(chat_id=chat_id, retention=retention_organic, source='organic', source_text='органически (без усилий компании)')
    actions = extract_actions()
    posts = extract_posts()
    graphs_actions_and_posts = draw_and_send_actions_and_posts(chat_id=chat_id, actions=actions, posts=posts)
    
    users_count >> msg_users_count >> demographic >> graphs_demographic >> new_users >> graphs_new_users >> retention_ads >> graphs_retention_ads >> retention_organic >> graphs_retention_ogranic >> actions >> posts >> graphs_actions_and_posts
    
    
app_metrics_dag = app_metrics_i_arkhincheev()