# Автоматизация отчета об основных метриках социальной сети
Собран отчет по работе всего приложения (новостная лента и мессенджер) как единого целого и настроен на ежедневную отправку в Telegram. Для каждой метрики создана соответствующая функция подсчета и визуализации:
1. Количества пользователей;
2. Демографических метрик (возраст, пол, город, страна, ОС)
3. Динамики привлечения новых пользователей по определенным каналам привлечения;
4. Retention rate пользователей;
5. Динамики действий пользователей.
Автоматизация настроена с помощью Airflow.

### Инструменты: pandahouse, matplotlib, seaborn, telegram, airflow
