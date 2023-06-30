# Парсинг квартир

Реализована почти автоматическая система парсинга квартир в новостройках (актуально на 01.09.21). Цель - собрать датасет для мониторинга изменения цены. Для задачи выбраны сайты застройщиков ПИК, Самолет, А101. Также добавлен парсинг отзывов по новостройкам из сайтов Avaho, Cian, Mskguru, Novostroy M. Поскольку парсинг осуществляется на html-страницах, необходимо время от времени корретировать код проекта, так как код html страницы может обновляться.

## Подготовка и запуск проекта (Linux)

Установка необходимых библиотек:

```
pip install -r requirements.txt
```

Cоздайте .env файл и впишите TELEBOT_TOKEN и CHAT_ID для доступа к телеграму. Для получения токена TELEBOT_TOKEN нужно обратиться к боту BotFather, создайте новый бот по команде /newbot, после необходимых шагов по созданию вы получите токен.

Запуск скриптов на фоне:

```
nohup python3 timer_parsing_flats.py &
```
```
nohup python3 timer_parsing_reviews.py &
```
