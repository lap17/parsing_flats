# Парсинг квартир

Реализована почти автоматическая система парсинга квартир в новостройках (актуально на 01.09.21). Цель - собрать датасет для мониторинга изменения цены. Для задачи выбраны сайты застройщиков ПИК, Самолет, А101. Также добавлен парсинг отзывов по новостройкам из сайтов Avaho, Cian, Mskguru, Novostroy M. Поскольку парсинг осуществляется на html-страницах, необходимо время от времени корретировать код проекта, так как код html страницы может обновляться.

## Подготовка и запуск проекта (Linux)

Обновление пакетов:

```
sudo apt-get update
```

Установка браузера Chrome:

```
wget http://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_88.0.4324.96-1_amd64.deb
```
```
sudo apt-get install ./google-chrome-stable_88.0.4324.96-1_amd64.deb
```

Установка драйвера для Chrome:

```
wget https://chromedriver.storage.googleapis.com/88.0.4324.96/chromedriver_linux64.zip
```
```
sudo unzip chromedriver_linux64.zip chromedriver -d /usr/local/bin/
```

Установка необходимых библиотек:

```
pip install -r requirements.txt
```

Cоздайте .env файл и впишите TELEBOT_TOKEN, RAPID_API и CHAT_ID для доступа к телеграму. Для получения токена TELEBOT_TOKEN нужно обратиться к боту BotFather, создайте новый бот по команде /newbot, после необходимых шагов по созданию вы получите токен. RAPID_API используется для доступа к поисковику google от rapidapi.com.

Запуск скриптов на фоне:

```
nohup python3 timer_parsing_flats.py &
```
```
nohup python3 timer_parsing_reviews.py &
```
