import telebot
import os

PATH = '/usr/local/bin/chromedriver'
RAPID_API = os.getenv('RAPID_API')
TELEBOT_TOKEN = os.getenv('TELEBOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
tb = telebot.TeleBot(TELEBOT_TOKEN)


def send_message_to_telegram(message):
    tb.send_message(CHAT_ID, message, disable_web_page_preview = True, parse_mode='HTML')
