from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
import datetime
from bs4 import BeautifulSoup as soup
from others import PATH, send_message_to_telegram


all_columns = ["Ссылка", "Дата", "Стоимость", "Число комнат", 
               "Район", "Корпус", "Этаж", "Дата сдачи квартиры",
               "Цена за м2", "Площадь","Ссылка на планировку квартиры"]

opt = Options()
opt.add_argument("--headless")
opt.add_argument('--no-sandbox')
opt.add_argument('--incognito')
opt.add_argument('--disable-dev-shm-usage') 


def a101_parsing_flats_all_jobs():
    try:
        send_message_to_telegram('Начало парсинга A101') 
        df_extract = get_data()
        if len(df_extract)==0:
            send_message_to_telegram('Ошибка парсинга проектов')
            return
        df_extract = df_extract.drop_duplicates(subset=['Ссылка'], keep='last').reset_index(drop=True)
        df_extract = df_extract.dropna(subset=['Ссылка'], how='all').reset_index(drop = True)
        send_message_to_telegram('Всего квартир на данный момент '+str(len(df_extract)))
        df_a101_init = pd.read_excel('a101_flats.xlsx')
        send_message_to_telegram('В базе на данный момент '+str(len(df_a101_init)))
        intersection_in = df_extract['Ссылка'].isin(df_a101_init['Ссылка'])
        intersection_not_in = ~intersection_in
        df_in = df_extract[intersection_in]
        df_not_in = df_extract[intersection_not_in]
        num_exist_flats = len(df_in)
        num_new_flats = len(df_not_in)
        send_message_to_telegram('Число распарсенных квартир, которые имеются в датасете '+str(num_exist_flats))
        send_message_to_telegram('Число новых квартир для добавления в датасет '+str(num_new_flats))  
        df_a101_update = update_price(df_a101_init, df_in)
        send_message_to_telegram('Число квартир с обновлением цены '+str(len(df_a101_update)))
        df_to_db = pd.concat([df_a101_update, df_not_in], ignore_index=True)
        send_message_to_telegram('Всего добавлено в базу '+str(len(df_to_db)))
        df_all = pd.concat([df_a101_init, df_to_db], ignore_index=True)
        df_all.to_excel('a101_flats.xlsx', index = False)
    except Exception as e:
        send_message_to_telegram(e)


def get_num_aparts_by_url(url_for_get_num_aparts):
    try:
        browser = webdriver.Chrome(PATH, options=opt)
        browser.implicitly_wait(10)
        browser.get(url_for_get_num_aparts)
        time.sleep(2)
        elem = browser.find_element_by_class_name('button.primary.ToResultsButton__2gHhv')
        str_num_aparts = elem.text
        str_num_aparts = str_num_aparts.replace(' ', '')
        str_num_aparts = ''.join(filter(str.isdigit, str_num_aparts))
        num_aparts = int(str_num_aparts) + 30
        half_num_aparts = int(num_aparts/2)
        browser.quit()
        return half_num_aparts
    except Exception as e:
        print(e)
        browser.quit()
        return 0


def get_data():
    #поочередно включен фильтр по количесвту комнат в квартире
    dict_type_apart = [-2,-1,1,2,3]
    df_extract = pd.DataFrame()
    for i in range(len(dict_type_apart)):
        type_apart = str(dict_type_apart[i])
        url_for_get_num_aparts = 'https://a101.ru/kvartiry/?ordering=actual_price&room={}'.format(type_apart)
        half_num_aparts = get_num_aparts_by_url(url_for_get_num_aparts)
        url_expensive = 'https://a101.ru/kvartiry/?ordering=-actual_price&room={}'.format(type_apart)
        url_inexpensive = 'https://a101.ru/kvartiry/?ordering=actual_price&room={}'.format(type_apart)
        df_aparts_exp = get_list_aparts_by_url(url_expensive, half_num_aparts)
        time.sleep(2)
        df_aparts_inexp = get_list_aparts_by_url(url_inexpensive, half_num_aparts)
        df_extract_type = pd.concat([df_aparts_exp, df_aparts_inexp], ignore_index=True)
        df_extract = pd.concat([df_extract, df_extract_type], ignore_index=True)
    return df_extract


#для загрузки половины списка квартир
#необходимо кликать на кнопку показа доп. квартир
def get_list_aparts_by_url(url, half_num_aparts):
    try:
        click, noclick = 0, 0
        browser = webdriver.Chrome(PATH, options=opt)
        browser.implicitly_wait(10)
        browser.get(url)
        time.sleep(2)
        num_clicks = int(half_num_aparts/19)
        num_attempts = 6
        for i in range(num_clicks):
            try:
                wait = WebDriverWait(browser, 300).until(EC.element_to_be_clickable((By.XPATH, '//div[@class="button primary loadMore__1O_uH"]')))
                for i in range(num_attempts):
                    try:
                        button = browser.find_element_by_xpath('//div[@class="button primary loadMore__1O_uH"]')
                        time.sleep(2)
                        button.click()
                        click += 1
                        break
                    except Exception as e:
                        continue
                print('click:', click, 'noclick:', noclick, end='')
                print('\r', end='')
            except Exception as e:
                noclick += 1
                print('click:', click, 'noclick:', noclick, end='')
                print('\r', end='')
                break
        time.sleep(5)
        text = browser.page_source
        flats = browser.find_elements_by_xpath('//a[@class="link__2l6P6"]')
        page_soup = soup(text , "html")
        flats_list = []
        for i in range(len(flats)):
            url = flats[i].get_attribute('href')
            row = get_flat_info_by_url(url, page_soup)
            if len(row)!=0:
                flats_list.append(row)
        browser.quit()
        df_new = pd.DataFrame(flats_list, columns = all_columns)
        return df_new 
    except Exception as e:
        browser.quit()
        print(e)
        return pd.DataFrame(columns = all_columns)


def get_flat_info_by_url(link, page_soup):
    try:
        cut_link = link.replace('https://a101.ru','')
        current_html_code = page_soup.select("a[href*='{}']".format(cut_link))
        if len(current_html_code)==0:
            return []
        else:
            link_img_flat = current_html_code[0].find('img',{"alt": "Схема квартиры"})['src']
            str_num_rooms = current_html_code[0].find('p',{"class": "strong1 room__3svln"}).getText()
            str_num_rooms = ''.join(filter(str.isdigit, str_num_rooms))
            num_rooms = int(str_num_rooms)
            date_today = datetime.datetime.today().strftime("%Y-%m-%d-%H-%M")
            list_param = current_html_code[0].findAll('p',{"class": "p-normal"})
            raion = list_param[0].getText()
            raion = replace_symbols(raion)
            korpus = list_param[1].getText()
            korpus = replace_symbols(korpus)
            floor = list_param[2].getText()
            floor = replace_symbols(floor)
            completion_date = list_param[3].getText()
            completion_date = replace_symbols(completion_date)
            price_m2 = list_param[4].getText()
            price_m2 = replace_symbols(price_m2)
            str_price = current_html_code[0].find('div',{"class": "item__1mKNw _price__SGoS8"}).getText()
            str_price = str_price.replace(' ','')
            str_price = str_price.replace('\n','')
            str_price = str_price.replace('₽','')
            price = int(str_price)
            area = current_html_code[0].find('p',{"class": "strong1 area__34SL3"}).getText()
            area = replace_symbols(area)
            area = area.replace('м2','')
            row = [link, date_today, price , num_rooms , raion, korpus, floor, completion_date, price_m2, area, link_img_flat]
            return row
    except Exception as e:
        print(e)
        return []


def replace_symbols(string):
    string = string.replace(' ','')
    string = string.replace('\n','') 
    return string


def update_price(df, df_for_update):
    df_for_update = df_for_update.reset_index(drop=True)
    indexes_to_append = []
    for i in range(len(df_for_update)):
        url_update = df_for_update['Ссылка'].iloc[i]
        price_update = int(df_for_update['Стоимость'].iloc[i])
        apart = df[df['Ссылка']==url_update]
        if len(apart)==0:
            continue
        ind_apart = apart.index[-1]
        price_old = int(df.loc[ind_apart]['Стоимость'])
        if price_old==price_update:
            continue
        else:
            indexes_to_append.append(i)
    if len(indexes_to_append)==0:
        return pd.DataFrame()
    df_to_append = df_for_update.iloc[indexes_to_append]
    return df_to_append
