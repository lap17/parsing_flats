from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
import datetime
from bs4 import BeautifulSoup as soup
from multiprocessing.pool import Pool
import multiprocessing
import numpy as np
from others import PATH, send_message_to_telegram

opt = Options()
opt.add_argument("--headless")
opt.add_argument('--no-sandbox')
opt.add_argument('--incognito')
opt.add_argument('--disable-dev-shm-usage')  


#попробуем определить автоматически номера проектов
#их можно увидеть в url после выбора в меню
def get_number_projects():
    try:
        browser = webdriver.Chrome(PATH, options=opt)
        browser.implicitly_wait(10)
        browser.get('https://samolet.ru/flats')
        time.sleep(2)
        elems = browser.find_elements_by_class_name('r-flats-filter__cell._size-m')
        for el in elems:
            label = el.find_element_by_class_name('r-filter-cell__label')
            if 'Выберите проект' in label.text:
                break
        tag_menu = el.find_element_by_class_name('multiselect__tags')
        tag_menu.click()
        time.sleep(1)
        tags = el.find_elements_by_class_name('multiselect__element')
        len_list = len(tags) - 1
        for i in range(len_list):
            tags[i].click()
            time.sleep(1)
            if i == len_list - 1:
                url_str_1 = browser.current_url
                tags[i-1].click()
                time.sleep(1)
                tags[i+1].click()
                time.sleep(1)
                url_str_2 = browser.current_url
        ind_1 = url_str_1.find('project=')
        str_nums_1 = url_str_1[ind_1+len('project='):]
        list_str_nums_1 = str_nums_1.split(',')
        ind_2 = url_str_2.find('project=')
        str_nums_2 = url_str_2[ind_2+len('project='):]
        list_str_nums_2 = str_nums_2.split(',')
        list_str_nums = list(set(list_str_nums_1 + list_str_nums_2))
        number_projects = list(map(int, list_str_nums))
        browser.quit()
        return number_projects
    except Exception as e:
        print(e)
        browser.quit()
        return []


def get_num_aparts_by_url(url_for_get_num_aparts):
    try:
        browser = webdriver.Chrome(PATH, options=opt)
        browser.implicitly_wait(10)
        browser.get(url_for_get_num_aparts)
        time.sleep(2)
        elem = browser.find_element_by_class_name('r-flats-filter__count')
        str_num_aparts = elem.text
        str_num_aparts = str_num_aparts.replace(' ', '')
        str_num_aparts = ''.join(filter(str.isdigit, str_num_aparts))
        num_aparts = int(str_num_aparts) + 30
        half_num_aparts = int(num_aparts/2)
        half_num_aparts = int(round(half_num_aparts/10,0))*10
        browser.quit()
        return half_num_aparts
    except Exception as e:
        browser.quit()
        return 200


#находим половину квартир для отфильтрованного поиска
#так как при загрузке полного списка квартир может вылететь браузер
#первую половину находим при включенном фильтре поиска квартир по дороговизне
#вторую половину находим при включенном фильтре поиска квартир по дешевизне
#затем их объединяем
def get_data():
    number_projects = get_number_projects()
    df_extract = pd.DataFrame()
    for i in range(len(number_projects)):
        type_apart = str(number_projects[i])
        url_for_get_num_aparts = 'https://samolet.ru/flats/?ordering=-price,pk&free=1&rooms={}'.format(type_apart)
        half_num_aparts = get_num_aparts_by_url(url_for_get_num_aparts)
        url_expensive = 'https://samolet.ru/flats/?ordering=-price,pk&free=1&rooms={}'.format(type_apart)
        url_inexpensive = 'https://samolet.ru/flats/?ordering=price,pk&free=1&rooms={}'.format(type_apart)
        df_aparts_exp = get_list_aparts_by_url(url_expensive, half_num_aparts)
        time.sleep(1)
        df_aparts_inexp = get_list_aparts_by_url(url_inexpensive, half_num_aparts)
        df_extract_type = pd.concat([df_aparts_exp, df_aparts_inexp], ignore_index=True)
        df_extract = pd.concat([df_extract, df_extract_type], ignore_index=True)
        print(len(df_extract))
    return df_extract


def get_price(link, page_soup):
    try:
        current_html_code = page_soup.select("a[href*='{}']".format(link))
        if len(current_html_code)==0:
            return None
        else:
            str_price = current_html_code[0].find('span',{"class": "card__price-current"}).getText()
            str_price = str_price.replace(' ','')
            str_price = str_price.replace('₽','')
            price = int(str_price)
            return price
    except Exception as e:
        print(e)
        return None


def get_list_aparts_by_url(url, half_num_aparts):
    try:
        click, noclick = 0, 0
        browser = webdriver.Chrome(PATH, options=opt)
        browser.implicitly_wait(10)
        browser.get(url)
        time.sleep(2)
        wait = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, '//button[@class="r-btn _size-m _primary"]')))
        num_clicks = int((half_num_aparts/12)+1)
        num_attempts = 5
        for i in range(num_clicks):
            try:
                wait = WebDriverWait(browser, 120).until(EC.element_to_be_clickable((By.XPATH, '//button[@class="r-btn _size-m _primary"]')))
                for i in range(num_attempts):
                    try:
                        button = browser.find_element_by_xpath('//button[@class="r-btn _size-m _primary"]')
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
        time.sleep(20)
        text = browser.page_source
        page_soup = soup(text , "html")
        containers = page_soup.findAll("div", {"class": "flats-container__list-card"})
        flats_list = []
        for i in range(len(containers)):
            url = containers[i].find("a", {"class": "card"})['href']
            if 'ordering' in url:
                url = url.rsplit('/', 1)[0] + '/'
            price = get_price(url, page_soup)
            flats_list.append([url, price])
        df_extract = pd.DataFrame(flats_list, columns = ['url','price'])
        browser.quit()
        return df_extract
    except Exception as e:
        print(e)
        browser.quit()
        return pd.DataFrame(columns = ['url','price'])


def update_price(df, df_for_update):
    samolet = {'ID': [], 'Квартира': [], 'Ссылка': [], 'Проект': [], 'Корпус': [], 'Секция': [], 'Этаж/Этажность': [],
          'Жилая площадь,м2': [], 'Общая площадь,м2': [], 'Номер квартиры': [], 'Номер планировки': [],
          'Тип планировки': [], 'Тип': [], 'Евро-планировка': [], 'Заселение до': [], 'Вид из окна': [],
          'Стоимость,руб': [], 'Планировка': [], 'Достоинства': [], 'Дата': []}
    columns = []
    samolet_keys = list(samolet.keys())
    for column in samolet_keys:
        columns.append(column)
    columns.remove('Стоимость,руб')
    columns.remove('Дата')
    date_today = datetime.datetime.today().strftime("%Y-%m-%d-%H-%M")
    for i in range(len(df_for_update)):
        url_update = df_for_update['url'].iloc[i]
        price_update = int(df_for_update['price'].iloc[i])
        apart = df[df['Ссылка']==url_update]
        if len(apart)==0:
            continue
        ind_apart = apart.index[-1]
        price_old = int(df.loc[ind_apart]['Стоимость,руб'])
        if price_old==price_update:
            continue
        else:
            samolet['Стоимость,руб'].append(price_update)
            samolet['Дата'].append(date_today)     
            for column in columns:
                samolet[column].append(df.loc[ind_apart][column]) 
    df_samolet_update = pd.DataFrame(samolet)
    return df_samolet_update 


def get_cpu_count():
    cpu_count = 4
    real_cpu_count = multiprocessing.cpu_count()
    if cpu_count >= real_cpu_count:
        cpu_count = real_cpu_count - 1
    return cpu_count


def parallel_get_info_new_flats(flats_list):
    if len(flats_list)==0:
        return pd.DataFrame(), []
    columns_df = ['ID', 'Квартира', 'Ссылка', 'Проект', 'Корпус',
 'Секция', 'Этаж/Этажность', 'Жилая площадь,м2', 'Общая площадь,м2',
 'Номер квартиры', 'Номер планировки', 'Тип планировки','Тип',
 'Евро-планировка','Заселение до','Вид из окна','Стоимость,руб',
 'Планировка','Достоинства','Дата']
    cpu_count = get_cpu_count()
    result_get_info_flats = Pool(cpu_count).map(get_flat_info,flats_list)
    list_flats_info = []
    empty_info_flats = []
    for res in result_get_info_flats:
        if (len(res[1])==0) or (len(res[1])!=20):
            empty_info_flats.append(res[0])
        else:
            list_flats_info.append(res[1])
    df_samolet_new = pd.DataFrame(list_flats_info,columns = columns_df)
    df_samolet_new = df_samolet_new.dropna(subset=['Стоимость,руб'], how='all').reset_index(drop = True)
    return df_samolet_new, empty_info_flats


def sequential_get_info_new_flats(flats_list):
    if len(flats_list)==0:
        return pd.DataFrame(), []
    columns_df = ['ID', 'Квартира', 'Ссылка', 'Проект', 'Корпус',
 'Секция', 'Этаж/Этажность', 'Жилая площадь,м2', 'Общая площадь,м2',
 'Номер квартиры', 'Номер планировки', 'Тип планировки','Тип',
 'Евро-планировка','Заселение до','Вид из окна','Стоимость,руб',
 'Планировка','Достоинства','Дата']
    result_get_info_flats = []
    for i in range(len(flats_list)):
        res = get_flat_info(flats_list[i])
        time.sleep(1)
        result_get_info_flats.append(res)
    list_flats_info = []
    empty_info_flats = []
    for res in result_get_info_flats:
        if (len(res[1])==0) or (len(res[1])!=20):
            empty_info_flats.append(res[0])
        else:
            list_flats_info.append(res[1])
    df_samolet_new = pd.DataFrame(list_flats_info,columns = columns_df)
    df_samolet_new = df_samolet_new.dropna(subset=['Стоимость,руб'], how='all').reset_index(drop = True)
    return df_samolet_new, empty_info_flats


def get_flat_info(flat):
    samolet = {'ID': np.nan, 'Квартира': np.nan, 'Ссылка': np.nan, 'Проект': np.nan, 'Корпус': np.nan, 'Секция': np.nan, 'Этаж/Этажность': np.nan,
          'Жилая площадь,м2': np.nan, 'Общая площадь,м2': np.nan, 'Номер квартиры': np.nan, 'Номер планировки': np.nan,
          'Тип планировки': np.nan, 'Тип': np.nan, 'Евро-планировка': np.nan, 'Заселение до': np.nan, 'Вид из окна': np.nan,
          'Стоимость,руб': np.nan, 'Планировка': np.nan, 'Достоинства': np.nan, 'Дата': np.nan}
    try:
        browser = webdriver.Chrome(PATH, options=opt)
        browser.implicitly_wait(10) 
        browser.get(flat)   
        # информация о квартире
        try:
            text = browser.page_source
            page_soup = soup(text , "html")
            labels = page_soup.findAll("div", {"class": "r-flat-d-aside__row-name"})
            info = page_soup.findAll("div", {"class": "r-flat-d-aside__row-val"})
            for i in range(len(info)):
                try:
                    el_label = labels[i].text
                    el_info = info[i].text
                    if 'Заселение до' in el_label:
                        samolet['Заселение до'] = el_info
                    if 'Проект' in el_label:
                        samolet['Проект'] = el_info
                    if 'Этаж' in el_label:
                        #print(el_info)
                        el_info = el_info.replace('\n', '')
                        el_info = el_info.replace(' ', '')
                        el_info = el_info.replace('из', '/')
                        samolet['Этаж/Этажность'] = el_info
                    if 'Номер квартиры' in el_label:
                        samolet['Номер квартиры'] = el_info
                    if 'Тип планировки' in el_label:
                        samolet['Тип планировки'] = el_info
                    if 'Типовая группа' in el_label:
                        samolet['Тип'] = el_info
                except Exception as e:
                    continue
        except Exception as e:
            print(e)
            browser.quit()
            return [flat,[]]
        if 'ordering' in flat:
            flat = flat.rsplit('/', 1)[0] + '/'
        samolet['Ссылка'] = flat
        # тип квартиры  
        flat_type = browser.find_element_by_xpath('//h2[@class="r-flat-head__title"]')
        samolet['Квартира'] = flat_type.text.split(' ')[0]
        str_flat_type = flat_type.text.replace(' квартира', '').replace(' планировка', '')
        square = str_flat_type.split(' ', 1)[1]
        if ' м²' in square:
            square = square.replace(' м²','')
            square = square.replace(',','.')
            samolet['Общая площадь,м2'] = str(float(square))
        # цена квартиры        
        price = browser.find_element_by_xpath('//div[@class="r-flat-head__price"]')
        str_price = price.text
        int_price = str_price.replace(' ', '').replace('₽', '')
        samolet['Стоимость,руб'] = str(int(int_price))        
        # ссылка на планировку 
        try:
            link = browser.find_element_by_xpath('//img[@class="r-flat-d-plans__layout-image"]')
            samolet['Планировка'] = link.get_attribute('src') 
        except Exception as e:
            pass
        samolet['ID'] = (str(samolet['Проект']).replace(' ', '') + '-' + str(samolet['Квартира']) + 
                      '-' + str(samolet['Корпус']) + '-' + str(samolet['Секция']) + 
                      '-' + str(samolet['Этаж/Этажность']).replace(' ', '') + '-' + str(samolet['Номер квартиры']))
        samolet['Дата'] = datetime.datetime.today().strftime("%Y-%m-%d-%H-%M")
        row = list(samolet.values())
        browser.quit()
        return [flat,row] 
    except Exception as e:
        print('Новые квартиры')
        print(e)
        browser.quit()
        return [flat,[]]


def samolet_parsing_flats_all_jobs():
    try:
        send_message_to_telegram('Начало парсинга Samolet') 
        df_extract = get_data()
        if len(df_extract)==0:
            send_message_to_telegram('Ошибка парсинга проектов')
            return
        df_extract = df_extract.drop_duplicates(subset=['url'], keep='last').reset_index(drop=True)
        df_extract = df_extract.dropna(subset=['url'], how='all').reset_index(drop = True)
        send_message_to_telegram('Всего квартир на данный момент ' +str(len(df_extract)))
        df_samolet_init = pd.read_excel('samolet_flats.xlsx')
        send_message_to_telegram('В базе на данный момент ' +str(len(df_samolet_init)))
        intersection_in = df_extract['url'].isin(df_samolet_init['Ссылка'])
        intersection_not_in = ~intersection_in
        df_in = df_extract[intersection_in]
        df_not_in = df_extract[intersection_not_in]
        num_exist_flats = len(df_in)
        num_new_flats = len(df_not_in)
        send_message_to_telegram('Число распарсенных квартир, которые имеются в датасете ' +str(num_exist_flats))
        send_message_to_telegram('Число новых квартир для добавления в датасет ' +str(num_new_flats))        
        df_samolet_update  = update_price(df_samolet_init, df_in)
        send_message_to_telegram('Число квартир с обновлением цены ' +str(len(df_samolet_update)))
        #новые квартиры для добавления в датасет
        flats_list= list(df_not_in['url'])
        df_samolet_new_1, empty_info_flats_1 = parallel_get_info_new_flats(flats_list)
        df_samolet_new_2, empty_info_flats_2 = sequential_get_info_new_flats(empty_info_flats_1)
        df_samolet_new = pd.concat([df_samolet_new_1, df_samolet_new_2], ignore_index=True)
        send_message_to_telegram('Обработано новых квартир ' +str(len(df_samolet_new)) + ' из ' + str(num_new_flats))
        df_to_db = pd.concat([df_samolet_update, df_samolet_new], ignore_index=True)
        send_message_to_telegram('Всего добавлено в базу ' +str(len(df_to_db)))
        df_all = pd.concat([df_samolet_init, df_to_db], ignore_index=True)
        df_all.to_excel('samolet_flats.xlsx', index = False)
    except Exception as e:
        send_message_to_telegram(e)
