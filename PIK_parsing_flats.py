from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from multiprocessing.pool import Pool
import multiprocessing
from bs4 import BeautifulSoup as soup
import pandas as pd
import time
import datetime
import numpy as np
from others import PATH, send_message_to_telegram

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--incognito')
chrome_options.add_argument('--disable-dev-shm-usage') 

    
def get_list_projects():
    try:
        browser = webdriver.Chrome(options = chrome_options, executable_path=PATH)
        browser.implicitly_wait(10)
        browser.get("https://www.pik.ru/projects")   
        time.sleep(2)
        #название класса html элемента может поменяться (раз-два в месяц)
        #поэтому нужно вручную проверять html код страницы
        #чтобы найти актуальное название класса
        project = browser.find_elements_by_xpath('//a[@class="sc-clYhRO jmfOeA"]')
        projects = []
        for i in range(len(project)):
            url = project[i].get_attribute('href')
            #заменяем ссылки, чтобы сразу попасть на страницу с квартирами
            url = url.replace('https://www.pik.ru/', 'https://www.pik.ru/search/')
            projects.append(url)
        # не забываем закрывать браузер после парсинга,
        # чтобы не допускать утечки памяти
        browser.quit()
        return projects
    except Exception as e:
        print(e)
        browser.quit()
        return []


#извлекаем цену из общей таблицы квартир по ссылке на проект
#для добавления новых цен имеющихся квартир в датасете
def get_price(link, page_soup):
    try:
        cut_link = link.replace('https://www.pik.ru','')
        current_html_code = page_soup.select("a[href*='{}']".format(cut_link))
        if len(current_html_code)==0:
            return None
        else:
            # название класса html элемента может поменяться
            str_price = current_html_code[0].find('div',{"class": "sc-eetwQk fcyuGR"}).getText()
            str_price = str_price.replace(' ','')
            str_price = str_price.replace('₽','')
            str_price = str_price.replace('или','')
            price = int(str_price)
            return price
    except Exception as e:
        print(e)
        return None


def get_flats(project):
    try:
        browser = webdriver.Chrome(PATH, options=chrome_options)
        flats = []
        time.sleep(2)
        browser.implicitly_wait(10)
        browser.get(project)
        # скроллим страницу до конца, что бы открыть все квартиры
        time.sleep(2)
        len_of_page = browser.execute_script("window.scrollTo(0, document.body.scrollHeight);                                         var lenOfPage=document.body.scrollHeight;return lenOfPage;")
        match = False
        while(match == False):
            last_count = len_of_page
            time.sleep(5)
            browser.implicitly_wait(10)
            len_of_page = browser.execute_script("window.scrollTo(0, document.body.scrollHeight);                                                var lenOfPage=document.body.scrollHeight;return lenOfPage;")
            if last_count == len_of_page:
                match = True
        flat_elements = browser.find_elements_by_xpath('//a[@class="sc-bWFPNQ fMFpUc"]')
        text = browser.page_source
        page_soup = soup(text , "html")
        for link in range(len(flat_elements)):
            url = flat_elements[link].get_attribute('href')
            price = get_price(url, page_soup)
            flats.append([url, price])
        browser.quit()
        df_extract = pd.DataFrame(flats, columns = ['url','price'])
        print(len(df_extract))
        if len(df_extract)==50:
            return [project, []]
        return [project,df_extract]
    except Exception as e:
        browser.quit()
        print(e)
        return [project, []]


def get_cpu_count():
    cpu_count = 4
    real_cpu_count = multiprocessing.cpu_count()
    if cpu_count >= real_cpu_count:
        cpu_count = real_cpu_count - 1
    return cpu_count


def parallel_get_url_flats(projects):
    if len(projects)==0:
        return pd.DataFrame(), []
    cpu_count = get_cpu_count()
    #первый прогон (параллельная обработка)
    result_get_flats = Pool(cpu_count).map(get_flats,projects)
    flats_1 = pd.DataFrame()
    empty_projects = []
    for res in result_get_flats:
        if len(res[1])==0:
            empty_projects.append(res[0])
        else:
            flats_1 = pd.concat([flats_1, res[1]], ignore_index=True)
    #второй прогон для необработанных проектов
    result_get_flats = []
    for i in range(len(empty_projects)):
        res = get_flats(empty_projects[i])
        result_get_flats.append(res)
    flats_2 = pd.DataFrame()
    empty_projects_2 = []
    for res in result_get_flats:
        if len(res[1])==0:
            empty_projects_2.append(res[0])
        else:
            flats_2 = pd.concat([flats_2, res[1]], ignore_index=True)
    flats = pd.concat([flats_1, flats_2], ignore_index=True)
    if len(flats)!=0:
        flats = flats.drop_duplicates(subset=['url'], keep='last').reset_index(drop=True)
        flats = flats.dropna(subset=['price'], how='all').reset_index(drop = True)
    return flats, empty_projects_2


def sequential_get_url_flats(projects):
    if len(projects)==0:
        return pd.DataFrame(), []
    result_get_flats = []
    for i in range(len(projects)):
        res = get_flats(projects[i])
        result_get_flats.append(res)
    flats = pd.DataFrame()
    empty_projects = []
    for res in result_get_flats:
        if len(res[1])==0:
            empty_projects.append(res[0])
        else:
            flats = pd.concat([flats, res[1]], ignore_index=True)
    if len(flats)!=0:
        flats = flats.drop_duplicates(subset=['url'], keep='last').reset_index(drop=True)
        flats = flats.dropna(subset=['price'], how='all').reset_index(drop = True)
    return flats, empty_projects


def update_price(df, df_for_update):
    pik = {'Дата': [], 'ID': [], 'Квартира': [], 'Ссылка': [], 'Проект': [], 'Стоимость': [], 'В ипотеку': [], 'Площадь,м2': [], 'Этаж': [], 'Корпус': [], 'Артикул': [], 'Номер на этаже': [], 'Секция': [], 'Тип': [], 'Заселение до': [], 'PDF': []}
    columns = []
    pik_keys = list(pik.keys())
    for column in pik_keys:
        columns.append(column)
    columns.remove('Стоимость')
    columns.remove('Дата')
    date_today = datetime.datetime.today().strftime("%Y-%m-%d-%H-%M")
    for i in range(len(df_for_update)):
        url_update = df_for_update['url'].iloc[i]
        price_update = int(df_for_update['price'].iloc[i])
        apart = df[df['Ссылка']==url_update]
        if len(apart)==0:
            continue
        ind_apart = apart.index[-1]
        price_old = int(df.loc[ind_apart]['Стоимость'])
        if price_old==price_update:
            continue
        else:
            pik['Стоимость'].append(price_update)
            pik['Дата'].append(date_today)     
            for column in columns:
                pik[column].append(df.loc[ind_apart][column]) 
    df_pik_update = pd.DataFrame(pik)
    return df_pik_update


def parallel_get_info_new_flats(flats_list):
    if len(flats_list)==0:
        return pd.DataFrame(), []
    col1 = ['Дата', 'ID', 'Квартира', 'Ссылка', 'Проект', 'Площадь,м2', 'PDF', 'Стоимость',
    'Этаж', 'Корпус', 'Номер на этаже', 'Секция', 'В ипотеку', 'Артикул', 'Тип','Заселение до']
    cpu_count = get_cpu_count()
    result_get_info_flats = Pool(cpu_count).map(update_flats, flats_list)
    list_flats_info = []
    empty_info_flats = []
    for res in result_get_info_flats:
        if (len(res[1])==0) or (len(res[1])!=16):
            empty_info_flats.append(res[0])
        else:
            list_flats_info.append(res[1])
    df_pik_new = pd.DataFrame(list_flats_info, columns = col1)
    df_pik_new = df_pik_new.dropna(subset=['Стоимость'], how='all').reset_index(drop = True)
    return df_pik_new, empty_info_flats


def sequential_get_info_new_flats(flats_list):
    if len(flats_list)==0:
        return pd.DataFrame(), []
    col1 = ['Дата', 'ID', 'Квартира', 'Ссылка', 'Проект', 'Площадь,м2', 'PDF', 'Стоимость',
    'Этаж', 'Корпус', 'Номер на этаже', 'Секция', 'В ипотеку', 'Артикул', 'Тип','Заселение до']
    result_get_info_flats = []
    for i in range(len(flats_list)):
        res = update_flats(flats_list[i])
        time.sleep(1)
        result_get_info_flats.append(res)
    list_flats_info = []
    empty_info_flats = []
    for res in result_get_info_flats:
        if (len(res[1])==0) or (len(res[1])!=16):
            empty_info_flats.append(res[0])
        else:
            list_flats_info.append(res[1])
    df_pik_new = pd.DataFrame(list_flats_info, columns = col1)
    df_pik_new = df_pik_new.dropna(subset=['Стоимость'], how='all').reset_index(drop = True)
    return df_pik_new, empty_info_flats


def update_flats(url_flat):
    try:
        browser = webdriver.Chrome(PATH, options=chrome_options)
        dict_flat_info = {'Стоимость': np.nan, 'Этаж': np.nan, 'Корпус': np.nan, 'Номер на этаже': np.nan, 'Секция': np.nan, 'В ипотеку': np.nan, 'Артикул': np.nan, 'Тип': np.nan, 'Заселение': np.nan}
        ID = []
        row = [] 
        browser.implicitly_wait(10)
        browser.get(url_flat)
        time.sleep(2)
        project = browser.find_element_by_xpath('//div[@class="ComplexMenu-i "]')
        text = browser.page_source
        page_soup = soup(text , "html")
        elements = page_soup.findAll("div", {"class": "styles__FlatInfoItem-sc-3fbqpu-0 bsBRUv"})
        list_keys = dict_flat_info.keys()
        for el in elements:
            key = el.findAll("span", {"class": "sc-bdfBQB cInRnc Typography"})[0].getText()
            if key in list_keys:
                value = el.findAll("span", {"class": "sc-bdfBQB cUpDDV Typography"})[0].getText()
                dict_flat_info[key] = value
        str_price = page_soup.findAll("div", {"class": "styles__FlatInfoPrice-sc-6f1dkk-2 kXpOqE"})[0].getText()
        price = str(int(str_price.replace(' ', '').replace('₽', '')))
        dict_flat_info['Стоимость'] = price
        pdf_link = page_soup.findAll("a", {"type": "pdf"})[0]['href']
        flat = page_soup.findAll("span", {"class": "sc-bdfBQB cInRnc Typography"})[0].getText()
        # формируем id по данным из функции label_info_id
        ID.append(((project.text.split('\n')[0]).replace(' ', '') + '-' + flat.split(' ')[0] + 
                          '-' + str(dict_flat_info['Корпус']) + '-' + str(dict_flat_info['Секция']) + '-' + 
                           str(dict_flat_info['Этаж']).replace(' ', '') + '-' + 
                   str(dict_flat_info['Номер на этаже'])))   
        row.append(datetime.datetime.today().strftime("%Y-%m-%d-%H-%M"))
        row.append(ID[0])     
        row.append(flat.split(' ')[0])  # квартира
        row.append(url_flat) # ссылка
        row.append(project.text.split('\n')[0])  # проект
        row.append(flat.split(' ')[1])  # площадь 
        row.append(pdf_link)    # pdf
        # стоимость, этаж, корпус, номер на этаже
        # секция, в ипотеку, артикул, тип, дата заселения
        for label in dict_flat_info:  
            row.append(dict_flat_info[label])
        return [url_flat,row]
    except Exception as e:
        print(e)
        browser.quit()
        return [url_flat,[]]


def pik_parsing_flats_all_jobs():
    try:
        send_message_to_telegram('Начало парсинга PIK') 
        projects = get_list_projects()
        if len(projects)==0:
            projects = get_list_projects()
        num_projects = len(projects)
        if num_projects==0:
            send_message_to_telegram('Ошибка парсинга проектов')
            return
        else:
            send_message_to_telegram('Всего проектов для парсинга ' + str(num_projects)) 
        flats_1, empty_projects_1 = parallel_get_url_flats(projects)
        flats_2, empty_projects_2 = sequential_get_url_flats(empty_projects_1)
        flats = pd.concat([flats_1, flats_2], ignore_index=True)
        num_processing_projects = num_projects - len(empty_projects_2)
        send_message_to_telegram('Обработано проектов ' +str(num_processing_projects) + ' из ' + str(num_projects))
        send_message_to_telegram('Всего квартир на данный момент ' +str(len(flats)))
        df_pik_init = pd.read_excel('pik.ru.xlsx')
        send_message_to_telegram('В базе на данный момент ' +str(len(df_pik_init)))
        intersection_in = flats['url'].isin(df_pik_init['Ссылка'])
        intersection_not_in = ~intersection_in
        df_in = flats[intersection_in]
        df_not_in = flats[intersection_not_in]
        num_exist_flats = len(df_in)
        num_new_flats = len(df_not_in)
        send_message_to_telegram('Число распарсенных квартир, которые имеются в датасете ' +str(num_exist_flats))
        send_message_to_telegram('Число новых квартир для добавления в датасет ' +str(num_new_flats))
        df_pik_update = update_price(df_pik_init, df_in)
        send_message_to_telegram('Число квартир с обновлением цены ' +str(len(df_pik_update)))
        flats_list = list(df_not_in['url'])
        time.sleep(5)
        df_pik_new_1, empty_info_flats_1 = parallel_get_info_new_flats(flats_list)
        df_pik_new_2, empty_info_flats_2 = sequential_get_info_new_flats(empty_info_flats_1)
        df_pik_new = pd.concat([df_pik_new_1, df_pik_new_2], ignore_index=True)
        if len(df_pik_new)!=0:
            df_pik_new = df_pik_new[['Дата', 'ID', 'Квартира', 'Ссылка', 'Проект', 'Стоимость', 'В ипотеку', 'Площадь,м2', 'Этаж', 'Корпус', 'Артикул','Номер на этаже', 'Секция', 'Тип', 'Заселение до', 'PDF']]
        send_message_to_telegram('Обработано новых квартир ' +str(len(df_pik_new)) + ' из ' + str(num_new_flats))
        df_to_db = pd.concat([df_pik_update, df_pik_new], ignore_index=True)
        send_message_to_telegram('Всего добавлено в базу ' +str(len(df_to_db)))
        df_all = pd.concat([df_pik_init, df_to_db], ignore_index=True)
        df_all.to_excel('pik.ru.xlsx', index = False)
    except Exception as e:
        send_message_to_telegram(e)
