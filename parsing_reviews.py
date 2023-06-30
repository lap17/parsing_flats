import pandas as pd
import datetime
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from others import PATH, RAPID_API, send_message_to_telegram
import multiprocessing
import time
from multiprocessing.pool import Pool

chrome_options = Options()
chrome_options.add_argument("start-maximized")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)
chrome_options.add_argument("--headless")
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument("--disable-blink-features=AutomationControlled")


def get_cpu_count():
    #по умолчанию возьмем 4 ядра
    cpu_count = 4
    real_cpu_count = multiprocessing.cpu_count()
    if cpu_count >= real_cpu_count:
        cpu_count = real_cpu_count - 1
    return cpu_count


def get_new_data(df_old, df_new, column_name, table_name):
    send_message_to_telegram('В '+ table_name +' всего распарсено данных по отзывам ' + str(len(df_new)))
    intersection_in = df_new[column_name].isin(df_old[column_name])
    intersection_not_in = ~intersection_in
    df_not_in = df_new[intersection_not_in]
    return df_not_in


def all_jobs_parsing_reviews():
    try:
        df_avaho_init = pd.read_excel("dataset_review_avaho.xlsx")
        df_cian_init = pd.read_excel("dataset_review_cian.xlsx")
        df_mskguru_init = pd.read_excel("dataset_review_mskguru.xlsx")
        df_novostroy_m_init = pd.read_excel("dataset_review_novostroy_m.xlsx")
        send_message_to_telegram('Начало парсинга отзывов')
        projects = get_list_projects()
        num_projects = len(projects)
        send_message_to_telegram('Всего проектов для парсинга ' + str(num_projects))
        cpu_count = get_cpu_count()
        result_get_reviews = Pool(cpu_count).map(all_jobs_project,projects)
        df_dataset_avaho_new, df_dataset_cian_new, df_dataset_novostroy_m_new, df_dataset_mskguru_new, count = processing_results(result_get_reviews)
        send_message_to_telegram('Всего распарсено ссылок на сайты отзывов ' + str(num_projects*4 - count))
        df_to_db_avaho = get_new_data(df_avaho_init, df_dataset_avaho_new, 'text_review', 'avaho_rev_table')
        df_to_db_cian = get_new_data(df_cian_init, df_dataset_cian_new, 'list_reviews', 'cian_rev_table')
        df_to_db_novostroy_m = get_new_data(df_novostroy_m_init, df_dataset_novostroy_m_new, 'text_review', 'novostroy_m_rev_table')
        df_to_db_mskguru = get_new_data(df_novostroy_m_init, df_mskguru_init, df_dataset_mskguru_new, 'list_reviews', 'mskguru_rev_table')
        df_all_avaho = pd.concat([df_avaho_init, df_to_db_avaho], ignore_index=True)
        df_all_avaho.to_excel("dataset_review_avaho.xlsx", index = False)
        df_all_cian = pd.concat([df_cian_init, df_to_db_cian], ignore_index=True)
        df_all_cian.to_excel("dataset_review_cian.xlsx", index = False)
        df_all_mskguru = pd.concat([df_mskguru_init, df_to_db_mskguru], ignore_index=True)
        df_all_mskguru.to_excel("dataset_review_mskguru.xlsx", index = False)
        df_all_novostroy_m = pd.concat([df_novostroy_m_init, df_to_db_novostroy_m], ignore_index=True)
        df_all_novostroy_m.to_excel("dataset_review_novostroy_m.xlsx", index = False)
    except Exception as e:
        send_message_to_telegram(e)


def get_list_projects():
    df_a101_init = pd.read_excel('a101_flats.xlsx')
    df_pik_init = pd.read_excel('pik.ru.xlsx')
    df_samolet_init = pd.read_excel('samolet_flats.xlsx')
    a101_projects = list(df_a101_init['Район'].unique())
    pik_projects = list(df_pik_init['Проект'].unique())
    samolet_projects = list(df_samolet_init['Проект'].unique())
    projects = a101_projects + pik_projects + samolet_projects
    projects = [x for x in projects if (x == x) & (x!=None)]
    return projects


#ограничение бесплатного API поиска в google - 600 запросов в месяц
#хватит для парсинга отзывов 2-3 раза в месяц
#подключить API можно на сайте rapidapi.com
def get_urls_by_query(query):
    try:
        url = "https://google-search3.p.rapidapi.com/api/v1/search/q="+query+"&num=100"
        headers = {
        'x-rapidapi-key': RAPID_API,
        'x-rapidapi-host': "google-search3.p.rapidapi.com"
        }
        response = requests.request("GET", url, headers=headers)
        json_file = response.json()
        res_list = json_file['results']
        list_url = []
        for res in res_list:
            url = res['link']
            list_url.append(url)
        return list_url
    except Exception as e:
        send_message_to_telegram(e)
        return []


def get_cian_link(list_url):
    res_url = ''
    for url in list_url:
        if ('cian.ru' in url) & ('otzyvy' in url):
            res_url = url
            break
    return res_url


def get_novostroy_m_link(list_url):
    res_url = ''
    for url in list_url:
        if ('novostroy-m' in url) & ('otzyvy' in url):
            res_url = url
            break
    return res_url


def get_avaho_link(list_url):
    res_url = ''
    for url in list_url:
        if ('avaho' in url) & ('otzyvy' in url):
            res_url = url
            break
    return res_url


def get_mskguru_link(list_url):
    res_url = ''
    for url in list_url:
        if ('mskguru' in url) & ('reviews' in url):
            res_url = url
            break
    return res_url


def get_list_url_by_site(project_name):
    list_url = get_urls_by_query('Жилищный комплекс '+ project_name +' отзывы')
    cian_link = get_cian_link(list_url)
    novostroy_m_link = get_novostroy_m_link(list_url)
    avaho_link = get_avaho_link(list_url)
    mskguru_link = get_mskguru_link(list_url)
    if cian_link == '':
        list_url = get_urls_by_query('cian Жилищный комплекс '+ project_name +' отзывы')
        cian_link = get_cian_link(list_url)
    if novostroy_m_link == '':
        list_url = get_urls_by_query('novostroy-m Жилищный комплекс '+ project_name +' отзывы')
        novostroy_m_link = get_novostroy_m_link(list_url)
    if avaho_link == '':
        list_url = get_urls_by_query('avaho Жилищный комплекс '+ project_name +' отзывы')
        avaho_link = get_avaho_link(list_url)
    if mskguru_link == '':
        list_url = get_urls_by_query('mskguru Жилищный комплекс '+ project_name +' отзывы')
        mskguru_link = get_mskguru_link(list_url)
    return cian_link, novostroy_m_link, avaho_link, mskguru_link


def get_reviews_by_url_cian(url):
    try:
        driver = webdriver.Chrome(PATH, options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Chrome/88.0.4324.96'})
        time.sleep(1)
        driver.implicitly_wait(10)
        driver.get(url)
        time.sleep(5.5)
        elem_rate = driver.find_element_by_class_name('_7c0dfc97a0--subtitle-list--3_h8N')
        time.sleep(1)
        str_rate = elem_rate.text
        str_rate = str_rate.split('\n')[-1]
        str_rate = str_rate[ : str_rate.find(" ")]
        str_rate = str_rate.replace(',','.')
        rate = float(str_rate)
        elem_reviews = driver.find_elements_by_class_name('review_component-container-xe88Xbat._7c0dfc97a0--review_container--376gW')
        time.sleep(1)
        list_reviews = []
        for review in elem_reviews:
            try:
                button = review.find_elements_by_class_name('review_component-review_view_all-8m9n8ST2')
                if len(button)!=0:
                    button[0].click()
                text_review = review.find_element_by_class_name('review_component-review-to0Y4YrX').text
                list_reviews.append(text_review)
            except Exception as e:
                print(e)
                continue
        driver.quit()
        df = pd.DataFrame(list_reviews, columns = ['list_reviews'])
        df['rate'] = rate
        return df
    except Exception as e:
        print(url)
        print(e)
        driver.quit()
        return pd.DataFrame()


def get_reviews_by_url_novostroy_m(url):
    try:
        driver = webdriver.Chrome(PATH, options=chrome_options)
        time.sleep(1)
        driver.implicitly_wait(10)
        driver.get(url)
        time.sleep(2)
        try:
            while True:
                button = driver.find_elements_by_class_name('js-load-more.download-more.def_btn')
                if len(button)==0:
                    break
                button[0].click()
                time.sleep(2)
        except Exception as e:
            pass
        list_reviews = []
        table_reviews = driver.find_elements_by_class_name('row.review_row')
        for review in table_reviews:
            try:
                html_rating_value_review = review.find_element_by_class_name('review_mark_line_item.box_only_bottom_md')
                rating_value_review = html_rating_value_review.find_element_by_css_selector("meta").get_attribute("content")
                html_text_review = review.find_element_by_class_name('review_description_line_item.box_only_bottom_lg.clearfix')
                text_review = html_text_review.text
                try:
                    lpluses = review.find_elements_by_class_name("review_plus_line_item.mb_10")
                    if len(lpluses)!=0:
                        pluses = lpluses[0].text
                    else:
                        pluses = ''
                except Exception as e:
                    pluses = ''
                try:
                    lminuses = review.find_elements_by_class_name("review_plus_line_item.clearfix.box_only_bottom_md")
                    if len(lminuses)!=0:
                        minuses = lminuses[0].text
                    else:
                        minuses = ''
                except Exception as e:
                    minuses = ''
                list_reviews.append([pluses, minuses, text_review, rating_value_review])
            except Exception as e:
                continue
        expert_rating = driver.find_element_by_xpath("//div[@itemprop='aggregateRating']").text
        expert_rating = expert_rating.replace(',','.')
        expert_rating = float(expert_rating)
        try:
            user_rating = driver.find_element_by_xpath("//div[@class='rait_star_item f_s_0']").text
            user_rating = user_rating.replace(',','.')
            user_rating = float(user_rating)
        except Exception as e:
            user_rating = ''
        driver.quit()
        df = pd.DataFrame(list_reviews, columns = ['pluses', 'minuses', 'text_review', 'rating_value_review'])
        df['expert_rating'] = expert_rating
        df['user_rating'] = user_rating
        return df
    except Exception as e:
        print(url)
        print(e)
        driver.quit()
        return pd.DataFrame()


def get_reviews_by_url_avaho(url):
    try:
        driver = webdriver.Chrome(PATH, options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Chrome/88.0.4324.96'})
        time.sleep(1)
        driver.implicitly_wait(10)
        driver.get(url)
        time.sleep(2)
        try:
            button = driver.find_element_by_class_name('btn.btn-lg.btn-primary')
            time.sleep(1)
            button.click()
            time.sleep(5)
        except Exception as e:
            pass
        list_reviews = []
        html_reviews = driver.find_element_by_class_name('mc-reviews')
        table_reviews = html_reviews.find_elements_by_xpath("//div[@itemprop='review']")
        for review in table_reviews:
            try:
                text_review = review.find_element_by_class_name('mc-review-body').text
                list_selectors = review.find_elements_by_css_selector("meta")
                rating_value_review = ''
                for selector in list_selectors:
                    if selector.get_attribute("itemprop")=='ratingValue':
                        rating_value_review = selector.get_attribute("content")
                list_reviews.append([text_review, rating_value_review])
            except Exception as e:
                continue
        user_rating = driver.find_element_by_xpath("//span[@class='mc-rating'][@itemprop='ratingValue']").text
        user_rating = float(user_rating)
        driver.quit()
        df = pd.DataFrame(list_reviews, columns = ['text_review', 'rating_value_review'])
        df['user_rating'] = user_rating
        return df
    except Exception as e:
        print(url)
        print(e)
        driver.quit()
        return pd.DataFrame()


def get_reviews_by_url_mskguru(url):
    try:
        driver = webdriver.Chrome(PATH, options=chrome_options)
        time.sleep(2)
        driver.implicitly_wait(10)
        driver.get(url)
        time.sleep(2)
        try:
            while True:
                button = driver.find_elements_by_class_name('button_g.load_more_comments')
                if len(button)==0:
                    break
                button[0].click()
                time.sleep(2)
        except Exception as e:
            pass
        comments_block = driver.find_element_by_id('comments_list')
        list_reviews = []
        table_reviews = comments_block.find_elements_by_class_name("text")
        for review in table_reviews:
            try:
                text_review = review.text
                list_reviews.append(text_review)
            except Exception as e:
                continue
        driver.quit()
        df = pd.DataFrame(list_reviews, columns = ['list_reviews'])
        return df
    except Exception as e:
        print(url)
        print(e)
        driver.quit()
        return pd.DataFrame()


def all_jobs_project(project_name):
    try:
        cian_link, novostroy_m_link, avaho_link, mskguru_link = get_list_url_by_site(project_name)
        if mskguru_link=='':
            reviews_mskguru = pd.DataFrame() 
        else:
            reviews_mskguru = get_reviews_by_url_mskguru(mskguru_link)
            if len(reviews_mskguru)!=0:
                reviews_mskguru['project'] = project_name
        if novostroy_m_link=='':
            reviews_novostroy_m = pd.DataFrame()
        else:
            reviews_novostroy_m = get_reviews_by_url_novostroy_m(novostroy_m_link)
            if len(reviews_novostroy_m)!=0:
                reviews_novostroy_m['project'] = project_name
        if avaho_link=='':
            reviews_avaho = pd.DataFrame()
        else:
            reviews_avaho = get_reviews_by_url_avaho(avaho_link)
            if len(reviews_avaho)!=0:
                reviews_avaho['project'] = project_name
        if cian_link=='':
            reviews_cian = pd.DataFrame()
        else:
            reviews_cian = get_reviews_by_url_cian(cian_link)
            if len(reviews_cian)!=0:
                reviews_cian['project'] = project_name
        return [reviews_cian, reviews_novostroy_m, reviews_mskguru, reviews_avaho]
    except Exception as e:
        print('error ' + project_name)
        print(e)
        return []


def processing_results(result_get_reviews):
    df_dataset_avaho_new = pd.DataFrame()
    df_dataset_cian_new = pd.DataFrame()
    df_dataset_novostroy_m_new = pd.DataFrame()
    df_dataset_mskguru_new = pd.DataFrame()
    count_empty = 0
    for i in range(len(result_get_reviews)):
        list_df_project = result_get_reviews[i]
        reviews_cian = list_df_project[0]
        if len(reviews_cian)==0:
            count_empty+=1
        reviews_novostroy_m = list_df_project[1]
        if len(reviews_novostroy_m)==0:
            count_empty+=1
        reviews_mskguru = list_df_project[2]
        if len(reviews_mskguru)==0:
            count_empty+=1
        reviews_avaho = list_df_project[3]
        if len(reviews_avaho)==0:
            count_empty+=1
        df_dataset_cian_new = pd.concat([df_dataset_cian_new, reviews_cian], ignore_index=True)
        df_dataset_novostroy_m_new = pd.concat([df_dataset_novostroy_m_new, reviews_novostroy_m], ignore_index=True)
        df_dataset_mskguru_new = pd.concat([df_dataset_mskguru_new, reviews_mskguru], ignore_index=True)
        df_dataset_avaho_new = pd.concat([df_dataset_avaho_new, reviews_avaho], ignore_index=True)
    str_date = datetime.datetime.today().strftime("%Y-%m-%d-%H-%M")
    if len(df_dataset_avaho_new)!=0:
        df_dataset_avaho_new['date'] = str_date
    if len(df_dataset_cian_new)!=0:
        df_dataset_cian_new['date'] = str_date
    if len(df_dataset_novostroy_m_new)!=0:
        df_dataset_novostroy_m_new['date'] = str_date
    if len(df_dataset_mskguru_new)!=0:
        df_dataset_mskguru_new['date'] = str_date
    return df_dataset_avaho_new, df_dataset_cian_new, df_dataset_novostroy_m_new, df_dataset_mskguru_new, count_empty
