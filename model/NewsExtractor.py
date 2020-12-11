import json
import logging
import sys
from os import listdir, mkdir
from os.path import isfile, join, isdir

import pandas as pd
import requests
from bs4 import BeautifulSoup


class NewsExtractor:
    URL_TIMEOUT = 60

    def __init__(self, page_count=20, data_dir="."):
        urls = NewsExtractor.collect_news("https://koronavirus.gov.hu/hirek", page_count)
        category = "data"
        for category in ["data", "other", "noimg"]:
            news_array = NewsExtractor.collect_all_content(urls[category])
            news_df = pd.DataFrame(news_array)
            news_df.apply(lambda row: NewsExtractor.save_to_file(row, category, data_dir), axis=1)
            print("{}: {}".format(category, len(news_array)))

    @staticmethod
    def collect_news(url, level=80):
        """ Collect the urls, title of the news """
        try:
            news = {"data": [], "other": [], "noimg": []}
            page_news = requests.get(url, timeout=NewsExtractor.URL_TIMEOUT)
            soup_news = BeautifulSoup(page_news.content, 'html.parser')
            rows_news = soup_news.select("div.article-teaser a")
            url_base = "https://koronavirus.gov.hu"
            for item in rows_news:
                if "/cikkek" in item["href"]:
                    base_img = url_base + "/sites/default/files/styles/large/public/aktualis_adatok"
                    if len(item.select("img")) > 0:
                        adat_flag = (base_img in item.select("img")[0]["src"])
                        if adat_flag:
                            news["data"].append(url_base + item["href"])
                        else:
                            news["other"].append(url_base + item["href"])
                    else:
                        news["noimg"].append(url_base + item["href"])

            next_page = soup_news.select("ul.pagination li.next a")
            if level > 0 and len(next_page) > 0:
                next_level = NewsExtractor.collect_news(url_base + next_page[0]["href"], level - 1)
                news["data"] = news["data"] + next_level["data"]
                news["other"] = news["other"] + next_level["other"]
                news["noimg"] = news["noimg"] + next_level["noimg"]
            return news
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout):
            print("News request timeout")
            return {"data": [], "other": [], "noimg": []}

    @staticmethod
    def collect_all_content(urls):
        """ Collect the content of the news (all article from the url list) """
        news_array = []
        for url in urls:
            news_array.append(NewsExtractor.collect_news_content(url))
        return news_array

    @staticmethod
    def collect_news_content(url):
        """ Collect the content of one article """
        # print(url)
        try:
            content = {"url": url, "h1": "", "date": "", "body": ""}
            page_news = requests.get(url, timeout=NewsExtractor.URL_TIMEOUT)
            soup_news = BeautifulSoup(page_news.content, 'html.parser')
            if len(soup_news.select("div.container h1")) > 0:
                content["h1"] = soup_news.select("div.container h1")[0]
            if len(soup_news.select("div.container p i")) > 0:
                content["date"] = soup_news.select("div.container p i")[0]
            if len(soup_news.select("div.page_body")) > 0:
                content["body"] = soup_news.select("div.page_body")[0]
            return content
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout):
            print("News details request timeout")
            return {"url": url, "h1": "", "date": "", "body": ""}

    @staticmethod
    def save_to_file(row, category, data_dir):
        """ Save the data to a json file """
        date = row["date"].text.replace(",", "").replace(".", "")
        date_array = date.split(" ")
        date_array[1] = date_array[1].replace("december", "12")
        date_array[1] = date_array[1].replace("november", "11")
        date_array[1] = date_array[1].replace("október", "10")
        date_array[1] = date_array[1].replace("szeptember", "09")
        date_array[1] = date_array[1].replace("augusztus", "08")
        date_array[1] = date_array[1].replace("július", "07")
        date_array[1] = date_array[1].replace("június", "06")
        date_array[1] = date_array[1].replace("május", "05")
        date_array[1] = date_array[1].replace("április", "04")
        date_array[1] = date_array[1].replace("március", "03")
        if len(date_array[2]) == 1:
            date_array[2] = "0{}".format(date_array[2])
        date_array.pop(3)
        date = "{}-{}-{} {}".format(date_array[0], date_array[1], date_array[2], date_array[3])
        month = "{}-{}".format(date_array[0], date_array[1])
        url = row["url"]
        h1 = row["h1"].text
        body = row["body"].text
        if not isdir("{}/news_hu/{}".format(data_dir, month)):
            mkdir("{}/news_hu/{}".format(data_dir, month))
        filename = "{}/news_hu/{}/{}_{}.json".format(data_dir, month, category, date)
        with open(filename, 'w') as f:
            json_object = [{"date": date, "url": url, "h1": h1, "body": body}]
            json.dump(json_object, f)
        return filename


class NewsReader:
    def __init__(self, data_dir, month="2020-05", date="2020-05-01", string_filter=True):
        ColoredFormatter.init()
        my_path = "{}/news_hu/{}".format(data_dir, month)
        only_files = [f for f in listdir(my_path) if isfile(join(my_path, f))]
        dfs = []
        for file in only_files:
            dfs.append(NewsReader.read_from_file("{}/{}".format(my_path, file)))
        pd.concat(dfs).sort_values(["date"]).apply(
            lambda row: self.show(row["date"], row["url"], row["body"], date, string_filter),
            axis=1
        )
        self.repartition = pd.concat(dfs).sort_values(["date"])

    @staticmethod
    def show(date, url, body, selected_date, string_filter=True):
        my_date = pd.Timestamp(selected_date)
        string1 = "Újabb magyar állampolgároknál"
        string2 = "szorulnak lélegeztetőgépre"
        string3 = "ápolnak"
        string4 = "lélegeztetőgépen"
        if my_date < date < my_date + pd.DateOffset(1) \
                and (not string_filter or string1 in body or string2 in body or string3 in body or string4 in body):
            logging.info(date)
            logging.info(url)
            body_formatted_1 = body.replace(
                string1,
                ColoredFormatter.str_color("underline", string1)
            )
            body_formatted_2 = body_formatted_1.replace(
                string2,
                ColoredFormatter.str_color("underline", string2)
            )
            body_formatted_3 = body_formatted_2.replace(
                string3,
                ColoredFormatter.str_color("underline", string3)
            )
            body_formatted_4 = body_formatted_3.replace(
                string4,
                ColoredFormatter.str_color("underline", string4)
            )
            logging.info(body_formatted_4)
        return False

    @staticmethod
    def read_from_file(filename):
        """ Read one saved file to a data frame """
        return pd.read_json(filename)


class ColoredFormatter:
    colors = {
        'pink': '\033[95m',
        'blue': '\033[94m',
        'green': '\033[92m',
        'yellow': '\033[93m',
        'red': '\033[91m',
        'ENDC': '\033[0m',
        'bold': '\033[1m',
        'underline': '\033[4m'
    }

    @staticmethod
    def init():
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    @staticmethod
    def str_color(color, data):
        return ColoredFormatter.colors[color] + str(data) + ColoredFormatter.colors['ENDC']

# USE:
# from NewsExtractor import NewsExtractor, NewsReader
# NewsExtractor(2)
# NewsReader(month="2020-05", date="2020-05-09")
