import time

import pandas as pd
import requests as rq
from bs4 import BeautifulSoup
import datetime
import logging
import threading as thr

date_threshold = datetime.datetime.now() - datetime.timedelta(14)
url = "https://www.prnewswire.com/news-releases/news-releases-list/?page={}&pagesize=100"
logging.basicConfig(filename="StockIt.log")
all_articles = []
stop_flag = False

def store_data():
    try:
        articles_df = pd.DataFrame(all_articles)
        articles_df = articles_df.drop_duplicates(keep="first")
        print(articles_df)
        articles_df.to_csv("test.csv", index=False)
    except Exception as e:
        print(e)
        logging.exception("Exception store_data\n", exc_info=True)


def parse_page(page_html):
    try:
        global all_articles, stop_flag
        soup = BeautifulSoup(page_html, features="html.parser")
        news_articles = soup.findAll('a', class_='newsreleaseconsolidatelink')
        for article in news_articles:
            try:
                article_data = {"link":article['href'], "header": "", "time": None, "thumbnail": "", "summary": ""}
                try:
                    date_tag = article.select("h3 > small")[0].text
                    article_data["time"] = datetime.datetime.strptime(date_tag, "%b %d, %Y, %H:%M ET")
                except Exception as e:
                    today = datetime.datetime.today()
                    article_data["time"] = datetime.datetime.strptime(date_tag, "%H:%M ET")
                    article_data["time"] = article_data["time"].replace(year=today.year,month=today.month,day=today.day)
                if article_data["time"] < date_threshold:
                    stop_flag = True
                    break
                article_data["time"] = datetime.datetime.strftime(article_data["time"], "%b %d %Y %H:%M:%S")
                articles_images = article.findAll("img")
                if len(articles_images) > 0:
                    article_data["thumbnail"] = article.findAll("img")[0]['src']
                article_data["header"] = str(article.select("h3")[0].text).split("\n")[2]
                article.small.decompose()
                article_data["summary"] = article.select("p")[0].text
                all_articles.append(article_data)
            except Exception as e:
                print(e)
                logging.exception("Exception article read!\n",exc_info=True)
    except Exception as e:
        print(e)
        logging.exception("Exception parse_page", exc_info=True)


def collect_articles(page_no_):
    try:
        attempts = 0
        while attempts < 2:
            try:
                temp = url.format(page_no_)
                logging.info(temp)
                result = rq.get(temp)
                if result.status_code == 200:
                    print("Collection complete")
                    open("jg.html", "w", encoding="utf-8").write(result.text)
                    parse_page(result.text)
                    break
                else:
                    logging.info("For page number {}, response received: {}\n".format(page_no_, str(result)))
            except Exception as e:
                print(e)
                logging.exception("Exception collect_articles\n", exc_info=True)
                attempts += 1
        if attempts > 5:
            logging.error("Could not read page number: {}".format(page_no_))
    except Exception as e:
        print(e)
        logging.exception("Exception collect_articles", exc_info=True)


if __name__ == "__main__":
    try:
        page_no = 1
        while not stop_flag:
            thread = thr.Thread(target=collect_articles, args=(page_no,))
            while thr.active_count() > 20:
                time.sleep(10)
            print("Starting Thread {}".format(page_no))
            page_no += 1
            thread.start()
        while thr.active_count() != 1:
            time.sleep(10)
        store_data()
    except Exception as e:
        print(e)
        logging.exception("main exception!", exc_info=True)
