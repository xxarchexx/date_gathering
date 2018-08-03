import logging
import sys
import pandas as pd
import matplotlib
import sqlite3
from scrappers.scrapper import Scrapper
from storages.storage import Store

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SCRAPPED_FILE = 'scrapped_data.txt'
TABLE_FORMAT_FILE = 'data.csv'


def gather_process(pageCounts):
    logger.info("gather")    
    scrapper = Scrapper(pageCounts)
    scrapper.scrap_process()
    
  

def convert_data_to_table_format():
    logger.info("transform already done")
    pass


def stats_of_data(self):  
    Store.Stats_data(self)

if __name__ == '__main__':
    """
    why main is so...?
    https://stackoverflow.com/questions/419163/what-does-if-name-main-do
    """
    logger.info("Work started")
    stats_of_data(Store)
    
    print(sys.argv[1])

    if sys.argv[1] == 'gather':
        gather_process(1)

    elif sys.argv[1] == 'transform':
        convert_data_to_table_format()

    elif sys.argv[1] == 'stats':
        #статистика сайта Stackoverflow репутация  человека (user_id) в зависимости от кол-ва его постов ответов.(грубо указано кол-во страниц вывода данных из ответов(комментарии) пользователя, детально можно доработать, просто при долгих обработках сервер блокируется хотя запрос делал раз в 10 или 5 секунд) 
        stats_of_data(Store)

    logger.info("work ended")

