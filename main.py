# Imports
from timeloop import Timeloop
from datetime import timedelta
import os

from func.posts_parsing import asinc_parsing_main_page_hab
from func.habs_parsing import get_habs_info

import logging

# create logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s",
    filename='current_files/logs_app.txt'
)
logger = logging.getLogger('main')

RUN_INTERVAL = timedelta(minutes=10)
DATABASE = 'habr_data_asinc'
HAB_TABLE = 'habr_habs'
POST_TABLE = 'posts'
HABS_PARSING = ['Научно-популярное',
                'Программирование',
                'Информационная безопасность']

get_habs_info(database_name=DATABASE, hab_table_name=HAB_TABLE)

tl = Timeloop()
clear = lambda: os.system('cls')


def main_asinc_parser():
    global HABS_PARSING
    logger.info('start parsing posts from main page (by some habs)')
    for hab in HABS_PARSING:
        logger.info(f'scrapping {hab}')
        asinc_parsing_main_page_hab(database_name=DATABASE, post_table_name=POST_TABLE,
                                    hab_table_name=HAB_TABLE, hab_name=hab, parallel_query=5)
    clear()


@tl.job(interval=RUN_INTERVAL)
def run_with_timeloops():
    main_asinc_parser()


if __name__ == '__main__':
    main_asinc_parser()
    tl.start(block=True)
