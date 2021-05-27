# Imports
from sqlite3 import connect
import requests
from bs4 import BeautifulSoup
import logging


# create logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s",
    filename='current_files/logs_app.txt'
)
logger = logging.getLogger('habs_scrapping')


def get_habs_info(database_name='habr_data', hab_table_name='habr_habs'):
    '''
        CREATE/SUPPLEMENT table with habs info
            database_name -- str,
            hab_table_name -- str,
    '''
    # create/connect to habs table in database
    conn = connect(f'current_files/{database_name}.sqlite.db', isolation_level=None)
    db = conn.cursor()
    db.execute("""PRAGMA encoding = 'UTF-8'""")
    db.execute(f"""CREATE TABLE IF NOT EXISTS {hab_table_name}(hab_table_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                               hab VARCHAR(255), hab_url VARCHAR(255))""")
    db.execute(f"""CREATE UNIQUE INDEX IF NOT EXISTS db_id ON {hab_table_name}(hab_table_id)""")
    habs_list_url = 'https://habr.com/ru/hubs/'
    try:
        habs_r = requests.get(habs_list_url)
    except Exception as e:
        logger.error(e)

    hab_soup = BeautifulSoup(habs_r.text, 'html.parser')

    try:
        proc_status = True
        hab_titles = hab_soup.find_all("a", {"class": "list-snippet__title-link"})
        hab_urls = [i['href'] for i in hab_titles]
        habs = [i.get_text() for i in hab_titles]
    except Exception as e:
        proc_status = False
        logger.error(e)

    if proc_status:
        db.executemany(f"""INSERT OR IGNORE INTO {hab_table_name}(hab, hab_url)
                           VALUES (?, ?)""",
                       [(hab, hab_url) for hab, hab_url in zip(habs, hab_urls)])
        conn.commit()
