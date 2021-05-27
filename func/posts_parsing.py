# Imports
from tqdm.auto import tqdm
import requests
from sqlite3 import connect
from bs4 import BeautifulSoup

import asyncio
from multiprocessing.dummy import Pool as ThreadPool
import nest_asyncio
nest_asyncio.apply()

from func.asinc_parth import run_get_html, parallel_post_parsing
import os
import json
import logging

# create logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s",
    filename='/current_files/logs_app.txt'
)
logger = logging.getLogger('posts_scrapping')


def asinc_parsing_posts(post_urls, conn, post_table_name, hab_table_id, verbose=True):
    '''
        Function that parses the post:
          post_urls -- list[str], habr post urls from select hab
          conn -- sqlite3.connect("post_db_name.db"), database where we should add post info
          post_table_name -- str, table name with posts
          hab_table_id -- hab id in hab table
          verbose -- bool, if TRUE show progress
    '''
    # run asinc parsing posts urls
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError as ex:
        if "There is no current event loop in thread" in str(ex):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop = asyncio.get_event_loop()
        else:
            logger.error(ex)

    future = asyncio.ensure_future(run_get_html(post_urls))
    responses = loop.run_until_complete(future)
    # make dict with posts content
    docs = [{'html': html, 'url': url} for html, url in zip(responses.result(), post_urls)]

    # start flows
    pool = ThreadPool(len(post_urls))
    try:
        _ = pool.map(parallel_post_parsing, docs)
    except Exception as e:
        logger.critical(e)

    # close flows
    pool.close()
    pool.join()

    # add all parsing posts info in database
    inserts = []
    for url in post_urls:
        currentFile = f"curr_post_{url[25:-1]}.json"
        try:
            curr_proc_status = True
            with open(currentFile, encoding='utf8') as f:
                post = json.load(f)
            inserts += [(post['author'], post['author_url'], post['title'], url,
                         post['pub_time'], post['content'], hab_table_id)]
            os.remove(currentFile)
        except Exception as e:
            curr_proc_status = False
            logger.error(f'Error load json: {currentFile}\n{e}')

        if verbose:
            logger.info('{} --> {}'.format(url, curr_proc_status))

    try:
        db = conn.cursor()
        db.executemany(f"""INSERT OR IGNORE INTO {post_table_name}(author, author_url, title, post_url, pub_time, content, hab_table_id)
                              VALUES (?, ?, ?, ?, ?, ?, ?)""",
                       inserts)
        conn.commit()
        db.close()
    except Exception as e:
        logger.error(e)

    return conn


def asinc_parsing_main_page_hab(database_name, post_table_name, hab_table_name, hab_name,
                                parallel_query=5, verbose=True):
    """
        Get post ids all posts on main page in select hab
            database_name -- str, name of sqlite3 database in format file.db
            post_table_name -- str, name of post table
            hab_table_name -- str, name of habs table
            hab_name -- str, hab name on habr
            parallel_query -- int, number of concurrent requests
            verbose -- bool, if TRUE show progress
    """
    # create/connect to post table in database
    conn = connect(f'current_files/{database_name}.sqlite.db', isolation_level=None)
    db = conn.cursor()
    db.execute("""PRAGMA encoding = 'UTF-8'""")
    db.execute(f"""CREATE TABLE IF NOT EXISTS {post_table_name}(post_table_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                author VARCHAR(255), author_url VARCHAR(255), title VARCHAR(255),
                                                                post_url VARCHAR(255), pub_time TEXT, content TEXT,
                                                                hab_table_id INTEGER NOT NULL REFERENCES {hab_table_name}(hab_table_id),
                                                                UNIQUE(post_url))""")
    # tags VARCHAR(255), views_count INTEGER, comment_count INTEGER, vote VARCHAR(255)
    # get/load html doc from main hab page
    db.execute(f"""SELECT hab_table_id, hab_url  
                      FROM {hab_table_name}
                      WHERE hab LIKE '{hab_name}';""")
    out = db.fetchall()[0]
    main_page_r = requests.get(out[1])
    # start parsing
    mp_soup = BeautifulSoup(main_page_r.text, 'html.parser')  # instead of html.parser
    mp_doc = mp_soup.find_all("li", {
        "class": "content-list__item content-list__item_post shortcuts_item"})  # find all need post ids
    post_ids = []
    for post in mp_doc:
        try:
            if post['id'] != 'effect':
                post_ids += [post['id']]
        except:
            pass

    post_urls = ["https://habr.com/ru/post/{}/".format(post_id[5:]) for post_id in post_ids]
    for i in tqdm(range(0, len(post_urls), parallel_query)):
        urls = post_urls[i:i + parallel_query]
        conn = asinc_parsing_posts(post_urls=urls, conn=conn,
                                   post_table_name=post_table_name, hab_table_id=out[0])

