# Imports
from bs4 import BeautifulSoup
import re
import json
import asyncio
from aiohttp import ClientSession
import logging


# create logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s",
    filename='current_files/logs_app.txt'
)
logger = logging.getLogger('asinc_scrapping')



async def fetch(sem, url, session):
    '''
        Make asinc query by url. Return html doc
            sem -- asyncio.Semaphore(), connections
            url -- str,
            session -- aiohttp.ClientSession(), current ClientSession
    '''
    try:
        async with session.get(url) as response:
            body = await response.read()
            return body
    except Exception as e:
        logger.error(e)


async def bound_fetch(sem, url, session):
    '''
        Getter function with semaphore
            sem -- asyncio.Semaphore(), connections
            url -- str,
            session -- aiohttp.ClientSession(), current ClientSession
    '''
    async with sem:
        return await fetch(sem, url, session)


async def run_get_html(urls):
    '''
        Asinc getter html by all urls
            urls -- list[str],
    '''
    tasks = []
    # up to 50 connections can be made at the same time
    sem = asyncio.Semaphore(50)

    async with ClientSession() as session:
        for url in urls:
            task = asyncio.ensure_future(bound_fetch(sem, url, session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses
        return responses


def parallel_post_parsing(doc, full_info=False):
    '''
        Asinc parsing post contents. Return json files for every post
            doc -- dict['htlm': str, 'url': str], html doc and ulr for each post
            full_info -- bool, if TRUE return full post info (tags, views_count, vote, comment_count)
    '''
    currentFile = f"curr_post_{doc['url'][25:-1]}.json"
    post_soup = BeautifulSoup(doc['html'], 'html5lib')

    try:
        author = post_soup.find("span", {"class": "user-info__nickname user-info__nickname_small"}).get_text()
        author_url = "https://habr.com/ru/users/{}/".format(str(author))
        title = post_soup.find("span", {"class": "post__title-text"}).get_text()
        pub_time = post_soup.find("span", {"class": "post__time"})['data-time_published']
        content = post_soup.find("div", {"id": "post-content-body"})
        content = str(content)
        if full_info:
            tags = post_soup.find_all("a", {"class": "inline-list__item-link post__tag "})
            tags = ', '.join([tag.get_text() for tag in tags])
            views_count = int(post_soup.find("span", {"class": "post-stats__views-count"}).get_text())
            vote = post_soup.find("div", {"class": "voting-wjt voting-wjt_post js-post-vote"}).get_text()
            vote = re.sub(r'\n', '', vote)
            comment_count = int(post_soup.find("span", {"class": "post-stats__comments-count",
                                                       "id": "post-stats-comments-count"}).get_text())
    except Exception as e:
        logger.error(f"Error parsing html doc -- {doc['url']}\n{e}")

    # Записываем статью в json
    try:
        post = {'author': author, 'author_url': author_url, 'title': title, 'pub_time': pub_time, 'content': content}
        with open(currentFile, 'w', encoding='utf8') as write_file:
            json.dump(post, write_file, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Error json dump -- {doc['url']}\n{e}")