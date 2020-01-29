import re
import csv
import logging
import argparse
from datetime import datetime
from urllib3.exceptions import MaxRetryError

from requests.exceptions import HTTPError, ConnectionError

from common import config
import news_page_objects as news

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

is_root_path = re.compile(r"^/.+$")
is_well_formed_link = re.compile(r'^https?://.+/.+$')

def _fetch_article(news_site_id, host, link):
    logger.info(f"Start fetching article at {link=}")

    article = None
    try:
        article = news.ArticlePage(news_site_id, _build_link(host, link))
    except (HTTPError, MaxRetryError, ConnectionError) as e:
        logger.warning("Error while fetching the article", exc_info=False)

    if article and not article.body:
        logger.warning("Invalid article. There is no body")
        return None

    return article

def _build_link(host, link):
    if is_well_formed_link.match(link):
        return link
    
    if is_root_path.match(link):
        return f"{host}{link}"

    return f"{host}/{link}"


def _news_scraper(news_site_id):
    host = config()["news_sites"][news_site_id]["url"]

    logger.info(f"Beginning scraper for {host=}")
    homepage = news.HomePage(news_site_id, host)
    articles = []
    for link in homepage.article_links:
        article = _fetch_article(news_site_id, host, link)
        if article:
            logger.info("Article fetched!!")
            articles.append(article)

    _save_articles(news_site_id, articles)


def _save_articles(news_site_id, articles):
    #now = datetime.now().strftime("%Y_%m_%d")
    #output_filename = f"{news_site_id}_{now}_articles.csv"
    output_filename = f"{news_site_id}.csv"

    csv_headers = list(filter(lambda property: not property.startswith("_"), dir(articles[0])))
    with open(output_filename, mode="w+") as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)
        for article in articles:
            row = [str(getattr(article, prop)) for prop in csv_headers]
            writer.writerow(row)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    news_sites_choices = list(config()["news_sites"].keys())
    parser.add_argument('news_site',
                        help="The news site to be scraped",
                        type=str,
                        choices=news_sites_choices)

    args = parser.parse_args()
    _news_scraper(args.news_site)
