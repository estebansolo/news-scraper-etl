import argparse
import logging
import pandas as pd

from article import Article
from base import Base, Engine, Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(filename):
    session = Session()
    Base.metadata.create_all(Engine)

    articles = pd.read_csv(filename)

    for index, row in articles.iterrows():
        logger.info(f"Loading article id: {row['uid']} into DB.")

        article = Article(
            row["uid"],
            row["body"],
            row["host"],
            row["newspaper_id"],
            row["n_tokens_body"],
            row["n_tokens_title"],
            row["title"],
            row["url"],
        )

        session.add(article)

    session.commit()
    session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("filename", help="File you want to load into the db", type=str)

    args = parser.parse_args()

    main(args.filename)
