import argparse
import logging
import hashlib
from urllib.parse import urlparse

import nltk
import pandas as pd
from nltk.corpus import stopwords

# nltk.download("punkt")
# nltk.download("stopwords")

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)
stop_words = set(stopwords.words("spanish"))


def _read_data(filename):
    logger.info(f"Reading file {filename}")

    return pd.read_csv(filename)


def _extract_newspaper_id(filename):
    logger.info(f"Extracting newspaper id")
    newspaper_id = filename.split("_")[0]

    logger.info(f"Newspaper id detected: {newspaper_id}")
    return newspaper_id


def _add_newspaper_id_column(df, newspaper_id):
    logger.info(f"Filling newspaper_id column with: {newspaper_id}")
    df["newspaper_id"] = newspaper_id
    return df


def _extract_host(df):
    logger.info("Extracting host from urls")

    df["host"] = df["url"].apply(lambda url: urlparse(url).netloc)
    return df


def _fill_missing_titles(df):
    logger.info("Filling missing titles")

    missing_titles_mask = df["title"].isna()

    missing_titles = (
        df[missing_titles_mask]["url"]
        .str.extract(r"(?P<missing_titles>[^/]+)$")
        .applymap(lambda title: title.split("-"))
        .applymap(lambda title_work_list: " ".join(title_work_list))
    )

    df.loc[missing_titles_mask, "title"] = missing_titles.loc[:, "missing_titles"]

    return df


def _generate_uids_for_rows(df):
    logger.info("Generating ids for each row")

    uids = df.apply(lambda row: hashlib.md5(bytes(row["url"].encode())), axis=1).apply(
        lambda hash_object: hash_object.hexdigest()
    )

    df["uid"] = uids
    df.set_index("uid", inplace=True)

    return df


def _remove_new_lines_from_body(df):
    logger.info("Remove new lines from body")

    df["body"] = df.apply(lambda row: row["body"].replace("\n", ""), axis=1)

    return df


def _tokenize_column(df, column_name):
    df[f"n_tokens_{column_name}"] = (
        df.dropna()
        .apply(lambda row: nltk.word_tokenize(row[column_name]), axis=1)
        .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
        .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
        .apply(
            lambda word_list: list(
                filter(lambda word: word not in stop_words, word_list)
            )
        )
        .apply(lambda valid_word_list: len(valid_word_list))
    )

    return df


def _remove_duplicates_entries(df, column_name):
    logger.info("Removing duplicates entries")

    df.drop_duplicates(subset=[column_name], keep="first", inplace=True)

    return df


def _drop_rows_with_missing_values(df):
    logger.info("Dropping rows with missing data")
    return df.dropna()


def _save_data(df, filename):
    clean_filename = f"clean_{filename}"
    logger.info(f"Saving data at location: {clean_filename}")
    df.to_csv(clean_filename)


def main(filename):
    logger.info("Starting cleaning process")

    df = _read_data(filename)
    newspaper_id = _extract_newspaper_id(filename)
    df = _add_newspaper_id_column(df, newspaper_id)
    df = _extract_host(df)
    df = _fill_missing_titles(df)
    df = _generate_uids_for_rows(df)
    df = _remove_new_lines_from_body(df)
    df = _tokenize_column(df, "title")
    df = _tokenize_column(df, "body")
    df = _remove_duplicates_entries(df, "title")
    df = _drop_rows_with_missing_values(df)
    _save_data(df, filename)

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="The path to the dirty data", type=str)

    args = parser.parse_args()

    df = main(args.filename)
    print(df)
