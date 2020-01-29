import logging
import subprocess

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

news_sites_ids = ["eluniversal", "elpais", "eltiempo"]


def main():
    _extract()
    _transform()
    _load()


def _extract():
    logger.info("Starting extract process")

    for news_site_id in news_sites_ids:
        subprocess.run(["python", "main.py", news_site_id], cwd="./extract")
        subprocess.run(
            [
                "find",
                ".",
                "-name",
                f"{news_site_id}*",
                "-exec",
                "mv",
                f"{news_site_id}.csv",
                f"../transform/{news_site_id}.csv",
                ";",
            ],
            cwd="./extract",
        )


def _transform():
    logger.info("Starting transform process")

    for news_site_id in news_sites_ids:
        dirty_data_filename = f"{news_site_id}.csv"
        clean_data_filename = f"clean_{dirty_data_filename}"

        subprocess.run(["python", "main.py", dirty_data_filename], cwd="./transform")
        subprocess.run(["rm", dirty_data_filename], cwd="./transform")
        subprocess.run(
            ["mv", clean_data_filename, f"../load/{news_site_id}.csv"], cwd="./transform"
        )


def _load():
    logger.info("Starting load process")

    for news_site_id in news_sites_ids:
        clean_data_filename = f"{news_site_id}.csv"

        subprocess.run(["python", "main.py", clean_data_filename], cwd="./load")
        subprocess.run(["rm", clean_data_filename], cwd="./load")


if __name__ == "__main__":
    main()
