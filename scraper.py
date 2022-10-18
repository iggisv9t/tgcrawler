import pandas as pd
import datetime
from snscrape import modules
import snscrape
import os
import sqlite3
from sqlalchemy import create_engine, inspect
from collections import Counter
import numpy as np

basepath = "tg.db"
today = datetime.date.today()
path = "channels.csv"


def is_exception(name):
    exceptions = {"s", "joinchat", "c", "addstickers", "vote", ""}
    name = str(name).split("?")[0]
    if name.lower().endswith("bot"):
        return True
    if name.startswith("+"):
        return True
    if "url=http" in name:
        return True
    if name in exceptions:
        return True

    return False


def get_channels(random=False):
    path = "channels.csv"
    if random:
        return pd.read_csv(path).drop_duplicates(subset="chname").sample(frac=1)
    else:
        return pd.read_csv(path).drop_duplicates(subset="chname")


def update_channels():
    # load seed list
    channels_df = pd.read_csv(path)
    channels_df.dropna(subset=["chname"], inplace=True)
    # channels_df = channels_df[pd.isnull(channels_df["last_updated"])]
    # mask = np.array([c.lower() in str(l).lower()
    #         for l, c in channels_df[['link', 'chname']].values])

    # channels_df = channels_df[mask].copy()

    conn = create_engine("sqlite:///" + basepath)
    insp = inspect(conn)
    # if not a first run
    if insp.has_table("links", schema="main"):
        query = """SELECT DISTINCT target_link link, target_name chname,
                 COUNT(*) degree FROM links
                WHERE target_name != source_name
                GROUP BY target_name"""
        scrapped_links = pd.read_sql(query, con=conn).drop_duplicates(
            subset=["link", "chname"], keep="first"
        )
        print(scrapped_links.head())
        scrapped_links = pd.concat(
            [channels_df[["link", "chname", "degree"]], scrapped_links]
        )
        print(scrapped_links.head())
    else:
        scrapped_links = channels_df

    print(scrapped_links.head())
    scrapped_links.dropna(subset=["chname"], inplace=True)
    scrapped_links["chname"] = scrapped_links["chname"].apply(lambda x: x.lower())

    print(scrapped_links.head())

    scrapped_links.drop_duplicates(subset="chname", keep="last", inplace=True)
    scrapped_links = scrapped_links[
        ~scrapped_links["chname"].apply(is_exception)
    ].copy()
    print(scrapped_links.head())

    # check if already scrapped
    # TODO: move it to scraping

    print(scrapped_links.head())
    # backup old list
    os.rename("channels.csv", "channels.csv.bkp.{}".format(today.strftime("%y%m%d")))

    scrapped_links.sort_values("degree", ascending=False, inplace=True)
    scrapped_links[["link", "chname", "degree"]].to_csv("channels.csv", index=False)


def scrape(name):
    scraper = modules.telegram.TelegramChannelScraper(name)
    channels = []
    content = []
    for item in scraper.get_items():
        if hasattr(item, "content"):
            content.append((item.url, item.content, item.date))
        if hasattr(item, "outlinks"):
            for link in item.outlinks:
                if "t.me" in link:
                    # TODO: fix links like utm_source=t.me
                    channels.append(
                        (name, link, link.split("/")[3].split("?")[0], item.url)
                    )

    return content, channels


def is_updated(chname):
    conn = create_engine("sqlite:///" + basepath)
    insp = inspect(conn)
    if insp.has_table("updates", schema="main"):
        query = """SELECT * FROM updates WHERE LOWER(chname) = '{}'""".format(
            str(chname).lower()
        )
        updates = pd.read_sql(query, con=conn)
        if updates.shape[0] > 0:
            return True
        else:
            return False
    else:
        return False


def scrape_step(limit=None, random=False):
    for i, name in enumerate(get_channels(random)["chname"].values):
        if not (limit is None):
            if i >= limit:
                update_channels()
                break
        if is_exception(name):
            print("Entity {} skipped".format(name))
            continue
        print("{} scraping channel: {}".format(i, name))
        if not is_updated(name):
            try:
                content, channels = scrape(name)
            except snscrape.base.ScraperException as e:
                print("channel {} not scrapped due to exception: {}".format(name, e))
                continue

            content_df = pd.DataFrame(content, columns=["url", "content", "date"])
            content_df["channel"] = name
            last_updated = pd.DataFrame(
                [(name, today)], columns=["chname", "last_updated"]
            )
            channels_df = pd.DataFrame(
                channels,
                columns=["source_name", "target_link", "target_name", "source_link"],
            )

            with sqlite3.connect(basepath) as conn:
                content_df.to_sql("content", con=conn, if_exists="append")
                channels_df.to_sql("links", con=conn, if_exists="append")
                last_updated.to_sql("updates", con=conn, if_exists="append")

        else:
            print("skiping as already parsed, last updated: {}".format(last_updated))

    update_channels()


if __name__ == "__main__":
    while True:
        update_channels()
        scrape_step(random=np.random.choice([True, False]), limit=10)
