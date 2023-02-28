import pandas as pd
import datetime
from snscrape import modules
import snscrape
import os

# import sqlite3
from sqlalchemy import create_engine, inspect
import numpy as np
import argparse

today = datetime.date.today()
defaultpath = "channels.csv"

parser = argparse.ArgumentParser()
parser.add_argument("--channel")
parser.add_argument("--ignoreupdated", default=False)
parser.add_argument("--seed", default=defaultpath)
parser.add_argument("--lowmemory", default=False)
parser.add_argument("--seeded", default=True)
parser.add_argument("--limit", default=10, type=int)
args = parser.parse_args()

# TODO: make one connection for all the time
def get_conn():
    with open("creds.txt") as fp:
        creds = fp.readline().replace("\n", "")
        conn = create_engine(creds)
        return conn


def is_exception(name):
    exceptions = {"s", "joinchat", "c", "addstickers", "vote", "", "iv"}
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
    path = args.seed
    if random:
        return pd.read_csv(path).drop_duplicates(subset="chname").sample(frac=1)
    else:
        return pd.read_csv(path).drop_duplicates(subset="chname")


def update_channels(low_memory=False, with_seed=False):
    path = args.seed
    # load seed list
    if with_seed:
        channels_df = pd.read_csv(path, usecols=["chname", "degree"])
        channels_df.dropna(subset=["chname"], inplace=True)

    
    conn = get_conn()
    insp = inspect(conn)
    # if not a first run
    if insp.has_table("links", schema="public"):
        if low_memory:
            query = """SELECT target_name chname, COUNT(*) degree FROM links
                    WHERE target_name != links.source_name
                    GROUP BY target_name 
                    HAVING LOWER(target_name) NOT IN (SELECT chname FROM updates) 
                    ORDER BY RANDOM() LIMIT 500;"""
        else:
            query = """SELECT target_name chname, COUNT(*) degree FROM links
                    WHERE target_name != links.source_name
                    GROUP BY target_name
                    HAVING LOWER(target_name) NOT IN (SELECT chname FROM updates);"""
        scrapped_links = pd.read_sql(query, con=conn).drop_duplicates(
            subset=["chname"], keep="first"
        )
        print('scrapped_links')
        print(scrapped_links.head())
        if with_seed:
            print('scrapped_links with seed')
            scrapped_links = pd.concat([channels_df[["chname", "degree"]],
                                         scrapped_links])
            print(scrapped_links.head())
    else:
        scrapped_links = channels_df

    
    # print(scrapped_links.head())
    scrapped_links.dropna(subset=["chname"], inplace=True)
    scrapped_links["chname"] = scrapped_links["chname"].apply(lambda x: x.lower())

    
    # print(scrapped_links.head())

    scrapped_links.drop_duplicates(subset="chname", keep="last", inplace=True)
    scrapped_links = scrapped_links[
        ~scrapped_links["chname"].apply(is_exception)
    ].copy()
    print('Final list')
    print(scrapped_links.head())
    # backup old list
    os.rename(path, path + ".bkp.{}".format(today.strftime("%y%m%d")))

    scrapped_links.sort_values("degree", ascending=False, inplace=True)
    scrapped_links[["chname", "degree"]].to_csv(path, index=False)


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
    # conn = create_engine("sqlite:///" + basepath)
    conn = get_conn()
    insp = inspect(conn)
    if insp.has_table("updates", schema="public"):
        query = """SELECT * FROM updates WHERE chname = '{}'""".format(
            str(chname).lower()
        )
        updates = pd.read_sql(query, con=conn)
        if updates.shape[0] > 0:
            return True
        else:
            return False
    else:
        return False
    
def filter_updated(channels):
    channels_str = ",".join(["'{}'".format(ch) for ch in channels])
    query = "SELECT chname FROM updates WHERE chname NOT IN ({})".format(channels_str)
    print(query)
    conn = get_conn()
    insp = inspect(conn)
    if insp.has_table("updates", schema="public"):
        updates = pd.read_sql(query, con=conn)
        return list(updates['chname'].values)
    else:
        return []

def scrape_step(channels, limit=None):
    i = 0
    # if not args.ignoreupdated:
    #     channels = filter_updated(channels)

    for name in channels:
        if not (limit is None):
            if i >= limit:
                # update_channels()
                break
        if is_exception(name):
            print("Entity {} skipped".format(name))
            continue

        print("{} scraping channel: {}".format(i, name))
        if (not is_updated(name)) or args.ignoreupdated:
        
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
            last_updated['chname'] = last_updated["chname"].apply(lambda x: x.lower())
            channels_df = pd.DataFrame(
                channels,
                columns=["source_name", "target_link", "target_name", "source_link"],
            )
            channels_df.drop_duplicates(subset=['source_link', 'target_link'],
                inplace=True)

            conn = get_conn()
            content_df.to_sql("content", con=conn, if_exists="append", index=False)
            channels_df.to_sql("links", con=conn, if_exists="append", index=False)
            last_updated.to_sql("updates", con=conn, if_exists="append", index=False)
            i += 1

        else:
            print("skiping as already parsed")

    # update_channels()


if __name__ == "__main__":
    if args.channel:
        channels = [args.channel]
        scrape_step(channels)
    else:
        while True:
            update_channels(low_memory=args.lowmemory, with_seed=args.seeded)
            channels = get_channels(random=np.random.choice([True, False]))[
                "chname"
            ].values
            scrape_step(channels, limit=args.limit)
            