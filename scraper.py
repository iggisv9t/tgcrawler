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
# WTF argparse?
parser.add_argument('--seeded', default=True, type=lambda x: (str(x).lower() == 'true'))
parser.add_argument("--limit", default=10, type=int)
args = parser.parse_args()


def is_exception(name):
    exceptions = {"s", "joinchat", "c", "addstickers", "vote", "",
                  "iv", "share", "proxy", "setlanguage", "addlist"}
    name = str(name).split("?")[0]
    if name.lower().endswith("bot"):
        return True
    if name.startswith("+"):
        return True
    if "url=http" in name:
        return True
    if "%" in name:
        return True
    if name in exceptions:
        return True
    if '-' in name:
        return True

    return False


class Scraper(object):
    def __init__(self, low_memory=False, with_seed=False, limit=10):
        self.conn = None
        self.low_memory = low_memory
        self.with_seed = with_seed
        self.limit = limit
        self.check_tables()
        
    def check_tables(self):
        conn = self.get_conn()
        insp = inspect(conn)
        if not insp.has_table("updates", schema="public"):
            # TODO: implement or make warning
            pass
        if not insp.has_table("links", schema="public"):
             # TODO: implement or make warning
            pass

    def get_conn(self):
        if self.conn is None:
            with open("creds.txt") as fp:
                creds = fp.readline().replace("\n", "")
                self.conn = create_engine(creds)
        return self.conn

    def clean_channels(self, scraped_links):
        scraped_links.dropna(subset=["chname"], inplace=True)
        scraped_links["chname"] = scraped_links["chname"].apply(lambda x: x.lower())

        scraped_links.drop_duplicates(subset="chname", keep="last", inplace=True)
        scraped_links = scraped_links[
            ~scraped_links["chname"].apply(is_exception)
        ].copy()
        return scraped_links

    def is_updated(self, chname):
        conn = self.get_conn()

        query = """SELECT * FROM updates WHERE chname = '{}'""".format(
            str(chname).lower()
        )
        updates = pd.read_sql(query, con=conn)
        if updates.shape[0] > 0:
            return True, updates['last_updated'].max().date()
        else:
            return False, datetime.date(year=1970, month=1, day=1)

    def update_channels(self):
        print('inside update_channels')
        print('Seeded: {}'.format(self.with_seed))
        path = args.seed
        if self.with_seed:
            channels_df = pd.read_csv(path, usecols=["chname", "degree"])
            channels_df.dropna(subset=["chname"], inplace=True)

        conn = self.get_conn()
        
        if self.low_memory:
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
        scraped_links = pd.read_sql(query, con=conn).drop_duplicates(
            subset=["chname"], keep="first"
        )

        if self.with_seed:
            print("scraped_links with seed")
            scraped_links = pd.concat(
                [channels_df[["chname", "degree"]], scraped_links]
            )
            print(scraped_links.head())
    
        scraped_links = self.clean_channels(scraped_links)

        # backup old list
        if os.path.exists(path):
            os.rename(path, path + ".bkp.{}".format(today.strftime("%y%m%d")))

        scraped_links.sort_values("degree", ascending=False, inplace=True)
        scraped_links[["chname", "degree"]].to_csv(path, index=False)

        return scraped_links[["chname", "degree"]]

    def scrape(self, name, stop_date):
        scraper = modules.telegram.TelegramChannelScraper(name)
        channels = []
        content = []
        for item in scraper.get_items():
            # print(type(item.date))
            if item.date.replace(tzinfo=None) < pd.to_datetime(stop_date):
                return content, channels
            
            if hasattr(item, "content"):
                content.append((item.url, item.content, item.date))
            if hasattr(item, "outlinks"):
                for link in item.outlinks:
                    if "t.me" in link:
                        # TODO: fix links like utm_source=t.me
                        channels.append(
                            (
                                name,
                                link,
                                link.split("/")[3].split("?")[0],
                                item.url,
                            )
                        )

        return content, channels

    def scrape_step(self, channels):
        i = 0
        for j, name in enumerate(channels):
            if not (self.limit is None):
                if i >= self.limit:
                    # update_channels()
                    break
            if is_exception(name):
                print("Entity {} skipped".format(name))
                continue

            print("{}, {} scraping channel: {}".format(j, i, name))


            if args.ignoreupdated:
                stop_date = datetime.date(year=1970, month=1, day=1)
                is_updated = False
            else:
                is_updated, stop_date = self.is_updated(name)
            
            # print(stop_date, type(stop_date))
            # print(today, type(today))
            if datetime.timedelta(days=1) < abs(stop_date - today):
                # print('parsing')
                try:
                    content, channels = self.scrape(name, stop_date)
                except snscrape.base.ScraperException as e:
                    print(
                        "channel {} not scrapped due to exception: {}".format(name, e)
                    )
                    continue

                last_updated = pd.DataFrame(
                    [(name.lower(), today)], columns=["chname", "last_updated"]
                )

                if len(content) > 0:
                    content_df = pd.DataFrame(content, columns=["url", "content", "date"])
                    content_df["channel"] = name

                if len(channels) > 0:
                    channels_df = pd.DataFrame(
                        channels,
                        columns=[
                            "source_name",
                            "target_link",
                            "target_name",
                            "source_link",
                        ],
                    )
                    # print(channels_df.head())
                    # Filter channels df
                    channels_df["source_name"] = channels_df["source_name"].apply(
                        lambda x: x.lower()
                    )
                    channels_df["target_name"] = channels_df["target_name"].apply(
                        lambda x: x.lower()
                    )
                    # print(channels_df.head())
                    channels_df.drop_duplicates(
                        subset=["source_link", "target_link"], inplace=True
                    )
                    channels_df = channels_df[
                        ~channels_df["source_name"].apply(lambda x: is_exception(x))
                    ].copy()
                    channels_df = channels_df[
                        ~channels_df["target_name"].apply(lambda x: is_exception(x))
                    ].copy()

                # Push to DB
                conn = self.get_conn()
                if len(content) > 0:
                    content_df.to_sql("content", con=conn, if_exists="append", index=False)
                if len(channels) > 0:
                    channels_df.to_sql("links", con=conn, if_exists="append", index=False)
                last_updated.to_sql(
                    "updates", con=conn, if_exists="append", index=False
                )
                i += 1

            else:
                print("skiping as already parsed")

if __name__ == "__main__":
    scraper = Scraper(low_memory=args.lowmemory,
                with_seed=args.seeded, limit=args.limit)

    if args.channel:
        channels = [args.channel]
        scraper.scrape_step(channels)
    else:
        while True:
            scrapped = scraper.update_channels()
            channels = list(scrapped['chname'].values)
            scraper.scrape_step(channels)