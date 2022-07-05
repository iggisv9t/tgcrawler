import pandas as pd
import datetime
from snscrape import modules
import snscrape
import os

basedir = os.path.dirname(os.path.abspath(__file__))
today = datetime.date.today()

def check_exceptions(name):
    exceptions = {'s'}
    name = name.split('?')[0]
    if name.endswith("bot"):
        return True
    if name.startswith('+'):
        return True
    if name == 'joinchat':
        return True
    if 'url=http' in name:
        return True
    if name in exceptions:
        return True
    

def get_channels():
    path = 'channels.csv'
    return pd.read_csv('channels.csv').drop_duplicates(subset='chname')

def update_channels():
    
    channels_df = pd.read_csv('channels.csv')
    dirs = [d for d in os.listdir(basedir) if os.path.isdir(d)]
    for dirname in dirs:
        fnames = os.listdir(os.path.join(basedir, dirname))
        for fname in fnames:
            if fname.startswith('links') and fname.endswith('.csv.gz'):
                links = pd.read_csv(os.path.join(basedir, dirname, fname))
                links.dropna(inplace=True)
                links.rename(columns={'target_link': 'link', 'target_name': 'chname'}, inplace=True)
                links['last_updated'] = None
                channels_df = pd.concat([channels_df, links[['link', 'chname', 'last_updated']]], axis=0)

                update_date = fname.split("_")[-1].split('.')[0]
                update_date = datetime.datetime.strptime(update_date, '%y%m%d')
                # channels_df[channels_df['chname'] == dirname]['last_updated'] = update_date
                daterow = pd.DataFrame([['nolink', dirname, update_date]], columns=['link', 'chname', 'last_updated'])

                channels_df = pd.concat([channels_df, daterow], axis=0)

    # backup old list
    os.rename('channels.csv', 'channels.csv.bkp.{}'.format(today.strftime('%y%m%d')))
    channels_df.drop_duplicates(subset="chname", keep='last', inplace=True)
    channels_df.to_csv('channels.csv', index=False)
    

def scrape(name):
    scraper = modules.telegram.TelegramChannelScraper(name)
    channels = []
    content = []
    for item in scraper.get_items():
        if hasattr(item, "content"):
            content.append((item.url, item.content, item.date))
        if hasattr(item, "outlinks"):
            for link in item.outlinks:
                if 't.me' in link:
                    channels.append((name, link, link.split('/')[3].split('?')[0], item.url))

    return content, channels

def scrape_step(limit=None):
    for i, (link, name, last_updated) in enumerate(get_channels().values):
        if not (limit is None):
            if i >= limit:
                update_channels()
                break
        if check_exceptions(name):
            print('Entity {} skipped'.format(name))
            continue
        print("{} scraping channel: {}".format(i, name))
        if pd.isnull(last_updated):
            try:
                content, channels = scrape(name)
            except snscrape.base.ScraperException as e:
                print("channel {} not scrapped due to exception: {}".format(name, e))

            content_df = pd.DataFrame(content, columns=['url', 'content', 'date'])
            path = os.path.join(basedir, name)
            os.makedirs(path, exist_ok=True)
            savename = 'content_{}_{}.csv.gz'.format(name, today.strftime('%y%m%d'))
            content_df.to_csv(os.path.join(path, savename), index=False, compression='gzip')

            channels_df = pd.DataFrame(channels, columns=['source_name', 'target_link', 'target_name', 'source_link'])
            savename = 'links_{}_{}.csv.gz'.format(name, today.strftime('%y%m%d'))
            channels_df.to_csv(os.path.join(path, savename), index=False, compression='gzip')
        else:
            print('skiping as already parsed, last updated: {}'.format(last_updated))

    update_channels()

if __name__ == "__main__":
    update_channels()
    scrape_step()