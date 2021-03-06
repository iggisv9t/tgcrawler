# What is this
This is simple telegram crawler and parser based on [bellingcat's scraper](https://github.com/bellingcat/snscrape)  
It parses posts from channels, looks for `t.me` links and tries to parse them.  
Try to look [here](https://iggisv9t.xyz/telegram/index.html) (may require some RAM)
![screenshot](screenshot.png)

[(how to publish large graphs online)](https://medium.com/@iggisv9t/what-to-watch-tonight-scraping-imdb-and-visualizing-its-data-as-interactive-website-328a794498a2)

# Setup
- You should have basic python knowledge to use this.
- First create [virtualenv](https://docs.python.org/3/library/venv.html)  
- Then install dependencies: `pip install -r requirements.txt`
- Check source code. It expects `tg.db` sqlite3 database in `../` and `channels.csv` as seed list to start scraping.
Database will be created automatically, just edit the `basepath` variable. `channels.csv` should be created before start. Expected columns: `chname, link, last_updated`. `chname` is channel id, link -- is source of channel (doesn't matter for seed list), `last_updated` sould be `pd.NaT` or just `NaN`.

# Usage
`python scraper.py`

# Similar projects
https://antcating.github.io/telegram_connections_frontend/

# Contributing
Please open issue before any pull request.

# Gratitude
Big thanks to [@antcating](https://github.com/antcating) for inspiration  
And Bellingcat for [snscrape](https://github.com/bellingcat/snscrape)
