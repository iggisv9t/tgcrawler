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
- Check source code. It expects `creds.txt` for sqlalchemy connection string for your DB in `./` and `channels.csv` as seed list to start scraping.
`channels.csv` should be created before start. Expected columns: `chname, degree`, where chname is channel id, degree -- is the number of connected channels.

# Usage
`python scraper.py`

# Similar projects
https://antcating.github.io/telegram_connections_frontend/

# Contributing
1. Please open issue before any pull request.
2. All code contributions require forks
3. Suggest features and ideas in [discussions](https://github.com/iggisv9t/tgcrawler/discussions)
4. Feel free to develop it in your own way in your fork

# Gratitude
Big thanks to [@antcating](https://github.com/antcating) for inspiration  
And Bellingcat for [snscrape](https://github.com/bellingcat/snscrape)
