# Add path to scraping scripts
import sys
sys.path.append('Scraping')
sys.path.append('Database_scripts')
sys.path.append('Preprocessing scripts')

#Colab paths
sys.path.append('/content/Apartments')
sys.path.append('/content/Apartments/Scraping')
sys.path.append('/content/Apartments/Database_scripts')
sys.path.append('/content/Apartments/Preprocessing_scripts')
sys.path.append('/Apartments/Scraping')
sys.path.append('/Apartments/Database_scripts')
sys.path.append('/Apartments/Preprocessing_scripts')

from otodomScraper import ScrapingOtodom
from db_manipulation import DatabaseManipulation
from otodom import Preprocessing_Otodom
import pandas as pd
import configparser
import urllib
from sqlalchemy import create_engine
from typing import Tuple, List, Callable, DefaultDict, Union

#Sleep while scraping to to avoid blocking
def scrape_with_sleeping(otodom_scraper: Callable, otodom_links: List[str], time_split: int, time_sleep: int, split_size: int, offers: bool) -> Union[List[str], pd.DataFrame]:
  """Sleep while scraping to avoid blocking and return list of downloaded links or pandas data frame with details
        
  Parameters
  ----------
  otodom_scraper : Callable
    OtodomScraper object
  otodom_links : list
      list with links
  time_split : int
      After how many links the "sleeping" is triggered
  time_sleep : int
    number of seconds for which the algorithm "falls asleep"
  split_size : int
    value divided by total number of links it is used to create splits to relieve RAM memory
  offers : boolean
    whether these are links to apartments offers

  Returns
  ------
  otodom: list or pd.DataFrame
      list with scraped links or data frame with details
  """

  otodom = []
  n_pages = len(otodom_links)
  every = np.linspace(start=0, stop=n_pages, num=math.ceil(n_pages/time_split) + 1)[1:]
  prev_split = 0
  for split in every:
    split = int(split)

    if offers:
      if len(otodom) == 0:
        otodom = otodom_scraper.get_details(offers=otodom_links[prev_split:split],split_size=split_size)
      else:
        otodom = otodom.append(otodom_scraper.get_details(offers=otodom_offers[prev_split:split],split_size=split_size))
    else:
      otodom = otodom + otodom_scraper.get_offers(pages=otodom_links[prev_split:split], split_size=split_size)

    
    prev_split = split
    time.sleep(time_sleep)

  return otodom

if __name__ == "__main__":
    # Database connection

    config = configparser.ConfigParser()
    config.read('/content/Apartments/Database_scripts/config.ini')

    database_manipulation = DatabaseManipulation(config = config, config_database = "DATABASE", table_name_links = "active_links",
                                                 table_name_offers = "preprocessing_data", table_name_to_scrape = "to_scrape",
                                                 table_name_process_stage = "process_stage", split_size = 1000)

    # ===Otodom===
    otodom_scraper = ScrapingOtodom(page='https://www.otodom.pl/pl/oferty/wynajem/mieszkanie/', page_name='https://www.otodom.pl', max_threads=20)

    # Get links to scrape
    otodom_pages = otodom_scraper.get_pages()
    otodom_offers = scrape_with_sleeping(otodom_scraper = otodom_scraper, otodom_links = otodom_pages, time_split = 600, time_sleep = 400, split_size = 100, offers = False)
    to_scrape = database_manipulation.push_to_database_links(activeLinks = otodom_offers, page_name = "Otodom")

    #Push to scrape links to database
    del database_manipulation
    database_manipulation = DatabaseManipulation(config = config, config_database = "DATABASE", table_name_links = "active_links",
                                                 table_name_offers = "preprocessing_offers", table_name_to_scrape = "to_scrape",
                                                 table_name_process_stage = "process_stage", split_size = 1000)


    database_manipulation.push_to_scrape(to_scrape, "Otodom")

    # Scrape details
    otodom_scraped = scrape_with_sleeping(otodom_scraper = otodom_scraper, otodom_links = list(to_scrape["link"]), time_split = 700, time_sleep = 400, split_size = 500, offers = True)

    # Prepare offers to insert into table
    otodom_scraped_c = otodom_scraped.copy().reset_index().drop(['index'], axis=1)
    otodom_preprocess = Preprocessing_Otodom(apartment_details=otodom_scraped_c.where(pd.notnull(otodom_scraped_c), None),
                                             information_types=otodom_scraped_c.columns)
    otodom_table = otodom_preprocess.create_table()
    otodom_table=otodom_table.where(pd.notnull(otodom_table), None)

    # Insert details into table
    del database_manipulation
    database_manipulation = DatabaseManipulation(config = config, config_database = "DATABASE", table_name_links = "active_links",
                                                 table_name_offers = "preprocessing_offers", table_name_to_scrape = "to_scrape",
                                                 table_name_process_stage = "process_stage", split_size = 1000)

    database_manipulation.push_to_database_offers(offers=otodom_table, page_name = "Otodom")

