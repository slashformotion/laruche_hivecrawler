import datetime
import json
import pathlib as pl
from pprint import pprint

from beem.blockchain import Blockchain
from beem.community import Community
from beem.hive import Hive
from beem.nodelist import NodeList
from beem.comment import Comment, ContentDoesNotExistsException
from beem.account import Account, AccountDoesNotExistsException

import dateutil.tz as tz

##### CONFIGURATION
laruche_community = "hive-196396" 
allow_tags = ['fr', 'laruche']
blacklist_tags = []
days = 1
only_main_post = True
KEEPED_DIRECTORY = pl.Path(__file__).parent / pl.Path("KEPT")
name_logger = 'LARUCHE_HIVECRAWLER'
la_ruche = 'laruche'
##### END CONFIG

### SETUP LOGGING
import logging

class Logger(logging.Logger):
    def __init__(self, level = "INFO", name = "Basic Logger : "):
        super().__init__(name)
        self.handler = logging.StreamHandler()
        self.formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        self.handler.setFormatter(self.formatter)
        self.addHandler(self.handler)
        
        
        
        assert isinstance(level, str)
        assert level in ["DEBUG", "INFO", "WARNING", "ERROR","CRITICAL"]

        if level == "INFO":
            self.setLevel(logging.INFO)
        elif level == "DEBUG":
            self.setLevel(logging.DEBUG)
        elif level == "WARNING":
            self.setLevel(logging.WARNING)  
        elif level == "ERROR":
            self.setLevel(logging.ERROR)  
        elif level == "CRITICAL":
            self.setLevel(logging.CRITICAL)

logger = Logger(name=name_logger)


### SETUP BEEM LIB STUFF
UTC_TIMEZONE = tz.UTC
NODELIST_INSTANCE = NodeList()
NODELIST_INSTANCE.update_nodes()
HIVE_INSTANCE = Hive(node=NODELIST_INSTANCE.get_hive_nodes())
BLOCKCHAIN_INSTANCE = Blockchain(blockchain_instance=HIVE_INSTANCE)

not_today = datetime.datetime.now(UTC_TIMEZONE) - datetime.timedelta(days=days, hours=1)
start = datetime.datetime(not_today.year, not_today.month, not_today.day)

today = datetime.datetime.now(UTC_TIMEZONE)
stop = datetime.datetime(today.year, today.month, today.day)

start_block_id = BLOCKCHAIN_INSTANCE.get_estimated_block_num(start, accurate=True)
start_block_id = 45519422

stop_block_id = BLOCKCHAIN_INSTANCE.get_estimated_block_num(stop, accurate=True)
stop_block_id = 45519430
logger.info(f"start time: {start}")
logger.info(f"start block: {start_block_id}")
logger.info(f"start time: {stop}")
logger.info(f"start block: {stop_block_id}")

laruche_subscribers = Community(laruche_community).get_subscribers()


KEEPED = []
#### LOOP
watched_count = 0

logger.info("STREAM STARTED")

for com_json in BLOCKCHAIN_INSTANCE.stream(opNames=['comment'], start=start_block_id, stop=stop_block_id):
    watched_count+= 1
    logger.info(f"watched count:  {watched_count}, kept: {len(KEEPED)} ,  cur: @{com_json.get('author')}/{com_json.get('permlink')}")
    try:
        comment = Comment(
            '@{}/{}'.format(
                com_json.get('author'),
                com_json.get('permlink')
            ),
            blockchain_instance=HIVE_INSTANCE
            )
    except ContentDoesNotExistsException as e:
        logger.warning(e)
        continue

    if not only_main_post and not comment.is_main_post():
        print('not main post')
        continue
    
    # print(com_json)
    if com_json.get('json_metadata') != '':
        metadata = json.loads(com_json.get('json_metadata'))
    
    for b_tag in blacklist_tags:
        if b_tag in metadata.get('tags', []):
            continue
    
    lock = False
    for a_tag in allow_tags:
        if a_tag in metadata.get('tags', []):
            lock = True
    if not lock:
        # print('not lock')
        continue

    try:
        account = Account(com_json.get('author'), blockchain_instance=HIVE_INSTANCE)
    except AccountDoesNotExistsException as e:
        logger.warning(e)
        continue

    account_following = account.get_following()

    if la_ruche in account_following:
        continue
    
    if com_json.get('author') in laruche_subscribers:
        continue

    KEEPED.append(comment.authorperm)


### DUMP THE BAG ! 
with (KEEPED / pl.Path(f"{today.strftime('%Y-%m-%d')}-days{days}.json")).open('r') as flux:
    json.dump(KEEPED, flux)