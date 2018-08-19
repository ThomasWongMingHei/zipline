"""
Module for building a complete daily dataset from Quandl's WIKI dataset.
"""
from io import BytesIO
import tarfile
from zipfile import ZipFile

from click import progressbar
from logbook import Logger
import pandas as pd
import requests
from six.moves.urllib.parse import urlencode
from six import iteritems
from trading_calendars import register_calendar_alias

from zipline.utils.deprecate import deprecated
from . import core as bundles
import numpy as np
import pymongo

#################################################################
# The data must be re-indexed and fill na with 0 
#################################################################

def QT_mongo2df(uri,dbname,collectionname):
    
    client = pymongo.MongoClient(uri)
    db = client.get_database(dbname)
    df = pd.DataFrame(list(db[collectionname].find({})))
    try:
        df.drop(['_id'], axis=1,inplace=True)
        df.drop_duplicates(keep='last', inplace=True)
    except:
        print('Record not found',collectionname)
    client.close()
    return df 

DayDBurl  = 'mongodb://Thomas:Thomas123@ds125402.mlab.com:25402/zipline'
DayDBname = 'zipline'


MinuteDBurl   = 'mongodb://Thomas:Thomas123@ds147420.mlab.com:47420/timeseries'
MinuteDBname  = 'minutebar'

Metadataurl='D://Quantopian/metadata/metadata.csv'

###################################################################

log = Logger(__name__)

def gen_asset_metadata(metadataurl, show_progress):
    if show_progress:
        log.info('Generating asset metadata.')

    metadataframe=pd.read_csv(metadataurl)
    metadataframe.set_index('sid',inplace=True)
    metadataframe.drop(columns=['dburl', 'dbname','dbcollection'],inplace=True)
    return metadataframe




def parse_pricing_and_vol(metadataf,sessions):
    for row in metadataf.itertuples():
        asset_id = row[0]
        _asset_data=QT_mongo2df(row[-3],row[-2],row[-1])
        asset_data = _asset_data.reindex(sessions.tz_localize(None),method='pad').fillna(0.0)
        yield asset_id, asset_data


@bundles.register('mongodir')
def quandl_bundle(environ,
                  asset_db_writer,
                  minute_bar_writer,
                  daily_bar_writer,
                  adjustment_writer,
                  calendar,
                  start_session,
                  end_session,
                  cache,
                  show_progress,
                  output_dir):
    """
    mongodb for equities in US 
    Need to support equities in other exchange(trading_calendars)

    """
    

    # download asset_metadata from arctic/mongodb location 
    asset_metadata = gen_asset_metadata(Metadataurl,show_progress)

    asset_db_writer.write(equities=asset_metadata)

    symbol_map = asset_metadata.symbol
    sessions = calendar.sessions_in_range(start_session, end_session)
    metadataframe=pd.read_csv(metadataurl)
    metadataframe.set_index('sid',inplace=True)

    # download daily_bar_data_from
    raw_data.set_index(['date', 'symbol'], inplace=True)
    daily_bar_writer.write(parse_pricing_and_vol(metadataframe,sessions),show_progress=show_progress)

    