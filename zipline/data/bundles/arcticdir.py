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



###################################################################################################################
from arctic import Arctic

store = Arctic('localhost')

Dailycollection='QT_Daily'
Minutecollection='QT_TimeSeries'

Dailyprocessedcollection='Daily'
Minuteprocessedcollection='TimeSeries'

Datefield='DATE'
Minutefield='DATE'


def QT_select_range(df,fieldname,start,end):
    
    #723181 DateString = '01-Jan-1980'
    df.sort_index(inplace=True)
    df=df.truncate(start,end)
    return df

# Input: store: Arctic Engine, security: list of security 
# Output: list of dataframe of security with required range
def load_dailydata(store,security,startdate,enddate):
    
    if type(security)==list:
        output=[]
        for ticker in security:
            library = store[Dailyprocessedcollection]
            daydf=library.read(ticker)
            newdf=QT_select_range(daydf.data,Datefield,startdate,enddate)
            output=output+[newdf]
        return output
    else:
        library = store[Dailyprocessedcollection]
        daydf=library.read(security)
        newdf=QT_select_range(daydf.data,Datefield,startdate,enddate)
        return newdf

# Input: startdate and enddate: To get the whole time series for 2018-06-05, the enddate is 2018-06-06...
def load_minutedata(store,security,startdate,enddate):
    
    if type(security)==list:
        output=[]
        for ticker in security:
            library = store[Minuteprocessedcollection]
            tsdf=library.read(ticker,date_range=pd.date_range(start=startdate, end=enddate))
            newdf=QT_select_range(tsdf.data,Minutefield,startdate,enddate)
            output=output+[newdf]
        return output
    else:
        library = store[Minuteprocessedcollection]
        tsdf=library.read(security)
        newdf=QT_select_range(tsdf.data,Minutefield,startdate,enddate)
        return newdf
######################################################################################################################


log = Logger(__name__)


def load_data_table(file,
                    index_col,
                    show_progress=False):
    """ Load data table from zip file provided by Quandl.
    """
    with ZipFile(file) as zip_file:
        file_names = zip_file.namelist()
        assert len(file_names) == 1, "Expected a single file from Quandl."
        wiki_prices = file_names.pop()
        with zip_file.open(wiki_prices) as table_file:
            if show_progress:
                log.info('Parsing raw data.')
            data_table = pd.read_csv(
                table_file,
                parse_dates=['date'],
                index_col=index_col,
                usecols=[
                    'ticker',
                    'date',
                    'open',
                    'high',
                    'low',
                    'close',
                    'volume',
                    'ex-dividend',
                    'split_ratio',
                ],
            )

    data_table.rename(
        columns={
            'ticker': 'symbol',
            'ex-dividend': 'ex_dividend',
        },
        inplace=True,
        copy=False,
    )
    return data_table


def fetch_data_table(api_key,
                     show_progress,
                     retries):
    """ Fetch WIKI Prices data table from Quandl
    """
    for _ in range(retries):
        try:
            if show_progress:
                log.info('Downloading WIKI metadata.')

            metadata = pd.read_csv(
                format_metadata_url(api_key)
            )
            # Extract link from metadata and download zip file.
            table_url = metadata.loc[0, 'file.link']
            if show_progress:
                raw_file = download_with_progress(
                    table_url,
                    chunk_size=ONE_MEGABYTE,
                    label="Downloading WIKI Prices table from Quandl"
                )
            else:
                raw_file = download_without_progress(table_url)

            return load_data_table(
                file=raw_file,
                index_col=None,
                show_progress=show_progress,
            )

        except Exception:
            log.exception("Exception raised reading Quandl data. Retrying.")

    else:
        raise ValueError(
            "Failed to download Quandl data after %d attempts." % (retries)
        )


def gen_asset_metadata(data, show_progress):
    if show_progress:
        log.info('Generating asset metadata.')

    data = data.groupby(
        by='symbol'
    ).agg(
        {'date': [np.min, np.max]}
    )
    data.reset_index(inplace=True)
    data['start_date'] = data.date.amin
    data['end_date'] = data.date.amax
    del data['date']
    data.columns = data.columns.get_level_values(0)

    data['exchange'] = 'QUANDL'
    data['auto_close_date'] = data['end_date'].values + pd.Timedelta(days=1)
    return data


def parse_splits(data, show_progress):
    if show_progress:
        log.info('Parsing split data.')

    data['split_ratio'] = 1.0 / data.split_ratio
    data.rename(
        columns={
            'split_ratio': 'ratio',
            'date': 'effective_date',
        },
        inplace=True,
        copy=False,
    )
    return data


def parse_dividends(data, show_progress):
    if show_progress:
        log.info('Parsing dividend data.')

    data['record_date'] = data['declared_date'] = data['pay_date'] = pd.NaT
    data.rename(
        columns={
            'ex_dividend': 'amount',
            'date': 'ex_date',
        },
        inplace=True,
        copy=False,
    )
    return data


def parse_pricing_and_vol(data,
                          sessions,
                          symbol_map):
    for asset_id, symbol in iteritems(symbol_map):
        asset_data = data.xs(
            symbol,
            level=1
        ).reindex(
            sessions.tz_localize(None)
        ).fillna(0.0)
        yield asset_id, asset_data


@bundles.register('arctic')
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
    quandl_bundle builds a daily dataset using Quandl's WIKI Prices dataset.
    For more information on Quandl's API and how to obtain an API key,
    please visit https://docs.quandl.com/docs#section-authentication
    """
    
    # download 
    raw_data = fetch_data_table(
        api_key,
        show_progress,
        environ.get('QUANDL_DOWNLOAD_ATTEMPTS', 5)
    )

    # download asset_metadata from arctic/mongodb location 
    asset_metadata = gen_asset_metadata(
        raw_data[['symbol', 'date']],
        show_progress
    )

    asset_db_writer.write(equities=asset_metadata)

    symbol_map = asset_metadata.symbol
    sessions = calendar.sessions_in_range(start_session, end_session)


    # download daily_bar_data_from


    raw_data.set_index(['date', 'symbol'], inplace=True)
    daily_bar_writer.write(
        parse_pricing_and_vol(
            raw_data,
            sessions,
            symbol_map
        ),
        show_progress=show_progress
    )

    raw_data.reset_index(inplace=True)
    raw_data['symbol'] = raw_data['symbol'].astype('category')
    raw_data['sid'] = raw_data.symbol.cat.codes
    adjustment_writer.write(
        splits=parse_splits(
            raw_data[[
                'sid',
                'date',
                'split_ratio',
            ]].loc[raw_data.split_ratio != 1],
            show_progress=show_progress
        ),
        dividends=parse_dividends(
            raw_data[[
                'sid',
                'date',
                'ex_dividend',
            ]].loc[raw_data.ex_dividend != 0],
            show_progress=show_progress
        )
    )
