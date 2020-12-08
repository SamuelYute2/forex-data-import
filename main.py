import os

import pandas as pd


def parse_dukascopy_raw_date_time(datetime):
    datetime = datetime.split(' ')
    date = datetime[0].split('.')
    time = datetime[1].split(':')

    return {
        "day": date[0],
        "month": date[1],
        "year": date[2],
        "hour": time[0],
        "min": time[1],
        "sec": time[2][:2]
    }

def convert_dukascopy_date_time_to_str(p_d_t):
    return p_d_t['year'] + "-" + p_d_t['month'] + "-" + p_d_t['day'] + " " + p_d_t['hour'] + ":" + p_d_t['min'] + ":" + \
           p_d_t['sec']

def parse_dukascopy_date(date):
    return convert_dukascopy_date_time_to_str(parse_dukascopy_raw_date_time(date))


def resample_ohlc(ohlc, interval):
    ohlc_dict = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
    ohlc = ohlc.resample(interval).agg(ohlc_dict).dropna(how='any')

    cols = ['open', 'high', 'low', 'close', 'volume']
    return ohlc[cols]


def create_folder_if_not_exists(currency_pair, year):
    base_path = 'FXData/CurrencyPairs'

    currency_pair_path = base_path + '/' + currency_pair
    if not os.path.exists(currency_pair_path):
        os.mkdir(currency_pair_path)

    year_path = currency_pair_path + '/' + year
    if not os.path.exists(year_path):
        os.mkdir(year_path)


def import_from_dukascopy_ohlc(path, currency_pair, year, delimiter=','):
    ohlc = pd.read_csv(path + '.csv',
                       parse_dates=True,
                       date_parser=parse_dukascopy_date,
                       usecols=['datetime', 'open', 'high', 'low', 'close', 'volume'],
                       index_col=0,
                       na_values=['nan'],
                       delimiter=delimiter)

    intervals = ['1Min', '5Min', '15Min', '30Min', '1H', '2H', '4H', '6H', '8H', '1D', '1W', '1M']
    create_folder_if_not_exists(currency_pair, year)

    for interval in intervals:
        ohlc = resample_ohlc(ohlc, interval)
        ohlc = ohlc[(ohlc.open != ohlc.high) | (ohlc.high != ohlc.low) | (ohlc.low != ohlc.close)].round(5)
        ohlc.to_csv('FXData/CurrencyPairs/'+str(currency_pair)+'/'+str(year)+'/'+str(interval)+'.csv')


years = ['2018', '2019']

currency_pairs = [
    'EURUSD'
]

for currency_pair in currency_pairs:
    for year in years:
        path = currency_pair + 'M1' + year
        import_from_dukascopy_ohlc(path=path, currency_pair=currency_pair, year=year, delimiter=',')
