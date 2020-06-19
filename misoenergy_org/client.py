import pandas as pd
import re
from datetime import date, datetime, time
import pytz

class MisoClient:

    base_url = "https://docs.misoenergy.org/marketreports"
    date_format = '%Y%m%d'
    timezone = pytz.timezone('US/Eastern')

    def __init__(self):
        pass

    def da_expost_lmp(self, day: date):
        url = f"{self.base_url}/{day.strftime(self.date_format)}_da_expost_lmp.csv"
        csv = pd.read_csv(url, skiprows=4)
        timestamps = list(filter(lambda column: re.search('HE (\d+)', column),csv.columns))
        attributes = list(filter(lambda column: column not in timestamps, csv.columns))
        timeseries = pd.melt(csv, id_vars=attributes, value_vars=timestamps)
        timeseries['TS'] = [self.timezone.localize(datetime.combine(day, time(hour=int(re.search('\d+', var).group()) - 1, minute=0, second=0))) for var in timeseries['variable']]
        timeseries = timeseries.pivot_table(index=['TS','Node'], columns='Value', values='value').reset_index()
        return timeseries

    def day_ahead_lmp(self, d: date, value: str = 'LMP'):
        url = f"{self.base_url}/{d.strftime('%Y%m%d')}_da_expost_lmp.csv"
        r = pd.read_csv(url, skiprows=4)
        indexed = r.set_index('Node')
        timeseries = indexed[indexed['Value'] == value].drop(columns=['Type','Value']).T
        hours = [int(re.search('\d+', t).group()) for t in timeseries.index.values]
        timeseries['TS'] = [datetime(year=d.year, month=d.month, day=d.day, hour=h-1) for h in hours]
        timeseries.set_index('TS')
        return timeseries
