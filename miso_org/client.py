import pandas as pd
import re
from datetime import date, datetime, timedelta

class MisoClient:

    base_url = "https://docs.misoenergy.org/marketreports"

    def __init__(self):
        pass

    def day_ahead_lmp(self, d: date, value: str = 'LMP'):
        url = f"{self.base_url}/{d.strftime('%Y%m%d')}_da_expost_lmp.csv"
        r = pd.read_csv(url, skiprows=4)
        indexed = r.set_index('Node')
        timeseries = indexed[indexed['Value'] == value].drop(columns=['Type','Value']).T
        hours = [int(re.search('\d+', t).group()) for t in timeseries.index.values]
        timeseries['TS'] = [datetime(year=d.year, month=d.month, day=d.day, hour=h-1) for h in hours]
        timeseries.set_index('TS')
        return timeseries
