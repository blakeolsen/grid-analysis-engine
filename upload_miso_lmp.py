from google.cloud import bigquery
from miso_org import client
from datetime import date, timedelta

bq = bigquery.Client()
miso = client.DayAheadMarketClient()

def get_for_day(day: date):
    return miso.day_ahead_lmp(day).rename(lambda w: w.replace('.', '_'), axis='columns')

start_date = date(2019,1,1)
total = get_for_day(start_date)
for days_after in range(365-1):
    day = start_date + timedelta(days=days_after+1)
    print(day)
    total = total.append(get_for_day(day), ignore_index=True)

table_prefix = 'grid-analysis-engine.misoenergy_org'
table_id = f"{table_prefix}.miso_lmp_expost_2019"
table = bq.create_table(table_id)
bq.load_table_from_dataframe(total, table)
