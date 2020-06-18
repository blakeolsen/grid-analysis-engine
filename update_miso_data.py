import argparse
from datetime import date, datetime, timedelta
from google.cloud import bigquery
from miso_org.client import MisoClient

table_base_name = "grid-analysis-engine.misoenergy_org.miso_{}"
choices = ['da_expost_lmp']


def get_or_create_table(table_name: str):
    try:
        table = bq.get_table(table_name)
    except Exception:
        print(f"Table: {table_name} does not exist, do you want to create it? If yes type `y`")
        create_table = input()
        if create_table == 'y':
            table = bq.create_table(table_name)
    return table


def exists(client: bigquery.Client, table_name: str, d: date):
    job_config = bigquery.QueryJobConfig()
    sql = f"""
    SELECT TS 
    FROM {table_name}
    WHERE TS BETWEEN '{datetime(year=d.year, month=d.month, day=d.day, hour=0).isoformat(sep=' ')} UTC' AND '{datetime(year=d.year, month=d.month, day=d.day, hour=23).isoformat(sep=' ')} UTC'
    """
    results = client.query(sql, job_config=job_config)
    return results.result().total_rows > 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="ETL job for miso data")
    parser.add_argument('data_set', type=str, choices=choices,
                        help='dataset to gather')
    parser.add_argument('--start', type=str, dest='start_date',
                        help='start date in ISOFormat')
    parser.add_argument('--end', type=str, dest='end_date',
                        help='end date in ISOFormat')
    args = parser.parse_args()
    data_set = args.data_set
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)

    miso = MisoClient()
    bq = bigquery.Client()
    table_name = table_base_name.format(data_set)
    table = get_or_create_table(table_name)

    current_date = start_date
    delta = timedelta(days=1)

    while current_date < end_date:
        if exists(bq, table_name, current_date):
            print(f"{str(current_date)} already exists... skipping")
        else:
            print(f"fetching {str(current_date)}... starting")
            data = miso.day_ahead_lmp(current_date)
            print(f"fetching {str(current_date)}... complete")
            data = data.rename(lambda w: w.replace('.', '_'), axis='columns')
            print(f"loading {str(current_date)}... starting")
            bq.load_table_from_dataframe(data, table)
            print(f"loading {str(current_date)}... complete")

        current_date += delta

    print(f"finished loading data for dates {str(start_date)} to {str(end_date)}")
