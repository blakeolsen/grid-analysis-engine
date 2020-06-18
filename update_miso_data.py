import argparse
from datetime import date, timedelta
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


def get_timestamps(client: bigquery.Client, table_name: str):
    job_config = bigquery.QueryJobConfig()
    sql = f"""
    SELECT TS 
    FROM {table_name}
    """
    results = client.query(sql, job_config=job_config)
    results.result()
    return

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

        current_date += end_date



