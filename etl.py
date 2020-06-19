import argparse
from datetime import date, datetime, timedelta
from google.cloud import bigquery
from misoenergy_org.client import MisoClient

project_id = "grid-analysis-engine"
miso = MisoClient()
datasets = {
    'misoenergy_org.da_expost_lmp': miso.da_expost_lmp
}


def get_or_create_table(table_name: str):
    try:
        table = bq.get_table(table_name)
    except Exception:
        print(f"Table: {table_name} does not exist, do you want to create it? If yes type `y`")
        create_table = input()
        if create_table == 'y':
            table = bq.create_table(table_name)
    return table


def exists(client: bigquery.Client, table: bigquery.Table, d: date):
    def date_in_table():
        job_config = bigquery.QueryJobConfig()
        sql = f"""
        SELECT TS 
        FROM {table_name}
        WHERE TS BETWEEN '{datetime(year=d.year, month=d.month, day=d.day, hour=0).isoformat(sep=' ')} UTC' AND '{datetime(year=d.year, month=d.month, day=d.day, hour=23).isoformat(sep=' ')} UTC'
        """
        results = client.query(sql, job_config=job_config)
        return results.result().total_rows > 0

    return (table.num_rows > 0) and date_in_table()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="ETL job for miso data")
    parser.add_argument('dataset', type=str, choices=datasets.keys(),
                        help='dataset to gather')
    parser.add_argument('--start', type=str, dest='start_date',
                        help='start date in ISOFormat')
    parser.add_argument('--end', type=str, dest='end_date',
                        help='end date in ISOFormat')
    args = parser.parse_args()
    dataset = args.dataset
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)

    bq = bigquery.Client()
    dataset_id = dataset.split('.')[0]
    table_id = dataset.split('.')[1]
    table_name = f"{project_id}.{dataset_id}.{table_id}"
    table = get_or_create_table(table_name)

    current_date = start_date
    delta = timedelta(days=1)

    while current_date < end_date:
        if exists(bq, table, current_date):
            print(f"{str(current_date)} already exists... skipping")
        else:
            print(f"fetching {str(current_date)}... starting")
            data = datasets[dataset](current_date)
            print(f"fetching {str(current_date)}... complete")
            print(f"loading {str(current_date)}... starting")
            bq.load_table_from_dataframe(data, table)
            print(f"loading {str(current_date)}... complete")

        current_date += delta

    print(f"finished loading data for dates {str(start_date)} to {str(end_date)}")
