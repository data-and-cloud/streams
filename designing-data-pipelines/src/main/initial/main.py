import logging
import re
import subprocess
import sys
from datetime import date, datetime
from os import environ, path, getcwd
from pathlib import Path
from typing import Iterable

# noinspection PyPackageRequirements
from google.cloud.bigquery import Client as BigQueryClient, LoadJobConfig, SourceFormat
# noinspection PyPackageRequirements
from google.cloud.storage import Client as StorageClient, Blob
# noinspection PyPackageRequirements
from google.cloud.logging_v2 import Client as LoggingClient


environ['project'] = 'design-of-data-pipelines'
environ['tracking_data_bucket'] = 'tracking-data-215588091788'
environ['full_table_id'] = 'design-of-data-pipelines.tracking_data.raw'
environ['partitioning_field'] = 'event_time'


logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logging_client = LoggingClient(project=environ['project'])
logging_client.setup_logging(
    log_level=logging.INFO,
    labels={'stream': 'designing-data-pipelines'}
)


def load_missing_partitions() -> None:
    storage_client = StorageClient()

    blobs = list(_yield_all_blobs(storage_client=storage_client))
    blobs_with_dates = [
        (blob, _extract_date_from_blob_name(blob.name))
        for blob in blobs
    ]

    # noinspection PyTypeChecker
    bigquery_client = BigQueryClient(project=environ['project'])
    full_table_id = environ['full_table_id']
    partition_dates = set(_yield_all_partitions(bigquery_client=bigquery_client, full_table_id=full_table_id))

    for blob, blob_date in blobs_with_dates:
        if blob_date not in partition_dates:
            load_partition_from_blob(
                blob=blob,
                blob_date=blob_date,
                bigquery_client=bigquery_client,
                full_table_id=full_table_id
            )


def _yield_all_blobs(storage_client: StorageClient) -> Iterable[Blob]:
    tracking_data_bucket = environ['tracking_data_bucket']
    return storage_client.list_blobs(tracking_data_bucket)


def _extract_date_from_blob_name(blob_name: str) -> date:
    match = re.match(r'zip/(\d{4}-\d{2}-\d{2})\.csv\.zip', blob_name)

    if match:
        return datetime.strptime(match.groups()[0], '%Y-%m-%d').date()

    else:
        # Replace by proper exception type
        raise Exception(f'Invalid blob name {blob_name}')


def _yield_all_partitions(bigquery_client: BigQueryClient, full_table_id: str) -> Iterable[date]:
    partitioning_field = environ['partitioning_field']

    query = _create_partitions_query(partitioning_field=partitioning_field, full_table_id=full_table_id)
    query_job = bigquery_client.query(query=query)
    rows = query_job.result()
    for row in rows:
        yield row[partitioning_field]


def _create_partitions_query(partitioning_field: str, full_table_id: str) -> str:
    # for larger tables consider to include filtering on partitions e.g. by only considering the last 30 days as
    # a default
    return (
        f'SELECT DATE({partitioning_field}) AS {partitioning_field} '
        f'FROM `{full_table_id}` '
        f'GROUP BY DATE({partitioning_field})'
    )


def load_partition_from_blob(
        blob: Blob,
        blob_date: date,
        bigquery_client: BigQueryClient,
        full_table_id: str
):
    logging.info(f'Processing partition {blob_date.strftime("%Y-%m-%d")}')

    local_csv_file = _extract_csv_from_blob(blob=blob, blob_date=blob_date)

    job_config = _create_bigquery_csv_job_config()

    with open(local_csv_file, 'rb') as csv_file:
        job = bigquery_client.load_table_from_file(
            file_obj=csv_file,
            destination=f'{full_table_id}${blob_date.strftime("%Y%m%d")}',
            job_config=job_config
        )
        job.result()


def _extract_csv_from_blob(blob: Blob, blob_date: date) -> str:
    local_out_dir = 'out'

    blob_date_str = blob_date.strftime("%Y-%m-%d")
    local_zip_file = f'{local_out_dir}/{blob_date_str}.zip'

    _create_dir(local_out_dir)
    blob.download_to_filename(filename=local_zip_file)

    _unzip(file_path=local_zip_file, target_dir=local_out_dir)

    return f'{local_out_dir}/{blob_date_str}.csv'


def _create_dir(dir_name: str) -> None:
    local_dir = path.join(getcwd(), dir_name)
    with Path(local_dir) as path_creator:
        path_creator.mkdir(exist_ok=True)


def _unzip(file_path: str, target_dir: str) -> None:
    subprocess.run(['tar', '-xf', file_path, '-C', target_dir])


def _create_bigquery_csv_job_config() -> LoadJobConfig:
    return LoadJobConfig(
        source_format=SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        autodetect=False
    )


if __name__ == "__main__":
    load_missing_partitions()
