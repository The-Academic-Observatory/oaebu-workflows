# Copyright 2023-2024 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Aniek Roelofs, Keegan Smith


import logging
import os
from typing import List, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from google.cloud.bigquery import SourceFormat, TimePartitioningType, WriteDisposition, Client
from google.oauth2 import service_account
from apiclient import discovery

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.files import save_jsonl_gz, load_jsonl
from observatory_platform.google.gcs import gcs_blob_uri, gcs_upload_files, gcs_blob_name_from_path, gcs_download_blob
from observatory_platform.google.bigquery import bq_load_table, bq_table_id, bq_create_dataset
from observatory_platform.url_utils import retry_get_url
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.files import add_partition_date
from observatory_platform.airflow.release import PartitionRelease, set_task_state
from observatory_platform.airflow.workflow import CloudWorkspace, cleanup


class UclDiscoveryRelease(PartitionRelease):
    def __init__(
        self,
        dag_id: str,
        run_id: str,
        data_interval_start: pendulum.DateTime,
        data_interval_end: pendulum.DateTime,
        partition_date: pendulum.DateTime,
    ):
        """Construct a UclDiscoveryRelease instance.

        :param dag_id: The ID of the DAG
        :param run_id: The Airflow run ID.
        :param data_interval_start: The start of the data interval.
        :param data_interval_end: The end of the data interval.
        :param partition_date: The partition date for this release.
        """
        super().__init__(dag_id=dag_id, run_id=run_id, partition_date=partition_date)
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end
        self.download_country_file_name = "ucl_discovery_country.jsonl.gz"
        self.download_totals_file_name = "ucl_discovery_totals.jsonl.gz"
        self.transform_file_name = "ucl_discovery.jsonl.gz"

    @property
    def download_country_path(self):
        return os.path.join(self.download_folder, "ucl_discovery_country.jsonl.gz")

    @property
    def download_totals_path(self):
        return os.path.join(self.download_folder, "ucl_discovery_totals.jsonl.gz")

    @property
    def transform_path(self):
        return os.path.join(self.transform_folder, "ucl_discovery.jsonl.gz")

    @property
    def download_country_blob_name(self):
        return gcs_blob_name_from_path(self.download_country_path)

    @property
    def download_totals_blob_name(self):
        return gcs_blob_name_from_path(self.download_totals_path)

    @property
    def transform_blob_name(self):
        return gcs_blob_name_from_path(self.transform_path)

    @staticmethod
    def from_dict(dict_: dict):
        return UclDiscoveryRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            data_interval_start=pendulum.from_format(dict_["data_interval_start"], "YYYY-MM-DD"),
            data_interval_end=pendulum.from_format(dict_["data_interval_end"], "YYYY-MM-DD"),
            partition_date=pendulum.from_format(dict_["partition_date"], "YYYY-MM-DD"),
        )

    def to_dict(self) -> dict:
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "data_interval_start": self.data_interval_start.to_date_string(),
            "data_interval_end": self.data_interval_end.to_date_string(),
            "partition_date": self.partition_date.to_date_string(),
        }


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    sheet_id: str,
    data_partner: Union[str, OaebuPartner] = "ucl_discovery",
    bq_dataset_description: str = "UCL Discovery dataset",
    bq_table_description: str = "UCL Discovery table",
    api_dataset_id: str = "ucl",
    oaebu_service_account_conn_id: str = "oaebu_service_account",
    max_threads: int = os.cpu_count() * 2,
    schedule: str = "0 0 4 * *",  # run on the 4th of every month
    start_date: pendulum.DateTime = pendulum.datetime(2015, 6, 1),
    catchup: bool = True,
    max_active_runs: int = 10,
):
    """Construct a UclDiscovery DAG.

    :param dag_id: The ID of the DAG
    :param cloud_workspace: The CloudWorkspace object for this DAG
    :param sheet_id:  The ID of the google sheet match eprint ID to ISBN13
    :param data_partner: The name of the data partner
    :param bq_dataset_description: Description for the BigQuery dataset
    :param bq_table_description: Description for the biguery table
    :param api_dataset_id: The ID to store the dataset release in the API
    :param oaebu_service_account_conn_id: Airflow connection ID for the oaebu service account
    :param max_threads: The maximum number threads to utilise for parallel processes
    :param schedule: The schedule interval of the DAG
    :param start_date: The start date of the DAG
    :param catchup: Whether to catchup the DAG or not
    :param max_active_runs: The maximum number of concurrent DAG runs
    """
    data_partner = partner_from_str(data_partner)

    @dag(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=catchup,
        max_active_runs=max_active_runs,
        tags=["oaebu"],
        default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=5)},
    )
    def ucl_discovery():
        @task()
        def make_release(**context) -> List[dict]:
            """Creates a new ucl discovery release instance

            :param context: the context passed from the PythonOperator.
            See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
            :return: A list with one ucldiscovery release instance.
            """

            data_interval_start = context["data_interval_start"].start_of("month")
            data_interval_end = context["data_interval_end"].start_of("month")
            partition_date = data_interval_start.end_of("month")
            run_id = context["run_id"]

            logging.info(
                f"Interval Start: {data_interval_start}, Interval End:{data_interval_end}, Partition date: {partition_date}, Run ID: {run_id}"
            )
            return UclDiscoveryRelease(
                dag_id,
                context["run_id"],
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
                partition_date=partition_date,
            ).to_dict()

        @task()
        def download(release: dict, **context) -> None:
            """Fownload the ucl discovery data for a given release.
            :param releases: The UCL discovery release.
            """

            release = UclDiscoveryRelease.from_dict(release)
            mappings = get_isbn_eprint_mappings(sheet_id, oaebu_service_account_conn_id, release.partition_date)
            with ThreadPoolExecutor(max_threads) as executor:
                futures = []
                for eprint_id in mappings.keys():
                    future = executor.submit(
                        download_discovery_stats, eprint_id, release.data_interval_start, release.partition_date
                    )
                    futures.append(future)
                totals = []
                country = []
                for future in as_completed(futures):
                    c, t = future.result()
                    country.append(c)
                    totals.append(t)

            logging.info(f"Saving totals data to file: {release.download_totals_path}")
            save_jsonl_gz(release.download_totals_path, totals)
            logging.info(f"Saving country data to file: {release.download_country_path}")
            save_jsonl_gz(release.download_country_path, country)

            success = gcs_upload_files(
                bucket_name=cloud_workspace.download_bucket,
                file_paths=[release.download_country_path, release.download_totals_path],
            )
            set_task_state(success, context["ti"].task_id, release=release)

        @task()
        def transform(release: dict, **context) -> None:
            """Transform the ucl discovery data for a given release."""

            release = UclDiscoveryRelease.from_dict(release)
            # Download files from GCS
            success = gcs_download_blob(
                bucket_name=cloud_workspace.download_bucket,
                blob_name=release.download_country_blob_name,
                file_path=release.download_country_path,
            )
            if not success:
                raise FileNotFoundError(f"Error downloading file: {release.download_country_blob_name}")
            success = gcs_download_blob(
                bucket_name=cloud_workspace.download_bucket,
                blob_name=release.download_totals_blob_name,
                file_path=release.download_totals_path,
            )
            if not success:
                raise FileNotFoundError(f"Error downloading file: {release.download_totals_blob_name}")

            # Load the records and sort them by eprint id
            mappings = get_isbn_eprint_mappings(sheet_id, oaebu_service_account_conn_id, release.partition_date)
            country_records = load_jsonl(release.download_country_path)
            totals_records = load_jsonl(release.download_totals_path)
            country_records = sorted(country_records, key=lambda x: x["set"]["value"])  # ["set"]["value"] = eprint_id
            totals_records = sorted(totals_records, key=lambda x: x["set"]["value"])
            if not len(country_records) == len(totals_records):
                raise RuntimeError(f"{len(country_records)} != {len(totals_records)}")

            with ThreadPoolExecutor(max_threads) as executor:
                futures = []
                for country_record, totals_record in zip(country_records, totals_records):
                    isbn = mappings[country_record["set"]["value"]]["ISBN13"]
                    title = mappings[country_record["set"]["value"]]["title"]
                    future = executor.submit(transform_discovery_stats, country_record, totals_record, isbn, title)
                    futures.append(future)
                results = []
                for future in as_completed(futures):
                    results.append(future.result())

            # Add the release date to the data as a parition field
            results = add_partition_date(
                results, release.partition_date, TimePartitioningType.MONTH, partition_field="release_date"
            )
            save_jsonl_gz(release.transform_path, results)
            success = gcs_upload_files(
                bucket_name=cloud_workspace.transform_bucket, file_paths=[release.transform_path]
            )
            set_task_state(success, context["ti"].task_id, release=release)

        @task()
        def bq_load(release: dict, **context) -> None:
            """Loads the transformed data into BigQuery"""

            release = UclDiscoveryRelease.from_dict(release)
            bq_create_dataset(
                project_id=cloud_workspace.project_id,
                dataset_id=data_partner.bq_dataset_id,
                location=cloud_workspace.data_location,
                description=bq_dataset_description,
            )

            uri = gcs_blob_uri(cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.transform_path))
            table_id = bq_table_id(cloud_workspace.project_id, data_partner.bq_dataset_id, data_partner.bq_table_name)
            client = Client(project=cloud_workspace.project_id)
            state = bq_load_table(
                uri=uri,
                table_id=table_id,
                schema_file_path=data_partner.schema_path,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                partition_type=TimePartitioningType.MONTH,
                partition=True,
                partition_field="release_date",
                write_disposition=WriteDisposition.WRITE_APPEND,
                table_description=bq_table_description,
                ignore_unknown_values=True,
                client=client,
            )
            set_task_state(state, context["ti"].task_id, release=release)

        @task()
        def add_new_dataset_releases(release: dict, **context) -> None:
            """Adds release information to API."""

            release = UclDiscoveryRelease.from_dict(release)
            client = Client(project=cloud_workspace.project_id)
            api = DatasetAPI(project_id=cloud_workspace.project_id, dataset_id=api_dataset_id, client=client)
            api.seed_db()
            dataset_release = DatasetRelease(
                dag_id=dag_id,
                dataset_id=api_dataset_id,
                dag_run_id=release.run_id,
                created=pendulum.now(),
                modified=pendulum.now(),
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
                partition_date=release.partition_date,
            )
            api.add_dataset_release(dataset_release)

        @task()
        def cleanup_workflow(release: dict, **context) -> None:
            """Delete all files, folders and XComs associated with this release."""

            release = UclDiscoveryRelease.from_dict(release)
            cleanup(dag_id=dag_id, execution_date=context["execution_date"], workflow_folder=release.workflow_folder)

        task_check_dependencies = check_dependencies(airflow_conns=[oaebu_service_account_conn_id])
        xcom_release = make_release()
        task_download = download(xcom_release)
        task_transform = transform(xcom_release)
        task_bq_load = bq_load(xcom_release)
        task_add_new_dataset_releases = add_new_dataset_releases(xcom_release)
        task_cleanup_workflow = cleanup_workflow(xcom_release)

        (
            task_check_dependencies
            >> xcom_release
            >> task_download
            >> task_transform
            >> task_bq_load
            >> task_add_new_dataset_releases
            >> task_cleanup_workflow
        )

    return ucl_discovery()


def get_isbn_eprint_mappings(sheet_id: str, service_account_conn_id: str, cutoff_date: pendulum.DateTime) -> dict:
    """Get the eprint id to isbn mapping from the google sheet

    :param sheet_id: The ID of the google sheet.
    :param credentials: The credentials object to authenticate with.
    :param cutoff_date: The cutoff date. If an item is published after this date, it will be skipped.
    """
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    service_account_conn = BaseHook.get_connection(service_account_conn_id)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_conn.extra_dejson, scopes=scopes
    )
    service = discovery.build("sheets", "v4", credentials=credentials)
    result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range="isbn_mapping").execute()
    sheet_contents = result.get("values")
    if not sheet_contents:
        raise ValueError(f"No content found for sheet with ID {sheet_id}")

    items = []
    header = sheet_contents[0]
    if not all(heading in header for heading in ["ISBN13", "discovery_eprintid", "date", "title_list_title"]):
        raise ValueError(f"Invalid header found for sheet: {header}")
    for row in sheet_contents[1:]:
        items.append(dict(zip(header, row)))

    mappings = {}
    for item in items:
        eprint_id = item.get("discovery_eprintid")
        isbn = item.get("ISBN13")
        title = item.get("title_list_title")
        if not eprint_id or not isbn:
            logging.warn(f"Item with missing information will be skipped: {item}")
            continue
        if pendulum.parse(item["date"]) > cutoff_date:
            logging.info(f"Item released after cutoff date will be skipped: {item}")
            continue
        mappings[eprint_id] = {"ISBN13": isbn, "title": title}

    return mappings


def download_discovery_stats(eprint_id: str, start_date: pendulum.DateTime, end_date: pendulum.DateTime):
    """Downloads the discovery stats for a given eprint ID within a specified date range.

    :param eprint_id: The eprint ID of the item to get the stats for.
    :param start_date: The start date of the date range.
    :param end_date: The end date of the date range.
    :return: A tuple containing the country statistics and the total downloads statistics.
    """
    countries_url = (
        "https://discovery.ucl.ac.uk/cgi/stats/get"
        f"?from={start_date.format('YYYYMMDD')}&to={end_date.format('YYYYMMDD')}"
        f"&irs2report=eprint&set_name=eprint&set_value={eprint_id}&datatype=countries&top=countries"
        "&view=Table&limit=all&export=JSON"
    )
    totals_url = (
        "https://discovery.ucl.ac.uk/cgi/stats/get"
        f"?from={start_date.format('YYYYMMDD')}&to={end_date.format('YYYYMMDD')}"
        f"&irs2report=eprint&set_name=eprint&set_value={eprint_id}&datatype=downloads&graph_type=column"
        "&view=Google%3A%3AGraph&date_resolution=month&title=Download+activity+-+last+12+months&export=JSON"
    )
    response = retry_get_url(countries_url)
    country = response.json()
    response = retry_get_url(totals_url)
    totals = response.json()

    # Perform some checks on the returned data
    timescale = (start_date.format("YYYYMMDD"), end_date.format("YYYYMMDD"))
    country_timescale = (country["timescale"]["from"], country["timescale"]["to"])
    totals_timescale = (totals["timescale"]["from"], totals["timescale"]["to"])
    if country_timescale != timescale or totals_timescale != timescale:
        raise ValueError(
            f"Invalid timescale value(s): country: {country['timescale']} | totals: {totals['timescale']} != {timescale}"
        )
    if country["set"]["value"] != eprint_id or totals["set"]["value"] != eprint_id:
        raise ValueError(
            f"Invalid eprint ID values downloaded: {totals['set']['value']} | {country['set']['value']} != {eprint_id}"
        )

    return country, totals


def transform_discovery_stats(country_record: dict, totals_record: dict, isbn: str, title: str) -> dict:
    """Transforms the discovery stats for a single set of records

    :param country_record: The country record
    :param totals_record: The totals record
    :param isbn: The isbn that matches the eprint id
    :return: The transformed stats
    """
    # Sanity check the records
    country_eprint_id = country_record["set"]["value"]
    totals_eprint_id = totals_record["set"]["value"]
    if country_eprint_id != totals_eprint_id:
        raise ValueError(f"Country and totals eprint ID do not match: {country_eprint_id} != {totals_eprint_id}")

    country_timescale = country_record["timescale"]
    totals_timescale = totals_record["timescale"]
    if country_timescale != totals_timescale:
        raise ValueError(f"Timescales do not match: {country_timescale} != {totals_timescale}")

    # If there are no downloads for the time period, there is no "records" field in country stats
    country_records = country_record.get("records", [])

    transformed = {
        "ISBN": isbn,
        "title": title,
        "eprint_id": totals_record["set"]["value"],
        "timescale": totals_record["timescale"],
        "origin": totals_record["origin"],
        "total_downloads": totals_record["records"][0]["count"],
        "country": country_records,
    }
    return transformed
