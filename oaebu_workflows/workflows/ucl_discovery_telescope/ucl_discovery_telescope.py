# Copyright 2023 Curtin University
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
from airflow.hooks.base import BaseHook
from google.cloud.bigquery import SourceFormat, TimePartitioningType, WriteDisposition
from google.oauth2 import service_account
from apiclient import discovery

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api
from observatory.platform.airflow import AirflowConns
from observatory.platform.files import save_jsonl_gz, load_jsonl
from observatory.platform.gcs import gcs_blob_uri, gcs_upload_files, gcs_blob_name_from_path
from observatory.platform.bigquery import bq_load_table, bq_table_id, bq_create_dataset
from observatory.platform.utils.url_utils import retry_get_url
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.files import add_partition_date
from observatory.platform.workflows.workflow import (
    Workflow,
    PartitionRelease,
    cleanup,
    set_task_state,
    check_workflow_inputs,
)


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

        self.download_country_path = os.path.join(self.download_folder, "ucl_discovery_country.jsonl.gz")
        self.download_totals_path = os.path.join(self.download_folder, "ucl_discovery_totals.jsonl.gz")
        self.transform_path = os.path.join(self.transform_folder, "ucl_discovery.jsonl.gz")


class UclDiscoveryTelescope(Workflow):
    """The UCL Discovery telescope."""

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        sheet_id: str,
        data_partner: Union[str, OaebuPartner] = "ucl_discovery",
        bq_dataset_description: str = "UCL Discovery dataset",
        bq_table_description: str = "UCL Discovery table",
        api_dataset_id: str = "ucl",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        oaebu_service_account_conn_id: str = "oaebu_service_account",
        max_threads: int = os.cpu_count() * 2,
        schedule: str = "0 0 4 * *",  # run on the 4th of every month
        start_date: pendulum.DateTime = pendulum.datetime(2015, 6, 1),
        catchup: bool = True,
        max_active_runs: int = 10,
    ):
        """Construct a UclDiscoveryTelescope instance.

        :param dag_id: The ID of the DAG
        :param cloud_workspace: The CloudWorkspace object for this DAG
        :param sheet_id:  The ID of the google sheet match eprint ID to ISBN13
        :param data_partner: The name of the data partner
        :param bq_dataset_description: Description for the BigQuery dataset
        :param bq_table_description: Description for the biguery table
        :param api_dataset_id: The ID to store the dataset release in the API
        :param observatory_api_conn_id: Airflow connection ID for the overvatory API
        :param oaebu_service_account_conn_id: Airflow connection ID for the oaebu service account
        :param max_threads: The maximum number threads to utilise for parallel processes
        :param schedule: The schedule interval of the DAG
        :param start_date: The start date of the DAG
        :param catchup: Whether to catchup the DAG or not
        :param max_active_runs: The maximum number of concurrent DAG runs
        """
        super().__init__(
            dag_id,
            start_date,
            schedule,
            catchup=catchup,
            max_active_runs=max_active_runs,
            airflow_conns=[observatory_api_conn_id, oaebu_service_account_conn_id],
            tags=["oaebu"],
        )

        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.sheet_id = sheet_id
        self.data_partner = partner_from_str(data_partner)
        self.bq_dataset_description = bq_dataset_description
        self.bq_table_description = bq_table_description
        self.api_dataset_id = api_dataset_id
        self.oaebu_service_account_conn_id = oaebu_service_account_conn_id
        self.max_threads = max_threads
        self.observatory_api_conn_id = observatory_api_conn_id

        check_workflow_inputs(self)

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> List[UclDiscoveryRelease]:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'. There will only be 1 release, but it is passed on as a list so the
        SnapshotTelescope template methods can be used.

        :param kwargs: the context passed from the PythonOperator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
        :return: A list with one ucldiscovery release instance.
        """
        data_interval_start = kwargs["data_interval_start"].start_of("month")
        data_interval_end = kwargs["data_interval_end"].start_of("month")
        partition_date = data_interval_start.end_of("month")
        run_id = kwargs["run_id"]

        logging.info(
            f"Interval Start: {data_interval_start}, Interval End:{data_interval_end}, Partition date: {partition_date}, Run ID: {run_id}"
        )
        return UclDiscoveryRelease(
            self.dag_id,
            kwargs["run_id"],
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
            partition_date=partition_date,
        )

    def download(self, release: UclDiscoveryRelease, **kwargs):
        """Fownload the ucl discovery data for a given release.
        :param releases: The UCL discovery release.
        """
        mappings = get_isbn_eprint_mappings(self.sheet_id, self.oaebu_service_account_conn_id, release.partition_date)

        with ThreadPoolExecutor(self.max_threads) as executor:
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

    def upload_downloaded(self, release: UclDiscoveryRelease, **kwargs):
        """Uploads the downloaded files to GCS"""
        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.download_bucket,
            file_paths=[release.download_country_path, release.download_totals_path],
        )
        set_task_state(success, kwargs["ti"].task_id, release=release)

    def transform(self, release: UclDiscoveryRelease, **kwargs):
        """Transform the ucl discovery data for a given release."""
        mappings = get_isbn_eprint_mappings(self.sheet_id, self.oaebu_service_account_conn_id, release.partition_date)

        # Load the records and sort them by eprint id
        country_records = load_jsonl(release.download_country_path)
        totals_records = load_jsonl(release.download_totals_path)
        country_records = sorted(country_records, key=lambda x: x["set"]["value"])  # ["set"]["value"] = eprint_id
        totals_records = sorted(totals_records, key=lambda x: x["set"]["value"])
        assert len(country_records) == len(totals_records), f"{len(country_records)} != {len(totals_records)}"

        with ThreadPoolExecutor(self.max_threads) as executor:
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
        print(results)
        save_jsonl_gz(release.transform_path, results)

    def upload_transformed(self, release: UclDiscoveryRelease, **kwargs):
        """Uploads the transformed file to GCS"""
        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.transform_bucket, file_paths=[release.transform_path]
        )
        set_task_state(success, kwargs["ti"].task_id, release=release)

    def bq_load(self, release: UclDiscoveryRelease, **kwargs) -> None:
        """Loads the transformed data into BigQuery"""
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.data_partner.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.bq_dataset_description,
        )

        uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.transform_path))
        table_id = bq_table_id(
            self.cloud_workspace.project_id, self.data_partner.bq_dataset_id, self.data_partner.bq_table_name
        )
        state = bq_load_table(
            uri=uri,
            table_id=table_id,
            schema_file_path=self.data_partner.schema_path,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            partition_type=TimePartitioningType.MONTH,
            partition=True,
            partition_field="release_date",
            write_disposition=WriteDisposition.WRITE_APPEND,
            table_description=self.bq_table_description,
            ignore_unknown_values=True,
        )
        set_task_state(state, kwargs["ti"].task_id, release=release)

    def add_new_dataset_releases(self, release: UclDiscoveryRelease, **kwargs) -> None:
        """Adds release information to API."""
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.api_dataset_id,
            dag_run_id=release.run_id,
            data_interval_start=kwargs["data_interval_start"],
            data_interval_end=kwargs["data_interval_end"],
            partition_date=release.partition_date,
        )
        api.post_dataset_release(dataset_release)

    def cleanup(self, release: UclDiscoveryRelease, **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""
        cleanup(dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder)


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
