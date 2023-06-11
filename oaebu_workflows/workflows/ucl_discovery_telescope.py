# Copyright 2020 Curtin University
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

# Author: Aniek Roelofs


import csv
import logging
import os
from datetime import timedelta
from typing import List, Tuple

import pendulum
from airflow.exceptions import AirflowSkipException
from google.cloud.bigquery import SourceFormat, TimePartitioningType, WriteDisposition

from oaebu_workflows.config import schema_folder as default_schema_folder
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api
from observatory.platform.airflow import AirflowConns
from observatory.platform.files import save_jsonl_gz
from observatory.platform.gcs import gcs_blob_uri, gcs_upload_files, gcs_blob_name_from_path
from observatory.platform.bigquery import bq_find_schema, bq_load_table, bq_table_id, bq_create_dataset
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
    def __init__(self, dag_id: str, run_id: str, start_date: pendulum.DateTime, end_date: pendulum.DateTime):
        """Construct a UclDiscoveryRelease instance.

        :param dag_id: The ID of the DAG
        :param run_id: The Airflow run ID.
        :param start_date: The start date of the DAG the start date of the download period.
        :param end_date: the end date of the download period, also used as release date for BigQuery table and
        file paths.
        """
        super().__init__(dag_id=dag_id, run_id=run_id, partition_date=end_date)

        self.start_date = start_date
        self.end_date = end_date
        self.eprint_metadata_url = (
            "https://discovery.ucl.ac.uk/cgi/search/archive/advanced/export_discovery_CSV.csv?"
            "screen=Search&dataset=archive&_action_export=1&output=CSV"
            "&exp=0|1|-date/creators_name/title|archive|-|date:date:ALL:EQ:-"
            f'{self.end_date.strftime("%Y")}|primo:primo:ANY:EQ:open'
            "|type:type:ANY:EQ:book|-|eprint_status:eprint_status:ANY:EQ:archive"
            "|metadata_visibility:metadata_visibility:ANY:EQ:show"
        )
        self.countries_url = (
            "https://discovery.ucl.ac.uk/cgi/stats/get?from="
            f'{self.start_date.strftime("%Y%m%d")}&to='
            f'{self.end_date.strftime("%Y%m%d")}&irs2report=eprint&datatype=countries&top=countries'
            f"&view=Table&limit=all&set_name=eprint&export=CSV&set_value="
        )
        self.download_path = os.path.join(self.download_folder, "ucl_discovery.txt")
        self.transform_path = os.path.join(self.download_folder, "ucl_discovery.jsonl.gz")


class UclDiscoveryTelescope(Workflow):
    """The UCL Discovery telescope."""

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "ucl",
        bq_table_name: str = "ucl_discovery",
        bq_dataset_description: str = "UCL Discovery dataset",
        bq_table_description: str = None,
        api_dataset_id: str = "ucl",
        schema_folder: str = default_schema_folder(),
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        schedule_interval: str = "@monthly",
        start_date: pendulum.DateTime = pendulum.datetime(2008, 1, 1),
        catchup: bool = True,
        max_active_runs: int = 10,
    ):
        """Construct a UclDiscoveryTelescope instance.
        :param dag_id: The ID of the DAG
        :param cloud_workspace: The CloudWorkspace object for this DAG
        :param bq_dataset_id: The BigQuery dataset ID
        :param bq_table_name: The BigQuery table name
        :param bq_dataset_description: Description for the BigQuery dataset
        :param bq_table_description: Description for the biguery table
        :param api_dataset_id: The ID to store the dataset release in the API
        :param schema_folder: The path to the SQL schema folder
        :param observatory_api_conn_id: Airflow connection ID for the overvatory API
        :param schedule_interval: The schedule interval of the DAG
        :param start_date: The start date of the DAG
        :param catchup: Whether to catchup the DAG or not
        :param max_active_runs: The maximum number of concurrent DAG runs
        """
        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            catchup=catchup,
            max_active_runs=max_active_runs,
            airflow_conns=[observatory_api_conn_id],
            tags=["oaebu"],
        )

        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.bq_dataset_description = bq_dataset_description
        self.bq_table_description = bq_table_description
        self.api_dataset_id = api_dataset_id
        self.schema_folder = schema_folder
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
        # Get start and end date (end_date = partition_date)
        start_date = kwargs["execution_date"]
        end_date = kwargs["next_execution_date"] - timedelta(days=1)
        run_id = kwargs["run_id"]

        logging.info(f"Start date: {start_date}, end date:{end_date}, run_id: {run_id}")
        releases = [UclDiscoveryRelease(dag_id=self.dag_id, run_id=run_id, start_date=start_date, end_date=end_date)]
        return releases

    def download(self, releases: List[UclDiscoveryRelease], **kwargs):
        """Task to download the ucldiscovery release for a given month.
        :param releases: a list with the ucldiscovery release.
        """
        for release in releases:
            logging.info(f"Downloading metadata from {release.eprint_metadata_url}")
            response = retry_get_url(release.eprint_metadata_url, num_retries=5)
            response_content = response.content.decode("utf-8")
            csv_reader = csv.DictReader(response_content.splitlines())
            try:
                next(csv_reader)
            except StopIteration:
                raise AirflowSkipException("No metadata available for the year of this release date.")
            logging.info(f"Saving metadata to file: {release.download_path}")
            with open(release.download_path, "w") as f:
                f.write(response_content)

    def upload_downloaded(self, releases: List[UclDiscoveryRelease], **kwargs):
        """Uploads the downloaded onix file to GCS"""
        for release in releases:
            success = gcs_upload_files(
                bucket_name=self.cloud_workspace.download_bucket, file_paths=[release.download_path]
            )
            set_task_state(success, kwargs["ti"].task_id, release=release)

    def transform(self, releases: List[UclDiscoveryRelease], **kwargs):
        """Task to transform the ucldiscovery release for a given month."""
        for release in releases:
            results = ucl_dicovery_transform(
                release.start_date, release.end_date, release.download_path, release.countries_url
            )
            results = add_partition_date(
                results, release.partition_date, TimePartitioningType.MONTH, partition_field="release_date"
            )
            save_jsonl_gz(release.transform_path, results)

    def upload_transformed(self, releases: List[UclDiscoveryRelease], **kwargs):
        """Uploads the transformed file to GCS"""
        for release in releases:
            success = gcs_upload_files(
                bucket_name=self.cloud_workspace.transform_bucket, file_paths=[release.transform_path]
            )
            set_task_state(success, kwargs["ti"].task_id, release=release)

    def bq_load(self, releases: List[UclDiscoveryRelease], **kwargs) -> None:
        """Loads the sales and traffic data into BigQuery"""
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.bq_dataset_description,
        )

        for release in releases:
            uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.transform_path))
            table_id = bq_table_id(self.cloud_workspace.project_id, self.bq_dataset_id, self.bq_table_name)
            state = bq_load_table(
                uri=uri,
                table_id=table_id,
                schema_file_path=bq_find_schema(path=self.schema_folder, table_name=self.bq_table_name),
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                partition_type=TimePartitioningType.MONTH,
                partition=True,
                partition_field="release_date",
                write_disposition=WriteDisposition.WRITE_APPEND,
                table_description=self.bq_table_description,
                ignore_unknown_values=True,
            )
            set_task_state(state, kwargs["ti"].task_id, release=release)

    def add_new_dataset_releases(self, releases: List[UclDiscoveryRelease], **kwargs) -> None:
        """Adds release information to API."""
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        for release in releases:
            dataset_release = DatasetRelease(
                dag_id=self.dag_id,
                dataset_id=self.api_dataset_id,
                dag_run_id=release.run_id,
                data_interval_start=kwargs["data_interval_start"],
                data_interval_end=kwargs["data_interval_end"],
                partition_date=release.partition_date,
            )
            api.post_dataset_release(dataset_release)

    def cleanup(self, releases: List[UclDiscoveryRelease], **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""
        for release in releases:
            cleanup(
                dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder
            )


def ucl_dicovery_transform(
    start_date: pendulum.DateTime, end_date: pendulum.DateTime, csv_path: str, country_url: str
) -> List[dict]:
    """Parse the csv file and for each eprint id store the relevant metadata in a dictionary and get the downloads
    per country (between begin_date and end_date). The list of dictionaries is stored in a gzipped json lines file.
    There might be multiple rows for 1 eprint id. Some columns only have a value in the first row, some columns
    have values in multiple rows.

    :param start_date: The start date of the DAG The begin date of download period
    :param end_date: The end date of download period
    :param csv_path: The path to the downloaded CSV file
    :param country_url: The UCL Discovery country URL
    :return: The transformed data
    """

    begin_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")
    total_downloads = 0
    downloads_per_country = []
    single_row_columns = {}
    with open(csv_path, "r") as f:
        csv_reader = csv.DictReader(f)

        previous_id = None
        results = []
        multi_row_columns = {
            "creators_name_family": [],
            "creators_name_given": [],
            "subjects": [],
            "divisions": [],
            "lyricists_name_family": [],
            "lyricists_name_given": [],
            "editors_name_family": [],
            "editors_name_given": [],
        }
        for row in csv_reader:
            eprintid = row["eprintid"]
            # row with a new eprint id
            if previous_id != eprintid:
                # add results of previous eprint id
                if previous_id:
                    result = create_result_dict(
                        begin_date,
                        end_date,
                        total_downloads,
                        downloads_per_country,
                        multi_row_columns,
                        single_row_columns,
                    )
                    results.append(result)
                    for column in multi_row_columns:
                        multi_row_columns[column] = []

                # store results of current eprint id
                single_row_columns = {
                    "eprintid": row["eprintid"],
                    "book_title": row["title"],
                    "ispublished": row["ispublished"],
                    "keywords": row["keywords"].split(", "),
                    "abstract": row["abstract"],
                    "date": row["date"],
                    "publisher": row["publisher"],
                    "official_url": row["official_url"],
                    "oa_status": row["oa_status"],
                    "language": row["language"],
                    "doi": row["doi"],
                    "isbn": row["isbn_13"],
                    "isbn10": row["isbn"],
                    "language_elements": row["language_elements"],
                    "series": row["series"],
                    "pagerange": row["pagerange"],
                    "pages": row["pages"],
                }

                downloads_per_country, total_downloads = get_downloads_per_country(country_url + eprintid)

            # append results of current eprint id
            for column in multi_row_columns:
                # For 'name' type columns, don't add empty strings as a name, but make sure that  a value is added
                # for both family and given, even if only 1 of the columns has a value.
                start_column_name = column.split("_")[0]  # 'creators' when column = 'creators_name_family'
                if start_column_name in ["creators", "lyricists", "editors"]:
                    name_family = row[start_column_name + "_name.family"]
                    name_given = row[start_column_name + "_name.given"]

                    name = name_family + name_given
                    if name:
                        column_name = ".".join(column.rsplit("_", 1))  # 'creators_name.family' when column =
                        # 'creators_name_family'
                        multi_row_columns[column].append(row[column_name])
                else:
                    if row[column]:
                        multi_row_columns[column].append(row[column])

            previous_id = eprintid

        # append results of last rows/eprint id
        result = create_result_dict(
            begin_date, end_date, total_downloads, downloads_per_country, multi_row_columns, single_row_columns
        )
        results.append(result)
    return results


def get_downloads_per_country(countries_url: str) -> Tuple[List[dict], int]:
    """Requests info on downloads per country for a specific eprint id

    :param countries_url: The url to the downloads per country info
    :return: Number of total downloads and list of downloads per country, country code and country name.
    """
    response = retry_get_url(countries_url, num_retries=5)
    response_content = response.content.decode("utf-8")
    if response_content == "\n":
        return [], 0
    response_csv = csv.DictReader(response_content.splitlines())
    results = []
    total_downloads = 0
    for row in response_csv:
        download_count = int(row["count"].strip('="'))
        country_code = row["value"]
        country_name = row["description"].split("</span>")[0].split(">")[-1]
        results.append({"country_code": country_code, "country_name": country_name, "download_count": download_count})
        total_downloads += download_count

    return results, total_downloads


def create_result_dict(
    begin_date: str,
    end_date: str,
    total_downloads: int,
    downloads_per_country: List[dict],
    multi_row_columns: dict,
    single_row_columns: dict,
) -> dict:
    """Create one result dictionary with info on downloads for a specific eprint id in a given time period.

    :param begin_date: The begin date of download period
    :param end_date: The end date of download period
    :param total_downloads: Total of downloads in that period
    :param downloads_per_country: List of downloads per country
    :param multi_row_columns: Dict of column names & values for columns that have values over multiple rows of an
    eprintid
    :param single_row_columns: Dict of column names & values for columns that have values only in the first row of an eprint id
    :return: Results dictionary
    """
    result = dict(
        begin_date=begin_date,
        end_date=end_date,
        total_downloads=total_downloads,
        downloads_per_country=downloads_per_country,
        **multi_row_columns,
        **single_row_columns,
    )
    # change empty strings to None so they don't show up in BigQuery table
    for k, v in result.items():
        result[k] = v if v != "" else None
    return result
