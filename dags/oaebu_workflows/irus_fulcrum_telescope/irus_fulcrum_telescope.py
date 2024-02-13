# Copyright 2022-2024 Curtin University
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

# Author: Keegan Smith

import logging
import os
from typing import List, Tuple, Union

import pendulum
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task
from google.cloud.bigquery import SourceFormat, WriteDisposition
from google.cloud.bigquery.table import TimePartitioningType

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory.platform.api import make_observatory_api, DatasetRelease
from observatory.platform.airflow import AirflowConns
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.files import save_jsonl_gz, load_jsonl, add_partition_date
from observatory.platform.gcs import gcs_blob_name_from_path, gcs_upload_files, gcs_blob_uri, gcs_download_blob
from observatory.platform.bigquery import bq_load_table, bq_create_dataset, bq_table_id
from observatory.platform.tasks import check_dependencies
from observatory.platform.workflows.workflow import PartitionRelease, set_task_state, cleanup
from observatory.platform.utils.url_utils import retry_get_url

IRUS_FULCRUM_ENDPOINT_TEMPLATE = (
    "https://irus.jisc.ac.uk/api/v3/irus/reports/irus_ir/?platform=235"
    "&requestor_id={requestor_id}&begin_date={start_date}&end_date={end_date}"
)


class IrusFulcrumRelease(PartitionRelease):
    def __init__(
        self,
        dag_id: str,
        run_id: str,
        data_interval_start: pendulum.DateTime,
        data_interval_end: pendulum.DateTime,
        partition_date: pendulum.DateTime,
    ):
        """Create a IrusFulcrumRelease instance.

        :param dag_id: The ID of the DAG
        :param run_id: The airflow run ID
        :param data_interval_start: The beginning of the data interval
        :param data_interval_end: The end of the data interval
        :param partition_date: The release/partition date
        """
        super().__init__(dag_id=dag_id, run_id=run_id, partition_date=partition_date)
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end
        self.download_totals_file_name = "fulcrum_totals.jsonl.gz"
        self.download_country_file_name = "fulcrum_country.json.gz"
        self.transfrom_file_name = "fulcrum.jsonl.gz"

    @property
    def download_totals_path(self):
        return os.path.join(self.download_folder, self.download_totals_file_name)

    @property
    def download_country_path(self):
        return os.path.join(self.download_folder, self.download_country_file_name)

    @property
    def transform_path(self):
        return os.path.join(self.transform_folder, self.transfrom_file_name)

    @property
    def download_totals_blob_name(self):
        return gcs_blob_name_from_path(self.download_totals_path)

    @property
    def download_country_blob_name(self):
        return gcs_blob_name_from_path(self.download_country_path)

    @property
    def transform_blob_name(self):
        return gcs_blob_name_from_path(self.transform_path)

    @staticmethod
    def from_dict(dict_: dict):
        return IrusFulcrumRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            data_interval_start=pendulum.parse(dict_["data_interval_start"]),
            data_interval_end=pendulum.parse(dict_["data_interval_end"]),
            partition_date=pendulum.parse(dict_["partition_date"]),
        )

    def to_dict(self):
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
    publishers: List[str],
    data_partner: Union[str, OaebuPartner] = "irus_fulcrum",
    bq_dataset_description: str = "IRUS dataset",
    bq_table_description: str = None,
    api_dataset_id: str = "fulcrum",
    observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
    irus_oapen_api_conn_id: str = "irus_api",
    catchup: bool = True,
    schedule: str = "0 0 4 * *",  # Run on the 4th of every month
    start_date: pendulum.DateTime = pendulum.datetime(2022, 4, 1),  # Earliest available data
):
    """The Fulcrum Telescope
    :param dag_id: The ID of the DAG
    :param cloud_workspace: The CloudWorkspace object for this DAG
    :param publishers: The publishers pertaining to this DAG instance (as listed in Fulcrum)
    :param data_partner: The name of the data partner
    :param bq_dataset_description: Description for the BigQuery dataset
    :param bq_table_description: Description for the biguery table
    :param api_dataset_id: The ID to store the dataset release in the API
    :param observatory_api_conn_id: Airflow connection ID for the overvatory API
    :param irus_oapen_api_conn_id: Airflow connection ID OAPEN IRUS UK (counter 5)
    :param catchup: Whether to catchup the DAG or not
    :param schedule: The schedule interval of the DAG
    :param start_date: The start date of the DAG
    """
    if bq_table_description is None:
        bq_table_description = "Fulcrum metrics as recorded by the IRUS platform"

    data_partner = partner_from_str(data_partner)

    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        tags=["oaebu"],
        default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=5)},
    )
    def irus_fulcrum():
        @task
        def make_release(**context) -> dict:
            """Create a IrusFulcrumRelease instance
            Dates are best explained with an example
            Say the dag is scheduled to run on 2022-04-07
            Interval_start will be 2022-03-01
            Interval_end will be 2022-04-01
            partition_date will be 2022-03-31
            """
            data_interval_start = context["data_interval_start"].start_of("month")
            data_interval_end = context["data_interval_end"].start_of("month")
            partition_date = data_interval_start.end_of("month")
            return IrusFulcrumRelease(
                dag_id,
                context["run_id"],
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
                partition_date=partition_date,
            ).to_dict()

        @task
        def download(release: dict, **context) -> None:
            """Task to download the Fulcrum data from IRUS and upload to cloud storage

            :param release: the IrusFulcrumRelease instance.
            """
            release = IrusFulcrumRelease.from_dict(release)
            requestor_id = BaseHook.get_connection(irus_oapen_api_conn_id).login
            totals_data, country_data = download_fulcrum_month_data(release.partition_date, requestor_id)
            if not totals_data or not country_data:
                raise RuntimeError(f"Data not available for supplied release month: {release.partition_date}")
            save_jsonl_gz(release.download_totals_path, totals_data)
            save_jsonl_gz(release.download_country_path, country_data)

            # Upload to GCS
            success = gcs_upload_files(
                bucket_name=cloud_workspace.download_bucket,
                file_paths=[release.download_totals_path, release.download_country_path],
            )
            set_task_state(success, context["ti"].task_id, release=release)

        @task
        def transform(release: dict, **context) -> None:
            """Task to transform the fulcrum data and upload to cloud storage"""

            release = IrusFulcrumRelease.from_dict(release)
            # Download files
            success = gcs_download_blob(
                bucket_name=cloud_workspace.download_bucket,
                blob_name=release.download_totals_blob_name,
                file_path=release.download_totals_path,
            )
            if not success:
                raise FileNotFoundError(f"Error downloading file: {release.download_totals_blob_name}")

            success = gcs_download_blob(
                bucket_name=cloud_workspace.download_bucket,
                blob_name=release.download_country_blob_name,
                file_path=release.download_country_path,
            )
            if not success:
                raise FileNotFoundError(f"Error downloading file: {release.download_country_blob_name}")

            logging.info(f"Transforming the Fulcrum dataset with the following publisher filter: {publishers}")
            totals_data = load_jsonl(release.download_totals_path)
            country_data = load_jsonl(release.download_country_path)
            transformed_data = transform_fulcrum_data(
                totals_data=totals_data,
                country_data=country_data,
                publishers=publishers,
            )
            transformed_data = add_partition_date(
                transformed_data,
                partition_date=release.partition_date.end_of("month"),
                partition_type=TimePartitioningType.MONTH,
                partition_field="release_date",
            )
            save_jsonl_gz(release.transform_path, transformed_data)

            # Upload to GCS
            success = gcs_upload_files(
                bucket_name=cloud_workspace.transform_bucket, file_paths=[release.transform_path]
            )
            set_task_state(success, context["ti"].task_id, release=release)

        @task
        def bq_load(release: dict, **context) -> None:
            """Load the transfromed data into bigquery"""
            release = IrusFulcrumRelease.from_dict(release)
            bq_create_dataset(
                project_id=cloud_workspace.project_id,
                dataset_id=data_partner.bq_dataset_id,
                location=cloud_workspace.data_location,
                description=bq_dataset_description,
            )

            # Load each transformed release
            uri = gcs_blob_uri(cloud_workspace.transform_bucket, release.transform_blob_name)
            table_id = bq_table_id(cloud_workspace.project_id, data_partner.bq_dataset_id, data_partner.bq_table_name)
            success = bq_load_table(
                uri=uri,
                table_id=table_id,
                schema_file_path=data_partner.schema_path,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                table_description=bq_table_description,
                partition=True,
                partition_type=TimePartitioningType.MONTH,
                write_disposition=WriteDisposition.WRITE_APPEND,
                partition_field="release_date",
                ignore_unknown_values=True,
            )
            set_task_state(success, context["ti"].task_id, release=release)

        @task
        def add_new_dataset_releases(release: dict, **context) -> None:
            """Adds release information to API."""
            release = IrusFulcrumRelease.from_dict(release)
            api = make_observatory_api(observatory_api_conn_id=observatory_api_conn_id)
            dataset_release = DatasetRelease(
                dag_id=dag_id,
                dataset_id=api_dataset_id,
                dag_run_id=release.run_id,
                data_interval_start=release.data_interval_start,
                data_interval_end=release.data_interval_end,
                partition_date=release.partition_date,
            )
            api.post_dataset_release(dataset_release)

        @task
        def cleanup_workflow(release: dict, **context) -> None:
            """Delete all files and folders associated with this release."""
            release = IrusFulcrumRelease.from_dict(release)
            cleanup(dag_id, execution_date=context["execution_date"], workflow_folder=release.workflow_folder)

        # Define DAG tasks
        task_check = check_dependencies(
            airflow_conns=[observatory_api_conn_id, irus_oapen_api_conn_id], start_date=start_date
        )
        xcom_release = make_release()
        task_download = download(xcom_release)
        task_transform = transform(xcom_release)
        task_bq_load = bq_load(xcom_release)
        task_add_release = add_new_dataset_releases(xcom_release)
        task_cleanup_workflow = cleanup_workflow(xcom_release)

        (
            task_check
            >> xcom_release
            >> task_download
            >> task_transform
            >> task_bq_load
            >> task_add_release
            >> task_cleanup_workflow
        )

    return irus_fulcrum()


def download_fulcrum_month_data(
    download_month: pendulum.DateTime,
    requestor_id: str,
    num_retries: str = 3,
) -> Tuple[List[dict], List[dict]]:
    """Download Fulcrum data for the release month

    :param download_month: The month to download usage data from
    :param requestor_id: The requestor ID - used to access irus platform
    :param num_retries: Number of attempts to make for the URL
    """
    download_month = download_month.format("YYYY-MM")
    base_url = IRUS_FULCRUM_ENDPOINT_TEMPLATE.format(
        requestor_id=requestor_id,
        start_date=download_month,
        end_date=download_month,
    )
    country_url = base_url + "&attributes_to_show=Country"
    logging.info(f"Downloading Fulcrum metrics for month: {download_month}")
    totals_data = retry_get_url(base_url, num_retries=num_retries).json()
    country_data = retry_get_url(country_url, num_retries=num_retries).json()
    totals_data = totals_data.get("Report_Items")
    country_data = country_data.get("Report_Items")

    return totals_data, country_data


def transform_fulcrum_data(
    totals_data: List[dict],
    country_data: List[dict],
    publishers: List[str] = None,
) -> List[dict]:
    """
    Transforms Fulcrum downloaded "totals" and "country" data.

    :param totals_data: Fulcrum usage data aggregated over all countries
    :param country_data: Fulcrum usage data split by country
    :param publishers: Fulcrum publishers to retain. If None, use all publishers
    """
    # Extract only the publishers related to this organisation name
    if publishers:
        totals_data = [i for i in totals_data if i["Publisher"] in publishers]
        country_data = [i for i in country_data if i["Publisher"] in publishers]

    # Total and Country-granulated results should all have the same item entries and be ordered the same, but we should check anyway
    c_ids = [i["IRUS_Item_ID"] for i in country_data]
    t_ids = [i["IRUS_Item_ID"] for i in totals_data]
    assert len(c_ids) == len(t_ids), "Country entry data is not the same length as total entry data"

    # Mapping the IDs to list elements
    c_id_mapping = {entry["IRUS_Item_ID"]: i for (i, entry) in enumerate(country_data)}
    t_id_mapping = {entry["IRUS_Item_ID"]: i for (i, entry) in enumerate(totals_data)}

    transformed_data = []
    for t_id, c_id in zip(t_ids, c_ids):
        transformed_row = {}
        t_entry = totals_data[t_id_mapping[t_id]]
        c_entry = country_data[c_id_mapping[c_id]]

        # Metrics with country granulation
        country_metrics = []
        for c_metric in c_entry["Performance_Instances"]:  # For each country
            country_metrics.append(
                {
                    "name": c_metric["Country"]["Country"],
                    "code": c_metric["Country"]["Country_Code"],
                    "Total_Item_Investigations": c_metric["Metric_Type_Counts"].get("Total_Item_Investigations"),
                    "Total_Item_Requests": c_metric["Metric_Type_Counts"].get("Total_Item_Requests"),
                    "Unique_Item_Investigations": c_metric["Metric_Type_Counts"].get("Unique_Item_Investigations"),
                    "Unique_Item_Requests": c_metric["Metric_Type_Counts"].get("Unique_Item_Requests"),
                }
            )

        # Total Metrics
        t_metric = t_entry["Performance_Instances"][0]
        total_item_investigations = t_metric["Metric_Type_Counts"].get("Total_Item_Investigations")
        total_item_requests = t_metric["Metric_Type_Counts"].get("Total_Item_Requests")
        unique_item_investigations = t_metric["Metric_Type_Counts"].get("Unique_Item_Investigations")
        unique_item_requests = t_metric["Metric_Type_Counts"].get("Unique_Item_Requests")

        # Row structure
        transformed_row = {
            "proprietary_id": t_id,  # t_id == c_id
            "ISBN": t_entry.get("ISBN"),
            "book_title": t_entry.get("Item"),
            "publisher": t_entry.get("Publisher"),
            "authors": t_entry.get("Authors"),
            "event_month": pendulum.parse(t_entry["Performance_Instances"][0]["Event_Month"]).format("YYYY-MM"),
            "total_item_investigations": total_item_investigations,
            "total_item_requests": total_item_requests,
            "unique_item_investigations": unique_item_investigations,
            "unique_item_requests": unique_item_requests,
            "country": country_metrics,
        }
        transformed_data.append(transformed_row)

    return transformed_data
