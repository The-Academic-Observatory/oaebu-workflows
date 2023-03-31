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

# Author: Keegan Smith

import logging
import os
from typing import Dict, List, Optional, Tuple

import pendulum
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from google.cloud.bigquery import SourceFormat
from google.cloud.bigquery.table import TimePartitioningType

from oaebu_workflows.config import schema_folder as default_schema_folder
from oaebu_workflows.dag_tag import Tag
from observatory.platform.utils.airflow_utils import AirflowConns, make_workflow_folder
from observatory.platform.utils.file_utils import list_to_jsonl_gz, list_files, load_jsonl, blob_name_from_path
from observatory.platform.utils.url_utils import retry_get_url
from observatory.platform.utils.config_utils import find_schema
from observatory.platform.utils.gc_utils import upload_files_to_cloud_storage
from observatory.platform.utils.workflow_utils import (
    bq_load_partition,
    table_ids_from_path,
    make_release_date,
    cleanup,
)
from observatory.platform.workflows.workflow import Release, Workflow

IRUS_FULCRUM_ENDPOINT_TEMPLATE = (
    "https://irus.jisc.ac.uk/api/v3/irus/reports/irus_ir/?platform=235"
    "&requestor_id={requestor_id}&begin_date={start_date}&end_date={end_date}"
)

ORG_PUBLISHER_MAPPINGS = {
    # https://www.press.umich.edu/browse/distributed_clients
    "University of Michigan Press": [
        "American Academy in Rome",
        "American Society of Papyrologists",
        "University of Michigan Bentley Library",
        "University of Michigan Center for Chinese Studies",
        "University of Michigan Center for Japanese Studies",
        "University of Michigan Center for South Asia Studies",
        "University of Michigan Center for South East Asian Studies",
        "University of Michigan Department of Near Eastern Studies",
        "University of Michigan Museum of Anthropological Archaeology",
        "University of Michigan Press",
    ]
}


class FulcrumRelease(Release):
    def __init__(self, dag_id: str, release_date: pendulum.DateTime):
        """Create a FulcrumRelease instance.

        :param dag_id: the DAG id.
        :param release_date: the date of the release.
        """
        self.dag_id = dag_id
        self.release_date = release_date


class FulcrumTelescope(Workflow):
    DAG_ID_PREFIX = "fulcrum"

    def __init__(
        self,
        workflow_id: int,
        organisation_name: str,
        project_id: str,
        download_bucket: str,
        transform_bucket: str,
        data_location: str,
        dag_id: str,
        start_date: pendulum.DateTime = pendulum.datetime(2022, 4, 1),  # Earliest available data
        schedule_interval: str = "0 0 14 * *",  # Run on the 14th of every month
        dataset_id: str = "fulcrum",
        merge_partition_field: str = "release_date",
        schema_folder: str = default_schema_folder(),
        source_format: str = SourceFormat.NEWLINE_DELIMITED_JSON,
        airflow_conns: List = None,
        catchup: bool = True,
        download_file_name="fulcrum.jsonl.gz",
        transform_file_name="fulcrum.jsonl.gz",
        dataset_description: str = "IRUS Fulcrum Data Feed",
        load_bigquery_table_kwargs=None,
    ):
        """The Fulcrum Telescope
        :param workflow_id: api workflow id.
        :param organisation_name: the Organisation the DAG will process.
        :param project_id: the google cloud project id
        :param download_bucket: the name of the google cloud download bucket
        :param transform_bucket: the name of the google cloud transform bucket
        :param data_location: the project location/region in google cloud platform
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param merge_partition_field: the field to partition the table on
        :param dataset_id: the BigQuery dataset id.
        :param schema_folder: the SQL schema path.
        :param source_format: the format of the data to load into BigQuery.
        :param dataset_description: description for the BigQuery dataset.
        :param table_descriptions: a dictionary with table ids and corresponding table descriptions.
        :param catchup: whether to catchup the DAG or not.
        :param airflow_conns: list of airflow connection keys, for each connection it is checked if it exists in airflow
        :param download_file_name: The local name of the downloaded file(s)
        :param tranform_file_name: The local name of the transformed file
        :param load_bigquery_table_kwargs: any kwargs to send to the bigquery table loader
        """

        if airflow_conns is None:
            airflow_conns = [AirflowConns.OAPEN_IRUS_UK_API]

        if load_bigquery_table_kwargs is None:
            load_bigquery_table_kwargs = {"ignore_unknown_values": True}

        if not ORG_PUBLISHER_MAPPINGS.get(organisation_name):
            raise AirflowException(f"Organisation {organisation_name} is not one of the expected name mappings")

        # Cloud workspace settings
        self.project_id = project_id
        self.download_bucket = download_bucket
        self.transform_bucket = transform_bucket

        # Database settings
        self.dataset_description = dataset_description
        self.dataset_id = dataset_id
        self.schema_folder = schema_folder
        self.source_format = source_format
        self.data_location = data_location

        # Fulcrum settings
        self.organisation_name = organisation_name
        self.merge_partition_field = merge_partition_field
        self.load_bigquery_table_kwargs = load_bigquery_table_kwargs

        # Workflow files/folders
        self.download_file_name = download_file_name
        self.transform_file_name = transform_file_name
        self.workflow_folder = None
        self.download_totals_folder = None
        self.download_country_folder = None
        self.transform_folder = None

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            workflow_id=workflow_id,
            airflow_conns=airflow_conns,
            catchup=catchup,
            tags=[Tag.oaebu],
        )

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.cleanup)
        self.add_task(self.add_new_dataset_releases)

    def make_release(self, **kwargs) -> FulcrumRelease:
        """Create a FulcrumRelease instance"""
        # Release date should be the month before the execution date
        release_date = make_release_date(**kwargs).subtract(months=1)
        release = FulcrumRelease(self.dag_id, release_date)

        # Make the download and transform folders
        self.workflow_folder = make_workflow_folder(self.dag_id, release_date)
        self.download_totals_folder = make_workflow_folder(self.dag_id, release_date, "download", "totals")
        self.download_country_folder = make_workflow_folder(self.dag_id, release_date, "download", "country")
        self.transform_folder = make_workflow_folder(self.dag_id, release_date, "transform")
        return release

    def download(self, release: FulcrumRelease, **kwargs):
        """Task to download the Fulcrum data for a release

        :param releases: the FulcrumRelease instance.
        """
        requestor_id = BaseHook.get_connection(AirflowConns.OAPEN_IRUS_UK_API).login
        download_month = release.release_date.format("YYYY-MM")
        totals_data, country_data = download_fulcrum_month_data(download_month, requestor_id)
        totals_path = os.path.join(self.download_totals_folder, self.download_file_name)
        country_path = os.path.join(self.download_country_folder, self.download_file_name)

        if not totals_data or not country_data:
            raise AirflowException(f"Data not available for supplied release date: {self.download_month}")

        list_to_jsonl_gz(totals_path, totals_data)
        list_to_jsonl_gz(country_path, country_data)

    def upload_downloaded(self, release: FulcrumRelease, **kwargs):
        """Upload the downloaded fulcrum data to the google cloud download bucket

        :param release: the FulcrumRelease instance
        :raises AirflowException: Raised if there is not exactly 1 file in the totals download folder
        :raises AirflowException: Raised if there is not exactly 1 file in the country download folder
        :raises AirflowException: Raised if totals blob upload fails for any reason
        :raises AirflowException: Raised if country blob upload fails for any reason
        """
        download_totals_files = list_files(self.download_totals_folder, self.download_file_name)
        if len(download_totals_files) != 1:
            raise AirflowException(
                f"Unexpected number of files in totals download folder. Expected 1, found {len(download_totals_files)}"
            )

        download_country_files = list_files(self.download_country_folder, self.download_file_name)
        if len(download_country_files) != 1:
            raise AirflowException(
                f"Unexpected number of files in country download folder. Expected 1, found {len(download_country_files)}"
            )

        totals_blob = blob_name_from_path(download_totals_files[0])
        success_totals = upload_files_to_cloud_storage(self.download_bucket, [totals_blob], download_totals_files)
        country_blob = blob_name_from_path(download_country_files[0])
        success_country = upload_files_to_cloud_storage(self.download_bucket, [country_blob], download_country_files)

        if not success_country and success_totals:
            raise AirflowException("Download blobs could not be uploaded to cloud storage")

    def transform(self, release: FulcrumRelease, **kwargs):
        """Task to transform the fulcrum data

        :param release: the FulcrumRelease instance.
        """
        totals_data = load_jsonl(os.path.join(self.download_totals_folder, self.download_file_name))
        country_data = load_jsonl(os.path.join(self.download_country_folder, self.download_file_name))
        transformed_data = transform_fulcrum_data(
            totals_data=totals_data,
            country_data=country_data,
            release_date=release.release_date,
            organisation_name=self.organisation_name,
            organisation_mappings=ORG_PUBLISHER_MAPPINGS,
        )
        list_to_jsonl_gz(os.path.join(self.transform_folder, self.transform_file_name), transformed_data)

    def upload_transformed(self, release: FulcrumRelease, **kwargs):
        """Upload the transformed fulcrum data to the google cloud download bucket

        :param release: the FulcrumRelease instance.
        :raises AirflowException: Raised if there is not exactly 1 file in the transform folder matching the expected name
        :raises AirflowException: Raised if transform blob upload fails for any reason
        """
        transform_files = list_files(self.transform_folder, self.transform_file_name)
        if len(transform_files) != 1:
            raise AirflowException(
                f"Unexpected number of files in transform folder. Expected 1, found {len(transform_files)}"
            )

        blob = blob_name_from_path(transform_files[0])
        success = upload_files_to_cloud_storage(self.transform_bucket, [blob], transform_files)
        if not success:
            raise AirflowException("Blob could not be uploaded to cloud storage")

    def bq_load(self, release: FulcrumRelease, **kwargs) -> None:
        """Load the transfromed data into bigquery

        :param release: the FulcrumRelease instance.
        """
        transform_files = list_files(self.transform_folder, self.transform_file_name)
        if len(transform_files) != 1:
            raise AirflowException(
                f"Unexpected number of files in transform folder. Expected 1, found {len(transform_files)}"
            )
        # Load each transformed release
        transform_blob = blob_name_from_path(transform_files[0])
        table_id, _ = table_ids_from_path(transform_files[0])
        schema_file_path = find_schema(path=self.schema_folder, table_name="fulcrum")
        bq_load_partition(
            schema_file_path=schema_file_path,
            project_id=self.project_id,
            transform_bucket=self.transform_bucket,
            transform_blob=transform_blob,
            dataset_id=self.dataset_id,
            data_location=self.data_location,
            table_id=table_id,
            release_date=release.release_date,
            source_format=self.source_format,
            partition_type=TimePartitioningType.MONTH,
            dataset_description=self.dataset_description,
            partition_field=self.merge_partition_field,
            **self.load_bigquery_table_kwargs,
        )

    def cleanup(self, release: FulcrumRelease, **kwargs) -> None:
        """Delete all files and folders associated with this release.

        :param release: the FulcrumRelease instance.
        """
        cleanup(self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=self.workflow_folder)


def download_fulcrum_month_data(
    download_month: str,
    requestor_id: str,
    num_retries: str = 3,
) -> Tuple[List[dict], List[dict]]:
    """Download Fulcrum data for the release month

    :param download_month: The month to download usage data from
    :param requestor_id: The requestor ID - used to access irus platform
    :param num_retries: Number of attempts to make for the URL
    """
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
    release_date: pendulum.DateTime,
    organisation_name: str = None,
    organisation_mappings: dict = ORG_PUBLISHER_MAPPINGS,
) -> List[dict]:
    """
    Transforms Fulcrum downloaded "totals" and "country" data.
    If organisation name is supplied, will extract only the data related to this organisation

    :param totals_data: Fulcrum usage data aggregated over all countries
    :param country_data: Fulcrum usage data split by country
    :param release_data: The release date of this data. Will insert the end of month into the data as a reference
    :param organisation_name: The name of the organisation/publisher to extract
    :param organisation_mappings: The organisation mappings dictionary
    """
    # Extract only the publishers related to this organisation name
    if organisation_name:
        totals_data = [i for i in totals_data if i["Publisher"] in organisation_mappings[organisation_name]]
        country_data = [i for i in country_data if i["Publisher"] in organisation_mappings[organisation_name]]

    # Total and Country-granulated results should all have the same item entries and be ordered the same, but we should check anyway
    c_ids = [i["IRUS_Item_ID"] for i in country_data]
    t_ids = [i["IRUS_Item_ID"] for i in totals_data]
    if len(c_ids) != len(t_ids):
        raise AirflowException("Country entry data is not the same length as total entry data")

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
            "release_date": release_date.end_of("month").format("YYYY-MM-DD"),
        }
        transformed_data.append(transformed_row)

    return transformed_data
