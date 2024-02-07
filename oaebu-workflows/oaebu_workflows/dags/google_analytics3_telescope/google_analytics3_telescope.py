# Copyright 2020-2023 Curtin University
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

from __future__ import annotations

import logging
import os
from typing import Dict, List, Tuple, Union

import pendulum
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from google.cloud.bigquery import TimePartitioningType, SourceFormat, WriteDisposition
from googleapiclient.discovery import Resource, build
from oauth2client.service_account import ServiceAccountCredentials

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api
from observatory.platform.files import save_jsonl_gz
from observatory.platform.airflow import AirflowConns
from observatory.platform.files import add_partition_date
from observatory.platform.gcs import gcs_upload_files, gcs_blob_uri, gcs_blob_name_from_path
from observatory.platform.bigquery import bq_load_table, bq_table_id, bq_create_dataset
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.workflows.workflow import (
    Workflow,
    PartitionRelease,
    cleanup,
    set_task_state,
    check_workflow_inputs,
)


class GoogleAnalytics3Release(PartitionRelease):
    def __init__(
        self,
        dag_id: str,
        run_id: str,
        data_interval_start: pendulum.DateTime,
        data_interval_end: pendulum.DateTime,
        partition_date: pendulum.DateTime,
    ):
        """Construct a GoogleAnalytics3Release.

        :param dag_id: The ID of the DAG
        :param run_id: The Airflow run ID
        :param data_interval_start: The start date of the DAG the start date of the download period.
        :param data_interval_end: end date of the download period, also used as release date for BigQuery table and file paths
        """
        super().__init__(dag_id=dag_id, run_id=run_id, partition_date=partition_date)
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end
        self.transform_path = os.path.join(self.transform_folder, f"{partition_date.format('YYYY_MM_DD')}.json.gz")


class GoogleAnalytics3Telescope(Workflow):
    """Google Analytics Telescope."""

    ANU_ORG_NAME = "ANU Press"

    def __init__(
        self,
        dag_id: str,
        organisation_name: str,
        cloud_workspace: CloudWorkspace,
        view_id: str,
        pagepath_regex: str,
        data_partner: Union[str, OaebuPartner] = "google_analytics3",
        bq_dataset_description: str = "Data from Google sources",
        bq_table_description: str = None,
        api_dataset_id: str = "google_analytics",
        oaebu_service_account_conn_id: str = "oaebu_service_account",
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        catchup: bool = True,
        start_date: pendulum.DateTime = pendulum.datetime(2018, 1, 1),
        schedule: str = "@monthly",
    ):
        """Construct a GoogleAnalytics3Telescope instance.
        :param dag_id: The ID of the DAG
        :param organisation_name: The organisation name as per Google Analytics
        :param cloud_workspace: The CloudWorkspace object for this DAG
        :param view_id: The Google Analytics view ID
        :param pagepath_regex: The pagepath regex
        :param data_partner: The name of the data partner
        :param bq_dataset_description: Description for the BigQuery dataset
        :param bq_table_description: Description for the biguery table
        :param api_dataset_id: The ID to store the dataset release in the API
        :param oaebu_service_account_conn_id: Airflow connection ID for the OAEBU service account
        :param observatory_api_conn_id: Airflow connection ID for the overvatory API
        :param catchup: Whether to catchup the DAG or not
        :param start_date: The start date of the DAG
        :param schedule: The schedule interval of the DAG
        """
        super().__init__(
            dag_id,
            start_date,
            schedule,
            catchup=catchup,
            airflow_conns=[oaebu_service_account_conn_id, observatory_api_conn_id],
            tags=["oaebu"],
        )

        self.dag_id = dag_id
        self.organisation_name = organisation_name
        self.cloud_workspace = cloud_workspace
        self.view_id = view_id
        self.pagepath_regex = pagepath_regex
        self.data_partner = partner_from_str(data_partner)
        self.bq_dataset_description = bq_dataset_description
        self.bq_table_description = bq_table_description
        self.api_dataset_id = api_dataset_id
        self.oaebu_service_account_conn_id = oaebu_service_account_conn_id
        self.observatory_api_conn_id = observatory_api_conn_id

        check_workflow_inputs(self)

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.download_transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> List[GoogleAnalytics3Release]:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
        :return: A list of grid release instances
        """
        # Get start and end date (data_interval_end = release_date)
        data_interval_start = kwargs["data_interval_start"].start_of("month")
        data_interval_end = kwargs["data_interval_end"].start_of("month")
        partition_date = data_interval_start.end_of("month")

        logging.info(
            f"Start date: {data_interval_start}, end date:{data_interval_end}, parition_date: {partition_date}"
        )
        releases = [
            GoogleAnalytics3Release(
                dag_id=self.dag_id,
                run_id=kwargs["run_id"],
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
                partition_date=partition_date,
            )
        ]
        return releases

    def check_dependencies(self, **kwargs) -> bool:
        """Check dependencies of DAG. Add to parent method to additionally check for a view id and pagepath regex

        :param kwargs: the context passed from the Airflow Operator.
        :return: True if dependencies are valid.
        """
        super().check_dependencies()

        if self.view_id is None or self.pagepath_regex is None:
            expected_extra = {"view_id": "the_view_id", "pagepath_regex": r"pagepath_regex"}
            raise AirflowException(
                f"View ID and/or pagepath regex is not set in 'extra' of telescope, extra example: " f"{expected_extra}"
            )
        return True

    def download_transform(self, releases: List[GoogleAnalytics3Release], **kwargs) -> None:
        """Task to download and transform the google analytics release for a given month.

        :param releases: a list with one google analytics release.
        """
        data_found = False
        for release in releases:
            service = initialize_analyticsreporting(self.oaebu_service_account_conn_id)
            results = get_reports(
                service,
                self.organisation_name,
                self.view_id,
                self.pagepath_regex,
                release.data_interval_start,
                release.data_interval_end.subtract(
                    days=1
                ),  # Subtract 1 day because GA uses inclusive dates, Airlfow data intervals are not
            )
            results = add_partition_date(
                results, release.partition_date, TimePartitioningType.MONTH, partition_field="release_date"
            )
            if results:
                save_jsonl_gz(release.transform_path, results)
                data_found = True
            else:
                if (pendulum.today("UTC") - self.data_interval_end).in_months() >= 26:
                    logging.info(
                        "No data available. Google Analytics data is only available for 26 months, see "
                        "https://support.google.com/analytics/answer/7667196?hl=en for more info"
                    )

        if not data_found:
            raise AirflowSkipException("No Google Analytics data available to download.")

    def upload_transformed(self, releases: List[GoogleAnalytics3Release], **kwargs) -> None:
        """Uploads the transformed file to GCS"""
        for release in releases:
            state = gcs_upload_files(
                bucket_name=self.cloud_workspace.transform_bucket, file_paths=[release.transform_path]
            )
            set_task_state(state, kwargs["ti"].task_id, release=release)

    def bq_load(self, releases: List[GoogleAnalytics3Release], **kwargs) -> None:
        """Loads the data into BigQuery"""
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.data_partner.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.bq_dataset_description,
        )
        for release in releases:
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

    def add_new_dataset_releases(self, releases: List[GoogleAnalytics3Release], **kwargs) -> None:
        """Adds release information to API."""
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        for release in releases:
            dataset_release = DatasetRelease(
                dag_id=self.dag_id,
                dataset_id=self.api_dataset_id,
                dag_run_id=release.run_id,
                data_interval_start=release.data_interval_start,
                data_interval_end=release.data_interval_end,
                partition_date=release.partition_date,
            )
            api.post_dataset_release(dataset_release)

    def cleanup(self, releases: List[GoogleAnalytics3Release], **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""
        for release in releases:
            cleanup(
                dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder
            )


def initialize_analyticsreporting(oaebu_service_account_conn_id: str) -> Resource:
    """Initializes an Analytics Reporting API V4 service object.

    :return: An authorized Analytics Reporting API V4 service object.
    """
    oaebu_account_conn = BaseHook.get_connection(oaebu_service_account_conn_id)

    scopes = ["https://www.googleapis.com/auth/analytics.readonly"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(oaebu_account_conn.extra_dejson, scopes=scopes)

    # Build the service object.
    service = build("analyticsreporting", "v4", credentials=creds, cache_discovery=False)

    return service


def list_all_books(
    service: Resource,
    view_id: str,
    pagepath_regex: str,
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
    organisation_name: str,
    metrics: list,
) -> Tuple[List[dict], list]:
    """List all available books by getting all pagepaths of a view id in a given period.
    Note: Google API will not return a result for any entry in which all supplied metrics are zero.
    However, it will return 'some' results if you supply no metrics, contrary to the documentation.
    Date ranges are inclusive.

    :param service: The Google Analytics Reporting service object.
    :param view_id: The view id.
    :param pagepath_regex: The regex expression for the pagepath of a book.
    :param data_interval_start: The start date of the DAG Start date of analytics period
    :param data_interval_end: End date of analytics period
    :param organisation_name: The organisation name.
    :param: metrics: The metrics to return return with the book results
    :return: A list with dictionaries, one for each book entry (the dict contains the pagepath, title and average time
    on page) and a list of all pagepaths.
    """
    # Get pagepath, pagetitle and average time on page for each path
    body = {
        "reportRequests": [
            {
                "viewId": view_id,
                "pageSize": 10000,
                "dateRanges": [
                    {
                        "startDate": data_interval_start.strftime("%Y-%m-%d"),
                        "endDate": data_interval_end.strftime("%Y-%m-%d"),
                    }
                ],
                "metrics": metrics,
                "dimensions": [{"name": "ga:pagepath"}, {"name": "ga:pageTitle"}],
                "dimensionFilterClauses": [
                    {
                        "operator": "AND",
                        "filters": [
                            {"dimensionName": "ga:pagepath", "operator": "REGEXP", "expressions": [pagepath_regex]}
                        ],
                    }
                ],
            }
        ]
    }

    # add all 6 custom dimensions for anu press
    if organisation_name == GoogleAnalytics3Telescope.ANU_ORG_NAME:
        for i in range(1, 7):
            body["reportRequests"][0]["dimensions"].append({"name": f"ga:dimension{str(i)}"})

    reports = service.reports().batchGet(body=body).execute()
    all_book_entries = reports["reports"][0]["data"].get("rows")
    next_page_token = reports["reports"][0].get("nextPageToken")

    while next_page_token:
        body["reportRequests"][0]["pageToken"] = next_page_token
        reports = service.reports().batchGet(body=body).execute()
        book_entries = reports["reports"][0]["data"].get("rows")
        next_page_token = reports["reports"][0].get("nextPageToken")
        all_book_entries += book_entries

    # create list with just pagepaths
    if all_book_entries:
        pagepaths = [path["dimensions"][0] for path in all_book_entries]
    else:
        pagepaths = []

    return all_book_entries, pagepaths


def create_book_result_dicts(
    book_entries: List[dict],
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
    organisation_name: str,
) -> Dict[dict]:
    """Create a dictionary to store results for a single book. Pagepath, title and avg time on page are already given.
    The other metrics will be added to the dictionary later.

    :param book_entries: List with dictionaries of book entries.
    :param data_interval_start: The start date of the DAG Start date of analytics period.
    :param data_interval_end: End date of analytics period.
    :param organisation_name: The organisation name.
    :return: Dict to store results
    """
    book_results = {}
    for entry in book_entries:
        pagepath = entry["dimensions"][0]
        pagetitle = entry["dimensions"][1]
        average_time = float(entry["metrics"][0]["values"][-1])
        book_result = {
            "url": pagepath,
            "title": pagetitle,
            "start_date": data_interval_start.strftime("%Y-%m-%d"),
            "end_date": data_interval_end.strftime("%Y-%m-%d"),
            "average_time": average_time,
            "unique_views": {"country": {}, "referrer": {}, "social_network": {}},
            "page_views": {"country": {}, "referrer": {}, "social_network": {}},
            "sessions": {"country": {}, "source": {}},
        }
        # add custom dimension data for ANU Press
        if organisation_name == GoogleAnalytics3Telescope.ANU_ORG_NAME:
            # matches dimension order in 'list_all_books'
            custom_dimensions = {
                "publication_id": entry["dimensions"][2],
                "publication_type": entry["dimensions"][3],
                "publication_imprint": entry["dimensions"][4],
                "publication_group": entry["dimensions"][5],
                "publication_whole_or_part": entry["dimensions"][6],
                "publication_format": entry["dimensions"][7],
            }
            book_result = dict(book_result, **custom_dimensions)
        book_results[pagepath] = book_result

    return book_results


def get_dimension_data(
    service: Resource,
    view_id: str,
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
    metrics: list,
    dimension: dict,
    pagepaths: list,
) -> list:
    """Get reports data from the Google Analytics Reporting service for a single dimension and multiple metrics.
    The results are filtered by pagepaths of interest and ordered by pagepath as well.

    :param service: The Google Analytics Reporting service.
    :param view_id: The view id.
    :param data_interval_start: The start date of the DAG The start date of the analytics period.
    :param data_interval_end: The end date of the analytics period.
    :param metrics: List with dictionaries of metric.
    :param dimension: The dimension.
    :param pagepaths: List with pagepaths to filter and sort on.
    :return: List with reports data for dimension and metrics.
    """
    body = {
        "reportRequests": [
            {
                "viewId": view_id,
                "pageSize": 10000,
                "dateRanges": [
                    {
                        "startDate": data_interval_start.strftime("%Y-%m-%d"),
                        "endDate": data_interval_end.strftime("%Y-%m-%d"),
                    }
                ],
                "metrics": metrics,
                "dimensions": [{"name": "ga:pagePath"}, dimension],
                "dimensionFilterClauses": [
                    {"filters": [{"dimensionName": "ga:pagePath", "operator": "IN_LIST", "expressions": pagepaths}]}
                ],
                "orderBys": [{"fieldName": "ga:pagepath"}],
            }
        ]
    }
    reports = service.reports().batchGet(body=body).execute()
    all_dimension_data = reports["reports"][0]["data"].get("rows")
    next_page_token = reports["reports"][0].get("nextPageToken")

    while next_page_token:
        body["reportRequests"][0]["pageToken"] = next_page_token
        reports = service.reports().batchGet(body=body).execute()
        dimension_data = reports["reports"][0]["data"].get("rows")
        next_page_token = reports["reports"][0].get("nextPageToken")
        all_dimension_data += dimension_data

    return all_dimension_data


def add_to_book_result_dict(
    book_results: dict, dimension: dict, pagepath: str, unique_views: dict, page_views: dict, sessions: dict
):
    """Add the 'unique_views', 'page_views' and 'sessions' results to the book results dict if these metrics are of interest for the
    current dimension.

    :param book_results: A dictionary with all book results.
    :param dimension: Current dimension for which 'unique_views' and 'sessions' data is given.
    :param pagepath: Pagepath of the book.
    :param unique_views: Number of unique views for the pagepath&dimension
    :param page_views: Number of page views for the pagepath&dimension
    :param sessions: Number of sessions for the pagepath&dimension
    :return: None
    """
    # map the dimension name to the field name in BigQuery. The ga:dimensionX are obtained from custom ANU press
    # dimensions
    mapping = {
        "ga:country": "country",
        "ga:fullReferrer": "referrer",
        "ga:socialNetwork": "social_network",
        "ga:source": "source",
    }
    column_name = mapping[dimension["name"]]
    if column_name in ["country", "referrer", "social_network"]:
        book_results[pagepath]["unique_views"][column_name] = unique_views
        book_results[pagepath]["page_views"][column_name] = page_views
    if column_name in ["country", "source"]:
        book_results[pagepath]["sessions"][column_name] = sessions


def get_reports(
    service: Resource,
    organisation_name: str,
    view_id: str,
    pagepath_regex: str,
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
) -> list:
    """Get reports data from the Google Analytics Reporting API.

    :param service: The Google Analytics Reporting service.
    :param organisation_name: Name of the organisation.
    :param view_id: The view id.
    :param pagepath_regex: The regex expression for the pagepath of a book.
    :param data_interval_start: The start date of the DAG Start date of analytics period
    :param data_interval_end: End date of analytics period
    :return: List with google analytics data for each book
    """

    metric_names = ["uniquePageviews", "Pageviews", "sessions", "avgTimeOnPage"]
    metrics = [{"expression": f"ga:{metric}"} for metric in metric_names]

    # list all books
    book_entries, pagepaths = list_all_books(
        service, view_id, pagepath_regex, data_interval_start, data_interval_end, organisation_name, metrics
    )
    # if no books in period return empty list and raise airflow skip exception
    if not book_entries:
        return []
    # create dict with dict for each book to store results
    book_results = create_book_result_dicts(book_entries, data_interval_start, data_interval_end, organisation_name)

    dimension_names = ["country", "fullReferrer", "socialNetwork", "source"]
    dimensions = [{"name": f"ga:{dimension}"} for dimension in dimension_names]

    # get data per dimension
    for dimension in dimensions:
        dimension_data = get_dimension_data(
            service, view_id, data_interval_start, data_interval_end, metrics, dimension, pagepaths
        )

        prev_pagepath = None
        unique_views = {}
        page_views = {}
        sessions = {}
        # entry is combination of book pagepath & dimension
        for entry in dimension_data:
            pagepath = entry["dimensions"][0]
            dimension_value = entry["dimensions"][1]  # e.g. 'Australia' for 'country' dimension

            if prev_pagepath and pagepath != prev_pagepath:
                add_to_book_result_dict(book_results, dimension, prev_pagepath, unique_views, page_views, sessions)

                unique_views = {}
                page_views = {}
                sessions = {}

            # add values if they are not 0
            # ["values"][n] maps to the nth value of metric_names
            unique_views_metric = int(entry["metrics"][0]["values"][0])
            page_views_metric = int(entry["metrics"][0]["values"][1])
            sessions_metric = int(entry["metrics"][0]["values"][2])
            if unique_views_metric > 0:
                unique_views[dimension_value] = unique_views_metric
            if page_views_metric > 0:
                page_views[dimension_value] = page_views_metric
            if sessions_metric > 0:
                sessions[dimension_value] = sessions_metric

            prev_pagepath = pagepath
        else:
            add_to_book_result_dict(book_results, dimension, prev_pagepath, unique_views, page_views, sessions)

    # transform nested dict to list of dicts
    for book, result in book_results.items():
        for field, value in result.items():
            # field is 'unique_views' or 'sessions'
            if isinstance(value, dict):
                # nested_field is 'country', 'referrer' or 'social_network'
                for nested_field, nested_value in value.items():
                    values = []
                    # k is e.g. 'Australia', v is e.g. 1
                    for k, v in nested_value.items():
                        values.append({"name": k, "value": v})
                    book_results[book][field][nested_field] = values

    # convert dict to list of results
    book_results = [book_results[k] for k in book_results]

    return book_results
