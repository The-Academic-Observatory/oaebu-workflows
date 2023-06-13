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

import gzip
import json
import logging
import os
import time
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

import pendulum
import requests
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.models.taskinstance import TaskInstance
from google.auth import environment_vars
from google.auth.transport.requests import AuthorizedSession
from google.cloud.bigquery import TimePartitioningType, SourceFormat, WriteDisposition, Client, Table
from google.oauth2.service_account import IDTokenCredentials
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials

from oaebu_workflows.config import schema_folder as default_schema_folder
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api, get_dataset_releases, get_latest_dataset_release
from observatory.platform.airflow import AirflowConns, is_first_dag_run
from observatory.platform.files import get_file_hash, save_jsonl_gz, add_partition_date
from observatory.platform.bigquery import (
    bq_load_table,
    bq_table_id,
    bq_find_schema,
    bq_create_dataset,
    bq_copy_table,
)
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.observatory_environment import find_free_port
from observatory.platform.workflows.workflow import (
    ChangefileRelease,
    Workflow,
    cleanup,
    set_task_state,
    check_workflow_inputs,
)
from observatory.platform.gcs import (
    gcs_copy_blob,
    gcs_create_bucket,
    gcs_download_blob,
    gcs_upload_file,
    gcs_upload_files,
    gcs_blob_uri,
    gcs_blob_name_from_path,
)


class OapenIrusMonth:
    def __init__(self, month: pendulum.DateTime, download_folder: str, transform_folder: str):
        """Initiates the OapenIrusMonth object. The class is for storing information on each month to download.

        :param month: _description_
        :param download_folder: _description_
        :param transform_folder: _description_
        """
        self.month = month
        self.download_path = os.path.join(download_folder, f"oapen_irus_uk_{self.month.format('YYYY_MM')}.jsonl.gz")
        self.transform_path = os.path.join(transform_folder, f"oapen_irus_uk_{self.month.format('YYYY_MM')}.jsonl.gz")
        self.download_blob_name = gcs_blob_name_from_path(self.download_path)
        self.transform_blob_name = gcs_blob_name_from_path(self.transform_path)


class OapenIrusUkRelease(ChangefileRelease):
    def __init__(
        self,
        dag_id: str,
        run_id: str,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        is_first_run: bool,
    ):
        """Create a OapenIrusUkRelease instance.

        :param dag_id: The ID of the DAG
        :param run_id: The Airflow run ID
        :param partition_date: The date of the partition/release
        """
        super().__init__(dag_id=dag_id, run_id=run_id, start_date=start_date, end_date=end_date)
        self.is_first_run = is_first_run
        self.cloud_function_path = os.path.join(self.download_folder, "oapen_cloud_function.zip")
        self.irus_months = self.generate_irus_months()

    def generate_irus_months(self) -> List[OapenIrusMonth]:
        """Generates the OapenIrusMonth objects for this release"""
        # Months between start and end
        diff_months = self.end_date.diff(self.start_date).in_months()
        months = [self.start_date.add(months=m) for m in range(diff_months)]
        # Create OapenIrus month objects
        irus_months = [
            OapenIrusMonth(month=m, download_folder=self.download_folder, transform_folder=self.transform_folder)
            for m in months
        ]
        return irus_months


class OapenIrusUkTelescope(Workflow):
    OAPEN_PROJECT_ID = "keegan-dev"  # "oapen-usage-data-gdpr-proof"  # The oapen project id.
    OAPEN_BUCKET = f"{OAPEN_PROJECT_ID}_cloud-function"  # Storage bucket with the source code
    FUNCTION_NAME = "oapen-access-stats"  # Name of the google cloud function
    FUNCTION_REGION = "europe-west1"  # Region of the google cloud function
    FUNCTION_SOURCE_URL = (
        "https://github.com/The-Academic-Observatory/oapen-irus-uk-cloud-function/releases/"
        "download/v1.2.0-alpha/oapen-irus-uk-cloud-function.zip"
    )  # URL to the zipped source code of the cloud function
    FUNCTION_MD5_HASH = "5e22fb664721e9a34d50bb25e56a5592"  # MD5 hash of the zipped source code
    FUNCTION_BLOB_NAME = "cloud_function_source_code.zip"  # blob name of zipped source code
    FUNCTION_TIMEOUT = 1800  # Timeout of cloud function in seconds. Maximum of 30 minutes
    # see https://cloud.google.com/functions/docs/2nd-gen/overview#enhanced_infrastructure
    COUNTER_4_START_DATE = pendulum.datetime(2023, 5, 1)  # pendulum.datetime(2015, 6, 1)
    COUNTER_5_START_DATE = pendulum.datetime(2020, 4, 1)

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_project_id: str = "oaebu_private_data",
        bq_dataset_id: str = "oapen_irus_uk",
        bq_table_name: str = "oapen_irus_uk",
        bq_dataset_description: str = "OAPEN Irus dataset",
        bq_table_description: str = None,
        schema_folder: str = default_schema_folder(),
        api_dataset_id: str = "oapen_irus_uk_master",
        max_cloud_function_instances: int = 100,
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        geoip_license_conn_id: str = "geoip_license_key",
        oapen_irus_api_conn_id: str = "oapen_irus_uk_api",
        oapen_irus_login_conn_id: str = "oapen_irus_uk_login",
        catchup: bool = False,
        start_date: pendulum.DateTime = COUNTER_4_START_DATE,
        schedule_interval: str = "0 0 4 * *",  # 4th of every month
        max_threads=100,  # os.cpu_count() * 2,
    ):
        """The OAPEN irus uk telescope.
        TODO: docs
        :param dag_id: The ID of the DAG
        :param cloud_workspace: The CloudWorkspace object for this DAG
        :param bq_dataset_id: The BigQuery dataset ID
        :param bq_table_name: The BigQuery table name
        :param bq_dataset_description: Description for the BigQuery dataset
        :param bq_table_description: Description for the biguery table
        :param schema_folder: The path to the SQL schema folder
        :param api_dataset_id: The ID to store the dataset release in the API
        :param max_cloud_function_instances: The limit on the maximum number of cloud function instances that may coexist at a given time
        :param observatory_api_conn_id: Airflow connection ID for the overvatory API
        :param geoip_license_conn_id: The Airflow connection ID for the GEOIP license
        :param oapen_irus_api_conn_id: The Airflow connection ID for IRUS API - for counter 5
        :param oapen_irus_login_conn_id: The Airflow connection ID for IRUS API (login) - for counter 4
        :param catchup: Whether to catchup the DAG or not
        :param start_date: The start date of the DAG
        :param schedule_interval: The schedule interval of the DAG
        """
        if bq_table_description is None:
            bq_table_description = {
                "oapen_irus_uk": "Access stats from the OAPEN IRUS UK platform. Before 2020-04 "
                "from: https://irus.jisc.ac.uk/IRUSConsult/irus-oapen/v2/. "
                "After 2020-04 from the OAPEN_SUSHI API (documentation not "
                "published)."
            }

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            catchup=catchup,
            airflow_conns=[
                observatory_api_conn_id,
                geoip_license_conn_id,
                oapen_irus_api_conn_id,
                oapen_irus_login_conn_id,
            ],
            tags=["oaebu"],
        )
        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.bq_project_id = bq_project_id
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.bq_dataset_description = bq_dataset_description
        self.bq_table_description = bq_table_description
        self.schema_folder = schema_folder
        self.api_dataset_id = api_dataset_id
        self.max_cloud_function_instances = max_cloud_function_instances
        self.observatory_api_conn_id = observatory_api_conn_id
        self.geoip_license_conn_id = geoip_license_conn_id
        self.oapen_irus_api_conn_id = oapen_irus_api_conn_id
        self.oapen_irus_login_conn_id = oapen_irus_login_conn_id
        self.max_threads = max_threads

        check_workflow_inputs(self)

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.fetch_releases)
        # create PythonOperator with task concurrency of 1, so tasks to create cloud function never run in parallel
        # Update: this shouldn't be able to happen - but i'll keep it anyway
        self.add_task(self.create_cloud_function, task_concurrency=1)
        self.add_task(self.call_cloud_function)
        self.add_task(self.transfer)
        self.add_task(self.download)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

    def fetch_releases(self, **kwargs):
        """Return release information with the start and end date. [start date, end date) Includes start date, excludes end date.

         :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: True to continue, False to skip.
        """

        dag_run = kwargs["dag_run"]
        is_first_run = is_first_dag_run(dag_run)
        # releases = get_dataset_releases(dag_id=self.dag_id, dataset_id=self.api_dataset_id)
        releases = []  # TODO: fix
        # prev_end_date = pendulum.instance(datetime.datetime.min)

        # Get start date
        if is_first_run:
            assert (
                len(releases) == 0
            ), "fetch_releases: there should be no DatasetReleases stored in the Observatory API on the first DAG run."

            start_date = self.COUNTER_4_START_DATE  # Get counter 4 stats in first run
        else:
            assert (
                len(releases) >= 1
            ), f"fetch_releases: there should be at least 1 DatasetRelease in the Observatory API after the first DAG run"
            start_date = self.COUNTER_5_START_DATE  # counter 5 onwards

            # The start date is the last end_date
            # prev_release = get_latest_dataset_release(releases, "changefile_end_date")
            # start_date = prev_release.changefile_end_date
            # prev_end_date = prev_release.changefile_end_date

        # End date is always the data_interval_end, although it is not inclusive
        end_date = kwargs["data_interval_end"]

        # prev_release = get_latest_dataset_release(releases, "changefile_end_date")

        # Print summary information
        logging.info(f"is_first_run: {is_first_run}")
        logging.info(f"start_date: {start_date}")
        logging.info(f"end_date: {end_date}")

        # Publish release information
        ti: TaskInstance = kwargs["ti"]
        msg = dict(
            is_first_run=is_first_run,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            # prev_end_date=prev_end_date.isoformat(),
        )
        ti.xcom_push(self.RELEASE_INFO, msg, kwargs["logical_date"])

        return True

    def make_release(self, **kwargs) -> OapenIrusUkRelease:
        """Create a list of OapenIrusUkRelease instances for a given month.
        Say the dag is scheduled to run on 2022-04-07
        start_date will be either 2015-06-01 or 2020-04-01 (depending on first_run = True/False)
        end-date will be 2022-04-01
        partition_date will be 2022-03-31

        :param kwargs: the context passed from the PythonOperator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
        :return: list of OapenIrusUkRelease instances
        """
        ti: TaskInstance = kwargs["ti"]
        msg = ti.xcom_pull(key=self.RELEASE_INFO, task_ids=self.fetch_releases.__name__, include_prior_dates=False)
        start_date, end_date, is_first_run = parse_release_msg(msg)
        release = OapenIrusUkRelease(
            dag_id=self.dag_id,
            run_id=kwargs["run_id"],
            start_date=start_date,
            end_date=end_date,
            is_first_run=is_first_run,
        )

        return release

    def create_cloud_function(self, release: OapenIrusUkRelease, **kwargs):
        """Task to create the cloud function for each release."""
        # set up cloud function variables
        oapen_project_id = OapenIrusUkTelescope.OAPEN_PROJECT_ID
        source_bucket = OapenIrusUkTelescope.OAPEN_BUCKET
        function_blob_name = OapenIrusUkTelescope.FUNCTION_BLOB_NAME
        location = f"projects/{oapen_project_id}/locations/{OapenIrusUkTelescope.FUNCTION_REGION}"
        full_name = f"{location}/functions/{OapenIrusUkTelescope.FUNCTION_NAME}"

        # zip source code and upload to bucket
        success, upload = upload_source_code_to_bucket(
            source_url=OapenIrusUkTelescope.FUNCTION_SOURCE_URL,
            project_id=oapen_project_id,
            bucket_name=source_bucket,
            blob_name=function_blob_name,
            cloud_function_path=release.cloud_function_path,
        )
        set_task_state(success, kwargs["ti"].task_id, release=release)

        # initialise cloud functions api
        creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ.get(environment_vars.CREDENTIALS))
        service = build("cloudfunctions", "v2beta", credentials=creds, cache_discovery=False, static_discovery=False)

        # update or create cloud function
        exists = cloud_function_exists(service, full_name)
        if not exists or upload is True:
            update = True if exists else False
            success, msg = create_cloud_function(
                service=service,
                location=location,
                full_name=full_name,
                source_bucket=source_bucket,
                blob_name=function_blob_name,
                max_active_runs=self.max_cloud_function_instances,
                update=update,
            )
            set_task_state(success, kwargs["ti"].task_id, release=release)
            logging.info(f"Creating or patching cloud function successful, response: {msg}")
        else:
            logging.info(f"Using existing cloud function, source code has not changed.")

    def call_cloud_function(self, release: OapenIrusUkRelease, **kwargs):
        """Task to call the cloud function for each release."""
        # set up cloud function variables
        location = f"projects/{OapenIrusUkTelescope.OAPEN_PROJECT_ID}/locations/{OapenIrusUkTelescope.FUNCTION_REGION}"
        full_name = f"{location}/functions/{OapenIrusUkTelescope.FUNCTION_NAME}"

        # initialise cloud functions api
        creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ.get(environment_vars.CREDENTIALS))
        service = build("cloudfunctions", "v2beta", credentials=creds, cache_discovery=False, static_discovery=False)

        function_uri = cloud_function_exists(service, full_name)
        worker_threads = len(release.irus_months) if len(release.irus_months) < self.max_threads else self.max_threads
        with ThreadPoolExecutor(max_workers=worker_threads) as executor:
            futures = []
            for irus_month in release.irus_months:
                # Set connection based on counter 4 vs counter 5
                if irus_month.month >= self.COUNTER_5_START_DATE:
                    airflow_conn = self.oapen_irus_api_conn_id
                else:
                    airflow_conn = self.oapen_irus_login_conn_id
                future = executor.submit(
                    call_cloud_function,
                    function_uri=function_uri,
                    release_date=irus_month.month.format("YYYY-MM"),
                    username=BaseHook.get_connection(airflow_conn).login,
                    password=BaseHook.get_connection(airflow_conn).password,
                    geoip_license_key=BaseHook.get_connection(self.geoip_license_conn_id).password,
                    publisher_name_v4=None,
                    publisher_uuid_v5=None,
                    bucket_name=OapenIrusUkTelescope.OAPEN_BUCKET,
                    blob_name=irus_month.download_blob_name,
                )
                futures.append(future)
            for future in futures:
                future.result()

    def transfer(self, release: OapenIrusUkRelease, **kwargs):
        """Task to transfer the file for each release month.

        :param release: the OapenIrusUkRelease instance.
        """
        worker_threads = len(release.irus_months) if len(release.irus_months) < self.max_threads else self.max_threads
        with ThreadPoolExecutor(max_workers=worker_threads) as executor:
            futures = []
            for irus_month in release.irus_months:
                future = executor.submit(
                    gcs_copy_blob,
                    blob_name=irus_month.download_blob_name,
                    src_bucket=OapenIrusUkTelescope.OAPEN_BUCKET,
                    dst_bucket=self.cloud_workspace.download_bucket,
                )
                futures.append(future)
            for future in futures:
                success = future.result()
                set_task_state(success, kwargs["ti"].task_id, release=release)

    def download(self, release: OapenIrusUkRelease, **kwargs):
        """Task to download the access stats to a local file for each release."""
        worker_threads = len(release.irus_months) if len(release.irus_months) < self.max_threads else self.max_threads
        with ThreadPoolExecutor(max_workers=worker_threads) as executor:
            futures = []
            for irus_month in release.irus_months:
                future = executor.submit(
                    gcs_download_blob,
                    bucket_name=self.cloud_workspace.download_bucket,
                    blob_name=irus_month.download_blob_name,
                    file_path=irus_month.download_path,
                )
                futures.append(future)
            for future in futures:
                success = future.result()
                set_task_state(success, kwargs["ti"].task_id, release=release)

    def transform(self, release: OapenIrusUkRelease, **kwargs):
        worker_threads = len(release.irus_months) if len(release.irus_months) < self.max_threads else self.max_threads
        with ThreadPoolExecutor(max_workers=worker_threads) as executor:
            futures = []
            for irus_month in release.irus_months:
                future = executor.submit(
                    transform_month,
                    download_path=irus_month.download_path,
                    transform_path=irus_month.transform_path,
                    release_date=irus_month.month,
                )
                futures.append(future)
            for future in futures:
                success = future.result()
                set_task_state(success, kwargs["ti"].task_id, release=release)

    def upload_transformed(self, release: OapenIrusUkRelease, **kwargs) -> None:
        """Uploads the transformed files to GCS for each release month"""
        file_paths = [im.transform_path for im in release.irus_months]
        blob_names = [im.transform_blob_name for im in release.irus_months]
        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.transform_bucket, file_paths=file_paths, blob_names=blob_names
        )
        set_task_state(success, kwargs["ti"].task_id, release=release)

    def bq_load(self, release: OapenIrusUkRelease, **kwargs) -> None:
        """Loads the data into BigQuery"""
        table_id = bq_table_id(self.cloud_workspace.project_id, self.bq_dataset_id, self.bq_table_name)
        if not release.is_first_run:
            bq_create_dataset(
                project_id=self.cloud_workspace.project_id,
                dataset_id=self.bq_dataset_id,
                location=self.cloud_workspace.data_location,
                description=self.bq_dataset_description,
            )
            bq_backup_table_id = f"{table_id}_backup"
            logging.info(f"Creating backup: '{bq_backup_table_id}' from table: '{table_id}'")
            bq_copy_table(src_table_id=table_id, dst_table_id=bq_backup_table_id)
            # Delete the original table
            logging.info(f"Deleting existing table: '{table_id}'")
            client = Client()
            table = Table(table_id)
            client.delete_table(table, not_found_ok=False)

        worker_threads = len(release.irus_months) if len(release.irus_months) < self.max_threads else self.max_threads
        with ThreadPoolExecutor(max_workers=worker_threads) as executor:
            futures = []
            for irus_month in release.irus_months:
                uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, irus_month.transform_blob_name)
                future = executor.submit(
                    bq_load_table,
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
                futures.append(future)
            for future in futures:
                state = future.result()
                set_task_state(state, kwargs["ti"].task_id, release=release)

    def add_new_dataset_releases(self, release: OapenIrusUkRelease, **kwargs) -> None:
        """Adds release information to API."""
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.api_dataset_id,
            dag_run_id=release.run_id,
            data_interval_start=release.data_interval_start,
            data_interval_end=release.data_interval_end,
            partition_date=release.partition_date,
        )
        api.post_dataset_release(dataset_release)

    def cleanup(self, release: OapenIrusUkRelease, **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""
        cleanup(dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder)


def upload_source_code_to_bucket(
    source_url: str, project_id: str, bucket_name: str, blob_name: str, cloud_function_path: str
) -> Tuple[bool, bool]:
    """Upload source code of cloud function to storage bucket

    :param source_url: The url to the zip file with source code
    :param project_id: The project id with the bucket
    :param bucket_name: The bucket name
    :param blob_name: The blob name
    :param cloud_function_path: The local path to the cloud function
    :return: Whether task was successful and whether file was uploaded
    """

    # Get zip file with source code from github release
    response = requests.get(source_url)
    with open(cloud_function_path, "wb") as f:
        f.write(response.content)

    # Check if current md5 hash matches expected md5 hash
    expected_md5_hash = OapenIrusUkTelescope.FUNCTION_MD5_HASH
    actual_md5_hash = get_file_hash(file_path=cloud_function_path, algorithm="md5")
    if expected_md5_hash != actual_md5_hash:
        raise AirflowException(f"md5 hashes do not match, expected: {expected_md5_hash}, actual: {actual_md5_hash}")

    # Create storage bucket
    gcs_create_bucket(bucket_name=bucket_name, location="europe-west1", project_id=project_id, lifecycle_delete_age=1)

    # upload zip to cloud storage
    success, upload = gcs_upload_file(
        bucket_name=bucket_name, blob_name=blob_name, file_path=cloud_function_path, project_id=project_id
    )
    return success, upload


def cloud_function_exists(service: Resource, full_name: str) -> Optional[str]:
    """Check if cloud function with a given name already exists

    :param service: Cloud function service
    :param full_name: Name of the cloud function
    :return: URI if cloud function exists, else None
    """
    try:
        response = service.projects().locations().functions().get(name=full_name).execute()
        uri = response["serviceConfig"]["uri"]
    except HttpError:
        return None
    except KeyError:
        logging.warn(f"Invalid response. Got: {response}")
        return None
    return uri


def create_cloud_function(
    service: Resource,
    location: str,
    full_name: str,
    source_bucket: str,
    blob_name: str,
    max_active_runs: int,
    update: bool,
) -> Tuple[bool, dict]:
    """Create cloud function.

    :param service: Cloud function service
    :param location: Location of the cloud function
    :param full_name: Name of the cloud function
    :param source_bucket: Name of bucket where the source code is stored
    :param blob_name: Blob name of source code inside bucket
    :param max_active_runs: The limit on the maximum number of function instances that may coexist at a given time
    :param update: Whether a new function is created or an existing one is updated
    :return: Status of the cloud function and error/success message
    """
    # os.environ["PORT"] = find_free_port()
    body = {
        "name": full_name,
        "environment": "GEN_2",
        "description": "Pulls oapen irus uk data and replaces ip addresses with city and country info.",
        "buildConfig": {
            "runtime": "python39",
            "entryPoint": "download",
            "source": {"storageSource": {"bucket": source_bucket, "object": blob_name}},
            "environmentVariables": {"PORT": "9080"},
        },
        "serviceConfig": {
            "timeoutSeconds": OapenIrusUkTelescope.FUNCTION_TIMEOUT,
            "availableMemory": "4096M",
            "maxInstanceCount": max_active_runs,
            "allTrafficOnLatestRevision": True,
        },
    }
    gcp_functions = service.projects().locations().functions()
    if update:
        response = gcp_functions.patch(name=full_name, updateMask=",".join(body.keys()), body=body).execute()
        logging.info(f"Patching cloud function, response: {response}")

    else:
        response = gcp_functions.create(
            parent=location, functionId=OapenIrusUkTelescope.FUNCTION_NAME, body=body
        ).execute()
        logging.info(f"Creating cloud function, response: {response}")

    operation_name = response.get("name")
    done = response.get("done")
    while not done:
        time.sleep(10)
        response = service.projects().locations().operations().get(name=operation_name).execute()
        done = response.get("done")

    error = response.get("error")
    response = response.get("response")
    success = bool(response)
    msg = response if response else error

    return success, msg


def call_cloud_function(
    function_uri: str,
    release_date: str,
    username: str,
    password: str,
    geoip_license_key: str,
    bucket_name: str,
    blob_name: str,
    publisher_name_v4: str = None,
    publisher_uuid_v5: str = None,
) -> None:
    """Iteratively call cloud function, until it has finished processing all publishers.
    When a publisher name/uuid  is given, there is only 1 publisher, if it is empty the cloud function will process
    all available publishers. In that case, when the data is downloaded from the new platform it can be done in 1
    iteration, however for the old platform two files have to be downloaded separately for each publisher,
    this might take longer than the timeout time of the cloud function, so the process is split up in multiple calls.

    :param function_uri: URI of the cloud function
    :param release_date: The release date in YYYY-MM
    :param username: Oapen username (email or requestor_id)
    :param password: Oapen password (password or api_key)
    :param geoip_license_key: License key of geoip database
    :param bucket_name: Name of the bucket to store oapen access stats data
    :param blob_name: Blob name to store oapen access stats data
    :param publisher_name_v4: URL encoded name of the publisher (used for counter version 4)
    :param publisher_uuid_v5: UUID of the publisher (used for counter version 5)
    """
    creds = IDTokenCredentials.from_service_account_file(
        os.environ.get(environment_vars.CREDENTIALS), target_audience=function_uri
    )
    authed_session = AuthorizedSession(creds)
    data = {
        "release_date": release_date,
        "username": username,
        "password": password,
        "geoip_license_key": geoip_license_key,
        "publisher_name_v4": publisher_name_v4,
        "publisher_uuid_v5": publisher_uuid_v5,
        "bucket_name": bucket_name,
        "blob_name": blob_name,
    }
    logging.info(f"Calling cloud function uri: {function_uri} for month: {release_date}")
    response = authed_session.post(
        function_uri,
        data=json.dumps(data),
        headers={"Content-Type": "application/json"},
        timeout=OapenIrusUkTelescope.FUNCTION_TIMEOUT,
    )
    logging.info(f"Call cloud function response status code: {response.status_code}, reason: {response.reason}")
    if response.status_code != 200:
        raise AirflowException("Cloud function unsuccessful")

    response_json = response.json()
    entries = response_json["entries"]
    if entries == 0:
        logging.warn(f"No access stats entries for publisher(s) in month {release_date}")
    logging.info(f"Processed {entries} entries for month {release_date}")


def transform_month(download_path, transform_path, release_date):
    """Transforms an OAPEN IRUS UK month from a gzipped json file

    :param download_path: The location of the downloaded file
    :param transform_path: The save location of the transformed output
    :param release_date: The release date of the data
    """
    with gzip.open(download_path, "r") as f:
        results = [json.loads(line) for line in f]
    results = add_partition_date(results, release_date, TimePartitioningType.MONTH, partition_field="release_date")
    save_jsonl_gz(transform_path, results)


def parse_release_msg(msg: Dict) -> Tuple[pendulum.DateTime, pendulum.DateTime, bool, pendulum.DateTime]:
    start_date = pendulum.parse(msg["start_date"])
    end_date = pendulum.parse(msg["end_date"])
    is_first_run = msg["is_first_run"]

    return start_date, end_date, is_first_run
