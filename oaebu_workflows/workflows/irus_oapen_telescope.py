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

# Author: Aniek Roelofs

import gzip
import json
import logging
import os
import time
from typing import Dict, List, Optional, Tuple, Union

import pendulum
import requests
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from google.auth import environment_vars
from google.auth.transport.requests import AuthorizedSession
from google.cloud.bigquery import TimePartitioningType, SourceFormat, WriteDisposition
from google.oauth2.service_account import IDTokenCredentials
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api
from observatory.platform.airflow import AirflowConns
from observatory.platform.files import get_file_hash, save_jsonl_gz
from observatory.platform.files import add_partition_date
from observatory.platform.bigquery import bq_load_table, bq_table_id, bq_create_dataset
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.workflows.workflow import (
    PartitionRelease,
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


class IrusOapenRelease(PartitionRelease):
    def __init__(
        self,
        dag_id: str,
        run_id: str,
        data_interval_start: pendulum.DateTime,
        data_interval_end: pendulum.DateTime,
        partition_date: pendulum.DateTime,
    ):
        """Create a IrusOapenRelease instance.

        :param dag_id: The ID of the DAG
        :param run_id: The Airflow run ID
        :param partition_date: The date of the partition/release
        """
        super().__init__(dag_id=dag_id, run_id=run_id, partition_date=partition_date)
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end
        self.download_path = os.path.join(self.download_folder, "irus_oapen.jsonl.gz")
        self.transform_path = os.path.join(self.transform_folder, "irus_oapen.jsonl.gz")
        self.blob_name = gcs_blob_name_from_path(
            os.path.join(self.download_folder, f'{self.partition_date.format("YYYY_MM")}.jsonl.gz')
        )
        self.cloud_function_path = os.path.join(self.download_folder, "oapen_cloud_function.zip")


class IrusOapenTelescope(Workflow):
    OAPEN_PROJECT_ID = "oapen-usage-data-gdpr-proof"  # The oapen project id.
    OAPEN_BUCKET = f"{OAPEN_PROJECT_ID}_cloud-function"  # Storage bucket with the source code
    FUNCTION_NAME = "oapen-access-stats"  # Name of the google cloud function
    FUNCTION_REGION = "europe-west1"  # Region of the google cloud function
    FUNCTION_SOURCE_URL = (
        "https://github.com/The-Academic-Observatory/oapen-irus-uk-cloud-function/releases/"
        "download/v1.1.9/oapen-irus-uk-cloud-function.zip"
    )  # URL to the zipped source code of the cloud function
    FUNCTION_MD5_HASH = "946bb4d7ca229b15aba36ad7b5ed56d0"  # MD5 hash of the zipped source code
    FUNCTION_BLOB_NAME = "cloud_function_source_code.zip"  # blob name of zipped source code
    FUNCTION_TIMEOUT = 1500  # Timeout of cloud function in seconds. Maximum of 60 minutes,
    # see https://cloud.google.com/functions/docs/2nd-gen/overview#enhanced_infrastructure

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        publisher_name_v4: str,
        publisher_uuid_v5: str,
        data_partner: Union[str, OaebuPartner] = "irus_oapen",
        bq_dataset_description: str = "IRUS dataset",
        bq_table_description: str = None,
        api_dataset_id: str = "oapen",
        max_cloud_function_instances: int = 0,
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        geoip_license_conn_id: str = "geoip_license_key",
        irus_oapen_api_conn_id: str = "irus_api",
        irus_oapen_login_conn_id: str = "irus_login",
        catchup: bool = True,
        start_date: pendulum.DateTime = pendulum.datetime(2015, 6, 1),
        schedule: str = "0 0 4 * *",  # Run on the 4th of every month
        max_active_runs: int = 5,
    ):
        """The OAPEN irus uk telescope.
        :param dag_id: The ID of the DAG
        :param cloud_workspace: The CloudWorkspace object for this DAG
        :param publisher_name_v4: The publisher's name for version 4
        :param publisher_uuid_v5: The publisher's uuid for version 5
        :param data_partner: The data partner
        :param bq_dataset_description: Description for the BigQuery dataset
        :param bq_table_description: Description for the biguery table
        :param api_dataset_id: The ID to store the dataset release in the API
        :param max_cloud_function_instances:
        :param observatory_api_conn_id: Airflow connection ID for the overvatory API
        :param geoip_license_conn_id: The Airflow connection ID for the GEOIP license
        :param irus_oapen_api_conn_id: The Airflow connection ID for IRUS API - for counter 5
        :param irus_oapen_login_conn_id: The Airflow connection ID for IRUS API (login) - for counter 4
        :param catchup: Whether to catchup the DAG or not
        :param start_date: The start date of the DAG
        :param schedule: The schedule interval of the DAG
        :param max_active_runs: The maximum number of concurrent DAG instances
        """
        if bq_table_description is None:
            bq_table_description = "OAPEN metrics as recorded by the IRUS platform"

        super().__init__(
            dag_id,
            start_date,
            schedule,
            catchup=catchup,
            airflow_conns=[
                observatory_api_conn_id,
                geoip_license_conn_id,
                irus_oapen_api_conn_id,
                irus_oapen_login_conn_id,
            ],
            max_active_runs=max_active_runs,
            tags=["oaebu"],
        )
        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.publisher_name_v4 = publisher_name_v4
        self.publisher_uuid_v5 = publisher_uuid_v5
        self.data_partner = partner_from_str(data_partner)
        self.bq_dataset_description = bq_dataset_description
        self.bq_table_description = bq_table_description
        self.api_dataset_id = api_dataset_id
        self.max_cloud_function_instances = max_cloud_function_instances
        self.observatory_api_conn_id = observatory_api_conn_id
        self.geoip_license_conn_id = geoip_license_conn_id
        self.irus_oapen_api_conn_id = irus_oapen_api_conn_id
        self.irus_oapen_login_conn_id = irus_oapen_login_conn_id

        check_workflow_inputs(self)

        self.add_setup_task(self.check_dependencies)
        # create PythonOperator with task concurrency of 1, so tasks to create cloud function never run in parallel
        self.add_task(self.create_cloud_function, task_concurrency=1)
        self.add_task(self.call_cloud_function)
        self.add_task(self.transfer)
        self.add_task(self.download_transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> List[IrusOapenRelease]:
        """Create a list of IrusOapenRelease instances for a given month.
        Say the dag is scheduled to run on 2022-04-07
        Interval_start will be 2022-03-01
        Interval_end will be 2022-04-01
        partition_date will be 2022-03-31

        :param kwargs: the context passed from the PythonOperator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
        :return: list of IrusOapenRelease instances
        """
        # Get release_date
        data_interval_start = kwargs["data_interval_start"].start_of("month")
        data_interval_end = kwargs["data_interval_end"].start_of("month")
        partition_date = data_interval_start.end_of("month")

        logging.info(f"Release/partition date: {partition_date}")
        releases = [
            IrusOapenRelease(
                dag_id=self.dag_id,
                run_id=kwargs["run_id"],
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
                partition_date=partition_date,
            )
        ]
        return releases

    def transfer(self, releases: List[IrusOapenRelease], **kwargs):
        """Task to transfer the file for each release.

        :param releases: the list of IrusOapenRelease instances.
        """
        for release in releases:
            success = gcs_copy_blob(
                blob_name=release.blob_name,
                src_bucket=IrusOapenTelescope.OAPEN_BUCKET,
                dst_bucket=self.cloud_workspace.download_bucket,
            )
            set_task_state(success, kwargs["ti"].task_id, release=release)

    def download_transform(self, releases: List[IrusOapenRelease], **kwargs):
        """Task to download the access stats to a local file for each release."""
        for release in releases:
            success = gcs_download_blob(
                bucket_name=self.cloud_workspace.download_bucket,
                blob_name=release.blob_name,
                file_path=release.download_path,
            )
            set_task_state(success, kwargs["ti"].task_id, release=release)

            # Read gzipped data and create list of dicts
            with gzip.open(release.download_path, "r") as f:
                results = [json.loads(line) for line in f]

            # Add partition date
            results = add_partition_date(
                results, release.partition_date, TimePartitioningType.MONTH, partition_field="release_date"
            )

            # Write list into gzipped JSON Lines file
            save_jsonl_gz(release.transform_path, results)

    def create_cloud_function(self, releases: List[IrusOapenRelease], **kwargs):
        """Task to create the cloud function for each release."""
        for release in releases:
            # set up cloud function variables
            oapen_project_id = IrusOapenTelescope.OAPEN_PROJECT_ID
            source_bucket = IrusOapenTelescope.OAPEN_BUCKET
            function_name = IrusOapenTelescope.FUNCTION_NAME
            function_region = IrusOapenTelescope.FUNCTION_REGION
            function_source_url = IrusOapenTelescope.FUNCTION_SOURCE_URL
            function_blob_name = IrusOapenTelescope.FUNCTION_BLOB_NAME
            location = f"projects/{oapen_project_id}/locations/{function_region}"
            full_name = f"{location}/functions/{function_name}"

            # zip source code and upload to bucket
            success, upload = upload_source_code_to_bucket(
                source_url=function_source_url,
                project_id=oapen_project_id,
                bucket_name=source_bucket,
                blob_name=function_blob_name,
                cloud_function_path=release.cloud_function_path,
            )
            set_task_state(success, kwargs["ti"].task_id, release=release)

            # initialise cloud functions api
            creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ.get(environment_vars.CREDENTIALS))
            service = build(
                "cloudfunctions", "v2beta", credentials=creds, cache_discovery=False, static_discovery=False
            )

            # update or create cloud function
            exists = cloud_function_exists(service, full_name)
            if not exists or upload is True:
                update = True if exists else False
                success, msg = create_cloud_function(
                    service,
                    location,
                    full_name,
                    source_bucket,
                    function_blob_name,
                    self.max_cloud_function_instances,
                    update,
                )
                set_task_state(success, kwargs["ti"].task_id, release=release)
                logging.info(f"Creating or patching cloud function successful, response: {msg}")
            else:
                logging.info(f"Using existing cloud function, source code has not changed.")

    def call_cloud_function(self, releases: List[IrusOapenRelease], **kwargs):
        """Task to call the cloud function for each release."""
        for release in releases:
            # set up cloud function variables
            oapen_project_id = IrusOapenTelescope.OAPEN_PROJECT_ID
            source_bucket = IrusOapenTelescope.OAPEN_BUCKET
            function_name = IrusOapenTelescope.FUNCTION_NAME
            function_region = IrusOapenTelescope.FUNCTION_REGION
            location = f"projects/{oapen_project_id}/locations/{function_region}"
            full_name = f"{location}/functions/{function_name}"
            geoip_license_key = BaseHook.get_connection(self.geoip_license_conn_id).password

            # get the publisher_uuid or publisher_id, both are set to empty strings when publisher id is 'oapen'
            if release.partition_date >= pendulum.datetime(2020, 4, 1):
                airflow_conn = self.irus_oapen_api_conn_id
            else:
                airflow_conn = self.irus_oapen_login_conn_id
            username = BaseHook.get_connection(airflow_conn).login
            password = BaseHook.get_connection(airflow_conn).password

            # initialise cloud functions api
            creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ.get(environment_vars.CREDENTIALS))
            service = build(
                "cloudfunctions", "v2beta", credentials=creds, cache_discovery=False, static_discovery=False
            )

            # Get cloud function uri
            function_uri = cloud_function_exists(service, full_name)

            call_cloud_function(
                function_uri,
                release.partition_date.format("YYYY-MM"),
                username,
                password,
                geoip_license_key,
                self.publisher_name_v4,
                self.publisher_uuid_v5,
                source_bucket,
                release.blob_name,
            )

    def upload_transformed(self, releases: List[IrusOapenRelease], **kwargs) -> None:
        """Uploads the transformed files to GCS for each release"""
        for release in releases:
            success = gcs_upload_files(
                bucket_name=self.cloud_workspace.transform_bucket,
                file_paths=[release.transform_path],
            )
            set_task_state(success, kwargs["ti"].task_id, release=release)

    def bq_load(self, releases: List[IrusOapenRelease], **kwargs) -> None:
        """Loads the sales and traffic data into BigQuery"""
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

    def add_new_dataset_releases(self, releases: List[IrusOapenRelease], **kwargs) -> None:
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

    def cleanup(self, releases: List[IrusOapenRelease], **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""
        for release in releases:
            cleanup(
                dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder
            )


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
    expected_md5_hash = IrusOapenTelescope.FUNCTION_MD5_HASH
    actual_md5_hash = get_file_hash(file_path=cloud_function_path, algorithm="md5")
    if expected_md5_hash != actual_md5_hash:
        raise AirflowException(f"md5 hashes do not match, expected: {expected_md5_hash}, actual: {actual_md5_hash}")

    # Create storage bucket
    gcs_create_bucket(bucket_name=bucket_name, location="EU", project_id=project_id, lifecycle_delete_age=1)

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
    body = {
        "name": full_name,
        "environment": "GEN_2",
        "description": "Pulls oapen irus uk data and replaces ip addresses with city and country info.",
        "buildConfig": {
            "runtime": "python39",
            "entryPoint": "download",
            "source": {"storageSource": {"bucket": source_bucket, "object": blob_name}},
        },
        "serviceConfig": {
            "timeoutSeconds": IrusOapenTelescope.FUNCTION_TIMEOUT,
            "availableMemory": "4096M",
            "maxInstanceCount": max_active_runs,
            "allTrafficOnLatestRevision": True,
        },
    }
    if update:
        update_mask = ",".join(body.keys())
        response = (
            service.projects()
            .locations()
            .functions()
            .patch(name=full_name, updateMask=update_mask, body=body)
            .execute()
        )
        logging.info(f"Patching cloud function, response: {response}")
    else:
        response = (
            service.projects()
            .locations()
            .functions()
            .create(parent=location, functionId=IrusOapenTelescope.FUNCTION_NAME, body=body)
            .execute()
        )
        logging.info(f"Creating cloud function, response: {response}")

    operation_name = response.get("name")
    done = response.get("done")
    while not done:
        time.sleep(10)
        response = service.projects().locations().operations().get(name=operation_name).execute()
        done = response.get("done")

    error = response.get("error")
    response = response.get("response")
    if response:
        msg = response
        success = True
    else:
        msg = error
        success = False

    return success, msg


def call_cloud_function(
    function_uri: str,
    release_date: str,
    username: str,
    password: str,
    geoip_license_key: str,
    publisher_name_v4: str,
    publisher_uuid_v5: str,
    bucket_name: str,
    blob_name: str,
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
    :param publisher_name_v4: URL encoded name of the publisher (used for counter version 4)
    :param publisher_uuid_v5: UUID of the publisher (used for counter version 5)
    :param bucket_name: Name of the bucket to store oapen access stats data
    :param blob_name: Blob name to store oapen access stats data
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
    finished = False
    while not finished:
        response = authed_session.post(
            function_uri,
            data=json.dumps(data),
            headers={"Content-Type": "application/json"},
            timeout=IrusOapenTelescope.FUNCTION_TIMEOUT,
        )
        logging.info(f"Call cloud function response status code: {response.status_code}, reason: {response.reason}")
        if response.status_code != 200:
            raise AirflowException("Cloud function unsuccessful")

        response_json = response.json()
        if response_json["unprocessed_publishers"]:
            data["unprocessed_publishers"] = response_json["unprocessed_publishers"]
            remaining_publishers = len(response_json["unprocessed_publishers"])
        else:
            finished = True
            remaining_publishers = 0

        entries = response_json["entries"]
        if entries == 0 and remaining_publishers == 0:
            raise AirflowSkipException("No access stats entries for publisher(s) in month.")

        logging.info(f"Processed {entries} entries in total. {remaining_publishers} publishers " f"left to process")
