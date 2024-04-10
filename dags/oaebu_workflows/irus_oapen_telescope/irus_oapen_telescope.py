# Copyright 2020-2024 Curtin University
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

import gzip
import json
import logging
import os
import time
from typing import List, Optional, Tuple, Union

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from google.auth import transport, compute_engine
from google.cloud.bigquery import TimePartitioningType, SourceFormat, WriteDisposition, Client
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError

from oaebu_workflows.oaebu_partners import OaebuPartner, partner_from_str
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.files import get_file_hash, save_jsonl_gz, add_partition_date
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.google.bigquery import bq_load_table, bq_table_id, bq_create_dataset
from observatory_platform.airflow.release import PartitionRelease, set_task_state
from observatory_platform.airflow.workflow import CloudWorkspace, cleanup
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.google.gcs import (
    gcs_copy_blob,
    gcs_create_bucket,
    gcs_download_blob,
    gcs_upload_file,
    gcs_upload_files,
    gcs_blob_uri,
    gcs_blob_name_from_path,
)

IRUS_FUNCTION_NAME = "oapen-access-stats"  # Name of the google cloud function
IRUS_FUNCTION_REGION = "europe-west1"  # Region of the google cloud function
IRUS_FUNCTION_SOURCE_URL = (
    "https://github.com/The-Academic-Observatory/oapen-irus-uk-cloud-function/releases/"
    "download/v1.1.9/oapen-irus-uk-cloud-function.zip"
)  # URL to the zipped source code of the cloud function
IRUS_FUNCTION_MD5_HASH = "946bb4d7ca229b15aba36ad7b5ed56d0"  # MD5 hash of the zipped source code
IRUS_FUNCTION_BLOB_NAME = "cloud_function_source_code.zip"  # blob name of zipped source code
IRUS_FUNCTION_TIMEOUT = 1500  # Timeout of cloud function in seconds. Maximum of 60 minutes,
# see https://cloud.google.com/functions/docs/2nd-gen/overview#enhanced_infrastructure


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
        self.download_file_name = "irus_oapen.jsonl.gz"
        self.transform_file_name = "irus_oapen.jsonl.gz"

    @property
    def download_path(self):
        return os.path.join(self.download_folder, self.download_file_name)

    @property
    def transform_path(self):
        return os.path.join(self.transform_folder, self.transform_file_name)

    @property
    def cloud_function_path(self):
        return os.path.join(self.download_folder, "oapen_cloud_function.zip")

    @property
    def download_blob_name(self):
        return gcs_blob_name_from_path(self.download_path)

    @property
    def transform_blob_name(self):
        return gcs_blob_name_from_path(self.transform_path)

    @staticmethod
    def from_dict(dict_: dict):
        return IrusOapenRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            data_interval_start=pendulum.parse(dict_["data_interval_start"]),
            data_interval_end=pendulum.parse(dict_["data_interval_end"]),
            partition_date=pendulum.parse(dict_["partition_date"]),
        )

    def to_dict(self) -> dict:
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "data_interval_start": self.data_interval_start.isoformat(),
            "data_interval_end": self.data_interval_end.isoformat(),
            "partition_date": self.partition_date.isoformat(),
        }


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    publisher_name_v4: str,
    publisher_uuid_v5: str,
    data_partner: Union[str, OaebuPartner] = "irus_oapen",
    bq_dataset_description: str = "IRUS dataset",
    bq_table_description: str = "OAPEN metrics as recorded by the IRUS platform",
    gdpr_oapen_project_id: str = "oapen-usage-data-gdpr-proof",
    gdpr_oapen_bucket_id: str = "oapen-usage-data-gdpr-proof_cloud-function",
    api_dataset_id: str = "dataset_api",
    max_cloud_function_instances: int = 0,
    geoip_license_conn_id: str = "geoip_license_key",
    irus_oapen_api_conn_id: str = "irus_api",
    irus_oapen_login_conn_id: str = "irus_login",
    catchup: bool = True,
    start_date: pendulum.DateTime = pendulum.datetime(2015, 6, 1),
    schedule: str = "0 0 4 * *",  # Run on the 4th of every month
    max_active_runs: int = 5,
    retries: int = 3,
    retry_delay: Union[int, float] = 5,
):
    """The OAPEN irus uk telescope.
    :param dag_id: The ID of the DAG
    :param cloud_workspace: The CloudWorkspace object for this DAG
    :param publisher_name_v4: The publisher's name for version 4
    :param publisher_uuid_v5: The publisher's uuid for version 5
    :param data_partner: The data partner
    :param bq_dataset_description: Description for the BigQuery dataset
    :param bq_table_description: Description for the biguery table
    :param gdpr_oapen_project_id: The gdpr-proof oapen project id.
    :param gdpr_oapen_bucket_id: The gdpr-proof oapen bucket
    :param api_dataset_id: The name of the Bigquery dataset to store the API release(s)
    :param max_cloud_function_instances:
    :param geoip_license_conn_id: The Airflow connection ID for the GEOIP license
    :param irus_oapen_api_conn_id: The Airflow connection ID for IRUS API - for counter 5
    :param irus_oapen_login_conn_id: The Airflow connection ID for IRUS API (login) - for counter 4
    :param catchup: Whether to catchup the DAG or not
    :param start_date: The start date of the DAG
    :param schedule: The schedule interval of the DAG
    :param max_active_runs: The maximum number of concurrent DAG instances
    :param max_active_runs: The maximum number of active DAG runs
    :param retries: The number of times to retry failed tasks
    :param retry_delay: The delay between retries in minutes
    """

    data_partner = partner_from_str(data_partner)

    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        tags=["oaebu"],
        max_active_runs=max_active_runs,
        default_args=dict(
            retries=retries, retry_delay=pendulum.duration(minutes=retry_delay), on_failure_callback=on_failure_callback
        ),
    )
    def irus_oapen():
        @task()
        def fetch_releases(**context) -> List[dict]:
            """Create a list of IrusOapenRelease instances for a given month.
            Say the dag is scheduled to run on 2022-04-07
            Interval_start will be 2022-03-01
            Interval_end will be 2022-04-01
            partition_date will be 2022-03-31

            :param context: the context passed from the PythonOperator.
            See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
            :return: list of IrusOapenRelease instances
            """

            # Get release_date
            data_interval_start = context["data_interval_start"].start_of("month")
            data_interval_end = context["data_interval_end"].start_of("month")
            partition_date = data_interval_start.end_of("month")

            logging.info(f"Release/partition date: {partition_date}")
            releases = [
                IrusOapenRelease(
                    dag_id=dag_id,
                    run_id=context["run_id"],
                    data_interval_start=data_interval_start,
                    data_interval_end=data_interval_end,
                    partition_date=partition_date,
                )
            ]
            return [r.to_dict() for r in releases]

        @task()
        def create_cloud_function_(releases: List[dict], **context):
            """Task to create the cloud function for each release."""

            release = IrusOapenRelease.from_dict(releases[0])
            # set up cloud function variables
            location = f"projects/{gdpr_oapen_project_id}/locations/{IRUS_FUNCTION_REGION}"
            full_name = f"{location}/functions/{IRUS_FUNCTION_NAME}"

            # zip source code and upload to bucket
            success, upload = upload_source_code_to_bucket(
                source_url=IRUS_FUNCTION_SOURCE_URL,
                project_id=gdpr_oapen_project_id,
                bucket_name=gdpr_oapen_bucket_id,
                blob_name=IRUS_FUNCTION_BLOB_NAME,
                cloud_function_path=release.cloud_function_path,
            )
            set_task_state(success, context["ti"].task_id, release=release)

            # initialise cloud functions api
            service = build("cloudfunctions", "v2beta", cache_discovery=False, static_discovery=False)

            # update or create cloud function
            exists = cloud_function_exists(service, full_name)
            if not exists or upload is True:
                update = True if exists else False
                success, msg = create_cloud_function(
                    service=service,
                    location=location,
                    full_name=full_name,
                    source_bucket=gdpr_oapen_bucket_id,
                    blob_name=IRUS_FUNCTION_BLOB_NAME,
                    max_active_runs=max_cloud_function_instances,
                    update=update,
                )
                set_task_state(success, context["ti"].task_id, release=release)
                logging.info(f"Creating or patching cloud function successful, response: {msg}")
            else:
                logging.info(f"Using existing cloud function, source code has not changed.")

        @task_group(group_id="process_release")
        def process_release(data, **context):
            @task()
            def call_cloud_function_(release: dict, **context):
                """Task to call the cloud function for each release."""

                release = IrusOapenRelease.from_dict(release)
                # set up cloud function variables
                location = f"projects/{gdpr_oapen_project_id}/locations/{IRUS_FUNCTION_REGION}"
                full_name = f"{location}/functions/{IRUS_FUNCTION_NAME}"
                geoip_license_key = BaseHook.get_connection(geoip_license_conn_id).password

                # get the publisher_uuid or publisher_id, both are set to empty strings when publisher id is 'oapen'
                if release.partition_date >= pendulum.datetime(2020, 4, 1):
                    airflow_conn = irus_oapen_api_conn_id
                else:
                    airflow_conn = irus_oapen_login_conn_id
                username = BaseHook.get_connection(airflow_conn).login
                password = BaseHook.get_connection(airflow_conn).password

                # initialise cloud functions api
                service = build("cloudfunctions", "v2beta", cache_discovery=False, static_discovery=False)

                # Get cloud function uri
                function_uri = cloud_function_exists(service, full_name)

                call_cloud_function(
                    function_uri=function_uri,
                    release_date=release.partition_date.format("YYYY-MM"),
                    username=username,
                    password=password,
                    geoip_license_key=geoip_license_key,
                    publisher_name_v4=publisher_name_v4,
                    publisher_uuid_v5=publisher_uuid_v5,
                    bucket_name=gdpr_oapen_bucket_id,
                    blob_name=release.download_blob_name,
                )

            @task()
            def transfer(release: dict, **context):
                """Task to transfer the file for each release.

                :param releases: the list of IrusOapenRelease instances.
                """

                release = IrusOapenRelease.from_dict(release)
                success = gcs_copy_blob(
                    blob_name=release.download_blob_name,
                    src_bucket=gdpr_oapen_bucket_id,
                    dst_bucket=cloud_workspace.download_bucket,
                )
                set_task_state(success, context["ti"].task_id, release=release)

            @task()
            def transform(release: dict, **context):
                """Task to download the access stats to a local file for each release."""

                release = IrusOapenRelease.from_dict(release)
                # Download files from GCS
                success = gcs_download_blob(
                    bucket_name=cloud_workspace.download_bucket,
                    blob_name=release.download_blob_name,
                    file_path=release.download_path,
                )
                if not success:
                    raise FileNotFoundError(f"Could not find file: {release.download_blob_name}")

                # Read gzipped data and create list of dicts
                with gzip.open(release.download_path, "r") as f:
                    results = [json.loads(line) for line in f]

                # Add partition date
                results = add_partition_date(
                    results, release.partition_date, TimePartitioningType.MONTH, partition_field="release_date"
                )

                # Write list into gzipped JSON Lines file
                save_jsonl_gz(release.transform_path, results)

                success = gcs_upload_files(
                    bucket_name=cloud_workspace.transform_bucket,
                    file_paths=[release.transform_path],
                )
                set_task_state(success, context["ti"].task_id, release=release)

            @task()
            def bq_load(release: dict, **context) -> None:
                """Loads the sales and traffic data into BigQuery"""

                release = IrusOapenRelease.from_dict(release)
                bq_create_dataset(
                    project_id=cloud_workspace.project_id,
                    dataset_id=data_partner.bq_dataset_id,
                    location=cloud_workspace.data_location,
                    description=bq_dataset_description,
                )
                client = Client(project=cloud_workspace.project_id)
                uri = gcs_blob_uri(cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.transform_path))
                table_id = bq_table_id(
                    cloud_workspace.project_id, data_partner.bq_dataset_id, data_partner.bq_table_name
                )
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

                release = IrusOapenRelease.from_dict(release)
                client = Client(project=cloud_workspace.project_id)
                api = DatasetAPI(project_id=cloud_workspace.project_id, dataset_id=api_dataset_id, client=client)
                api.seed_db()
                dataset_release = DatasetRelease(
                    dag_id=dag_id,
                    dataset_id=api_dataset_id,
                    dag_run_id=release.run_id,
                    created=pendulum.now(),
                    modified=pendulum.now(),
                    data_interval_start=release.data_interval_start,
                    data_interval_end=release.data_interval_end,
                    partition_date=release.partition_date,
                )
                api.add_dataset_release(dataset_release)

            @task()
            def cleanup_workflow(release: dict, **context) -> None:
                """Delete all files, folders and XComs associated with this release."""

                release = IrusOapenRelease.from_dict(release)
                cleanup(
                    dag_id=dag_id,
                    execution_date=context["execution_date"],
                    workflow_folder=release.workflow_folder,
                )

            (
                call_cloud_function_(data)
                >> transfer(data)
                >> transform(data)
                >> bq_load(data)
                >> add_new_dataset_releases(data)
                >> cleanup_workflow(data)
            )

        # Define DAG tasks
        task_check_dependencies = check_dependencies(
            airflow_conns=[geoip_license_conn_id, irus_oapen_api_conn_id, irus_oapen_login_conn_id]
        )
        xcom_release = fetch_releases()
        cloud_function_task = create_cloud_function_(xcom_release, task_concurrency=1)
        process_release_task_group = process_release.expand(data=xcom_release)

        (task_check_dependencies >> xcom_release >> cloud_function_task >> process_release_task_group)

    return irus_oapen()


def upload_source_code_to_bucket(
    source_url: str,
    project_id: str,
    bucket_name: str,
    blob_name: str,
    cloud_function_path: str,
    expected_md5_hash: str = IRUS_FUNCTION_MD5_HASH,
) -> Tuple[bool, bool]:
    """Upload source code of cloud function to storage bucket

    :param source_url: The url to the zip file with source code
    :param project_id: The project id with the bucket
    :param bucket_name: The bucket name
    :param blob_name: The blob name
    :param cloud_function_path: The local path to the cloud function
    :param expected_md5_hash: The expected md5 hash of the source code
    :return: Whether task was successful and whether file was uploaded
    """

    # Get zip file with source code from github release
    response = requests.get(source_url)
    with open(cloud_function_path, "wb") as f:
        f.write(response.content)

    # Check if current md5 hash matches expected md5 hash
    actual_md5_hash = get_file_hash(file_path=cloud_function_path, algorithm="md5")
    if actual_md5_hash != expected_md5_hash:
        raise AirflowException(f"md5 hashes do not match, expected: {expected_md5_hash}, actual: {actual_md5_hash}")

    # Create storage bucket
    gcs_create_bucket(bucket_name=bucket_name, location="EU", project_id=project_id, lifecycle_delete_age=1)

    # upload zip to cloud storage
    success, upload = gcs_upload_file(bucket_name=bucket_name, blob_name=blob_name, file_path=cloud_function_path)
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
            "timeoutSeconds": IRUS_FUNCTION_TIMEOUT,
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
            .create(parent=location, functionId=IRUS_FUNCTION_NAME, body=body)
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
    request = transport.requests.Request()
    creds = compute_engine.IDTokenCredentials(
        request=request, target_audience=function_uri, use_metadata_identity_endpoint=True
    )
    authed_session = transport.requests.AuthorizedSession(creds)
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
            timeout=IRUS_FUNCTION_TIMEOUT,
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
