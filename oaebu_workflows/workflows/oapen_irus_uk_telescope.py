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

import pendulum
import requests
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from google.auth import environment_vars
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery
from google.oauth2.service_account import IDTokenCredentials
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials

from oaebu_workflows.config import schema_folder as default_schema_folder
from observatory.api.client.model.organisation import Organisation
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.file_utils import get_file_hash, list_to_jsonl_gz
from observatory.platform.utils.gc_utils import (
    copy_blob_from_cloud_storage,
    create_cloud_storage_bucket,
    download_blob_from_cloud_storage,
    upload_file_to_cloud_storage,
)
from observatory.platform.utils.workflow_utils import (
    SubFolder,
    add_partition_date,
    blob_name,
    make_dag_id,
    make_org_id,
    workflow_path,
)
from observatory.platform.workflows.organisation_telescope import (
    OrganisationRelease,
    OrganisationTelescope,
)
from oaebu_workflows.dag_tag import Tag


class OapenIrusUkRelease(OrganisationRelease):
    def __init__(self, dag_id: str, release_date: pendulum.DateTime, organisation: Organisation):
        """Create a OapenIrusUkRelease instance.

        :param dag_id: the DAG id.
        :param release_date: the date of the release.
        :param organisation: the Organisation of which data is processed.
        """

        transform_files_regex = f"{OapenIrusUkTelescope.DAG_ID_PREFIX}.jsonl.gz"
        super().__init__(dag_id, release_date, organisation, transform_files_regex=transform_files_regex)
        self.organisation_id = make_org_id(organisation.name)

    @property
    def blob_name(self) -> str:
        """Blob name for the access stats data, includes telescope dir

        :return: Blob name
        """
        return blob_name(
            os.path.join(self.download_folder, f'{self.release_date.strftime("%Y_%m")}_{self.organisation_id}.jsonl.gz')
        )

    @property
    def download_path(self) -> str:
        """Creates path to store the downloaded oapen irus uk data

        :return: Full path to the download file
        """
        return os.path.join(self.download_folder, f"{OapenIrusUkTelescope.DAG_ID_PREFIX}.jsonl.gz")

    @property
    def transform_path(self) -> str:
        """Creates path to store the transformed oapen irus uk data

        :return: Full path to the transform file
        """
        return os.path.join(self.transform_folder, f"{OapenIrusUkTelescope.DAG_ID_PREFIX}.jsonl.gz")

    def create_cloud_function(self, max_instances: int):
        """Create/update the google cloud function that is inside the oapen project id if the source code has changed.

        :param max_instances: The limit on the maximum number of function instances that may coexist at a given time.
        :return: None.
        """

        # set up cloud function variables
        oapen_project_id = OapenIrusUkTelescope.OAPEN_PROJECT_ID
        source_bucket = OapenIrusUkTelescope.OAPEN_BUCKET
        function_name = OapenIrusUkTelescope.FUNCTION_NAME
        function_region = OapenIrusUkTelescope.FUNCTION_REGION
        function_source_url = OapenIrusUkTelescope.FUNCTION_SOURCE_URL
        function_blob_name = OapenIrusUkTelescope.FUNCTION_BLOB_NAME
        location = f"projects/{oapen_project_id}/locations/{function_region}"
        full_name = f"{location}/functions/{function_name}"

        # zip source code and upload to bucket
        success, upload = upload_source_code_to_bucket(
            function_source_url, oapen_project_id, source_bucket, function_blob_name
        )
        if not success:
            raise AirflowException("Could not upload source code of cloud function to bucket.")

        # initialise cloud functions api
        creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ.get(environment_vars.CREDENTIALS))
        service = build("cloudfunctions", "v2beta", credentials=creds, cache_discovery=False, static_discovery=False)

        # update or create cloud function
        exists = cloud_function_exists(service, full_name)
        if not exists or upload is True:
            update = True if exists else False
            success, msg = create_cloud_function(
                service, location, full_name, source_bucket, function_blob_name, max_instances, update
            )
            if success:
                logging.info(f"Creating or patching cloud function successful, response: {msg}")
            else:
                raise AirflowException(f"Creating or patching cloud function unsuccessful, error: {msg}")
        else:
            logging.info(f"Using existing cloud function, source code has not changed.")

    def call_cloud_function(self, publisher_name_v4: str, publisher_uuid_v5: str):
        """Call the google cloud function that is inside the oapen project id

        :param publisher_name_v4: the publisher name, used with OAPEN counter v4 data
        :param publisher_uuid_v5: the publisher UUID, used with OAPEN counter v5 data
        :return: None.
        """
        # set up cloud function variables
        oapen_project_id = OapenIrusUkTelescope.OAPEN_PROJECT_ID
        source_bucket = OapenIrusUkTelescope.OAPEN_BUCKET
        function_name = OapenIrusUkTelescope.FUNCTION_NAME
        function_region = OapenIrusUkTelescope.FUNCTION_REGION
        location = f"projects/{oapen_project_id}/locations/{function_region}"
        full_name = f"{location}/functions/{function_name}"
        geoip_license_key = BaseHook.get_connection(AirflowConns.GEOIP_LICENSE_KEY).password

        # get the publisher_uuid or publisher_id, both are set to empty strings when publisher id is 'oapen'
        if self.release_date >= pendulum.datetime(2020, 4, 1):
            airflow_conn = AirflowConns.OAPEN_IRUS_UK_API
        else:
            airflow_conn = AirflowConns.OAPEN_IRUS_UK_LOGIN
        username = BaseHook.get_connection(airflow_conn).login
        password = BaseHook.get_connection(airflow_conn).password

        # initialise cloud functions api
        creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ.get(environment_vars.CREDENTIALS))
        service = build("cloudfunctions", "v2beta", credentials=creds, cache_discovery=False, static_discovery=False)

        # Get cloud function uri
        function_uri = cloud_function_exists(service, full_name)

        call_cloud_function(
            function_uri,
            self.release_date.strftime("%Y-%m"),
            username,
            password,
            geoip_license_key,
            publisher_name_v4,
            publisher_uuid_v5,
            source_bucket,
            self.blob_name,
        )

    def transfer(self):
        """Transfer blob from bucket inside oapen project to bucket in airflow project.

        :return: None.
        """
        success = copy_blob_from_cloud_storage(self.blob_name, OapenIrusUkTelescope.OAPEN_BUCKET, self.download_bucket)
        if not success:
            raise AirflowException("Transfer blob unsuccessful")

    def download_transform(self):
        """Download blob with access stats to a local file.

        :return: None.
        """
        success = download_blob_from_cloud_storage(self.download_bucket, self.blob_name, self.download_path)
        if not success:
            raise AirflowException("Download blob unsuccessful")

        # Read gzipped data and create list of dicts
        with gzip.open(self.download_path, "r") as f:
            results = [json.loads(line) for line in f]

        # Add partition date
        results = add_partition_date(results, self.release_date, bigquery.TimePartitioningType.MONTH)

        # Write list into gzipped JSON Lines file
        list_to_jsonl_gz(self.transform_path, results)


class OapenIrusUkTelescope(OrganisationTelescope):
    DAG_ID_PREFIX = "oapen_irus_uk"

    OAPEN_PROJECT_ID = "oapen-usage-data-gdpr-proof"  # The oapen project id.
    OAPEN_BUCKET = f"{OAPEN_PROJECT_ID}_cloud-function"  # Storage bucket with the source code
    FUNCTION_NAME = "oapen-access-stats"  # Name of the google cloud function
    FUNCTION_REGION = "europe-west1"  # Region of the google cloud function
    FUNCTION_SOURCE_URL = (
        "https://github.com/The-Academic-Observatory/oapen-irus-uk-cloud-function/releases/"
        "download/v1.1.7/oapen-irus-uk-cloud-function.zip"
    )  # URL to the zipped source code of the cloud function
    FUNCTION_MD5_HASH = "0b43068db2bf9bb19caa7438dc40ca2b"  # MD5 hash of the zipped source code
    FUNCTION_BLOB_NAME = "cloud_function_source_code.zip"  # blob name of zipped source code
    FUNCTION_TIMEOUT = 1500  # Timeout of cloud function in seconds. Maximum of 60 minutes,
    # see https://cloud.google.com/functions/docs/2nd-gen/overview#enhanced_infrastructure

    def __init__(
        self,
        organisation: Organisation,
        publisher_name_v4: str,
        publisher_uuid_v5: str,
        dag_id: Optional[str] = None,
        start_date: pendulum.DateTime = pendulum.datetime(2015, 6, 1),
        schedule_interval: str = "0 0 14 * *",
        dataset_id: str = "oapen",
        schema_folder: str = default_schema_folder(),
        dataset_description: str = "OAPEN dataset",
        table_descriptions: Dict = None,
        catchup: bool = True,
        airflow_vars: List = None,
        airflow_conns: List = None,
        max_active_runs=5,
        max_cloud_function_instances: int = 0,
        workflow_id: int = None,
    ):

        """The OAPEN irus uk telescope.
        :param organisation: the Organisation the DAG will process.
        :param publisher_name_v4: the publisher name, used with OAPEN counter v4 data, obtained from the 'extra' info
        :param publisher_uuid_v5: the publisher UUID, used with OAPEN counter v5 data, obtained from the 'extra' info
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param schema_folder: the SQL schema path.
        :param dataset_description: description for the BigQuery dataset.
        :param table_descriptions: a dictionary with table ids and corresponding table descriptions.
        :param catchup:  whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow.
        :param airflow_conns: list of airflow connection keys, for each connection it is checked if it exists in airflow
        :param max_cloud_function_instances: the maximum cloud function instances.
        :param workflow_id: api workflow id.
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        if airflow_conns is None:
            airflow_conns = [
                AirflowConns.GEOIP_LICENSE_KEY,
                AirflowConns.OAPEN_IRUS_UK_API,
                AirflowConns.OAPEN_IRUS_UK_LOGIN,
            ]

        if table_descriptions is None:
            table_descriptions = {
                self.DAG_ID_PREFIX: "Access stats from the OAPEN IRUS UK platform. Before 2020-04 "
                "from: https://irus.jisc.ac.uk/IRUSConsult/irus-oapen/v2/. "
                "After 2020-04 from the OAPEN_SUSHI API (documentation not "
                "published)."
            }

        if dag_id is None:
            dag_id = make_dag_id(self.DAG_ID_PREFIX, organisation.name)

        super().__init__(
            organisation,
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            schema_folder,
            dataset_description=dataset_description,
            catchup=catchup,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
            max_active_runs=max_active_runs,
            workflow_id=workflow_id,
            table_descriptions=table_descriptions,
            tags=[Tag.oaebu],
        )
        self.max_cloud_function_instances = max_cloud_function_instances
        self.publisher_name_v4 = publisher_name_v4
        self.publisher_uuid_v5 = publisher_uuid_v5

        self.add_setup_task(self.check_dependencies)
        # create PythonOperator with task concurrency of 1, so tasks to create cloud function never run in parallel
        self.add_task(self.create_cloud_function, task_concurrency=1)
        self.add_task(self.call_cloud_function)
        self.add_task(self.transfer)
        self.add_task(self.download_transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load_partition)
        self.add_task(self.cleanup)
        self.add_task(self.add_new_dataset_releases)

    def make_release(self, **kwargs) -> List[OapenIrusUkRelease]:
        """Create a list of OapenIrusUkRelease instances for a given month.

        :return: list of OapenIrusUkRelease instances
        """
        # Get release_date
        release_date = pendulum.instance(kwargs["execution_date"]).end_of("month")

        logging.info(f"Release date: {release_date}")
        releases = [OapenIrusUkRelease(self.dag_id, release_date, self.organisation)]
        return releases

    def create_cloud_function(self, releases: List[OapenIrusUkRelease], **kwargs):
        """Task to create the cloud function for each release.

        :param releases: list of OapenIrusUkRelease instances
        :return: None.
        """
        for release in releases:
            release.create_cloud_function(self.max_cloud_function_instances)

    def call_cloud_function(self, releases: List[OapenIrusUkRelease], **kwargs):
        """Task to call the cloud function for each release.

        :param releases: list of OapenIrusUkRelease instances
        :return: None.
        """
        for release in releases:
            release.call_cloud_function(self.publisher_name_v4, self.publisher_uuid_v5)

    def transfer(self, releases: List[OapenIrusUkRelease], **kwargs):
        """Task to transfer the file for each release.

        :param releases: the list of OapenIrusUkRelease instances.
        :return: None.
        """
        for release in releases:
            release.transfer()

    def download_transform(self, releases: List[OapenIrusUkRelease], **kwargs):
        """Task to download the access stats to a local file for each release.

        :param releases: the list of OapenIrusUkRelease instances.
        :return: None.
        """
        for release in releases:
            release.download_transform()


def upload_source_code_to_bucket(
    source_url: str, project_id: str, bucket_name: str, blob_name: str
) -> Tuple[bool, bool]:
    """Upload source code of cloud function to storage bucket

    :param source_url: The url to the zip file with source code
    :param project_id: The project id with the bucket
    :param bucket_name: The bucket name
    :param blob_name: The blob name
    :return: Whether task was successful and whether file was uploaded
    """

    # Get zip file with source code from github release
    telescope_folder = workflow_path(SubFolder.downloaded.value, OapenIrusUkTelescope.DAG_ID_PREFIX)
    filepath = os.path.join(telescope_folder, "oapen_cloud_function.zip")

    response = requests.get(source_url)
    with open(filepath, "wb") as f:
        f.write(response.content)

    # Check if current md5 hash matches expected md5 hash
    expected_md5_hash = OapenIrusUkTelescope.FUNCTION_MD5_HASH
    actual_md5_hash = get_file_hash(file_path=filepath, algorithm="md5")
    if expected_md5_hash != actual_md5_hash:
        raise AirflowException(f"md5 hashes do not match, expected: {expected_md5_hash}, actual: {actual_md5_hash}")

    # Create storage bucket
    create_cloud_storage_bucket(bucket_name, location="EU", project_id=project_id, lifecycle_delete_age=1)

    # upload zip to cloud storage
    success, upload = upload_file_to_cloud_storage(bucket_name, blob_name, filepath, project_id=project_id)
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
            "timeoutSeconds": OapenIrusUkTelescope.FUNCTION_TIMEOUT,
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
            .create(parent=location, functionId=OapenIrusUkTelescope.FUNCTION_NAME, body=body)
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
):
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
    :return: None
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
            timeout=OapenIrusUkTelescope.FUNCTION_TIMEOUT,
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
