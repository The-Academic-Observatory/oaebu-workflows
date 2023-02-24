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

import os
import shutil
from typing import List
import logging

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Variable
from google.cloud.bigquery import SourceFormat

from observatory.platform.utils.airflow_utils import AirflowVars, make_workflow_folder
from oaebu_workflows.config import schema_folder as default_schema_folder
from oaebu_workflows.workflows.onix_telescope import parse_onix
from observatory.platform.utils.config_utils import find_schema
from observatory.platform.utils.url_utils import retry_get_url
from observatory.platform.utils.file_utils import list_to_jsonl_gz, load_jsonl, list_files, blob_name_from_path
from observatory.platform.utils.gc_utils import upload_files_to_cloud_storage
from observatory.platform.utils.workflow_utils import (
    SubFolder,
    bq_load_shard,
    table_ids_from_path,
    make_release_date,
    cleanup,
)
from observatory.platform.workflows.workflow import Workflow, Release

THOTH_URL = "{host_name}/specifications/{format_specification}/publisher/{publisher_id}"
DEFAULT_FORMAT_SPECIFICATION = "onix_3.0::oapen"
DEFAULT_HOST_NAME = "https://export.thoth.pub"


class ThothRelease(Release):
    def __init__(
        self,
        *,
        dag_id: str,
        release_date: pendulum.DateTime,
    ):
        """Construct a ThothRelease.

        :param dag_id: the DAG id.
        :param release_date: the release date.
        """
        self.release_date = release_date
        release_id = f'{dag_id}_{self.release_date.format("YYYY_MM_DD")}'
        super().__init__(dag_id, release_id)


class ThothTelescope(Workflow):
    DAG_ID_PREFIX = "thoth_onix"

    def __init__(
        self,
        *,
        dag_id: str,
        project_id: str,
        download_bucket: str,
        transform_bucket: str,
        data_location: str,
        publisher_id: str,
        airflow_vars: List[str] = None,
        start_date: pendulum.DateTime = pendulum.datetime(2022, 12, 1),
        schedule_interval: str = "@weekly",
        dataset_id: str = "onix",
        schema_folder: str = default_schema_folder(),
        source_format: str = SourceFormat.NEWLINE_DELIMITED_JSON,
        catchup: bool = False,
        workflow_id: int = None,
        host_name: str = "https://export.thoth.pub",
        format_specification: str = DEFAULT_FORMAT_SPECIFICATION,
        download_file_name: str = "onix.xml",
        transform_file_name: str = "onix.jsonl",
        dataset_description: str = "Thoth ONIX Feed",
    ):
        """Construct an ThothOnixTelescope instance.

        :param organisation_name: the organisation name.
        :param project_id: the Google Cloud project id.
        :param download_bucket: the Google Cloud download bucket.
        :param transform_bucket: the Google Cloud transform bucket.
        :param data_location: the location for the BigQuery dataset.
        :param publisher_id: the publisher ID. Can be found using Thoth's GrapihQL API
        :param airflow_vars: list of airflow variable dependencies, for each connection, it is checked if it exists in airflow.
        :param dag_id: the id of the DAG, by default this is automatically generated based on the DAG_ID_PREFIX
        and the organisation name.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param schema_folder: the SQL schema path.
        :param source_format: the format of the data to load into BigQuery.
        :param catchup: whether to catchup the DAG or not.
        :param workflow_id: api workflow id.
        :param format_specification: The format to use when downloadin Thoth's metadata
        :param download_file_name: The name of the file to write the ONIX data to
        :param transform_file_name: The name of the file to write the transformed ONIX data to
        :param dataset_description: The description to give to the BigQuery table
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        # Cloud workspace settings
        self.project_id = project_id
        self.download_bucket = download_bucket
        self.transform_bucket = transform_bucket

        # Databse settings
        self.dataset_description = dataset_description
        self.dataset_id = dataset_id
        self.data_location = data_location
        self.source_format = source_format
        self.schema_folder = schema_folder

        # Thoth settings
        self.publisher_id = publisher_id
        self.host_name = host_name
        self.format_specification = format_specification
        self.download_file_name = download_file_name
        self.transform_file_name = transform_file_name

        # Initialise folders
        self.download_folder = None
        self.transform_folder = None

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=catchup,
            airflow_vars=airflow_vars,
            workflow_id=workflow_id,
        )

        # self.organisation = organisation
        self.add_setup_task(self.check_dependencies)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.cleanup)
        self.add_task(self.add_new_dataset_releases)

    def make_release(self, **kwargs) -> ThothRelease:
        """Creates a new Thoth release instance

        :return: The Thoth release instance
        """
        release_date = make_release_date(**kwargs)
        release = ThothRelease(dag_id=self.dag_id, release_date=release_date)
        self.workflow_folder = make_workflow_folder(self.dag_id, release_date)
        self.download_folder = make_workflow_folder(self.dag_id, release_date, SubFolder.downloaded.value)
        self.transform_folder = make_workflow_folder(self.dag_id, release_date, SubFolder.transformed.value)
        return release

    def download(self, release: ThothRelease, **kwargs) -> None:
        """Task to download the ONIX release from Thoth.

        :param release: The Thoth release instance
        """
        thoth_download_onix(
            publisher_id=self.publisher_id,
            download_folder=self.download_folder,
            format_spec=self.format_specification,
            download_filename=self.download_file_name,
        )

    def upload_downloaded(self, release: ThothRelease, **kwargs) -> None:
        """Upload the downloaded thoth onix XML to google cloud bucket

        :param release: The Thoth release instance
        :raises AirflowException: Raised if there is not exactly 1 file in the donwload folder matching the expected name
        """
        download_files = list_files(self.download_folder, self.download_file_name)
        if len(download_files) != 1:
            raise AirflowException(
                f"Unexpected number of files in download folder. Expected 1, found {len(download_files)}"
            )

        blob = blob_name_from_path(download_files[0])
        success = upload_files_to_cloud_storage(self.download_bucket, [blob], download_files)
        if not success:
            raise AirflowException("Blob could not be uploaded to cloud storage")

    def transform(self, release: ThothRelease, **kwargs) -> None:
        """Task to transform the Thoth ONIX data

        :param release: The Thoth release instance
        """
        # Run through java onix parser
        logging.info("Parsing onix feed through onix parser")
        parse_onix(self.download_folder, self.transform_folder)

        # Collapse the keywords field and save
        logging.info("Transforming onix feed - collapsing keywords")
        transformed = thoth_collapse_subjects(load_jsonl(os.path.join(self.transform_folder, "full.jsonl")))
        list_to_jsonl_gz(os.path.join(self.transform_folder, self.transform_file_name), transformed)

    def upload_transformed(self, release: ThothRelease, **kwargs) -> None:
        """Upload the downloaded thoth onix .jsonl to google cloud bucket

        :param release: The Thoth release instance
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

    def bq_load(self, release: ThothRelease, **kwargs) -> None:
        """Task to load the transformed ONIX jsonl file to BigQuery.
        The table_id is set to the file name without the extension.

        :param release: The Thoth release instance
        """
        transform_files = list_files(self.transform_folder, self.transform_file_name)
        if len(transform_files) != 1:
            raise AirflowException(
                f"Unexpected number of files in transform folder. Expected 1, found {len(transform_files)}"
            )
        # Load each transformed release
        transform_blob = blob_name_from_path(transform_files[0])
        table_id, _ = table_ids_from_path(transform_files[0])
        schema_file_path = find_schema(path=self.schema_folder, table_name="onix")
        bq_load_shard(
            schema_file_path=schema_file_path,
            project_id=self.project_id,
            transform_bucket=self.transform_bucket,
            transform_blob=transform_blob,
            dataset_id=self.dataset_id,
            data_location=self.data_location,
            table_id=table_id,
            release_date=release.release_date,
            source_format=self.source_format,
            dataset_description=self.dataset_description,
        )

    def cleanup(self, release: ThothRelease, **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release.

        :param release: The Thoth release instance
        """
        cleanup(dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=self.workflow_folder)


def thoth_download_onix(
    publisher_id: str,
    download_folder: str,
    download_filename: str = "onix.xml",
    host_name: str = DEFAULT_HOST_NAME,
    format_spec: str = "onix_3.0::oapen",
    num_retries: int = 3,
) -> None:
    """Hits the Thoth API and requests the ONIX feed for a particular publisher.
    Creates a file called onix.xml at the specified location

    :param publisher_id: The ID of the publisher. Can be found using Thoth GraphiQL API
    :param download_folder: The path of the download folder
    :param download_filename: The name of the downloaded file
    :param host_name: The Thoth host URL
    :param format_spec: The ONIX format to use. Options can be found with the /formats endpoint of the API
    :param num_retries: The number of times to retry the download, given an unsuccessful return code
    """
    url = THOTH_URL.format(host_name=host_name, format_specification=format_spec, publisher_id=publisher_id)
    logging.info(f"Downloading ONIX XML from {url}")
    response = retry_get_url(url, num_retries=num_retries)
    if response.status_code != 200:
        raise AirflowException(
            f"Request for URL {url} was unsuccessful with code: {response.status_code}\nContent response: {response.content.decode('utf-8')}"
        )
    download_path = os.path.join(download_folder, download_filename)
    with open(download_path, "wb") as f:
        f.write(response.content)


def thoth_collapse_subjects(onix: List[dict]) -> List[dict]:
    """The book product table creation requires the keywords (under Subjects.SubjectHeadingText) to occur only once
    Thoth returns all keywords as separate entires. This function finds and collapses each keyword into a semi-colon
    separated string.

    :param onix: The onix feed
    :return: The onix feed after collapsing the keywords of each row
    """
    for row in onix:
        # Create the joined keywords in this row
        keywords = []
        for subject in row["Subjects"]:
            if subject["SubjectSchemeIdentifier"] != "Keywords":
                continue
            subject_heading_text = [i for i in subject["SubjectHeadingText"] if i is not None]  # Remove Nones
            if not subject_heading_text:  # Empty list
                continue
            keywords.append("; ".join(subject_heading_text))
        keywords = "; ".join(keywords)

        # Replace one of the subrows with the new keywords string
        keywords_replaced = False
        remove_indexes = []
        for i, subject in enumerate(row["Subjects"]):
            if subject["SubjectSchemeIdentifier"] == "Keywords":
                if not keywords_replaced:
                    subject["SubjectHeadingText"] = [keywords]
                    keywords_replaced = True
                else:
                    remove_indexes.append(i)

        # Remove additional "keywords" subrows
        for i in sorted(remove_indexes, reverse=True):
            del row["Subjects"][i]

    return onix
