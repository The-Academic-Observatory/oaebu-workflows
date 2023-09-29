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

from __future__ import annotations

import logging
import os
import shutil
import json
import requests
import xmltodict
from xml.parsers.expat import ExpatError
from tempfile import TemporaryDirectory
from dataclasses import dataclass

import pendulum
from airflow.exceptions import AirflowException
from onixcheck import validate as validate_onix
from google.cloud.bigquery import SourceFormat
from tenacity import (
    retry,
    stop_after_attempt,
    wait_chain,
    wait_fixed,
    retry_if_exception_type,
)

from oaebu_workflows.config import schema_folder as default_schema_folder
from oaebu_workflows.onix import (
    onix_parser_download,
    onix_parser_execute,
    onix_create_personname_fields,
)
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.api import make_observatory_api
from observatory.platform.airflow import AirflowConns
from observatory.platform.files import save_jsonl_gz, load_jsonl
from observatory.platform.utils.url_utils import get_user_agent
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.gcs import (
    gcs_upload_files,
    gcs_blob_uri,
    gcs_blob_name_from_path,
)
from observatory.platform.bigquery import (
    bq_load_table,
    bq_sharded_table_id,
    bq_find_schema,
    bq_create_dataset,
)
from observatory.platform.workflows.workflow import (
    SnapshotRelease,
    Workflow,
    make_snapshot_date,
    cleanup,
    set_task_state,
    check_workflow_inputs,
)


# Download job will wait 120 seconds between first 2 attempts, then 30 minutes for the following 3
DOWNLOAD_RETRY_CHAIN = wait_chain(*[wait_fixed(120) for _ in range(2)] + [wait_fixed(1800) for _ in range(3)])


class OapenMetadataRelease(SnapshotRelease):
    def __init__(self, dag_id: str, run_id: str, snapshot_date: pendulum.DateTime):
        """Construct a OapenMetadataRelease instance

        :param dag_id: The ID of the DAG
        :param run_id: The Airflow run ID
        :param snapshot_date: The date of the snapshot_date/release
        """
        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.download_path = os.path.join(self.download_folder, f"metadata_{snapshot_date.format('YYYYMMDD')}.xml")
        # Transform step outputs
        self.filtered_metadata = os.path.join(self.transform_folder, "filtered_metadata.xml")  # After schema parse
        self.validated_onix = os.path.join(self.transform_folder, "validated_onix.xml")  # After removal of errors
        self.invalid_products_path = os.path.join(self.transform_folder, "onix_invalid_products.xml")  # Errors
        self.parsed_onix = os.path.join(self.transform_folder, "full.jsonl")  # The result of the java parser
        self.transform_path = os.path.join(self.transform_folder, "onix.jsonl.gz")  # Final onix file


class OapenMetadataTelescope(Workflow):
    """Oapen Metadata Telescope"""

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        metadata_uri: str,
        bq_dataset_id: str = "onix",
        bq_table_name: str = "onix",
        bq_dataset_description: str = "OAPEN Metadata converted to ONIX",
        bq_table_description: str = None,
        api_dataset_id: str = "oapen",
        schema_folder: str = default_schema_folder(),
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        catchup: bool = False,
        start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
        schedule: str = "@weekly",
    ):
        """Construct a OapenMetadataTelescope instance.
        :param dag_id: The ID of the DAG
        :param cloud_workspace: The CloudWorkspace object for this DAG
        :param metadata_uri: The URI of the metadata XML file
        :param bq_dataset_id: The BigQuery dataset ID
        :param bq_table_name: The BigQuery table name
        :param bq_dataset_description: Description for the BigQuery dataset
        :param bq_table_description: Description for the biguery table
        :param api_dataset_id: The ID to store the dataset release in the API
        :param schema_folder: The path to the SQL schema folder
        :param observatory_api_conn_id: Airflow connection ID for the overvatory API
        :param catchup: Whether to catchup the DAG or not
        :param start_date: The start date of the DAG
        :param schedule: The schedule interval of the DAG
        """
        super().__init__(
            dag_id,
            start_date,
            schedule,
            airflow_conns=[observatory_api_conn_id],
            catchup=catchup,
            tags=["oaebu"],
        )
        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.metadata_uri = metadata_uri
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_name = bq_table_name
        self.bq_dataset_description = bq_dataset_description
        self.bq_table_description = bq_table_description
        self.api_dataset_id = api_dataset_id
        self.schema_folder = schema_folder
        self.observatory_api_conn_id = observatory_api_conn_id

        # Fixture file paths
        self.oapen_schema = os.path.join(self.schema_folder, "oapen_metadata_fields.json")

        check_workflow_inputs(self)

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.add_new_dataset_releases)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> OapenMetadataRelease:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for the keyword arguments that can be passed
        :return: The Oapen metadata release instance"""
        snapshot_date = make_snapshot_date(**kwargs)
        release = OapenMetadataRelease(self.dag_id, kwargs["run_id"], snapshot_date)
        return release

    def download(self, release: OapenMetadataRelease, **kwargs) -> None:
        """Task to download the OapenMetadataRelease release.

        :param kwargs: the context passed from the PythonOperator.
        :param release: an OapenMetadataRelease instance.
        """
        logging.info(f"Downloading metadata XML from url: {self.metadata_uri}")
        download_metadata(self.metadata_uri, release.download_path)

    def upload_downloaded(self, release: OapenMetadataRelease, **kwargs) -> None:
        """Task to upload the downloaded OAPEN metadata"""
        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.download_bucket,
            file_paths=[release.download_path],
        )
        set_task_state(success, kwargs["ti"].task_id, release=release)

    def transform(self, release: OapenMetadataRelease, **kwargs) -> None:
        """Transform the oapen metadata XML file into a valid ONIX file
        This involves several steps
        1) Parse the XML metadata to keep our desired fields
        2) Remove products containing errors
        3) Parse the validated ONIX file through the java parser to return .jsonl format
        4) Add the contributor.personname field
        """
        # Parse the downloaded metadata through the schema to extract relevant fields only
        with open(self.oapen_schema) as f:
            oapen_schema = json.load(f)
        with open(release.download_path, "rb") as f:
            metadata = xmltodict.parse(f)
        metadata = filter_through_schema(metadata, oapen_schema)
        with open(release.filtered_metadata, "w") as f:
            xmltodict.unparse(metadata, output=f, pretty=True)

        # Remove any products with errors in them
        remove_invalid_products(
            release.filtered_metadata,
            release.validated_onix,
            invalid_products_file=release.invalid_products_path,
        )

        # Assert that the new onix file is valid
        if validate_onix(release.validated_onix):
            raise AirflowException("Errors found in processed ONIX file. Cannot proceed without valid ONIX.")

        # Parse the onix file through the Java parser - should result in release.parsed_onix file
        success, parser_path = onix_parser_download()
        set_task_state(success, task_id=kwargs["ti"].task_id, release=release)
        with TemporaryDirectory() as tmp_dir:
            shutil.copy(release.validated_onix, os.path.join(tmp_dir, "input.xml"))
            success = onix_parser_execute(parser_path, input_dir=tmp_dir, output_dir=release.transform_folder)
            set_task_state(success, task_id=kwargs["ti"].task_id, release=release)

        # Add the Contributors.PersonName field
        onix = onix_create_personname_fields(load_jsonl(release.parsed_onix))
        save_jsonl_gz(release.transform_path, onix)

    def upload_transformed(self, release: OapenMetadataRelease, **kwargs) -> None:
        """Task to upload the transformed OAPEN metadata"""
        transform_files = [
            release.transform_path,
            release.invalid_products_path,
            release.parsed_onix,
        ]
        for expected_file in transform_files:
            if not os.path.exists(expected_file):
                raise FileNotFoundError(f"Expected file not present: {expected_file}")
        success = gcs_upload_files(
            bucket_name=self.cloud_workspace.transform_bucket,
            file_paths=transform_files,
        )
        set_task_state(success, kwargs["ti"].task_id, release=release)

    def bq_load(self, release: OapenMetadataRelease, **kwargs) -> None:
        """Load the transformed ONIX file into bigquery"""
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_dataset_id,
            location=self.cloud_workspace.data_location,
            description=self.bq_dataset_description,
        )
        schema_file_path = bq_find_schema(path=self.schema_folder, table_name=self.bq_table_name)
        uri = gcs_blob_uri(
            self.cloud_workspace.transform_bucket,
            gcs_blob_name_from_path(release.transform_path),
        )
        table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_dataset_id,
            self.bq_table_name,
            release.snapshot_date,
        )
        state = bq_load_table(
            uri=uri,
            table_id=table_id,
            schema_file_path=schema_file_path,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            table_description=self.bq_table_description,
        )
        set_task_state(state, kwargs["ti"].task_id, release=release)

    def add_new_dataset_releases(self, release: OapenMetadataRelease, **kwargs) -> None:
        """Adds release information to API."""
        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.api_dataset_id,
            dag_run_id=release.run_id,
            snapshot_date=release.snapshot_date,
            data_interval_start=kwargs["data_interval_start"],
            data_interval_end=kwargs["data_interval_end"],
        )
        api.post_dataset_release(dataset_release)

    def cleanup(self, release: OapenMetadataRelease, **kwargs) -> None:
        """Delete all files, folders and XComs associated with this release."""
        cleanup(
            dag_id=self.dag_id,
            execution_date=kwargs["execution_date"],
            workflow_folder=release.workflow_folder,
        )


@retry(
    stop=stop_after_attempt(5),
    wait=DOWNLOAD_RETRY_CHAIN,
    retry=retry_if_exception_type((ExpatError, ConnectionError, AirflowException)),
    reraise=True,
)
def download_metadata(uri: str, download_path: str) -> None:
    """Downloads the OAPEN metadata XML file
    OAPEN's downloader can give an incomplete file if the metadata is partially generated.
    In this scenario, we should wait until the metadata generator has finished.
    Otherwise, an attempt to parse the data will result in an XML ParseError.
    Another scenario is that OAPEN returns only a header in the XML. We want this to also raise an error.
    OAPEN metadata generation can take over an hour

    :param uri: the url to query for the metadata
    :param download_path: filepath to store te downloaded file
    :raises ConnectionError: raised if the response from the metadata server does not have code 200
    :raises AirflowException: raised if the response does not contain any Product fields
    """
    headers = {"User-Agent": f"{get_user_agent(package_name='oaebu_workflows')}"}
    response = requests.get(uri, headers=headers)
    if response.status_code != 200:
        raise ConnectionError(f"Expected status code 200 from url {uri}, instead got response: {response.text}")
    with open(download_path, "w") as f:
        f.write(response.content.decode("utf-8"))
    logging.info(f"Successfully downloadeded XML to {download_path}")

    # Attempt to parse the XML, will raise an ExpatError if it's invalid
    with open(download_path, "rb") as f:
        xmltodict.parse(f)
    logging.info("XML file is valid")

    # Check that more than just the header is returned
    if "<Product>" not in response.content.decode("utf-8"):
        raise AirflowException("No products found in metadata")


def filter_through_schema(input: dict, schema: dict):
    """
    This function recursively traverses the input dictionary and compares it to the provided schema.
    It retains only the fields and values that exist in the schema structure, and discards
    any fields that do not match the schema.

    # Example usage with a dictionary and schema:
        input_dict = {
            "name": "John",
            "age": 30,
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "zip": "10001"
            }
        }
        schema = {
            "name": null,
            "age": null,
            "address": {
                "street": null,
                "city": null
            }
        }
        filtered_dict = filter_dict_by_schema(input_dict, schema)
        filtered_dict will be:
        {
            "name": "John",
            "age": 30,
            "address": {
                "street": "123 Main St",
                "city": "New York"
            }
        }

    :param input: The dictionary to filter
    :param schema: The schema describing the desired structure of the dictionary
    """
    if isinstance(input, dict) and isinstance(schema, dict):
        return {key: filter_through_schema(value, schema.get(key)) for key, value in input.items() if key in schema}
    elif isinstance(input, list) and isinstance(schema, list):
        return [filter_through_schema(item, schema[0]) for item in input]
    else:
        return input


def remove_invalid_products(input_xml: str, output_xml: str, invalid_products_file: str = None) -> None:
    """Attempts to validate the input xml as an ONIX file. Will remove any products that contain errors.

    :param input_xml: The filepath of the xml file to validate
    :param output_xml: The output filepath
    :param invalid_products_file: The filepath to write the invalid products to. Ignored if unsupplied.
    """
    # Get the line numbers of any errors
    errors = validate_onix(input_xml)
    error_lines = [int(e.location.split(":")[-2]) - 1 for e in errors]

    # Ingest the file into a list and find onix products with errors
    with open(input_xml, "r") as f:
        metadata = f.readlines()

    invalid_products = [find_onix_product(metadata, line_number) for line_number in error_lines]
    invalid_references = set([product.record_reference for product in invalid_products])
    logging.info(
        f"Metadata feed has been trimmed and {len(invalid_references)} errors remain. Products with errors will be removed"
    )

    # Parse the xml to dictionary
    with open(input_xml, "rb") as f:
        metadata = xmltodict.parse(f)

    # Remove products matching the record references
    metadata["ONIXMessage"]["Product"] = [
        p for p in metadata["ONIXMessage"]["Product"] if p["RecordReference"] not in invalid_references
    ]

    # Create the clean XML file
    with open(output_xml, "w") as f:
        xmltodict.unparse(metadata, output=f, pretty=True)

    # Create the invalid product file
    if invalid_products_file:
        with open(invalid_products_file, "w") as f:
            metadata["ONIXMessage"]["Product"] = [p.product for p in invalid_products]
            xmltodict.unparse(metadata, output=f, pretty=True)
        logging.info(f"Invalid products written to {invalid_products_file}")


def find_onix_product(all_lines: list, line_index: int) -> OnixProduct:
    """Finds the range of lines encompassing a <Product> tag, given a line_number that is contained in the product

    :param all_lines: All lines in the onix file
    :param line_number: The line number associated with the product
    :return: A two-tuple of the start and end line numbers of the product
    :raises ValueError: Raised if the return would encompass a negative index, indicating the input line was not in a product
    """
    if line_index < 0 or line_index >= len(all_lines):
        raise IndexError(f"Supplied line index {line_index} is not within the length of the file: {len(all_lines)}")

    # Go up until we find <Product>
    begin_index = line_index
    begin_content = all_lines[begin_index]
    while not "<Product>" in begin_content:
        begin_index -= 1
        if begin_index < 0:
            raise ValueError(f"Product not found surrounding line {line_index}")
        begin_content = all_lines[begin_index]

    # Go up until we find </Product>
    finish_index = line_index
    finish_content = all_lines[finish_index]
    while not "</Product>" in finish_content:
        finish_index += 1
        if finish_index >= len(all_lines):
            raise ValueError(f"Product not found surrounding line {line_index}")
        finish_content = all_lines[finish_index]

    product_str = "".join(all_lines[begin_index : finish_index + 1])
    product = xmltodict.parse(product_str)["Product"]
    if not product:
        raise ValueError(f"Product field is empty for product at line {begin_index}")
    record_reference = product.get("RecordReference")
    if not record_reference:
        raise KeyError(f"No RecordReference for product: {product_str}")

    return OnixProduct(product, record_reference)


@dataclass
class OnixProduct:
    """Represents a single ONIX product and its identifying reference for simplicity"""

    product: dict
    record_reference: str
