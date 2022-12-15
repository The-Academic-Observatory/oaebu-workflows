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

# Author: Aniek Roelofs, Keegan Smith

from __future__ import annotations

import logging
import os
import tempfile
import json
import numpy as np
from typing import List, Tuple
from xml.etree import ElementTree
from xml.dom import minidom
import requests

import pendulum
from airflow.exceptions import AirflowException
from onixcheck import validate as validate_onix
from tenacity import retry, stop_after_attempt, wait_chain, wait_fixed, retry_if_exception_type

from oaebu_workflows.api_type_ids import DatasetTypeId
from oaebu_workflows.config import schema_folder as default_schema_folder
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.url_utils import get_user_agent
from observatory.platform.utils.workflow_utils import make_sftp_connection
from observatory.platform.workflows.stream_telescope import StreamTelescope, StreamRelease
from oaebu_workflows.dag_tag import Tag

# Download job will wait 120 seconds between first 2 attempts, then 30 minutes for the following 3
DOWNLOAD_RETRY_CHAIN = wait_chain(*[wait_fixed(120) for _ in range(2)] + [wait_fixed(1800) for _ in range(3)])


class OapenMetadataRelease(StreamRelease):
    def __init__(self, dag_id: str, start_date: pendulum.DateTime, end_date: pendulum.DateTime, first_release: bool):
        """Construct a OapenMetadataRelease instance
        :param dag_id: the id of the DAG.
        :param start_date: the start_date of the release.
        :param end_date: the end_date of the release.
        """
        super().__init__(dag_id, start_date, end_date, first_release)

    @property
    def download_path(self) -> str:
        """Path to store the original oapen metadata XML file"""
        return os.path.join(self.download_folder, f"oapen_metadata_{self.end_date.format('YYYYMMDD')}.xml")

    @property
    def transform_path(self) -> str:
        """Path to store the transformed oapen ONIX file"""
        return os.path.join(self.transform_folder, f"oapen_onix_{self.end_date.format('YYYYMMDD')}.xml")

    @property
    def invalid_products_path(self) -> str:
        """Path to store the transformed oapen ONIX file"""
        return os.path.join(
            self.transform_folder, f"oapen_onix_invalid_products_{self.end_date.format('YYYYMMDD')}.xml"
        )


class OapenMetadataTelescope(StreamTelescope):
    """Oapen Metadata Telescope"""

    METADATA_URL = "https://library.oapen.org/download-export?format=onix"
    DAG_ID = "oapen_metadata"
    SFTP_UPLOAD_DIR = "/telescopes/onix/oapen_press/upload"

    def __init__(
        self,
        workflow_id: int,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
        schedule_interval: str = "@weekly",
        dataset_id: str = "oapen",
        schema_folder: str = default_schema_folder(),
        airflow_vars: List = None,
        airflow_conns: List = None,
        dataset_type_id: str = DatasetTypeId.oapen_metadata,
        sftp_upload_dir: str = SFTP_UPLOAD_DIR,
    ):
        """Construct a OapenMetadataTelescope instance.

        :param workflow_id: api workflow id.
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param schema_folder: the SQL schema path.
        :param schema_prefix: the prefix used to find the schema path
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param sftp_upload_dir: The directory on the SFTP server to upload the parsed file to
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
            airflow_conns = [AirflowConns.SFTP_SERVICE]

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            merge_partition_field="",
            schema_folder=schema_folder,
            airflow_conns=airflow_conns,
            airflow_vars=airflow_vars,
            workflow_id=workflow_id,
            dataset_type_id=dataset_type_id,
            tags=[Tag.oaebu],
        )

        self.sftp_upload_dir = sftp_upload_dir
        self.onix_product_fields_file = os.path.join(self.schema_folder, "OAPEN_ONIX_product_fields.json")
        self.onix_header_fields_file = os.path.join(self.schema_folder, "OAPEN_ONIX_header_fields.json")

        self.add_setup_task(self.check_dependencies)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.upload_to_sftp_server)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> OapenMetadataRelease:
        # Make Release instance
        start_date, end_date, first_release = self.get_release_info(**kwargs)
        release = OapenMetadataRelease(self.dag_id, start_date, end_date, first_release)
        return release

    def download(self, release: OapenMetadataRelease, **kwargs):
        """Task to download the OapenMetadataRelease release.

        :param release: an OapenMetadataRelease instance.
        """
        logging.info(f"Downloading metadata XML from url: {OapenMetadataTelescope.METADATA_URL}")
        download_oapen_metadata(release.download_path)

    def transform(self, release: OapenMetadataRelease, **kwargs):
        """Transform the oapen metadata XML file into a valid ONIX file

        :param release: an OapenMetadataRelease instance.
        """
        with open(self.onix_product_fields_file) as f:
            onix_product_fields = json.load(f)
        with open(self.onix_header_fields_file) as f:
            onix_header_fields = json.load(f)
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            oapen_metadata_parse(
                release.download_path,
                tmp_file.name,
                onix_product_fields=onix_product_fields,
                onix_header_fields=onix_header_fields,
            )
            remove_invalid_products(
                tmp_file.name, release.transform_path, invalid_products_file=release.invalid_products_path
            )
        # Assert that the new onix file is valid
        errors = validate_onix(release.transform_path)
        if errors:
            raise AirflowException("Errors found in processed OAPEN ONIX file. Cannot proceed without valid ONIX.")

    def upload_to_sftp_server(self, release: OapenMetadataRelease, **kwargs):
        """Uploads the transformed ONIX file to the SFTP server

        :param release: an OapenMetadataRelease instance.
        """
        upload_path = os.path.join(self.sftp_upload_dir, os.path.basename(release.transform_path))
        logging.info(f"Uploading file '{release.transform_path}' to SFTP server path '{upload_path}'")
        with make_sftp_connection() as sftp_con:
            if not sftp_con.exists(self.sftp_upload_dir):
                raise AirflowException(f"SFTP server directory: {self.sftp_upload_dir} does not exist")
            sftp_con.put(release.transform_path, upload_path)


@retry(
    stop=stop_after_attempt(5),
    wait=DOWNLOAD_RETRY_CHAIN,
    retry=retry_if_exception_type((ElementTree.ParseError, ConnectionError)),
    reraise=True,
)
def download_oapen_metadata(download_path: str, metadata_url: str = OapenMetadataTelescope.METADATA_URL) -> None:
    """Downloads the OAPEN metadata XML file
    OAPEN's downloader can give an incomplete file if the metadata is partially generated.
    In this scenario, we should wait until the metadata generator has finished.
    Otherwise, an attempt to parse the data will result in an XML ParseError.
    OAPEN metadata generation can take over an hour,

    :param download_path: filepath to store te downloaded file
    :param metadata_url: the url to query for the metadata
    :raises ConnectionError: raised if the response from the metadata server does not have code 200
    """
    headers = {"User-Agent": f"{get_user_agent(package_name='oaebu_workflows')}"}
    response = requests.get(metadata_url, headers=headers)
    if response.status_code != 200:
        raise ConnectionError(
            f"Expected status code 200 from url {metadata_url}, instead got response: {response.text}"
        )
    with open(download_path, "w") as f:
        f.write(response.content.decode("utf-8"))
    logging.info(f"Successfully downloadeded XML to {download_path}")

    # Attempt to parse the XML, will raise an ElementTree.ParseError exception if it's invalid
    ElementTree.parse(download_path)
    logging.info("XML file is valid")


def oapen_metadata_parse(input_xml: str, output_xml: str, onix_product_fields: dict, onix_header_fields: dict) -> None:
    """For parsing OAPEN's metadata feed, keeping only the fields we require

    :param input_xml: The file path of the input metadata xml file.
    :param output_onix: The file path to use when writing the output onix xml
    :param onix_product_fields: The allowed fields and their structure. Empty list indicates data inclusion.

    onix_product_fields example:
    {"RecordReference": [],
    "Edition": {
        "EditionNumber": []}}
    Will search for the 'RecordReference' tag and fill its data. Then will search for the 'Edition" tag and look
    inside it for 'EditionNumber', if it finds this tag, it will fill its data

    :param input_xml: The metadata xml downloaded from OAPEN for parsing (filepath)
    :param output_xml: The filepath to output
    :param onix_product_fields: The product fields to retain as described in the above example
    :param onix_header_fields: The header fields to retain
    """

    # Load the ONIX XML file
    tree = ElementTree.parse(input_xml)

    # Create the header
    root = ElementTree.Element("ONIXMessage", {"xmlns": "http://ns.editeur.org/onix/3.0/reference", "release": "3.0"})
    root_element = tree.getroot()[0]
    header_child = ElementTree.Element("Header")
    process_xml_element(root_element, onix_header_fields, header_child)
    root.append(header_child)

    # Iterate through every element in the root, keeping those defined in the onix_product_fields
    for onix_elem in tree.getroot():
        if onix_elem.tag.endswith("Product"):
            child = ElementTree.Element("Product")
            process_xml_element(onix_elem, onix_product_fields, child)
            root.append(child)

    # Format and write trimmed metadta XML to a temporary file and look for remaining errors
    with open(output_xml, "wb") as f:
        # TODO: for python>=3.9, format using ElementTree.indent(tree, "   ")
        # then change write command to: tree.write(f, xml_declaration=True)
        f.write(minidom.parseString(ElementTree.tostring(root)).toprettyxml(indent="    ", encoding="UTF-8"))


def remove_invalid_products(input_xml: str, output_xml: str, invalid_products_file: str = None) -> None:
    """Attempts to validate the input xml as an ONIX file. Will remove any products that contain errors.

    :param input_xml: The filepath of the xml file to validate
    :param output_xml: The output filepath
    :param invalid_prouducts_file: The filepath to write the invalid products to. Ignored if unsupplied.
    """
    errors = validate_onix(input_xml)
    error_lines = [int(e.location.split(":")[-2]) for e in errors]  # Get the line numbers of the errors
    if error_lines:
        logging.info(
            f"OAPEN metadata feed has been trimmed and {len(error_lines)} errors remain. These products will be removed"
        )

    # Ingest the file into a list and find onix products with errors
    with open(input_xml, "r") as f:
        onix_file_lines = f.readlines()
    invalid_product_line_ranges = []
    for line_number in sorted(error_lines):
        invalid_product_line_ranges.append(find_onix_product(onix_file_lines, line_number))
    # Remove any duplicates in the product list
    invalid_product_line_ranges = list(set(tuple(i) for i in invalid_product_line_ranges))

    # Remove products with errors in them
    num_lines = len(onix_file_lines)
    logging.info(f"Removing {len(invalid_product_line_ranges)} invalid products from metadata feed")
    valid_line_numbers = list(np.linspace(1, num_lines, num_lines, dtype=int, endpoint=True))
    invalid_line_numbers = []
    for line_remove_start, line_remove_end in sorted(invalid_product_line_ranges, reverse=True):
        invalid_line_numbers.extend(valid_line_numbers[line_remove_start : line_remove_end + 1])
        del valid_line_numbers[line_remove_start : line_remove_end + 1]

    # Create the clean XML ONIX file
    clean_xml = [onix_file_lines[i - 1] for i in valid_line_numbers]
    with open(output_xml, "w") as f:
        for l in clean_xml:
            f.write(l)
        logging.info(f"XML written to {output_xml}")

    # Create the invalid product file
    if invalid_products_file:
        invalid_products = [onix_file_lines[i - 1] for i in invalid_line_numbers]
        with open(invalid_products_file, "w") as f:
            # Write the XML root
            f.write(onix_file_lines[0])
            f.write(onix_file_lines[1])
            for l in invalid_products:
                f.write(l)
            f.write(onix_file_lines[-1])  # Close the XML root
        logging.info(f"Invalid products written to {invalid_products_file}")


def find_onix_product(all_lines: list, line_number: int) -> Tuple[int, int]:
    """Finds the range of lines encompassing a <Product> tag, given a line_number that is contained in the product

    :param all_lines: All lines in the onix file
    :param line_number: The line number associated with the product
    :raises AirflowException: Raised if the return would encompass a negative index, indicating the input line was not in a product
    :return: A two-tuple of the start and end line numbers of the product
    """
    # Go up until we find <Product>
    negative_offset = 0
    line = all_lines[line_number - 1]  # -1 accounts for python zero indexing vs line number indexing
    while not "<Product>" in line:
        negative_offset += 1
        line = all_lines[line_number - negative_offset]

    # Go up until we find </Product>
    positive_offset = -1
    line = all_lines[line_number - 1]
    while not "</Product>" in line:
        positive_offset += 1
        line = all_lines[line_number + positive_offset]

    if (line_number - negative_offset) < 0:
        raise AirflowException(f"Product out of bounds for given line number {line_number}")

    return line_number - negative_offset, line_number + positive_offset


def process_xml_element(xml_elem: ElementTree.Element, viable_fields: dict, xml_parent: ElementTree.Element) -> None:
    """A recursive function that processes an XML by iterating through the tree and only keeping the elements if they
    are described as expected in the viable fields.
    For example, say we have an xml with this structure:
    <thing>
        <subthing>1
            <subsubthing>1</subsubthing>
        </subthing>
        <unimportantsubthing>5</unimportantsubthing>
    </thing>

    and the viable fields looks like this:
    {"thing":{
        "subthing":{
            "subsubthing":[]
            }
        }
    }
    Our resulting xml would look like this:
    <thing>
        <subthing>1
            <subsubthing>1</subsubthing>
        </subthing>
    </thing>

    Example use case:
    tree = ElementTree.parse(my_xml_file) # Load my xml file
    root = ElementTree.Element(tree.getroot().tag) # Create a new xml tree
    process_xml_element(tree.getroot(), viable_fields, root) # process a new tree
    with open('file.xml', 'w') as f: # write my tree to a file
        f.write(ElementTree.tostring(root)))

    :param xml_elem: The xml element to process
    :param viable_fields: A dictionary describing the fileds to retain
    :param xml_parent: The parent xml element to write to
    """
    if not viable_fields:  # Fields is an empty list
        xml_parent.text = xml_elem.text
        return

    for e in xml_elem:
        for field_key in viable_fields.keys():
            if e.tag.endswith(field_key):
                child = ElementTree.SubElement(xml_parent, field_key)
                process_xml_element(e, viable_fields[field_key], child)
