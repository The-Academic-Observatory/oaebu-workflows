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

# Author: Aniek Roelofs, Tuan Chien, Keegan Smith

import os
from unittest.mock import MagicMock
from tempfile import NamedTemporaryFile
from xml.etree import ElementTree
import json

import pendulum
import vcr
from airflow.exceptions import AirflowException

from oaebu_workflows.api_type_ids import DatasetTypeId, WorkflowTypeId
from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.oapen_metadata_telescope import (
    OapenMetadataRelease,
    OapenMetadataTelescope,
    download_oapen_metadata,
    oapen_metadata_parse,
    remove_invalid_products,
    find_onix_product,
    process_xml_element,
)
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    SftpServer,
    module_file_path,
    find_free_port,
)
from observatory.platform.utils.workflow_utils import (
    blob_name,
)
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.table_type import TableType
from observatory.platform.utils.airflow_utils import AirflowConns
from airflow.models import Connection


class TestOapenMetadataTelescope(ObservatoryTestCase):
    """Tests for the Oapen Metadata Telescope DAG"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        # Telescope run settings
        self.execution_date = pendulum.datetime(year=2021, month=2, day=1)

        # API environment
        self.host = "localhost"
        self.api_port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.api_port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.api_port)
        self.org_name = "Curtin Press"

        # SFTP server connection
        self.sftp_port = find_free_port()
        self.sftp_server = SftpServer(host=self.host, port=self.sftp_port)

        # VCR Cassettes
        self.valid_download_cassette = test_fixtures_folder("oapen_metadata", "oapen_metadata_cassette_valid.yaml")
        self.invalid_download_cassette = test_fixtures_folder("oapen_metadata", "oapen_metadata_cassette_invalid.yaml")
        self.bad_response_cassette = test_fixtures_folder("oapen_metadata", "oapen_metadata_cassette_bad_response.yaml")
        self.empty_download_cassette = test_fixtures_folder("oapen_metadata", "oapen_metadata_cassette_empty.yaml")

        # XML files for testing
        self.valid_download_xml = test_fixtures_folder("oapen_metadata", "oapen_metadata_download_valid.xml")
        self.invalid_download_xml = test_fixtures_folder("oapen_metadata", "oapen_metadata_download_invalid.xml")
        self.empty_xml = test_fixtures_folder("oapen_metadata", "oapen_metadata_download_empty.xml")
        self.valid_parsed_xml = test_fixtures_folder("oapen_metadata", "oapen_metadata_parsed_valid.xml")
        self.invalid_parsed_xml = test_fixtures_folder("oapen_metadata", "oapen_metadata_parsed_invalid.xml")
        self.valid_processed_xml = test_fixtures_folder("oapen_metadata", "oapen_metadata_processed_valid.xml")
        self.processing_test_before = test_fixtures_folder("oapen_metadata", "processing_test_before.xml")
        self.processing_test_after = test_fixtures_folder("oapen_metadata", "processing_test_after.xml")
        self.invalid_products_xml = test_fixtures_folder("oapen_metadata", "oapen_metadata_invalid_products.xml")
        # Valid fields json for the xml processing test
        self.processing_test_fields = test_fixtures_folder("oapen_metadata", "processing_test_valid_fields.json")

    def setup_api(self):
        name = "Oapen Metadata"
        workflow_type = WorkflowType(name=name, type_id=WorkflowTypeId.oapen_metadata)
        self.api.put_workflow_type(workflow_type)

        organisation = Organisation(
            name=self.org_name,
            project_id="project",
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
        )
        self.api.put_organisation(organisation)

        wf = Workflow(
            name=name,
            workflow_type=WorkflowType(id=1),
            organisation=Organisation(id=1),
            extra={},
        )
        self.api.put_workflow(wf)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            type_id=DatasetTypeId.oapen_metadata,
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Oapen Metadata Dataset",
            address="project.dataset.table",
            service="bigquery",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        self.api.put_dataset(dataset)

    def setup_connections(self, env: ObservatoryEnvironment):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.api_port}")
        env.add_connection(conn)
        # Add SFTP server connection
        conn = Connection(conn_id=AirflowConns.SFTP_SERVICE, uri=f"http://:password@{self.host}:{self.sftp_port}")
        env.add_connection(conn)

    def test_dag_structure(self):
        """Test that the Oapen Metadata DAG has the correct structure"""
        dag = OapenMetadataTelescope(workflow_id=1).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["upload_to_sftp_server"],
                "upload_to_sftp_server": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the OapenMetadata DAG can be loaded from a DAG bag"""
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.api_port)

        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "oapen_metadata_telescope.py")
            self.assert_dag_load(OapenMetadataTelescope.DAG_ID, dag_file)

    def test_workflow(self):
        """Test workflow task execution."""

        upload_dir_name = "upload"
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.api_port)
        self.dataset_id = env.add_dataset()
        wf = OapenMetadataTelescope(dataset_id=self.dataset_id, workflow_id=1, sftp_upload_dir=upload_dir_name)
        dag = wf.make_dag()

        with env.create():
            self.setup_connections(env)
            self.setup_api()

            with self.sftp_server.create() as sftp_root:
                sftp_dir_local_path = os.path.join(sftp_root, upload_dir_name)
                os.makedirs(sftp_dir_local_path)

                # first run
                with env.create_dag_run(dag, self.execution_date):
                    # Test that all dependencies are specified: no error should be thrown
                    env.run_task(wf.check_dependencies.__name__)

                    start_date, end_date, first_release = wf.get_release_info(
                        dag=dag,
                        data_interval_end=pendulum.datetime(2021, 1, 31),
                    )

                    # Use release info for other tasks
                    release = OapenMetadataRelease(wf.dag_id, start_date, end_date, first_release)

                    # Download task
                    with vcr.VCR().use_cassette(self.valid_download_cassette, record_mode="None"):
                        env.run_task(wf.download.__name__)

                    # Upload download task
                    env.run_task(wf.upload_downloaded.__name__)

                    # Transform task
                    env.run_task(wf.transform.__name__)

                    # Upload transform task
                    env.run_task(wf.upload_transformed.__name__)

                    # Upload to sftp server task
                    env.run_task(wf.upload_to_sftp_server.__name__)

                    # Test that these functions have worked as intended

                    # Test download task
                    self.assertEqual(1, len(os.listdir(os.path.dirname(release.download_path))))
                    self.assert_file_integrity(release.download_path, "6df963cd448fe3ec8acb76bf49b34928", "md5")

                    # Test that download file uploaded to BQ
                    self.assert_blob_integrity(
                        env.download_bucket, blob_name(release.download_path), release.download_path
                    )

                    # Test transform task
                    self.assertEqual(2, len(os.listdir(os.path.dirname(release.transform_path))))
                    self.assert_file_integrity(
                        release.transform_path, "181b9eefb66a00acb9066456e73dee82", "md5"
                    )  # The transformed XML file
                    self.assert_file_integrity(
                        release.invalid_products_path, "297173aa0a09aa1dc538eadfc48285c5", "md5"
                    )  # Invalid products file

                    # Test that transformed files uploaded to BQ
                    self.assert_blob_integrity(
                        env.transform_bucket, blob_name(release.transform_path), release.transform_path
                    )
                    self.assert_blob_integrity(
                        env.transform_bucket, blob_name(release.invalid_products_path), release.invalid_products_path
                    )

                    # Test that the transformed XML file was uploaded to SFTP server
                    self.assertEqual(1, len(os.listdir(sftp_dir_local_path)))
                    sftp_filepath = os.path.join(sftp_dir_local_path, os.path.basename(release.transform_path))
                    self.assert_file_integrity(sftp_filepath, "181b9eefb66a00acb9066456e73dee82", "md5")

                    # Test that all data deleted
                    download_folder, extract_folder, transform_folder = (
                        release.download_folder,
                        release.extract_folder,
                        release.transform_folder,
                    )
                    env.run_task(wf.cleanup.__name__)
                    self.assert_cleanup(download_folder, extract_folder, transform_folder)

    def test_airflow_vars(self):
        """Cover case when airflow_vars is given."""

        wf = OapenMetadataTelescope(workflow_id=1, airflow_vars=[AirflowVars.DOWNLOAD_BUCKET])
        self.assertEqual(
            set(wf.airflow_vars),
            {
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
                AirflowVars.DATA_LOCATION,
                AirflowVars.PROJECT_ID,
            },
        )

    def test_download_oapen_metadata(self):
        """Tests the function used to download the oapen metadata xml file"""
        download_file = NamedTemporaryFile(delete=False)

        # Remove the wait time before retries for testing
        download_oapen_metadata.retry.sleep = MagicMock()

        # For a valid XML
        with vcr.VCR().use_cassette(self.valid_download_cassette, record_mode="none", allow_playback_repeats=True):
            download_oapen_metadata(download_file.name)
        with open(download_file.name, "r") as f:
            downloaded_xml = f.readlines()
        with open(self.valid_download_xml, "r") as f:
            assertion_xml = f.readlines()
        assert len(downloaded_xml) == len(
            assertion_xml
        ), f"Downloaded 'valid' XML has {len(downloaded_xml)} lines. Expected {len(assertion_xml)}"
        assert downloaded_xml == assertion_xml, "Downloaded 'valid' XML is not equal to the expected XML"

        # For invalid XML
        with vcr.VCR().use_cassette(self.invalid_download_cassette, record_mode="none", allow_playback_repeats=True):
            self.assertRaises(ElementTree.ParseError, download_oapen_metadata, download_file.name)

        # For empty XML
        with vcr.VCR().use_cassette(self.empty_download_cassette, record_mode="none", allow_playback_repeats=True):
            self.assertRaises(ElementTree.ParseError, download_oapen_metadata, download_file.name)

        # For non-200 response code
        with vcr.VCR().use_cassette(self.bad_response_cassette, record_mode="none", allow_playback_repeats=True):
            self.assertRaises(ConnectionError, download_oapen_metadata, download_file.name)

    def test_oapen_metadata_parse(self):
        """Tests the function used to parse the relevant fields into an onix file"""
        with open(OapenMetadataTelescope(workflow_id=1).onix_product_fields_file) as f:
            onix_product_fields = json.load(f)
        with open(OapenMetadataTelescope(workflow_id=1).onix_header_fields_file) as f:
            onix_header_fields = json.load(f)
        parsed_file = NamedTemporaryFile(delete=False)

        # For a valid XML
        oapen_metadata_parse(
            self.valid_download_xml,
            parsed_file.name,
            onix_product_fields=onix_product_fields,
            onix_header_fields=onix_header_fields,
        )
        with open(parsed_file.name, "r") as f:
            parsed_xml = f.readlines()
        with open(self.valid_parsed_xml, "r") as f:
            assertion_xml = f.readlines()
        assert len(parsed_xml) == len(
            assertion_xml
        ), f"Downloaded 'valid' XML has {len(parsed_xml)} lines. Expected {len(assertion_xml)}"
        assert parsed_xml == assertion_xml, "Parsed 'valid' XML is not equal to the expected XML"

        # For invalid XML
        self.assertRaises(
            ElementTree.ParseError,
            oapen_metadata_parse,
            self.invalid_download_xml,
            parsed_file.name,
            onix_product_fields=onix_product_fields,
            onix_header_fields=onix_header_fields,
        )

        # For empty XML
        self.assertRaises(
            ElementTree.ParseError,
            oapen_metadata_parse,
            self.empty_xml,
            parsed_file.name,
            onix_product_fields=onix_product_fields,
            onix_header_fields=onix_header_fields,
        )

    def test_remove_invalid_products(self):
        """Tests the function used to remove invalid products from an xml file"""
        processed_file = NamedTemporaryFile(delete=False)
        invalid_products_file = NamedTemporaryFile(delete=False)

        # For a valid XML
        remove_invalid_products(
            self.valid_parsed_xml, processed_file.name, invalid_products_file=invalid_products_file.name
        )
        with open(processed_file.name) as f:
            processed_xml = f.readlines()
        with open(self.valid_processed_xml) as f:
            assertion_xml = f.readlines()
        assert len(processed_xml) == len(
            assertion_xml
        ), f"Downloaded 'valid' XML has {len(processed_xml)} lines. Expected {len(assertion_xml)}"
        assert processed_xml == assertion_xml, "Processed 'valid' XML is not equal to the expected XML"
        # Check the invalid products xml
        with open(invalid_products_file.name) as f:
            invalid_products_xml = f.readlines()
        with open(self.invalid_products_xml) as f:
            assertion_xml = f.readlines()
        assert len(invalid_products_xml) == len(
            assertion_xml
        ), f"Downloaded 'valid' XML has {len(invalid_products_xml)} lines. Expected {len(assertion_xml)}"
        assert (
            invalid_products_xml == assertion_xml
        ), "Generated 'invalid products' XML is not equal to the expected XML"

        # For an invalid XML
        self.assertRaises(AttributeError, remove_invalid_products, self.invalid_parsed_xml, processed_file.name)

        # For an empty XML
        self.assertRaises(AttributeError, remove_invalid_products, self.invalid_parsed_xml, processed_file.name)

    def test_find_onix_product(self):
        """Tests the function that determines the line range of a product that encompasses a given line"""
        with open(self.valid_parsed_xml) as f:
            all_lines = f.readlines()

        # Make assertions [(line_number, (expected_output_range))]
        assertions = [(150, (9, 165)), (170, (166, 224)), (225, (166, 224)), (226, (226, 289))]
        for line, product_range in assertions:
            output_range = find_onix_product(all_lines, line)
            assert (
                output_range == product_range
            ), f"Product range not as expected for inpiut line {line}. Expected {product_range}, got {output_range}"

        self.assertRaises(AirflowException, find_onix_product, all_lines, 1)
        self.assertRaises(IndexError, find_onix_product, all_lines, int(1e10))

    def test_process_xml_element(self):
        """Tests the function that removes undesired fields from an xml recusrively"""
        with open(self.processing_test_fields) as f:
            viable_fields = json.load(f)

        # Process the input xml
        tree = ElementTree.parse(self.processing_test_before)
        test_xml = ElementTree.Element(tree.getroot().tag)
        process_xml_element(tree.getroot(), viable_fields, test_xml)

        # Canonicalize the expected and test xmls and compare them
        expected_xml = ElementTree.canonicalize(from_file=self.processing_test_after, strip_text=True)
        test_xml = ElementTree.canonicalize(ElementTree.tostring(test_xml), strip_text=True)
        assert (
            expected_xml == test_xml
        ), f"Processed XML is not equal to expected XML. Expected {expected_xml}, got {test_xml}"
