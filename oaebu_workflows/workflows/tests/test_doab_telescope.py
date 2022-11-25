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

import os
from unittest.mock import patch

import httpretty
import pendulum
from airflow.exceptions import AirflowException
from click.testing import CliRunner
from google.cloud import bigquery

from oaebu_workflows.api_type_ids import DatasetTypeId
from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.doab_telescope import (
    DoabRelease,
    DoabTelescope,
    convert,
    transform_dict,
)
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.file_utils import get_file_hash
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
    find_free_port,
)
from observatory.platform.utils.workflow_utils import (
    blob_name,
    create_date_table_id,
    table_ids_from_path,
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
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.airflow_utils import AirflowConns
from airflow.models import Connection
from airflow.utils.state import State
import responses


class TestDoabTelescope(ObservatoryTestCase):
    """Tests for the DOAB telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestDoabTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.first_download_path = test_fixtures_folder("doab", "doab1.csv")
        self.first_execution_date = pendulum.datetime(year=2021, month=2, day=2)

        self.second_download_path = test_fixtures_folder("doab", "doab2.csv")
        self.second_execution_date = pendulum.datetime(year=2021, month=3, day=2)

        # API environment
        self.host = "localhost"
        self.port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "UCL Press"

    def setup_api(self):
        dt = pendulum.now("UTC")

        name = "Doab Telescope"
        workflow_type = WorkflowType(name=name, type_id=DoabTelescope.DAG_ID)
        self.api.put_workflow_type(workflow_type)

        organisation = Organisation(
            name=self.org_name,
            project_id="project",
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
        )
        self.api.put_organisation(organisation)

        telescope = Workflow(
            name=name,
            workflow_type=WorkflowType(id=1),
            organisation=Organisation(id=1),
            extra={},
        )
        self.api.put_workflow(telescope)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            type_id=DatasetTypeId.doab,
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Doab Dataset",
            address="project.dataset.table",
            service="bigquery",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        self.api.put_dataset(dataset)

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.port}")
        env.add_connection(conn)

    def test_dag_structure(self):
        """Test that the DOAB DAG has the correct structure.
        :return: None
        """

        dag = DoabTelescope(workflow_id=1).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load_partition"],
                "bq_load_partition": ["bq_delete_old"],
                "bq_delete_old": ["bq_append_new"],
                "bq_append_new": ["cleanup"],
                "cleanup": ["add_new_dataset_releases"],
                "add_new_dataset_releases": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the DOAB DAG can be loaded from a DAG bag.
        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)

        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "doab_telescope.py")
            self.assert_dag_load("doab", dag_file)

    def test_telescope(self):
        """Test the DOAB telescope end to end.
        :return: None.
        """
        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        dataset_id = env.add_dataset()

        # Setup Telescope
        telescope = DoabTelescope(dataset_id=dataset_id, workflow_id=1)
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            self.setup_connections(env)
            self.setup_api()
            # first run
            with env.create_dag_run(dag, self.first_execution_date) as m_dagrun:
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                start_date, end_date, first_release = telescope.get_release_info(
                    dag=dag,
                    data_interval_end=pendulum.datetime(2021, 2, 1),
                )

                # use release info for other tasks
                release = DoabRelease(telescope.dag_id, start_date, end_date, first_release)

                # Test download task
                with responses.RequestsMock() as rsps:
                    with open(self.first_download_path, "rb") as f:
                        body = f.read()
                    rsps.add(responses.GET, DoabTelescope.CSV_URL, body=body, status=200)
                    env.run_task(telescope.download.__name__)

                self.assertEqual(1, len(release.download_files))
                download_path = release.download_files[0]
                expected_file_hash = get_file_hash(file_path=self.first_download_path, algorithm="md5")
                self.assert_file_integrity(download_path, expected_file_hash, "md5")

                # Test that file uploaded
                env.run_task(telescope.upload_downloaded.__name__)
                self.assert_blob_integrity(env.download_bucket, blob_name(download_path), download_path)

                # Test that file transformed
                env.run_task(telescope.transform.__name__)

                self.assertEqual(1, len(release.transform_files))
                transform_path = release.transform_files[0]
                expected_file_hash = "97a86394"
                self.assert_file_integrity(transform_path, expected_file_hash, "gzip_crc")

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                self.assert_blob_integrity(env.transform_bucket, blob_name(transform_path), transform_path)

                # Test that load partition task is skipped for the first release
                ti = env.run_task(telescope.bq_load_partition.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test delete old task is skipped for the first release
                with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                    ti = env.run_task(telescope.bq_delete_old.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test append new creates table
                env.run_task(telescope.bq_append_new.__name__)
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                expected_rows = 4
                self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 1)

            # second run
            with env.create_dag_run(dag, self.second_execution_date) as m_dag_run:
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                start_date, end_date, first_release = telescope.get_release_info(
                    dag=dag,
                    data_interval_end=pendulum.datetime(2021, 3, 1),
                )

                self.assertEqual(release.end_date, start_date)
                self.assertEqual(pendulum.datetime(2021, 3, 1), end_date)
                self.assertFalse(first_release)

                # use release info for other tasks
                release = DoabRelease(telescope.dag_id, start_date, end_date, first_release)

                # Test download task
                with responses.RequestsMock() as rsps:
                    with open(self.second_download_path, "rb") as f:
                        body = f.read()
                    rsps.add(responses.GET, DoabTelescope.CSV_URL, body=body, status=200)
                    env.run_task(telescope.download.__name__)

                self.assertEqual(1, len(release.download_files))
                download_path = release.download_files[0]
                expected_file_hash = get_file_hash(file_path=self.second_download_path, algorithm="md5")
                self.assert_file_integrity(download_path, expected_file_hash, "md5")

                # Test that file uploaded
                env.run_task(telescope.upload_downloaded.__name__)
                self.assert_blob_integrity(env.download_bucket, blob_name(download_path), download_path)

                # Test that file transformed
                env.run_task(telescope.transform.__name__)

                self.assertEqual(1, len(release.transform_files))
                transform_path = release.transform_files[0]
                expected_file_hash = "19f6ba1e"
                self.assert_file_integrity(transform_path, expected_file_hash, "gzip_crc")

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                self.assert_blob_integrity(env.transform_bucket, blob_name(transform_path), transform_path)

                # Test that load partition task creates partition
                env.run_task(telescope.bq_load_partition.__name__)
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                table_id = create_date_table_id(partition_table_id, release.end_date, bigquery.TimePartitioningType.DAY)
                table_id = f"{self.project_id}.{telescope.dataset_id}.{table_id}"
                expected_rows = 4
                self.assert_table_integrity(table_id, expected_rows)

                # Test task deleted rows from main table
                with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                    env.run_task(telescope.bq_delete_old.__name__)
                table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                expected_rows = 3
                self.assert_table_integrity(table_id, expected_rows)

                # Test append new adds rows to table
                env.run_task(telescope.bq_append_new.__name__)
                table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                expected_rows = 7
                self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 1)
                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 2)

    def test_airflow_vars(self):
        """Cover case when airflow_vars is given."""

        telescope = DoabTelescope(workflow_id=1, airflow_vars=[AirflowVars.DOWNLOAD_BUCKET])
        self.assertEqual(
            set(telescope.airflow_vars),
            {
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
            },
        )

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_download(self, mock_variable_get):
        """Download release and check exception is raised when response is not 200 or csv is empty.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return:
        """
        start_date = pendulum.datetime(2020, 1, 1)
        end_date = pendulum.datetime(2020, 1, 31)
        release = DoabRelease("doab", start_date, end_date, False)

        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = "data"

            # Test exception is raised for invalid status code
            with httpretty.enabled():
                httpretty.register_uri(httpretty.GET, DoabTelescope.CSV_URL, status=400)

                with self.assertRaises(AirflowException):
                    release.download()

            # Test exception is raised for empty csv file
            with httpretty.enabled():
                empty_csv = "Column1,Column2"
                httpretty.register_uri(httpretty.GET, DoabTelescope.CSV_URL, body=empty_csv)

                with self.assertRaises(AirflowException):
                    release.download()

    def test_transform_dict(self):
        """Check transform_dict handling of invalid case."""
        nested_fields = ["dc.subject.classification"]
        list_fields = ["dc.subject.classification", "dc.date.issued", "BITSTREAM ISBN"]
        test_dict = {
            "field1": [{"1": "value1"}, "2"],
            "field2": None,
            "dc.subject.classification": "value1||value2",
            "dc.date.issued": "0000-01-01",
            "BITSTREAM ISBN": "123-5521-4521",
        }
        transformed_dict = {
            "field1": [{"1": "value1"}, "2"],
            "dc": {"subject": {"classification": {"value": ["value1", "value2"]}}},
            "dc_date_issued": [],
            "BITSTREAM_ISBN": ["12355214521"],
        }

        result = transform_dict(test_dict, convert, nested_fields, list_fields)
        self.assertDictEqual(result, transformed_dict)
