# Copyright 2021 Curtin University
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
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pendulum
from airflow.models.connection import Connection
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.utils.state import State
from airflow import DAG
import vcr

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.thoth_telescope import (
    ThothTelescope,
    thoth_download_onix,
    make_workflow_folder,
    blob_name_from_path,
    get_data_path,
    cleanup,
    DEFAULT_FORMAT_SPECIFICATION,
    DEFAULT_HOST_NAME,
)
from observatory.platform.utils.file_utils import load_jsonl
from observatory.platform.utils.gc_utils import bigquery_sharded_table_id
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
    find_free_port,
)
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.table_type import TableType
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.url_utils import retry_session
from airflow.models import Connection
from airflow.utils.state import State

FAKE_ORG_NAME = "fake_org_name"
FAKE_PUBLISHER_ID = "fake_publisher_id"


class TestThothTelescope(ObservatoryTestCase):
    """Tests for the Thoth telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestThothTelescope, self).__init__(*args, **kwargs)
        self.host = "localhost"
        self.api_port = find_free_port()
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.data_location = "us"

        # API environment
        self.host = "localhost"
        self.port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)

        # Fixtures
        self.download_cassette = os.path.join(test_fixtures_folder("thoth"), "thoth_download_cassette.yaml")
        self.test_table = os.path.join(test_fixtures_folder("thoth"), "test_table.jsonl")

    def setup_api(self):

        name = "Thoth Telescope"
        workflow_type = WorkflowType(name=name, type_id=ThothTelescope.DAG_ID_PREFIX)
        self.api.put_workflow_type(workflow_type)

        organisation = Organisation(
            name="Open Book Publishers",
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
            type_id="onix",
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="ONIX",
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
        """Test that the ONIX DAG has the correct structure."""

        dag = ThothTelescope(
            dag_id=f"{ThothTelescope.DAG_ID_PREFIX}_test",
            publisher_id=FAKE_PUBLISHER_ID,
            project_id="my-project",
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
            data_location=self.data_location,
        ).make_dag()

        self.assert_dag_structure(
            {
                "check_dependencies": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load"],
                "bq_load": ["cleanup"],
                "cleanup": ["add_new_dataset_releases"],
                "add_new_dataset_releases": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag."""
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "thoth_telescope.py")
            self.assert_dag_load(f"thoth_onix_open_book_publishers", dag_file)

    def test_telescope(self):
        """Test the Thoth telescope end to end."""
        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        dataset_id = env.add_dataset()

        # Create the Observatory environment and run tests
        with env.create():
            self.setup_connections(env)
            self.setup_api()

            # Setup Telescope
            execution_date = pendulum.datetime(year=2022, month=12, day=1)
            release_date = pendulum.datetime(year=2022, month=12, day=3)
            telescope = ThothTelescope(
                dag_id=f"{ThothTelescope.DAG_ID_PREFIX}_test",
                publisher_id="fake_publisher_id",
                project_id=self.project_id,
                download_bucket=env.download_bucket,
                transform_bucket=env.transform_bucket,
                data_location=self.data_location,
                dataset_id=dataset_id,
                workflow_id=1,
            )
            dag = telescope.make_dag()

            with env.create_dag_run(dag, execution_date):
                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test download
                thoth_vcr = vcr.VCR(record_mode="none")
                with thoth_vcr.use_cassette(self.download_cassette):
                    ti = env.run_task(telescope.download.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test upload downloaded
                ti = env.run_task(telescope.upload_downloaded.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test transform
                ti = env.run_task(telescope.transform.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test upload to cloud storage
                ti = env.run_task(telescope.upload_transformed.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test load into BigQuery
                ti = env.run_task(telescope.bq_load.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Make assertions
                download_file_path = os.path.join(telescope.download_folder, telescope.download_file_name)
                transform_file_path = os.path.join(telescope.transform_folder, telescope.transform_file_name)
                download_blob = blob_name_from_path(download_file_path)
                transform_blob = blob_name_from_path(transform_file_path)
                test_table_id = (
                    f"{self.project_id}.{dataset_id}.{bigquery_sharded_table_id(telescope.DAG_ID_PREFIX, release_date)}"
                )

                # Downloaded file
                self.assert_file_integrity(download_file_path, "d02bcbb97e887acbbbaa55a1b77f51ef", "md5")

                # Uploaded download blob
                self.assert_blob_integrity(env.download_bucket, download_blob, download_file_path)

                # Transformed file
                self.assert_file_integrity(transform_file_path, "782d3888162dac8f1afdaf20e42b278f", "md5")

                # Uploaded transform blob
                self.assert_blob_integrity(env.transform_bucket, transform_blob, transform_file_path)

                # Uploaded table
                self.assert_table_integrity(test_table_id, expected_rows=1)
                self.assert_table_content(test_table_id, load_jsonl(self.test_table))

                # Test cleanup
                ti = env.run_task(telescope.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertFalse(os.path.exists(telescope.workflow_folder))
                self.assertFalse(os.path.exists(telescope.download_folder))
                self.assertFalse(os.path.exists(telescope.transform_folder))

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 1)

    # Function tests
    @patch("oaebu_workflows.workflows.thoth_telescope.EnvironmentVariablesBackend.get_variable")
    @patch("oaebu_workflows.workflows.thoth_telescope.Variable.get")
    def test_get_data_path(self, mock_variable_get, mock_env_variable_get):
        """Tests the function that retrieves the data_path airflow variable"""
        # 1 - no variable available
        mock_env_variable_get.return_value = None
        mock_variable_get.return_value = None
        self.assertRaises(AirflowException, get_data_path)
        # 2 - available in environment backend
        mock_env_variable_get.return_value = "env_return"
        self.assertEqual("env_return", get_data_path())
        # 3 - available in google secrets
        mock_env_variable_get.return_value = None
        mock_variable_get.return_value = "google_return"
        self.assertEqual("google_return", get_data_path())
        # 4 - available in environment and google secrets. We prefer the environment backend usage
        mock_env_variable_get.return_value = "env_return"
        self.assertEqual("env_return", get_data_path())

    @patch("oaebu_workflows.workflows.thoth_telescope.EnvironmentVariablesBackend.get_variable")
    def test_make_workflow_folder(self, mock_get_variable):
        """Tests the make_workflow_folder function"""
        with TemporaryDirectory() as tempdir:
            mock_get_variable.return_value = tempdir
            path = make_workflow_folder(
                "test_dag", pendulum.datetime(year=1000, month=6, day=14), "sub_folder", "subsub_folder"
            )
            self.assertEqual(path, os.path.join(tempdir, f"test_dag/test_dag_1000_06_14/sub_folder/subsub_folder"))

    @patch("oaebu_workflows.workflows.thoth_telescope.EnvironmentVariablesBackend.get_variable")
    def test_blob_name_from_path(self, mock_get_variable):
        """Tests the blob_name from_path function"""
        with TemporaryDirectory() as tempdir:
            mock_get_variable.return_value = tempdir
            invalid_path = os.path.join("some", "fake", "invalid", "path", "file.txt")
            valid_path_1 = os.path.join(tempdir, "some", "fake", "valid", "path", "file.txt")
            valid_path_2 = os.path.join(valid_path_1, "")  # Trailing slash
            self.assertRaises(AirflowException, blob_name_from_path, invalid_path)
            self.assertEqual(blob_name_from_path(valid_path_1), "some/fake/valid/path/file.txt")
            self.assertEqual(blob_name_from_path(valid_path_2), "some/fake/valid/path/file.txt")

    def test_download_onix(self):
        """
        Tests the download_onix function.
        Will test that the function works as expected using proxy HTTP requests
        """
        with TemporaryDirectory() as tempdir:
            thoth_vcr = vcr.VCR(record_mode="none")
            with thoth_vcr.use_cassette(self.download_cassette):
                thoth_download_onix(FAKE_PUBLISHER_ID, tempdir, download_filename="fake_download.xml", num_retries=0)
            self.assert_file_integrity(
                os.path.join(tempdir, "fake_download.xml"), "d02bcbb97e887acbbbaa55a1b77f51ef", "md5"
            )

    def test_thoth_api(self):
        """Tests that HTTP requests to the thoth API are successful"""
        base_response = retry_session(num_retries=2).get(DEFAULT_HOST_NAME)
        format_response = retry_session(num_retries=2).get(
            f"{DEFAULT_HOST_NAME}/specifications/{DEFAULT_FORMAT_SPECIFICATION}"
        )
        self.assertEqual(base_response.status_code, 200)
        self.assertEqual(format_response.status_code, 200)

    def test_cleanup(self):
        """
        Tests the cleanup function.
        Creates a task and pushes and Xcom. Also creates a fake workflow directory.
        Both the Xcom and the directory should be deleted by the cleanup() function
        """

        def create_xcom(**kwargs):
            ti = kwargs["ti"]
            execution_date = kwargs["execution_date"]
            ti.xcom_push("topic", {"release_date": execution_date.format("YYYYMMDD"), "something": "info"})

        env = ObservatoryEnvironment(enable_api=False, enable_elastic=False)
        with env.create():
            execution_date = pendulum.datetime(2023, 1, 1)
            with DAG(
                dag_id="test_dag",
                schedule_interval="@daily",
                default_args={"owner": "airflow", "start_date": execution_date},
                catchup=True,
            ) as dag:
                kwargs = {"task_id": "create_xcom"}
                op = PythonOperator(python_callable=create_xcom, **kwargs)

            with TemporaryDirectory() as workflow_dir:
                # Create some files in the workflow folder
                subdir = os.path.join(workflow_dir, "test_directory")
                os.mkdir(subdir)

                # DAG Run
                with env.create_dag_run(dag=dag, execution_date=execution_date):
                    ti = env.run_task("create_xcom")
                    self.assertEqual("success", ti.state)
                    msgs = ti.xcom_pull(key="topic", task_ids="create_xcom", include_prior_dates=True)
                    self.assertIsInstance(msgs, dict)
                    cleanup("test_dag", execution_date, workflow_folder=workflow_dir, retention_days=0)
                    msgs = ti.xcom_pull(key="topic", task_ids="create_xcom", include_prior_dates=True)
                    self.assertEqual(msgs, None)
                    self.assertEqual(os.path.isdir(subdir), False)
                    self.assertEqual(os.path.isdir(workflow_dir), False)
