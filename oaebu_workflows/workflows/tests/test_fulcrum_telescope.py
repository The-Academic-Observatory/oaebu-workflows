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
from unittest.mock import patch

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.utils.state import State
import vcr

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.fulcrum_telescope import (
    FulcrumTelescope,
    blob_name_from_path,
    download_fulcrum_month_data,
    transform_fulcrum_data,
)
from observatory.platform.utils.file_utils import load_jsonl
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
from observatory.platform.utils.workflow_utils import table_ids_from_path
from airflow.models import Connection
from airflow.utils.state import State

FAKE_ORG_PUBLISHER_MAPPINGS = {"Fake Press": ["Fake Publisher 1", "Fake Publisher 2", "Fake Publisher 3"]}


class TestFulcrumTelescope(ObservatoryTestCase):
    """Tests for the Fulcrum telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestFulcrumTelescope, self).__init__(*args, **kwargs)
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
        self.download_cassette = os.path.join(test_fixtures_folder("fulcrum"), "fulcrum_download_cassette.yaml")
        self.test_table = os.path.join(test_fixtures_folder("fulcrum"), "test_final_table.jsonl")
        self.test_totals_download = os.path.join(test_fixtures_folder("fulcrum"), "test_totals_download.jsonl")
        self.test_country_download = os.path.join(test_fixtures_folder("fulcrum"), "test_country_download.jsonl")
        self.test_transform = os.path.join(test_fixtures_folder("fulcrum"), "test_transform.jsonl")

    def setup_api(self):

        name = "Fulcrum Telescope"
        workflow_type = WorkflowType(name=name, type_id=FulcrumTelescope.DAG_ID_PREFIX)
        self.api.put_workflow_type(workflow_type)

        organisation = Organisation(
            name="University of Michigan Press",
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
            type_id="fulcrum",
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Fulcrum",
            address="project.dataset.table",
            service="bigquery",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        self.api.put_dataset(dataset)

    def setup_connections(self, env):
        # Add Observatory API connection
        env.add_connection(
            Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.port}")
        )
        env.add_connection(Connection(conn_id=AirflowConns.OAPEN_IRUS_UK_API, uri=f"http://fake_api_login:@"))

    @patch("oaebu_workflows.workflows.fulcrum_telescope.ORG_PUBLISHER_MAPPINGS", FAKE_ORG_PUBLISHER_MAPPINGS)
    def test_dag_structure(self):
        """Test that the ONIX DAG has the correct structure and raises errors when necessary"""

        # Giving an org name not in the mappings should raise an error
        with self.assertRaises(AirflowException):
            FulcrumTelescope(
                workflow_id=1,
                organisation_name="Nonexistent Organisation",
                dag_id=f"{FulcrumTelescope.DAG_ID_PREFIX}_test",
                project_id="my-project",
                download_bucket="download_bucket",
                transform_bucket="transform_bucket",
                data_location=self.data_location,
            )

        dag = FulcrumTelescope(
            workflow_id=1,
            organisation_name="Fake Press",
            dag_id=f"{FulcrumTelescope.DAG_ID_PREFIX}_test",
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
            dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "fulcrum_telescope.py")
            self.assert_dag_load(f"fulcrum_university_of_michigan_press", dag_file)

    @patch("oaebu_workflows.workflows.fulcrum_telescope.ORG_PUBLISHER_MAPPINGS", FAKE_ORG_PUBLISHER_MAPPINGS)
    def test_telescope(self):
        """Test the Fulcrum telescope end to end."""
        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        dataset_id = env.add_dataset()

        # Create the Observatory environment and run tests
        with env.create():
            self.setup_connections(env)
            self.setup_api()

            # Setup Telescope
            execution_date = pendulum.datetime(year=2022, month=4, day=14)
            telescope = FulcrumTelescope(
                workflow_id=1,
                organisation_name="Fake Press",
                dag_id=f"{FulcrumTelescope.DAG_ID_PREFIX}_test",
                project_id=self.project_id,
                download_bucket=env.download_bucket,
                transform_bucket=env.transform_bucket,
                data_location=self.data_location,
                dataset_id=dataset_id,
            )
            dag = telescope.make_dag()

            # Add the fake requestor ID as a connection
            with env.create_dag_run(dag, execution_date):
                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test download
                fulcrum_vcr = vcr.VCR(record_mode="none")
                # with patch(
                #     "oaebu_workflows.workflows.fulcrum_telescope.download.BaseHook.get_connection", "fake_requestor_id"
                # ):
                with fulcrum_vcr.use_cassette(self.download_cassette):
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
                download_totals_file_path = os.path.join(telescope.download_totals_folder, telescope.download_file_name)
                download_country_file_path = os.path.join(
                    telescope.download_country_folder, telescope.download_file_name
                )
                transform_file_path = os.path.join(telescope.transform_folder, telescope.transform_file_name)
                download_totals_blob = blob_name_from_path(download_totals_file_path)
                download_country_blob = blob_name_from_path(download_country_file_path)
                transform_blob = blob_name_from_path(transform_file_path)
                test_table_id = f"{self.project_id}.{dataset_id}.{table_ids_from_path(transform_file_path)[0]}"

                # Downloaded files
                self.assert_file_integrity(download_totals_file_path, "95b7dceb", "gzip_crc")
                self.assert_file_integrity(download_country_file_path, "0a713d03", "gzip_crc")

                # Uploaded download blob
                self.assert_blob_integrity(env.download_bucket, download_totals_blob, download_totals_file_path)
                self.assert_blob_integrity(env.download_bucket, download_country_blob, download_country_file_path)

                # Transformed file
                self.assert_file_integrity(transform_file_path, "40a25e4e", "gzip_crc")

                # Uploaded transform blob
                self.assert_blob_integrity(env.transform_bucket, transform_blob, transform_file_path)

                # Uploaded table
                self.assert_table_integrity(test_table_id, expected_rows=3)
                self.assert_table_content(test_table_id, load_jsonl(self.test_table))

                # Test cleanup
                ti = env.run_task(telescope.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertFalse(os.path.exists(telescope.workflow_folder))
                self.assertFalse(os.path.exists(telescope.download_totals_folder))
                self.assertFalse(os.path.exists(telescope.download_country_folder))
                self.assertFalse(os.path.exists(telescope.transform_folder))

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 1)

    def test_download_fulcrum_month_data(self):
        """Tests the download_fuclrum_month_data function"""
        vcr_ = vcr.VCR(record_mode="none")
        with vcr_.use_cassette(self.download_cassette):
            actual_totals, actual_country = download_fulcrum_month_data(
                download_month="2022-04", requestor_id="fake_api_login", num_retries=0
            )
        expected_totals = load_jsonl(self.test_totals_download)
        expected_country = load_jsonl(self.test_country_download)

        # Make list order deterministic before testing
        actual_totals = [dict(sorted(d.items())) for d in actual_totals]
        actual_country = [dict(sorted(d.items())) for d in actual_country]
        expected_totals = [dict(sorted(d.items())) for d in expected_totals]
        expected_country = [dict(sorted(d.items())) for d in expected_country]
        self.assertListEqual(actual_totals, expected_totals)
        self.assertListEqual(actual_country, expected_country)

    def test_transform_fulcrum_data(self):
        """Tests the transform_fulcrum_data function"""
        totals = load_jsonl(self.test_totals_download)
        country = load_jsonl(self.test_country_download)
        actual_transform = transform_fulcrum_data(
            totals_data=totals,
            country_data=country,
            release_date=pendulum.datetime(year=2022, month=4, day=1),
            organisation_name="Fake Press",
            organisation_mappings=FAKE_ORG_PUBLISHER_MAPPINGS,
        )
        expected_transform = load_jsonl(self.test_transform)

        # Make list order deterministic before testing
        actual_transform = [dict(sorted(d.items())) for d in actual_transform]
        expected_transform = [dict(sorted(d.items())) for d in expected_transform]
        self.assertListEqual(actual_transform, expected_transform)
