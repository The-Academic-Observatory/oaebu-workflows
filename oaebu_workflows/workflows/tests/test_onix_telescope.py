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

# Author: James Diprose

import os
import shutil

import pendulum
from airflow.models.connection import Connection
from airflow.utils.state import State
from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.onix_telescope import OnixTelescope
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.file_utils import get_file_hash
from observatory.platform.utils.gc_utils import bigquery_sharded_table_id
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    SftpServer,
    module_file_path,
)
from observatory.platform.utils.workflow_utils import (
    SftpFolders,
    SubFolder,
    blob_name,
    workflow_path,
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


class TestOnixTelescope(ObservatoryTestCase):
    """Tests for the ONIX telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestOnixTelescope, self).__init__(*args, **kwargs)
        self.host = "localhost"
        self.api_port = 5000
        self.sftp_port = 3373
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.organisation_name = "Curtin Press"
        self.organisation_folder = "curtin_press"
        self.dataset_location = "us"
        self.date_regex = "\\d{8}"
        self.date_format = "%Y%m%d"

        # API environment
        self.host = "localhost"
        self.port = 5001
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "Curtin Press"

    def setup_api(self):
        dt = pendulum.now("UTC")

        name = "Ucl Discovery Telescope"
        workflow_type = WorkflowType(name=name, type_id=OnixTelescope.DAG_ID_PREFIX)
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
            type_id=OnixTelescope.DAG_ID_PREFIX,
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Ucl Discovery Dataset",
            address="project.dataset.table",
            service="bigquery",
            connection=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        self.api.put_dataset(dataset)

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.port}")
        env.add_connection(conn)

    def test_dag_structure(self):
        """Test that the ONIX DAG has the correct structure.

        :return: None
        """

        dag = OnixTelescope(
            organisation_name=self.organisation_name,
            project_id="my-project",
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
            dataset_location=self.dataset_location,
            date_regex=self.date_regex,
            date_format=self.date_format,
        ).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["list_release_info"],
                "list_release_info": ["move_files_to_in_progress"],
                "move_files_to_in_progress": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load"],
                "bq_load": ["move_files_to_finished"],
                "move_files_to_finished": ["cleanup"],
                "cleanup": ["add_new_dataset_releases"],
                "add_new_dataset_releases": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Geonames DAG can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        with env.create():
            self.setup_connections(env)
            self.setup_api()
            dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "onix_telescope.py")
            self.assert_dag_load("onix_curtin_press", dag_file)

    def test_telescope(self):
        """Test the ONIX telescope end to end.

        :return: None.
        """

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        sftp_server = SftpServer(host=self.host, port=self.sftp_port)
        dataset_id = env.add_dataset()

        # Create the Observatory environment and run tests

        with env.create():
            self.setup_connections(env)
            self.setup_api()
            with sftp_server.create() as sftp_root:
                # Setup Telescope
                execution_date = pendulum.datetime(year=2021, month=3, day=31)
                telescope = OnixTelescope(
                    organisation_name=self.organisation_name,
                    project_id=self.project_id,
                    download_bucket=env.download_bucket,
                    transform_bucket=env.transform_bucket,
                    dataset_location=self.dataset_location,
                    date_regex=self.date_regex,
                    date_format=self.date_format,
                    dataset_id=dataset_id,
                    workflow_id=1,
                )
                dag = telescope.make_dag()

                # Release settings
                release_date = pendulum.datetime(year=2021, month=3, day=30)
                release_id = f'{telescope.dag_id}_{release_date.strftime("%Y_%m_%d")}'
                download_folder = workflow_path(SubFolder.downloaded, telescope.dag_id, release_id)
                extract_folder = workflow_path(SubFolder.extracted, telescope.dag_id, release_id)
                transform_folder = workflow_path(SubFolder.transformed, telescope.dag_id, release_id)

                # Add SFTP connection
                conn = Connection(
                    conn_id=AirflowConns.SFTP_SERVICE, uri=f"ssh://:password@{self.host}:{self.sftp_port}"
                )
                env.add_connection(conn)
                with env.create_dag_run(dag, execution_date):
                    # Test that all dependencies are specified: no error should be thrown
                    ti = env.run_task(telescope.check_dependencies.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                    # Add ONIX file to SFTP server
                    onix_file_name = "20210330_CURTINPRESS_ONIX.xml"
                    onix_test_file = test_fixtures_folder("onix", onix_file_name)
                    # Create SftpFolders instance with local sftp_root path as root
                    local_sftp_folders = SftpFolders(telescope.dag_id, self.organisation_name, sftp_root)
                    os.makedirs(local_sftp_folders.upload, exist_ok=True)
                    onix_file_dst = os.path.join(local_sftp_folders.upload, onix_file_name)
                    shutil.copy(onix_test_file, onix_file_dst)

                    # Get release info from SFTP server and check that the correct release info is returned via Xcom
                    ti = env.run_task(telescope.list_release_info.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    expected_release_info = [{"release_date": release_date, "file_name": onix_file_name}]
                    release_info = ti.xcom_pull(
                        key=OnixTelescope.RELEASE_INFO,
                        task_ids=telescope.list_release_info.__name__,
                        include_prior_dates=False,
                    )

                    for release in release_info:
                        rdate = release["release_date"]
                        release["release_date"] = pendulum.parse(rdate)

                    self.assertEqual(expected_release_info, release_info)

                    # Test move file to in progress
                    ti = env.run_task(telescope.move_files_to_in_progress.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                    in_progress_path = os.path.join(local_sftp_folders.in_progress, onix_file_name)
                    self.assertFalse(os.path.isfile(onix_file_dst))
                    self.assertTrue(os.path.isfile(in_progress_path))

                    # Test download
                    ti = env.run_task(telescope.download.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                    download_file_path = os.path.join(download_folder, onix_file_name)
                    expected_file_hash = get_file_hash(file_path=onix_test_file, algorithm="md5")
                    self.assert_file_integrity(download_file_path, expected_file_hash, "md5")

                    # Test upload downloaded
                    ti = env.run_task(telescope.upload_downloaded.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                    self.assert_blob_integrity(env.download_bucket, blob_name(download_file_path), download_file_path)

                    # Test transform
                    ti = env.run_task(telescope.transform.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                    transform_file_path = os.path.join(transform_folder, "onix.jsonl")
                    expected_file_hash = "82faa8c7940a9766376a1f3862d35828"
                    self.assert_file_integrity(transform_file_path, expected_file_hash, "md5")

                    # Test upload to cloud storage
                    ti = env.run_task(telescope.upload_transformed.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                    self.assert_blob_integrity(
                        env.transform_bucket, blob_name(transform_file_path), transform_file_path
                    )

                    # Test load into BigQuery
                    ti = env.run_task(telescope.bq_load.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                    table_id = f"{self.project_id}.{dataset_id}.{bigquery_sharded_table_id(telescope.DAG_ID_PREFIX, release_date)}"
                    expected_rows = 1
                    self.assert_table_integrity(table_id, expected_rows)

                    # Test move files to finished
                    ti = env.run_task(telescope.move_files_to_finished.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    finished_path = os.path.join(local_sftp_folders.finished, onix_file_name)
                    self.assertFalse(os.path.isfile(local_sftp_folders.in_progress))
                    self.assertTrue(os.path.isfile(finished_path))

                    # Test cleanup
                    ti = env.run_task(telescope.cleanup.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                    self.assert_cleanup(download_folder, extract_folder, transform_folder)

                    # add_dataset_release_task
                    dataset_releases = get_dataset_releases(dataset_id=1)
                    self.assertEqual(len(dataset_releases), 0)
                    ti = env.run_task("add_new_dataset_releases")
                    self.assertEqual(ti.state, State.SUCCESS)
                    dataset_releases = get_dataset_releases(dataset_id=1)
                    self.assertEqual(len(dataset_releases), 1)
