# Copyright 2021-2024 Curtin University
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
from unittest.mock import patch

import pendulum
from airflow.models import Connection
from airflow.utils.state import State

from oaebu_workflows.onix_telescope.onix_telescope import OnixRelease, create_dag
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.config import test_fixtures_folder, module_file_path
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.date_utils import datetime_normalise
from observatory_platform.google.bigquery import bq_sharded_table_id
from observatory_platform.google.gcs import gcs_blob_name_from_path
from observatory_platform.sftp import SftpFolders
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.sandbox.test_utils import SandboxTestCase, find_free_port, load_and_parse_json
from observatory_platform.sandbox.sftp_server import SftpServer
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment


class TestOnixTelescope(SandboxTestCase):
    """Tests for the ONIX telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestOnixTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.sftp_root = "/"
        self.date_regex = "\\d{8}"
        self.sftp_port = find_free_port()

        # Test file
        fixtures_folder = test_fixtures_folder(workflow_module="onix_telescope")
        self.onix_xml_path = os.path.join(fixtures_folder, "20210330_CURTINPRESS_ONIX.xml")
        self.onix_json_path = os.path.join(fixtures_folder, "20210330_CURTINPRESS_ONIX.json")

    def test_dag_structure(self):
        """Test that the ONIX DAG has the correct structure."""
        dag = create_dag(
            dag_id="onix_telescope",
            cloud_workspace=self.fake_cloud_workspace,
            sftp_root=self.sftp_root,
            date_regex=self.date_regex,
        )
        self.assert_dag_structure(
            {
                "check_dependencies": ["fetch_releases"],
                "fetch_releases": [
                    "process_release.move_files_to_in_progress",
                    "process_release.download",
                    "process_release.transform",
                    "process_release.bq_load",
                    "process_release.move_files_to_finished",
                    "process_release.add_new_dataset_releases",
                    "process_release.cleanup_workflow",
                ],
                "process_release.move_files_to_in_progress": ["process_release.download"],
                "process_release.download": ["process_release.transform"],
                "process_release.transform": ["process_release.bq_load"],
                "process_release.bq_load": ["process_release.move_files_to_finished"],
                "process_release.move_files_to_finished": ["process_release.add_new_dataset_releases"],
                "process_release.add_new_dataset_releases": ["process_release.cleanup_workflow"],
                "process_release.cleanup_workflow": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Geonames DAG can be loaded from a DAG bag."""
        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id="onix",
                    name="ONIX Telescope",
                    class_name="oaebu_workflows.onix_telescope.onix_telescope.create_dag",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(date_regex=self.date_regex),
                )
            ],
        )
        with env.create():
            dag_file = os.path.join(module_file_path("dags"), "load_dags.py")
            self.assert_dag_load_from_config("onix", dag_file)

        # Errors should be raised if kwargs dict not supplied
        env.workflows[0].kwargs = {}
        with env.create():
            with self.assertRaises(AssertionError) as cm:
                self.assert_dag_load_from_config("onix", dag_file)
            msg = cm.exception.args[0]
            self.assertTrue("missing 1 required keyword-only argument" in msg)
            self.assertTrue("date_regex" in msg)

    def test_telescope(self):
        """Test the ONIX telescope end to end."""
        # Setup Observatory environmento
        env = SandboxEnvironment(self.project_id, self.data_location)
        sftp_server = SftpServer(host="localhost", port=self.sftp_port)

        with env.create(), sftp_server.create() as sftp_root:
            # Setup DAG
            logical_date = pendulum.datetime(year=2021, month=3, day=31)
            metadata_partner = partner_from_str("onix", metadata_partner=True)
            metadata_partner.bq_dataset_id = env.add_dataset()
            api_bq_dataset_id = env.add_dataset()
            sftp_service_conn_id = "sftp_service"
            dag_id = "onix_telescope_test"
            dag = create_dag(
                dag_id=dag_id,
                cloud_workspace=env.cloud_workspace,
                sftp_root="/",
                date_regex=self.date_regex,
                metadata_partner=metadata_partner,
                elevate_related_products=True,
                sftp_service_conn_id=sftp_service_conn_id,
                api_bq_dataset_id=api_bq_dataset_id,
            )

            # Add SFTP connection
            conn = Connection(conn_id=sftp_service_conn_id, uri=f"ssh://:password@localhost:{self.sftp_port}")
            env.add_connection(conn)
            with env.create_dag_run(dag, logical_date=logical_date):
                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task("check_dependencies")
                self.assertEqual(ti.state, State.SUCCESS)

                # Add ONIX file to SFTP server
                local_sftp_folders = SftpFolders(dag_id, sftp_service_conn_id, sftp_root)
                os.makedirs(local_sftp_folders.upload, exist_ok=True)
                onix_file_name = os.path.basename(self.onix_xml_path)
                onix_file_dst = os.path.join(local_sftp_folders.upload, onix_file_name)
                shutil.copy(self.onix_xml_path, onix_file_dst)

                # Get release info from SFTP server and check that the correct release info is returned via Xcom
                ti = env.run_task("fetch_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                release_dicts = ti.xcom_pull(task_ids="fetch_releases", include_prior_dates=False)
                expected_release_dicts = [
                    {
                        "dag_id": "onix_telescope_test",
                        "run_id": "scheduled__2021-03-31T00:00:00+00:00",
                        "snapshot_date": "2021-03-30",
                        "onix_file_name": "20210330_CURTINPRESS_ONIX.xml",
                    }
                ]
                self.assertEqual(release_dicts, expected_release_dicts)
                release = OnixRelease.from_dict(release_dicts[0])

                # Test move file to in progress
                ti = env.run_task("process_release.move_files_to_in_progress", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                in_progress_path = os.path.join(local_sftp_folders.in_progress, release.onix_file_name)
                self.assertFalse(os.path.isfile(onix_file_dst))
                self.assertTrue(os.path.isfile(in_progress_path))

                # Test download
                ti = env.run_task("process_release.download", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_file_integrity(release.download_path, "02e81f8c19442a64f0c947510f9e1c7b", "md5")
                self.assert_blob_integrity(
                    env.download_bucket, gcs_blob_name_from_path(release.download_path), release.download_path
                )

                # Test transform
                ti = env.run_task("process_release.transform", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_blob_integrity(
                    env.transform_bucket, gcs_blob_name_from_path(release.transform_path), release.transform_path
                )

                # Test load into BigQuery
                ti = env.run_task("process_release.bq_load", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                table_id = bq_sharded_table_id(
                    env.cloud_workspace.project_id,
                    metadata_partner.bq_dataset_id,
                    metadata_partner.bq_table_name,
                    release.snapshot_date,
                )
                self.assert_table_integrity(table_id, expected_rows=2)
                self.assert_table_content(table_id, load_and_parse_json(self.onix_json_path), primary_key="ISBN13")

                # Test move files to finished
                ti = env.run_task("process_release.move_files_to_finished", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                finished_path = os.path.join(local_sftp_folders.finished, onix_file_name)
                self.assertFalse(os.path.isfile(local_sftp_folders.in_progress))
                self.assertTrue(os.path.isfile(finished_path))

                # Set up the API
                api = DatasetAPI(bq_project_id=self.project_id, bq_dataset_id=api_bq_dataset_id)
                api.seed_db()
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="onix")
                self.assertEqual(len(dataset_releases), 0)

                # Add dataset release task
                now = pendulum.now()
                with patch("oaebu_workflows.onix_telescope.onix_telescope.pendulum.now") as mock_now:
                    mock_now.return_value = now
                    ti = env.run_task("process_release.add_new_dataset_releases", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="onix")
                self.assertEqual(len(dataset_releases), 1)
                expected_release = {
                    "dag_id": dag_id,
                    "entity_id": "onix",
                    "dag_run_id": release.run_id,
                    "created": datetime_normalise(now),
                    "modified": datetime_normalise(now),
                    "data_interval_start": "2021-03-31T00:00:00+00:00",
                    "data_interval_end": "2021-03-31T12:00:00+00:00",
                    "snapshot_date": "2021-03-30T00:00:00+00:00",
                    "partition_date": None,
                    "changefile_start_date": None,
                    "changefile_end_date": None,
                    "sequence_start": None,
                    "sequence_end": None,
                    "extra": {},
                }
                self.assertEqual(expected_release, dataset_releases[0].to_dict())

                # Test cleanup
                workflow_folder_path = release.workflow_folder
                ti = env.run_task("process_release.cleanup_workflow", map_index=0)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(workflow_folder_path)
