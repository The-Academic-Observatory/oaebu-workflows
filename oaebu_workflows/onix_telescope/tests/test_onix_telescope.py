# Copyright 2021-2023 Curtin University
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
from airflow.models import Connection
from airflow.utils.state import State

from oaebu_workflows.onix_telescope.onix_telescope import OnixTelescope, OnixRelease
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.config import test_fixtures_folder
from observatory.platform.api import get_dataset_releases
from observatory.platform.bigquery import bq_sharded_table_id
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.sftp import SftpFolders
from observatory.platform.observatory_config import Workflow
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    SftpServer,
    find_free_port,
    load_and_parse_json,
)


class TestOnixTelescope(ObservatoryTestCase):
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
        dag = OnixTelescope(
            dag_id="onix_telescope",
            cloud_workspace=self.fake_cloud_workspace,
            sftp_root=self.sftp_root,
            date_regex=self.date_regex,
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
                "move_files_to_finished": ["add_new_dataset_releases"],
                "add_new_dataset_releases": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Geonames DAG can be loaded from a DAG bag."""
        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id="onix",
                    name="ONIX Telescope",
                    class_name="oaebu_workflows.onix_telescope.onix_telescope.OnixTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(date_regex=self.date_regex),
                )
            ],
        )
        with env.create():
            self.assert_dag_load_from_config("onix")

        # Errors should be raised if kwargs dict not supplied
        env.workflows[0].kwargs = {}
        with env.create():
            with self.assertRaises(AssertionError) as cm:
                self.assert_dag_load_from_config("onix")
            msg = cm.exception.args[0]
            self.assertTrue("missing 1 required keyword-only argument" in msg)
            self.assertTrue("date_regex" in msg)

    def test_telescope(self):
        """Test the ONIX telescope end to end."""
        # Setup Observatory environment
        env = ObservatoryEnvironment(
            self.project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )
        sftp_server = SftpServer(host="localhost", port=self.sftp_port)
        dataset_id = env.add_dataset()

        # Create the Observatory environment and run tests

        with env.create(), sftp_server.create() as sftp_root:
            # Setup Telescope
            execution_date = pendulum.datetime(year=2021, month=3, day=31)
            partner = partner_from_str("onix", metadata_partner=True)
            partner.bq_dataset_id = dataset_id
            telescope = OnixTelescope(
                dag_id="onix_telescope_test",
                cloud_workspace=env.cloud_workspace,
                sftp_root="/",
                date_regex=self.date_regex,
                metadata_partner=partner,
            )
            dag = telescope.make_dag()

            # Release settings
            release_date = pendulum.datetime(year=2021, month=3, day=30)

            # Add SFTP connection
            conn = Connection(conn_id=telescope.sftp_service_conn_id, uri=f"ssh://:password@localhost:{self.sftp_port}")
            env.add_connection(conn)
            with env.create_dag_run(dag, execution_date):
                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # Add ONIX file to SFTP server
                local_sftp_folders = SftpFolders(telescope.dag_id, telescope.sftp_service_conn_id, sftp_root)
                os.makedirs(local_sftp_folders.upload, exist_ok=True)
                onix_file_name = os.path.basename(self.onix_xml_path)
                onix_file_dst = os.path.join(local_sftp_folders.upload, onix_file_name)
                shutil.copy(self.onix_xml_path, onix_file_dst)

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

                release = OnixRelease(
                    dag_id=telescope.dag_id,
                    run_id=env.dag_run.run_id,
                    snapshot_date=release_date,
                    onix_file_name=onix_file_name,
                )

                # Test move file to in progress
                ti = env.run_task(telescope.move_files_to_in_progress.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                in_progress_path = os.path.join(local_sftp_folders.in_progress, release.onix_file_name)
                self.assertFalse(os.path.isfile(onix_file_dst))
                self.assertTrue(os.path.isfile(in_progress_path))

                # Test download
                ti = env.run_task(telescope.download.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_file_integrity(release.download_path, "28f85c488ab01b0cff769d9da6b4be24", "md5")

                # Test upload downloaded
                ti = env.run_task(telescope.upload_downloaded.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_blob_integrity(
                    env.download_bucket, gcs_blob_name_from_path(release.download_path), release.download_path
                )

                # Test transform
                ti = env.run_task(telescope.transform.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_file_integrity(release.transform_path, "2164a300", "gzip_crc")

                # Test upload to cloud storage
                ti = env.run_task(telescope.upload_transformed.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_blob_integrity(
                    env.transform_bucket, gcs_blob_name_from_path(release.transform_path), release.transform_path
                )

                # Test load into BigQuery
                ti = env.run_task(telescope.bq_load.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                table_id = bq_sharded_table_id(
                    telescope.cloud_workspace.project_id,
                    telescope.metadata_partner.bq_dataset_id,
                    telescope.metadata_partner.bq_table_name,
                    release.snapshot_date,
                )
                self.assert_table_integrity(table_id, expected_rows=1)
                self.assert_table_content(table_id, load_and_parse_json(self.onix_json_path), primary_key="ISBN13")

                # Test move files to finished
                ti = env.run_task(telescope.move_files_to_finished.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                finished_path = os.path.join(local_sftp_folders.finished, onix_file_name)
                self.assertFalse(os.path.isfile(local_sftp_folders.in_progress))
                self.assertTrue(os.path.isfile(finished_path))

                # Add_dataset_release_task
                dataset_releases = get_dataset_releases(dag_id=telescope.dag_id, dataset_id=telescope.api_dataset_id)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task(telescope.add_new_dataset_releases.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dag_id=telescope.dag_id, dataset_id=telescope.api_dataset_id)
                self.assertEqual(len(dataset_releases), 1)

                # Test cleanup
                ti = env.run_task(telescope.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assert_cleanup(release.workflow_folder)
