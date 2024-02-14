# Copyright 2020-2024 Curtin University
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

import os
import shutil

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.utils.state import State
from click.testing import CliRunner

from oaebu_workflows.config import test_fixtures_folder, module_file_path
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.google_books_telescope.google_books_telescope import GoogleBooksRelease, create_dag, gb_transform
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    SftpServer,
    find_free_port,
)
from observatory.platform.bigquery import bq_table_id
from observatory.platform.observatory_config import Workflow
from observatory.platform.sftp import SftpFolders
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.api import get_dataset_releases


class TestGoogleBooksTelescope(ObservatoryTestCase):
    """Tests for the GoogleBooks telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestGoogleBooksTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.sftp_port = find_free_port()

    def test_dag_structure(self):
        """Test that the Google Books DAG has the correct structure."""
        dag = create_dag(dag_id="test_dag", cloud_workspace=self.fake_cloud_workspace, sftp_root="/")
        self.assert_dag_structure(
            {
                "check_dependencies": ["make_release"],
                "make_release": [
                    "move_files_to_in_progress",
                    "download",
                    "transform",
                    "move_files_to_finished",
                    "bq_load",
                    "add_new_dataset_releases",
                    "cleanup_workflow",
                ],
                "move_files_to_in_progress": ["download"],
                "download": ["transform"],
                "transform": ["move_files_to_finished"],
                "move_files_to_finished": ["bq_load"],
                "bq_load": ["add_new_dataset_releases"],
                "add_new_dataset_releases": ["cleanup_workflow"],
                "cleanup_workflow": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Google Books DAG can be loaded from a DAG bag."""
        # Run tests both for telescope with file suffixes and without
        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id="google_books",
                    name="My Google Books Telescope",
                    class_name="oaebu_workflows.google_books_telescope.google_books_telescope.create_dag",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )
        with env.create():
            dag_file = os.path.join(module_file_path("dags"), "load_dags.py")
            self.assert_dag_load_from_config("google_books", dag_file)

    def test_telescope(self):
        """Test the Google Books telescope end to end."""
        fixtures_folder = test_fixtures_folder(workflow_module="google_books_telescope")
        params = {
            "no_download_files": 2,
            "bq_rows": 4,
            "traffic_download_hash": "db4dca44d5231e0c4e2ad95db41b79b6",
            "traffic_transform_hash": "b8073007",
            "sales_download_hash": "6496518be1ea73694d0a8f89c0b42f20",
            "sales_transform_hash": "ebe49987",
            "test_files": {
                "GoogleBooksTrafficReport_2020_02.csv": os.path.join(
                    fixtures_folder, "GoogleBooksTrafficReport_2020_02.csv"
                ),
                "GoogleSalesTransactionReport_2020_02.csv": os.path.join(
                    fixtures_folder, "GoogleSalesTransactionReport_2020_02.csv"
                ),
            },
        }

        # Setup Observatory environment
        env = ObservatoryEnvironment(
            self.project_id, self.data_location, api_host="localhost", api_port=find_free_port()
        )
        sftp_server = SftpServer(host="localhost", port=self.sftp_port)
        dataset_id = env.add_dataset()

        # Create the Observatory environment and run tests
        with env.create():
            with sftp_server.create() as sftp_root:

                # Setup Telescope
                execution_date = pendulum.datetime(year=2021, month=3, day=31)
                sales_partner = partner_from_str("google_books_sales")
                sales_partner.bq_dataset_id = dataset_id
                traffic_partner = partner_from_str("google_books_traffic")
                traffic_partner.bq_dataset_id = dataset_id
                sftp_service_conn_id = "sftp_service"
                dag_id = "google_books_test"
                dag = create_dag(
                    dag_id=dag_id,
                    cloud_workspace=env.cloud_workspace,
                    sftp_root="/",  # Unintuitive, but this is correct
                    sales_partner=sales_partner,
                    traffic_partner=traffic_partner,
                    sftp_service_conn_id=sftp_service_conn_id,
                )

                # Add SFTP connection
                env.add_connection(
                    Connection(conn_id=sftp_service_conn_id, uri=f"ssh://:password@localhost:{self.sftp_port}")
                )
                with env.create_dag_run(dag, execution_date):
                    # Test that all dependencies are specified: no error should be thrown
                    ti = env.run_task("check_dependencies")
                    self.assertEqual(ti.state, State.SUCCESS)

                    # Add file to SFTP server
                    sftp_folders = SftpFolders(dag_id, sftp_conn_id=sftp_service_conn_id, sftp_root=sftp_root)
                    os.makedirs(sftp_folders.upload, exist_ok=True)
                    for file_name, file_path in params["test_files"].items():
                        upload_file = os.path.join(sftp_folders.upload, file_name)
                        shutil.copy(file_path, upload_file)

                    # Test that make release is successful
                    ti = env.run_task("make_release")
                    self.assertEqual(ti.state, State.SUCCESS)
                    release_dicts = ti.xcom_pull(task_ids="make_release", include_prior_dates=False)
                    expected_release_dicts = [
                        {
                            "dag_id": "google_books_test",
                            "run_id": "scheduled__2021-03-31T00:00:00+00:00",
                            "partition_date": "2020-02-29",
                            "sftp_files": [
                                "/workflows/google_books_test/in_progress/GoogleBooksTrafficReport_2020_02.csv",
                                "/workflows/google_books_test/in_progress/GoogleSalesTransactionReport_2020_02.csv",
                            ],
                        }
                    ]
                    self.assertEqual(release_dicts, expected_release_dicts)
                    release = GoogleBooksRelease.from_dict(release_dicts[0])

                    # Test move file to in progress
                    ti = env.run_task("move_files_to_in_progress")
                    self.assertEqual(ti.state, State.SUCCESS)
                    for file in release.sftp_files:
                        file_name = os.path.basename(file)
                        upload_file = os.path.join(sftp_folders.upload, file_name)
                        self.assertFalse(os.path.isfile(upload_file))
                        in_progress_file = os.path.join(sftp_folders.in_progress, file_name)
                        self.assertTrue(os.path.isfile(in_progress_file))

                    # Run main telescope tasks
                    ti = env.run_task("download")
                    self.assertEqual(ti.state, State.SUCCESS)
                    ti = env.run_task("transform")
                    self.assertEqual(ti.state, State.SUCCESS)
                    ti = env.run_task("move_files_to_finished")
                    self.assertEqual(ti.state, State.SUCCESS)
                    ti = env.run_task("bq_load")
                    self.assertEqual(ti.state, State.SUCCESS)

                    # Make assertions for the above tasks
                    # Test download
                    self.assertTrue(os.path.exists(release.download_traffic_path))
                    self.assertTrue(os.path.exists(release.download_sales_path))
                    self.assert_file_integrity(release.download_traffic_path, params["traffic_download_hash"], "md5")
                    self.assert_file_integrity(release.download_sales_path, params["sales_download_hash"], "md5")

                    # Test upload downloaded
                    self.assert_blob_integrity(
                        env.download_bucket,
                        gcs_blob_name_from_path(release.download_traffic_path),
                        release.download_traffic_path,
                    )
                    self.assert_blob_integrity(
                        env.download_bucket,
                        gcs_blob_name_from_path(release.download_sales_path),
                        release.download_sales_path,
                    )

                    # Test that file transformed
                    self.assertTrue(os.path.exists(release.transform_sales_path))
                    self.assertTrue(os.path.exists(release.transform_traffic_path))
                    self.assert_file_integrity(release.transform_sales_path, params["sales_transform_hash"], "gzip_crc")
                    self.assert_file_integrity(
                        release.transform_traffic_path, params["traffic_transform_hash"], "gzip_crc"
                    )

                    # Test that transformed file uploaded
                    self.assert_blob_integrity(
                        env.transform_bucket,
                        gcs_blob_name_from_path(release.transform_traffic_path),
                        release.transform_traffic_path,
                    )
                    self.assert_blob_integrity(
                        env.transform_bucket,
                        gcs_blob_name_from_path(release.transform_sales_path),
                        release.transform_sales_path,
                    )

                    # Test that files correctly moved to "finished"
                    for file in release.sftp_files:
                        file_name = os.path.basename(file)
                        in_progress_file = os.path.join(sftp_folders.in_progress, file_name)
                        self.assertFalse(os.path.isfile(in_progress_file))

                        finished_file = os.path.join(sftp_folders.finished, file_name)
                        self.assertTrue(os.path.isfile(finished_file))

                    # Test that data loaded into BigQuery
                    table_id = bq_table_id(
                        env.cloud_workspace.project_id,
                        sales_partner.bq_dataset_id,
                        sales_partner.bq_table_name,
                    )
                    self.assert_table_integrity(table_id, params["bq_rows"])
                    table_id = bq_table_id(
                        env.cloud_workspace.project_id,
                        traffic_partner.bq_dataset_id,
                        traffic_partner.bq_table_name,
                    )
                    self.assert_table_integrity(table_id, params["bq_rows"])

                    # Add_dataset_release_task
                    dataset_releases = get_dataset_releases(dag_id="google_books_test", dataset_id="google_books")
                    self.assertEqual(len(dataset_releases), 0)
                    ti = env.run_task("add_new_dataset_releases")
                    self.assertEqual(ti.state, State.SUCCESS)
                    dataset_releases = get_dataset_releases(dag_id="google_books_test", dataset_id="google_books")
                    self.assertEqual(len(dataset_releases), 1)

                    # Test cleanup
                    workflow_folder_path = release.workflow_folder
                    ti = env.run_task("cleanup_workflow")
                    self.assertEqual(ti.state, State.SUCCESS)
                    self.assert_cleanup(workflow_folder_path)

    def test_gb_transform(self):
        """Test sanity check in transform method when transaction date falls outside release month"""
        with CliRunner().isolated_filesystem():

            # Files and folders
            transform_dir = os.path.join(os.getcwd(), "transform")
            os.makedirs(transform_dir)
            fixtures_folder = test_fixtures_folder(workflow_module="google_books_telescope")
            sales_file_path = os.path.join(fixtures_folder, "GoogleSalesTransactionReport_2020_02.csv")
            traffic_file_path = os.path.join(fixtures_folder, "GoogleBooksTrafficReport_2020_02.csv")
            transform_sales_path = os.path.join(transform_dir, "GoogleSalesTransactionReport_2020_02.csv")
            transform_traffic_path = os.path.join(transform_dir, "GoogleBooksTrafficReport_2020_02.csv")

            # test transaction date inside of release month
            gb_transform(
                [sales_file_path, traffic_file_path],
                transform_sales_path,
                transform_traffic_path,
                pendulum.parse("2020-02-01"),
            )
            self.assertTrue(os.path.exists(transform_sales_path))
            self.assertTrue(os.path.exists(transform_traffic_path))

            # test transaction date before release month
            with self.assertRaises(AirflowException):
                gb_transform(
                    [sales_file_path, traffic_file_path],
                    transform_sales_path,
                    transform_traffic_path,
                    pendulum.parse("2020-01-31"),
                )

            # test transaction date after release month
            with self.assertRaises(AirflowException):
                gb_transform(
                    [sales_file_path, traffic_file_path],
                    transform_sales_path,
                    transform_traffic_path,
                    pendulum.parse("2020-03-01"),
                )
