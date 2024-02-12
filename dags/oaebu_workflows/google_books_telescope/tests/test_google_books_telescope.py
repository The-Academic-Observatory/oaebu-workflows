# Copyright 2020-2023 Curtin University
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
import shutil
from collections import defaultdict
from unittest.mock import patch

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.utils.state import State
from click.testing import CliRunner

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.oaebu_partners import partner_from_str
from oaebu_workflows.google_books_telescope.google_books_telescope import (
    GoogleBooksRelease,
    GoogleBooksTelescope,
)
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    SftpServer,
    find_free_port,
    random_id,
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
        dag = GoogleBooksTelescope(
            dag_id="test_dag", cloud_workspace=self.fake_cloud_workspace, sftp_root="/"
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
        """Test that the Google Books DAG can be loaded from a DAG bag."""
        # Run tests both for telescope with file suffixes and without
        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id="google_books",
                    name="My Google Books Telescope",
                    class_name="oaebu_workflows.google_books_telescope.google_books_telescope.GoogleBooksTelescope",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )
        with env.create():
            self.assert_dag_load_from_config("google_books")

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
                telescope = GoogleBooksTelescope(
                    dag_id="google_books_test",
                    cloud_workspace=env.cloud_workspace,
                    sftp_root="/",
                    sales_partner=sales_partner,
                    traffic_partner=traffic_partner,
                )
                dag = telescope.make_dag()

                # Add SFTP connection
                conn = Connection(
                    conn_id=telescope.sftp_service_conn_id, uri=f"ssh://:password@localhost:{self.sftp_port}"
                )
                env.add_connection(conn)
                with env.create_dag_run(dag, execution_date):
                    # Test that all dependencies are specified: no error should be thrown
                    ti = env.run_task(telescope.check_dependencies.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)

                    # Add file to SFTP server
                    local_sftp_folders = SftpFolders(telescope.dag_id, telescope.sftp_service_conn_id, sftp_root)
                    os.makedirs(local_sftp_folders.upload, exist_ok=True)
                    for file_name, file_path in params["test_files"].items():
                        upload_file = os.path.join(local_sftp_folders.upload, file_name)
                        shutil.copy(file_path, upload_file)

                    # Check that the correct release info is returned via Xcom
                    ti = env.run_task(telescope.list_release_info.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    release_info = ti.xcom_pull(
                        key=GoogleBooksTelescope.RELEASE_INFO,
                        task_ids=telescope.list_release_info.__name__,
                        include_prior_dates=False,
                    )

                    # Get release info from SFTP server and create expected release info
                    expected_release_info = defaultdict(list)
                    for file_name, file_path in params["test_files"].items():
                        expected_release_date = pendulum.from_format(file_name[-11:].strip(".csv"), "YYYY_MM").end_of(
                            "month"
                        )
                        release_date_str = expected_release_date.format("YYYYMMDD")
                        if release_date_str == "20200229":
                            expected_release_file = os.path.join(telescope.sftp_folders.in_progress, file_name)
                            expected_release_info[release_date_str].append(expected_release_file)
                    self.assertTrue(1, len(release_info))
                    self.assertEqual(expected_release_info["20200229"].sort(), release_info["20200229"].sort())

                    # Use release info for other tasks
                    releases = []
                    for release_date, sftp_files in release_info.items():
                        releases.append(
                            GoogleBooksRelease(
                                dag_id=telescope.dag_id,
                                run_id=env.dag_run.run_id,
                                partition_date=pendulum.parse(release_date),
                                sftp_files=sftp_files,
                            )
                        )
                    self.assertTrue(1, len(releases))
                    release = releases[0]

                    # Test move file to in progress
                    ti = env.run_task(telescope.move_files_to_in_progress.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    for file in release.sftp_files:
                        file_name = os.path.basename(file)
                        upload_file = os.path.join(local_sftp_folders.upload, file_name)
                        self.assertFalse(os.path.isfile(upload_file))
                        in_progress_file = os.path.join(local_sftp_folders.in_progress, file_name)
                        self.assertTrue(os.path.isfile(in_progress_file))

                    # Run main telescope tasks
                    ti = env.run_task(telescope.download.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    ti = env.run_task(telescope.upload_downloaded.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    ti = env.run_task(telescope.transform.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    ti = env.run_task(telescope.upload_transformed.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    ti = env.run_task(telescope.bq_load.__name__)
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

                    # Test that data loaded into BigQuery
                    table_id = bq_table_id(
                        telescope.cloud_workspace.project_id,
                        telescope.sales_partner.bq_dataset_id,
                        telescope.sales_partner.bq_table_name,
                    )
                    self.assert_table_integrity(table_id, params["bq_rows"])
                    table_id = bq_table_id(
                        telescope.cloud_workspace.project_id,
                        telescope.traffic_partner.bq_dataset_id,
                        telescope.traffic_partner.bq_table_name,
                    )
                    self.assert_table_integrity(table_id, params["bq_rows"])

                    # Test move files to finished
                    ti = env.run_task(telescope.move_files_to_finished.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    for file in release.sftp_files:
                        file_name = os.path.basename(file)
                        in_progress_file = os.path.join(local_sftp_folders.in_progress, file_name)
                        self.assertFalse(os.path.isfile(in_progress_file))

                        finished_file = os.path.join(local_sftp_folders.finished, file_name)
                        self.assertTrue(os.path.isfile(finished_file))

                    # Add_dataset_release_task
                    dataset_releases = get_dataset_releases(
                        dag_id=telescope.dag_id, dataset_id=telescope.api_dataset_id
                    )
                    self.assertEqual(len(dataset_releases), 0)
                    ti = env.run_task(telescope.add_new_dataset_releases.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    dataset_releases = get_dataset_releases(
                        dag_id=telescope.dag_id, dataset_id=telescope.api_dataset_id
                    )
                    self.assertEqual(len(dataset_releases), 1)

                    # Test cleanup
                    ti = env.run_task(telescope.cleanup.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    self.assert_cleanup(release.workflow_folder)

    @patch("observatory.platform.airflow.Variable.get")
    def test_gb_transform(self, mock_variable_get):
        """Test sanity check in transform method when transaction date falls outside release month

        :param mock_variable_get: Mock Airflow Variable 'data'
        """
        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = os.path.join(os.getcwd(), "data")

            # Objects to create release instance
            telescope = GoogleBooksTelescope(
                dag_id="google_books_test",
                cloud_workspace=self.fake_cloud_workspace,
                sftp_root="/",
                sales_partner=partner_from_str("google_books_sales"),
                traffic_partner=partner_from_str("google_books_traffic"),
            )
            fixtures_folder = test_fixtures_folder(workflow_module="google_books_telescope")
            sales_file_path = os.path.join(fixtures_folder, "GoogleSalesTransactionReport_2020_02.csv")
            traffic_file_path = os.path.join(fixtures_folder, "GoogleBooksTrafficReport_2020_02.csv")
            sftp_files = [
                os.path.join(telescope.sftp_folders.in_progress, os.path.basename(sales_file_path)),
                os.path.join(telescope.sftp_folders.in_progress, os.path.basename(traffic_file_path)),
            ]

            # test transaction date inside of release month
            release = GoogleBooksRelease(
                dag_id=telescope.dag_id,
                run_id=random_id(),
                partition_date=pendulum.parse("2020-02-01"),
                sftp_files=sftp_files,
            )
            shutil.copy(sales_file_path, os.path.join(release.download_folder, "google_books_sales.csv"))
            shutil.copy(traffic_file_path, os.path.join(release.download_folder, "google_books_traffic.csv"))
            telescope.transform([release])
            self.assertTrue(os.path.exists(release.transform_sales_path))

            # test transaction date before release month
            release = GoogleBooksRelease(
                dag_id=telescope.dag_id,
                run_id=random_id(),
                partition_date=pendulum.parse("2020-01-31"),
                sftp_files=sftp_files,
            )
            shutil.copy(sales_file_path, os.path.join(release.download_folder, "google_books_sales.csv"))
            shutil.copy(traffic_file_path, os.path.join(release.download_folder, "google_books_traffic.csv"))
            with self.assertRaises(AirflowException):
                telescope.transform([release])

            # test transaction date after release month
            release = GoogleBooksRelease(
                dag_id=telescope.dag_id,
                run_id=random_id(),
                partition_date=pendulum.parse("2020-03-01"),
                sftp_files=sftp_files,
            )
            shutil.copy(sales_file_path, os.path.join(release.download_folder, "google_books_sales.csv"))
            shutil.copy(traffic_file_path, os.path.join(release.download_folder, "google_books_traffic.csv"))
            with self.assertRaises(AirflowException):
                telescope.transform([release])
