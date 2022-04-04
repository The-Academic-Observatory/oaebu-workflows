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
import shutil
from collections import defaultdict
from unittest.mock import patch

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from click.testing import CliRunner

import observatory.api.server.orm as orm
from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.google_books_telescope import (
    GoogleBooksRelease,
    GoogleBooksTelescope,
)
from oaebu_workflows.identifiers import TelescopeTypes
from observatory.api.client.model.organisation import Organisation
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    SftpServer,
    module_file_path,
)
from observatory.platform.utils.workflow_utils import SftpFolders
from observatory.platform.utils.workflow_utils import blob_name, table_ids_from_path
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.telescope import Telescope
from observatory.api.client.model.telescope_type import TelescopeType
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.table_type import TableType
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.airflow_utils import AirflowConns
from airflow.models import Connection
from airflow.utils.state import State


class TestGoogleBooksTelescope(ObservatoryTestCase):
    """Tests for the GoogleBooks telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestGoogleBooksTelescope, self).__init__(*args, **kwargs)
        self.host = "localhost"
        self.api_port = 5000
        self.sftp_port = 3373
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.organisation_name = "anu-press"
        self.organisation_folder = "anu-press"

        # API environment
        self.host = "localhost"
        self.port = 5001
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = self.organisation_name

    def setup_api(self):
        dt = pendulum.now("UTC")

        name = "Google Books Telescope"
        telescope_type = TelescopeType(name=name, type_id=GoogleBooksTelescope.DAG_ID_PREFIX)
        self.api.put_telescope_type(telescope_type)

        organisation = Organisation(
            name=self.org_name,
            project_id="project",
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
        )
        self.api.put_organisation(organisation)

        telescope = Telescope(
            name=name,
            telescope_type=TelescopeType(id=1),
            organisation=Organisation(id=1),
            extra={},
        )
        self.api.put_telescope(telescope)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            type_id=GoogleBooksTelescope.DAG_ID_PREFIX,
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="Google Books Dataset",
            address="project.dataset.table",
            service="bigquery",
            connection=Telescope(id=1),
            dataset_type=DatasetType(id=1),
        )
        self.api.put_dataset(dataset)

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.port}")
        env.add_connection(conn)

    def test_dag_structure(self):
        """Test that the Google Books DAG has the correct structure.
        :return: None
        """

        organisation = Organisation(name=self.organisation_name)
        dag = GoogleBooksTelescope(organisation, accounts=None).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["list_release_info"],
                "list_release_info": ["move_files_to_in_progress"],
                "move_files_to_in_progress": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load_partition"],
                "bq_load_partition": ["move_files_to_finished"],
                "move_files_to_finished": ["cleanup"],
                "cleanup": ["add_new_dataset_releases"],
                "add_new_dataset_releases": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Google Books DAG can be loaded from a DAG bag.
        :return: None
        """
        # Run tests both for telescope with file suffixes and without
        for accounts in [None, {"accounts": ["foo", "bar"]}]:
            with self.subTest(accounts=accounts):
                env = ObservatoryEnvironment(
                    self.project_id, self.data_location, api_host=self.host, api_port=self.port
                )
                with env.create():
                    self.setup_connections(env)
                    self.setup_api()
                    dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "google_books_telescope.py")
                    self.assert_dag_load("google_books_anu-press", dag_file)

    def test_telescope(self):
        """Test the Google Books telescope end to end.

        :return: None.
        """
        params = [
            {
                "accounts": None,
                "no_download_files": 2,
                "bq_rows": 4,
                "traffic_download_hash": ["db4dca44d5231e0c4e2ad95db41b79b6"],
                "traffic_transform_hash": "b8073007",
                "sales_download_hash": ["6496518be1ea73694d0a8f89c0b42f20"],
                "sales_transform_hash": "ebe49987",
                "test_files": {
                    "GoogleBooksTrafficReport_2020_02.csv": test_fixtures_folder(
                        "google_books", "GoogleBooksTrafficReport_2020_02.csv"
                    ),
                    "GoogleSalesTransactionReport_2020_02.csv": test_fixtures_folder(
                        "google_books", "GoogleSalesTransactionReport_2020_02.csv"
                    ),
                },
            },
            {
                "accounts": ["foo", "bar"],
                "no_download_files": 4,
                "bq_rows": 8,
                "traffic_download_hash": ["bea9ad67b4b5c20dac38421090941482", "db4dca44d5231e0c4e2ad95db41b79b6"],
                "traffic_transform_hash": "cca664c2",
                "sales_download_hash": ["d7f61bf2dc44a6c0104f15b0ef588815", "6496518be1ea73694d0a8f89c0b42f20"],
                "sales_transform_hash": "cbeae337",
                "test_files": {
                    "GoogleBooksTrafficReport_foo2020_02.csv": test_fixtures_folder(
                        "google_books", "GoogleBooksTrafficReport_foo2020_02.csv"
                    ),
                    "GoogleBooksTrafficReport_bar2020_02.csv": test_fixtures_folder(
                        "google_books", "GoogleBooksTrafficReport_bar2020_02.csv"
                    ),
                    "GoogleSalesTransactionReport_foo2020_02.csv": test_fixtures_folder(
                        "google_books", "GoogleSalesTransactionReport_foo2020_02.csv"
                    ),
                    "GoogleSalesTransactionReport_bar2020_02.csv": test_fixtures_folder(
                        "google_books", "GoogleSalesTransactionReport_bar2020_02.csv"
                    ),
                    "GoogleSalesTransactionReport_foo2020_03.csv": test_fixtures_folder(
                        "google_books", "GoogleSalesTransactionReport_foo2020_03.csv"
                    ),
                },
            },
        ]
        # Run tests both for telescope with file suffixes and without
        for setup in params:
            with self.subTest(setup=setup):
                # Setup Observatory environment
                env = ObservatoryEnvironment(
                    self.project_id, self.data_location, api_host=self.host, api_port=self.port
                )
                sftp_server = SftpServer(host=self.host, port=self.sftp_port)
                dataset_id = env.add_dataset()

                # Create the Observatory environment and run tests
                with env.create():
                    self.setup_connections(env)
                    self.setup_api()
                    with sftp_server.create() as sftp_root:
                        # Setup Telescope
                        execution_date = pendulum.datetime(year=2021, month=3, day=31)
                        org = Organisation(
                            name=self.organisation_name,
                            project_id=self.project_id,
                            download_bucket=env.download_bucket,
                            transform_bucket=env.transform_bucket,
                        )
                        telescope = GoogleBooksTelescope(
                            org, accounts=setup["accounts"], dataset_id=dataset_id, workflow_id=1
                        )
                        dag = telescope.make_dag()

                        # Add SFTP connection
                        conn = Connection(
                            conn_id=AirflowConns.SFTP_SERVICE, uri=f"ssh://:password@{self.host}:{self.sftp_port}"
                        )
                        env.add_connection(conn)
                        with env.create_dag_run(dag, execution_date):
                            # Test that all dependencies are specified: no error should be thrown
                            env.run_task(telescope.check_dependencies.__name__)

                            # Add file to SFTP server
                            local_sftp_folders = SftpFolders(telescope.dag_id, self.organisation_name, sftp_root)
                            os.makedirs(local_sftp_folders.upload, exist_ok=True)
                            for file_name, file_path in setup["test_files"].items():
                                upload_file = os.path.join(local_sftp_folders.upload, file_name)
                                shutil.copy(file_path, upload_file)

                            # Check that the correct release info is returned via Xcom
                            ti = env.run_task(telescope.list_release_info.__name__)
                            release_info = ti.xcom_pull(
                                key=GoogleBooksTelescope.RELEASE_INFO,
                                task_ids=telescope.list_release_info.__name__,
                                include_prior_dates=False,
                            )

                            # Get release info from SFTP server and create expected release info
                            expected_release_info = defaultdict(list)
                            for file_name, file_path in setup["test_files"].items():
                                expected_release_date = pendulum.from_format(
                                    file_name[-11:].strip(".csv"), "YYYY_MM"
                                ).end_of("month")
                                release_date_str = expected_release_date.format("YYYYMMDD")
                                if release_date_str == "20200229":
                                    expected_release_file = os.path.join(telescope.sftp_folders.in_progress, file_name)
                                    expected_release_info[release_date_str].append(expected_release_file)
                            self.assertTrue(1, len(release_info))
                            self.assertEqual(expected_release_info["20200229"].sort(), release_info["20200229"].sort())

                            # use release info for other tasks
                            releases = []
                            for release_date, sftp_files in release_info.items():
                                releases.append(
                                    GoogleBooksRelease(
                                        telescope.dag_id,
                                        pendulum.parse(release_date),
                                        sftp_files,
                                        telescope.sftp_regex,
                                        org,
                                    )
                                )

                            # Test move file to in progress
                            env.run_task(telescope.move_files_to_in_progress.__name__)
                            for release in releases:
                                for file in release.sftp_files:
                                    file_name = os.path.basename(file)
                                    upload_file = os.path.join(local_sftp_folders.upload, file_name)
                                    self.assertFalse(os.path.isfile(upload_file))

                                    in_progress_file = os.path.join(local_sftp_folders.in_progress, file_name)
                                    self.assertTrue(os.path.isfile(in_progress_file))

                            # Test download
                            env.run_task(telescope.download.__name__)
                            for release in releases:
                                self.assertEqual(setup["no_download_files"], len(release.download_files))
                                files = release.download_files
                                files.sort()
                                traffic_count = 0
                                sales_count = 0
                                for file in files:
                                    if "Traffic" in file:
                                        expected_file_hash = setup["traffic_download_hash"][traffic_count]
                                        traffic_count += 1
                                    else:
                                        expected_file_hash = setup["sales_download_hash"][sales_count]
                                        sales_count += 1
                                    self.assert_file_integrity(file, expected_file_hash, "md5")

                            # Test upload downloaded
                            env.run_task(telescope.upload_downloaded.__name__)
                            for release in releases:
                                for file in release.download_files:
                                    self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

                            # Test that file transformed
                            env.run_task(telescope.transform.__name__)
                            for release in releases:
                                self.assertEqual(2, len(release.transform_files))
                                for file in release.transform_files:
                                    if "traffic" in file:
                                        expected_file_hash = setup["traffic_transform_hash"]
                                    else:
                                        expected_file_hash = setup["sales_transform_hash"]
                                    self.assert_file_integrity(file, expected_file_hash, "gzip_crc")

                            # Test that transformed file uploaded
                            env.run_task(telescope.upload_transformed.__name__)
                            for release in releases:
                                for file in release.transform_files:
                                    self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                            # Test that data loaded into BigQuery
                            env.run_task(telescope.bq_load_partition.__name__)
                            for release in releases:
                                for file in release.transform_files:
                                    table_id, _ = table_ids_from_path(file)
                                    table_id = f'{self.project_id}.{dataset_id}.{table_id}${release.release_date.strftime("%Y%m")}'
                                    expected_rows = setup["bq_rows"]
                                    self.assert_table_integrity(table_id, expected_rows)

                            # Test move files to finished
                            env.run_task(telescope.move_files_to_finished.__name__)
                            for release in releases:
                                for file in release.sftp_files:
                                    file_name = os.path.basename(file)
                                    in_progress_file = os.path.join(local_sftp_folders.in_progress, file_name)
                                    self.assertFalse(os.path.isfile(in_progress_file))

                                    finished_file = os.path.join(local_sftp_folders.finished, file_name)
                                    self.assertTrue(os.path.isfile(finished_file))

                            # Test cleanup
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

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_transform(self, mock_variable_get):
        """Test sanity check in transform method when transaction date falls outside release month

        :param mock_variable_get: Mock Airflow Variable 'data'
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = os.path.join(os.getcwd(), "data")

            # Objects to create release instance
            org = Organisation(
                name=self.organisation_name,
                project_id=self.project_id,
                download_bucket="download_bucket",
                transform_bucket="transform_bucket",
            )
            telescope = GoogleBooksTelescope(org, accounts=None, dataset_id="dataset_id")
            file_path = test_fixtures_folder("google_books", "GoogleSalesTransactionReport_2020_02.csv")
            file_name = os.path.basename(file_path)
            release_files = [os.path.join(telescope.sftp_folders.in_progress, file_name)]

            # test transaction date inside of release month
            release = GoogleBooksRelease(
                telescope.dag_id, pendulum.parse("2020-02-01"), release_files, telescope.sftp_regex, org
            )
            shutil.copy(file_path, os.path.join(release.download_folder, "GoogleSalesTransactionReport_2020_02.csv"))
            release.transform()
            self.assertEqual(1, len(release.transform_files))

            # test transaction date before release month
            release = GoogleBooksRelease(
                telescope.dag_id, pendulum.parse("2020-01-31"), release_files, telescope.sftp_regex, org
            )
            shutil.copy(file_path, os.path.join(release.download_folder, "GoogleSalesTransactionReport_2020_02.csv"))
            with self.assertRaises(AirflowException):
                release.transform()

            # test transaction date after release month
            release = GoogleBooksRelease(
                telescope.dag_id, pendulum.parse("2020-03-01"), release_files, telescope.sftp_regex, org
            )
            shutil.copy(file_path, os.path.join(release.download_folder, "GoogleSalesTransactionReport_2020_02.csv"))
            with self.assertRaises(AirflowException):
                release.transform()
