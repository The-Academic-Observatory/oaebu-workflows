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

# Author: Aniek Roelofs, Tuan Chien

import logging
import os
from unittest.mock import patch

import pendulum
import vcr
import httpretty
from airflow.exceptions import AirflowException
from click.testing import CliRunner
from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.oapen_metadata_telescope import (
    OapenMetadataRelease,
    OapenMetadataTelescope,
)
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.utils.workflow_utils import blob_name


class TestOapenMetadataTelescope(ObservatoryTestCase):
    """Tests for the Oapen Metadata telescope DAG"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        # Paths
        self.download_path = os.path.join(test_fixtures_folder(), "oapen_metadata", "oapen_metadata_2021-02-19.yaml")

    def setup_environment(self) -> ObservatoryEnvironment:
        """Setup observatory environment"""
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        self.dataset_id = env.add_dataset()
        return env

    def test_dag_structure(self):
        """Test that the Oapen Metadata DAG has the correct structure.

        :return: None
        """
        dag = OapenMetadataTelescope().make_dag()
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
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the OapenMetadata DAG can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create():
            dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "oapen_metadata_telescope.py")
            self.assert_dag_load("oapen_metadata", dag_file)

    def test_telescope(self):
        """Test telescope task execution."""

        env = self.setup_environment()
        telescope = OapenMetadataTelescope(dataset_id=self.dataset_id)
        dag = telescope.make_dag()
        execution_date = pendulum.parse("2021-02-12")
        start_date = pendulum.datetime(2018, 5, 14)
        end_date = pendulum.datetime(2021, 2, 13)
        release = OapenMetadataRelease(
            dag_id="oapen_metadata", start_date=start_date, end_date=end_date, first_release=True
        )

        with env.create():
            with env.create_dag_run(dag, execution_date):
                with CliRunner().isolated_filesystem():
                    # Test that all dependencies are specified: no error should be thrown
                    env.run_task(telescope.check_dependencies.__name__)

                    # Test download
                    with vcr.use_cassette(self.download_path):
                        env.run_task(telescope.download.__name__)
                    self.assertEqual(len(release.download_files), 1)

                    # Test upload_downloaded
                    env.run_task(telescope.upload_downloaded.__name__)
                    for file in release.download_files:
                        self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

                    # Test download
                    env.run_task(telescope.transform.__name__)
                    self.assertEqual(len(release.transform_files), 1)

                    # Test upload_transformed
                    env.run_task(telescope.upload_transformed.__name__)

                    for file in release.transform_files:
                        self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                    # Test bq_load partition
                    ti = env.run_task(telescope.bq_load_partition.__name__)
                    self.assertEqual(ti.state, "skipped")

                    # Test delete old task is skipped for the first release
                    ti = env.run_task(telescope.bq_delete_old.__name__)
                    self.assertEqual(ti.state, "skipped")

                    # Test bq_append_new
                    env.run_task(telescope.bq_append_new.__name__)
                    table_id = f"{self.project_id}.{telescope.dataset_id}.metadata"
                    expected_rows = 15310
                    self.assert_table_integrity(table_id, expected_rows)

                    # Test cleanup
                    download_folder, extract_folder, transform_folder = (
                        release.download_folder,
                        release.extract_folder,
                        release.transform_folder,
                    )

                    env.run_task(telescope.cleanup.__name__)
                    self.assert_cleanup(download_folder, extract_folder, transform_folder)

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_download(self, mock_variable_get):
        """Download release and check exception is raised when response is not 200 or csv is empty.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return:
        """
        start_date = pendulum.datetime(2020, 1, 1)
        end_date = pendulum.datetime(2020, 1, 31)
        release = OapenMetadataRelease("oapen_metadata", start_date, end_date, False)

        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = "data"

            # Test exception is raised for invalid status code
            with httpretty.enabled():
                httpretty.register_uri(httpretty.GET, OapenMetadataTelescope.CSV_URL, status=400)

                with self.assertRaises(AirflowException):
                    release.download()

            # Test exception is raised for empty csv file
            with httpretty.enabled():
                empty_csv = "Column1,Column2"
                httpretty.register_uri(httpretty.GET, OapenMetadataTelescope.CSV_URL, body=empty_csv)

                with self.assertRaises(AirflowException):
                    release.download()
