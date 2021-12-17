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

import os
from datetime import timedelta
from unittest.mock import patch

import httpretty
import pendulum
from airflow.exceptions import AirflowException
from click.testing import CliRunner
from google.cloud import bigquery
from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.oapen_metadata_telescope import (
    OapenMetadataRelease,
    OapenMetadataTelescope,
    convert,
    transform_dict,
)
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.utils.workflow_utils import (
    blob_name,
    create_date_table_id,
    table_ids_from_path,
)


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

        self.first_download_path = test_fixtures_folder("oapen_metadata", "oapen_metadata1.csv")
        self.first_execution_date = pendulum.datetime(year=2021, month=2, day=1)

        self.second_download_path = test_fixtures_folder("oapen_metadata", "oapen_metadata2.csv")
        self.second_execution_date = pendulum.datetime(year=2021, month=2, day=7)

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

        with env.create(task_logging=True):
            # first run
            with env.create_dag_run(dag, self.first_execution_date) as first_dagrun:
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                start_date, end_date, first_release = telescope.get_release_info(
                    execution_date=self.first_execution_date,
                    dag_run=first_dagrun,
                    dag=dag,
                    next_execution_date=self.second_execution_date,
                )

                # Use release info for other tasks
                release = OapenMetadataRelease(telescope.dag_id, start_date, end_date, first_release)

                # Test download task
                with httpretty.enabled():
                    self.setup_mock_file_download(OapenMetadataTelescope.CSV_URL, self.first_download_path)
                    env.run_task(telescope.download.__name__)

                self.assertEqual(1, len(release.download_files))
                download_path = release.download_files[0]
                expected_file_hash = "735584bffe046b9e073b6a63518a4044"
                self.assert_file_integrity(download_path, expected_file_hash, "md5")

                # Test that file uploaded
                env.run_task(telescope.upload_downloaded.__name__)
                self.assert_blob_integrity(env.download_bucket, blob_name(download_path), download_path)

                # Test that file transformed
                env.run_task(telescope.transform.__name__)

                self.assertEqual(1, len(release.transform_files))
                transform_path = release.transform_files[0]
                expected_file_hash = "fbf31c05"
                self.assert_file_integrity(transform_path, expected_file_hash, "gzip_crc")

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                self.assert_blob_integrity(env.transform_bucket, blob_name(transform_path), transform_path)

                # Test that load partition task is skipped for the first release
                ti = env.run_task(telescope.bq_load_partition.__name__)
                self.assertEqual(ti.state, "skipped")

                # Test delete old task is skipped for the first release
                with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                    ti = env.run_task(telescope.bq_delete_old.__name__)
                self.assertEqual(ti.state, "skipped")

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

                # second run
            with env.create_dag_run(dag, self.second_execution_date) as second_dagrun:
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                start_date, end_date, first_release = telescope.get_release_info(
                    execution_date=self.second_execution_date,
                    dag_run=second_dagrun,
                    dag=dag,
                    next_execution_date=pendulum.datetime(2021, 2, 14),
                )

                self.assertEqual(release.end_date + timedelta(days=1), start_date)
                self.assertEqual(pendulum.today("UTC") - timedelta(days=1), end_date)
                self.assertFalse(first_release)

                # use release info for other tasks
                release = OapenMetadataRelease(telescope.dag_id, start_date, end_date, first_release)

                # Test download task
                with httpretty.enabled():
                    self.setup_mock_file_download(OapenMetadataTelescope.CSV_URL, self.second_download_path)
                    env.run_task(telescope.download.__name__)

                self.assertEqual(1, len(release.download_files))
                download_path = release.download_files[0]
                expected_file_hash = "331b0dbe0b0e8c0e166015d15a2954d0"
                self.assert_file_integrity(download_path, expected_file_hash, "md5")

                # Test that file uploaded
                env.run_task(telescope.upload_downloaded.__name__)
                self.assert_blob_integrity(env.download_bucket, blob_name(download_path), download_path)

                # Test that file transformed
                env.run_task(telescope.transform.__name__)

                self.assertEqual(1, len(release.transform_files))
                transform_path = release.transform_files[0]
                expected_file_hash = "ab5c3e9d"
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

    def test_airflow_vars(self):
        """Cover case when airflow_vars is given."""

        telescope = OapenMetadataTelescope(airflow_vars=[AirflowVars.DOWNLOAD_BUCKET])
        self.assertEqual(set(telescope.airflow_vars), {AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET})

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

    def test_transform_dict(self):
        """Check transform_dict handling of invalid case."""
        nested_fields = ["dc.subject.classification"]
        list_fields = ["dc.subject.classification"]
        test_dict = {"field1": [{"1": "value1"}, "2"], "dc.subject.classification": "value1||value2", "field2": None}
        transformed_dict = {
            "field1": [{"1": "value1"}, "2"],
            "dc": {"subject": {"classification": {"value": ["value1", "value2"]}}},
            "field2": None,
        }

        result = transform_dict(test_dict, convert, nested_fields, list_fields)
        self.assertDictEqual(result, transformed_dict)
