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

# Author: Tuan Chien

import hashlib
import os
import unittest
from datetime import timedelta
from unittest.mock import MagicMock, patch
import vcr

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.state import State
from click.testing import CliRunner
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

from oaebu_workflows.config import schema_folder as default_schema_folder
from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.oaebu_partners import OaebuPartner
from oaebu_workflows.workflows.onix_workflow import (
    OnixWorkflow,
    OnixWorkflowRelease,
    CROSSREF_METADATA_URL,
    CROSSREF_EVENT_URL_TEMPLATE,
    CROSSREF_DELETED_EVENT_URL_TEMPLATE,
    CROSSREF_EDITED_EVENT_URL_TEMPLATE,
    download_crossref_events,
    download_crossref_metadata,
    transform_crossref_events,
    transform_event,
    transform_crossref_metadata,
    transform_metadata_item,
    isbns_from_onix,
    dois_from_onix,
    download_crossref_isbn_metadata,
    download_crossref_event_url,
    create_latest_views_from_dataset,
)
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.table_type import TableType
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from oaebu_workflows.seed.dataset_type_info import get_dataset_type_info
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.file_utils import load_jsonl
from observatory.platform.utils.config_utils import find_schema
from observatory.platform.utils.gc_utils import (
    run_bigquery_query,
    upload_files_to_cloud_storage,
    upload_file_to_cloud_storage,
)
from observatory.platform.utils.release_utils import get_dataset_releases
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    Table,
    bq_load_tables,
    make_dummy_dag,
    module_file_path,
    find_free_port,
)
from observatory.platform.utils.test_utils import random_id
from observatory.platform.utils.workflow_utils import (
    bq_load_partition,
    bq_load_shard,
    make_dag_id,
    table_ids_from_path,
)
from observatory.api.utils import seed_table_type, seed_dataset_type
from oaebu_workflows.seed.table_type_info import get_table_type_info
from oaebu_workflows.seed.dataset_type_info import get_dataset_type_info


class TestOnixWorkflowRelease(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        with patch("observatory.platform.utils.gc_utils.select_table_shard_dates") as mock_sel_table_suffixes:
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            self.release = OnixWorkflowRelease(
                dag_id="did",
                release_date=pendulum.datetime(2021, 4, 20),
                onix_release_date=pendulum.datetime(2021, 1, 1),
                gcp_project_id="pid",
                gcp_bucket_name="bucket",
            )

    def test_transform_bucket(self):
        self.assertEqual(self.release.transform_bucket, "bucket")

    def test_transform_folder(self):
        self.assertEqual(self.release.transform_folder, "did/20210420")

    def test_transform_files(self):
        self.assertEqual(
            self.release.transform_files,
            [
                self.release.workslookup_filename,
                self.release.workslookup_errors_filename,
                self.release.worksfamilylookup_filename,
            ],
        )

    def test_download_bucket(self):
        self.assertEqual(self.release.download_bucket, str())

    def test_download_files(self):
        self.assertEqual(self.release.download_files, list())

    def test_extract_files(self):
        self.assertEqual(self.release.extract_files, list())

    def test_download_folder(self):
        self.assertEqual(self.release.download_folder, str())

    def test_extract_folder(self):
        self.assertEqual(self.release.extract_folder, str())


class TestOnixWorkflow(ObservatoryTestCase):
    """
    Test the OnixWorkflow class.
    """

    onix_data = [
        {
            "ISBN13": "111",
            "RelatedWorks": [
                {
                    "WorkRelationCode": "Manifestation of",
                    "WorkIdentifiers": [
                        {"WorkIDType": "ISBN-13", "IDValue": "112"},
                    ],
                },
                {
                    "WorkRelationCode": "Manifestation of",
                    "WorkIdentifiers": [
                        {"WorkIDType": "ISBN-13", "IDValue": "113"},
                    ],
                },
            ],
            "RelatedProducts": [
                {"ProductRelationCodes": ["Replaces", "something random"], "ISBN13": "211"},
            ],
        },
        {
            "ISBN13": "112",
            "RelatedWorks": [
                {
                    "WorkRelationCode": "Manifestation of",
                    "WorkIdentifiers": [
                        {"WorkIDType": "ISBN-13", "IDValue": "112"},
                    ],
                },
            ],
            "RelatedProducts": [],
        },
        {
            "ISBN13": "211",
            "RelatedWorks": [
                {
                    "WorkRelationCode": "Manifestation of",
                    "WorkIdentifiers": [
                        {"WorkIDType": "ISBN-13", "IDValue": "211"},
                    ],
                },
            ],
            "RelatedProducts": [],
        },
    ]

    class MockTelescopeResponse:
        def __init__(self):
            self.organisation = Organisation(
                name="test",
                project_id="project_id",
            )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.telescope = TestOnixWorkflow.MockTelescopeResponse()
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.bucket_name = "bucket_name"

        # API environment
        self.host = "localhost"
        self.port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)

    def seed_db(self):
        table_type_info = get_table_type_info()
        seed_table_type(table_type_info=table_type_info, api=self.api)

        dataset_type_info = get_dataset_type_info(self.api)
        seed_dataset_type(api=self.api, dataset_type_info=dataset_type_info)

        @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
        @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
        @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
        def test_ctor_gen_dag_id(self, mock_sel_table_suffixes, mock_mr, mock_api):
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            mock_api.return_value = self.api

            with self.env.create():
                self.seed_db()
                with CliRunner().isolated_filesystem():
                    mock_mr.return_value = OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="",
                        onix_table_id="onix",
                    )
                    wf = OnixWorkflow(
                        org_name=self.telescope.organisation.name,
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                    )

                    release = wf.make_release(execution_date=pendulum.datetime(2021, 1, 1))
                    self.assertEqual(wf.dag_id, "onix_workflow_test")
                    self.assertEqual(
                        release.workslookup_filename, "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz"
                    )
                    self.assertEqual(
                        release.workslookup_errors_filename,
                        "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz",
                    )
                    self.assertEqual(
                        release.worksfamilylookup_filename,
                        "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz",
                    )

                    self.assertEqual(release.onix_table_id, "onix")
                    self.assertEqual(release.release_date, pendulum.datetime(2021, 1, 1, 0, 0, 0, 0))
                    self.assertEqual(release.project_id, "project_id")
                    self.assertEqual(release.transform_bucket, "bucket_name")
                    self.assertIsInstance(wf.operators[0][0], BaseSensorOperator)

        @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
        @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
        @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
        def test_ctor_gen_assign_dag_id(self, mock_sel_table_suffixes, mock_mr, mock_api):
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            mock_api.return_value = self.api

            with self.env.create():
                self.seed_db()
                with CliRunner().isolated_filesystem():
                    wf = OnixWorkflow(
                        org_name=self.telescope.organisation.name,
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        dag_id="dagid",
                    )

                    mock_mr.return_value = OnixWorkflowRelease(
                        dag_id="dagid",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="",
                        onix_table_id="onix",
                    )

                    release = wf.make_release(execution_date=pendulum.datetime(2021, 1, 1))

                    self.assertEqual(release.dag_id, "dagid")
                    self.assertEqual(release.workslookup_filename, "dagid/20210101/onix_workid_isbn.jsonl.gz")
                    self.assertEqual(
                        release.workslookup_errors_filename, "dagid/20210101/onix_workid_isbn_errors.jsonl.gz"
                    )
                    self.assertEqual(
                        release.worksfamilylookup_filename, "dagid/20210101/onix_workfamilyid_isbn.jsonl.gz"
                    )

                    self.assertEqual(release.onix_table_id, "onix")
                    self.assertEqual(release.release_date, pendulum.datetime(2021, 1, 1, 0, 0, 0, 0))
                    self.assertEqual(release.project_id, "project_id")
                    self.assertEqual(release.transform_bucket, "bucket_name")
                    self.assertIsInstance(wf.operators[0][0], BaseSensorOperator)

        @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
        @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
        @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
        def test_ctor(self, mock_sel_table_suffixes, mock_mr, mock_api):
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            mock_api.return_value = self.api

            with self.env.create():
                self.seed_db()
                with CliRunner().isolated_filesystem():
                    wf = OnixWorkflow(
                        org_name=self.telescope.organisation.name,
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                    )

                    mock_mr.return_value = OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )

                    release = wf.make_release(execution_date=pendulum.datetime(2021, 1, 1))

                    self.assertEqual(wf.dag_id, "onix_workflow_test")
                    self.assertEqual(wf.org_name, "test")
                    self.assertEqual(wf.gcp_bucket_name, "bucket_name")

                    self.assertEqual(release.dag_id, "onix_workflow_test")
                    self.assertEqual(release.release_date, pendulum.datetime(2021, 1, 1))
                    self.assertEqual(release.transform_folder, "onix_workflow_test/20210101")
                    self.assertEqual(release.worksid_table, "onix_workid_isbn")
                    self.assertEqual(release.worksid_error_table, "onix_workid_isbn_errors")
                    self.assertEqual(release.workfamilyid_table, "onix_workfamilyid_isbn")

                    self.assertEqual(
                        release.workslookup_filename, "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz"
                    )
                    self.assertEqual(
                        release.workslookup_errors_filename,
                        "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz",
                    )
                    self.assertEqual(
                        release.worksfamilylookup_filename,
                        "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz",
                    )

                    self.assertEqual(release.workflow_dataset, "onix_workflow")
                    self.assertEqual(release.project_id, "project_id")
                    self.assertEqual(release.onix_dataset_id, "onix")
                    self.assertEqual(release.data_location, "us")
                    self.assertEqual(release.dataset_description, "ONIX workflow tables")

                    self.assertEqual(release.onix_table_id, "onix")

        @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
        @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
        def test_ctor_dataset_overrides(self, mock_sel_table_suffixes, mock_mr):
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            with CliRunner().isolated_filesystem():
                release = OnixWorkflowRelease(
                    dag_id="dag",
                    release_date=pendulum.datetime(2000, 1, 1),
                    onix_release_date=pendulum.datetime(2000, 1, 1),
                    gcp_project_id="project",
                    gcp_bucket_name="bucket",
                )
                self.assertEqual(release.workflow_dataset, "onix_workflow")
                self.assertEqual(release.oaebu_intermediate_dataset, "oaebu_intermediate")
                self.assertEqual(release.oaebu_data_qa_dataset, "oaebu_data_qa")

                # Override
                release = OnixWorkflowRelease(
                    dag_id="dag",
                    release_date=pendulum.datetime(2000, 1, 1),
                    onix_release_date=pendulum.datetime(2000, 1, 1),
                    gcp_project_id="project",
                    gcp_bucket_name="bucket",
                    workflow_dataset="override",
                    oaebu_intermediate_dataset="override",
                    oaebu_data_qa_dataset="override",
                )

                self.assertEqual(release.workflow_dataset, "override")
                self.assertEqual(release.oaebu_intermediate_dataset, "override")
                self.assertEqual(release.oaebu_data_qa_dataset, "override")

        @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
        @patch("oaebu_workflows.workflows.onix_workflow.run_bigquery_query")
        @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
        def test_get_onix_records(self, mock_sel_table_suffixes, mock_bq_query, mock_api):
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            mock_bq_query.return_value = TestOnixWorkflow.onix_data
            mock_api.return_value = self.api

            with self.env.create():
                self.seed_db()
                with CliRunner().isolated_filesystem():
                    wf = OnixWorkflow(
                        org_name=self.telescope.organisation.name,
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                    )
                    records = wf.get_onix_records("project_id", "ds_id", "table_id")
                    self.assertEqual(len(records), 3)
                    self.assertEqual(records[0]["ISBN13"], "111")

        @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
        @patch("oaebu_workflows.workflows.onix_workflow.select_table_shard_dates")
        def test_make_release(self, mock_sel_table_suffixes, mock_api):
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            mock_api.return_value = self.api

            with self.env.create():
                self.seed_db()
                with CliRunner().isolated_filesystem():
                    wf = OnixWorkflow(
                        org_name=self.telescope.organisation.name,
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                    )

                    kwargs = {"next_execution_date": pendulum.datetime(2021, 2, 1)}
                    release = wf.make_release(**kwargs)
                    self.assertIsInstance(release, OnixWorkflowRelease)
                    self.assertEqual(release.dag_id, "onix_workflow_test")
                    self.assertEqual(release.release_date, pendulum.datetime(2021, 1, 31))
                    self.assertEqual(release.onix_release_date, pendulum.datetime(2021, 1, 1))
                    self.assertEqual(release.project_id, "project_id")
                    self.assertEqual(release.onix_dataset_id, "onix")
                    self.assertEqual(release.onix_table_id, "onix")
                    self.assertEqual(release.bucket_name, "bucket_name")

        @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
        @patch("oaebu_workflows.workflows.onix_workflow.select_table_shard_dates")
        def test_make_release_no_releases(self, mock_sel_table_suffixes, mock_api):
            mock_sel_table_suffixes.return_value = []
            mock_api.return_value = self.api

            with self.env.create():
                self.seed_db()
                with CliRunner().isolated_filesystem():
                    wf = OnixWorkflow(
                        org_name=self.telescope.organisation.name,
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                    )

                    with self.assertRaises(AirflowException):
                        kwargs = {"next_execution_date": pendulum.datetime(2021, 2, 1)}
                        wf.make_release(**kwargs)

        @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
        @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
        @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
        def test_cleanup(self, mock_sel_table_suffixes, mock_mr, mock_api):
            mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
            mock_api.return_value = self.api

            with self.env.create():
                self.seed_db()
                with CliRunner().isolated_filesystem():
                    wf = OnixWorkflow(
                        org_name=self.telescope.organisation.name,
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                    )

                    mock_mr.return_value = OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )

                    release = wf.make_release(execution_date=pendulum.datetime(2021, 1, 1))
                    self.assertTrue(os.path.isdir(release.transform_folder))
                    wf.cleanup(release)
                    self.assertFalse(os.path.isdir(release.transform_folder))

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    def test_dag_structure_ignore_google_analytics(self, mock_api):
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)
            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["jstor_country"].type_id,
                    dag_id_prefix="jstor",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="jstor_country",
                    isbn_field_name="isbn",
                    title_field_name="Book_Title",
                    sharded=False,
                ),
                OaebuPartner(
                    dataset_type_id=dataset_type_info["oapen_irus_uk"].type_id,
                    dag_id_prefix="oapen_irus_uk",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="oapen_irus_uk",
                    isbn_field_name="ISBN",
                    title_field_name="book_title",
                    sharded=False,
                ),
                OaebuPartner(
                    dataset_type_id=dataset_type_info["google_books_sales"].type_id,
                    dag_id_prefix="google_books",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="google_books_sales",
                    isbn_field_name="Primary_ISBN",
                    title_field_name="Title",
                    sharded=False,
                ),
                OaebuPartner(
                    dataset_type_id=dataset_type_info["google_books_traffic"].type_id,
                    dag_id_prefix="google_books",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="google_books_traffic",
                    isbn_field_name="Primary_ISBN",
                    title_field_name="Title",
                    sharded=False,
                ),
            ]

            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                dag = wf.make_dag()
                self.assert_dag_structure(
                    {
                        "google_books_test_sensor": ["aggregate_works"],
                        "jstor_test_sensor": ["aggregate_works"],
                        "oapen_irus_uk_test_sensor": ["aggregate_works"],
                        "onix_test_sensor": ["aggregate_works"],
                        "aggregate_works": ["upload_aggregation_tables"],
                        "upload_aggregation_tables": ["bq_load_workid_lookup"],
                        "bq_load_workid_lookup": ["bq_load_workid_lookup_errors"],
                        "bq_load_workid_lookup_errors": ["bq_load_workfamilyid_lookup"],
                        "bq_load_workfamilyid_lookup": ["create_oaebu_crossref_metadata_table"],
                        "create_oaebu_crossref_metadata_table": ["create_oaebu_crossref_events_table"],
                        "create_oaebu_crossref_events_table": ["create_oaebu_book_table"],
                        "create_oaebu_book_table": [
                            "create_oaebu_intermediate_table.dataset.google_books_sales",
                            "create_oaebu_intermediate_table.dataset.oapen_irus_uk",
                            "create_oaebu_intermediate_table.dataset.google_books_traffic",
                            "create_oaebu_intermediate_table.dataset.jstor_country",
                        ],
                        "create_oaebu_intermediate_table.dataset.jstor_country": ["create_oaebu_book_product_table"],
                        "create_oaebu_intermediate_table.dataset.oapen_irus_uk": ["create_oaebu_book_product_table"],
                        "create_oaebu_intermediate_table.dataset.google_books_sales": [
                            "create_oaebu_book_product_table"
                        ],
                        "create_oaebu_intermediate_table.dataset.google_books_traffic": [
                            "create_oaebu_book_product_table"
                        ],
                        "create_oaebu_book_product_table": [
                            "export_oaebu_table.book_product_year_metrics",
                            "export_oaebu_table.book_product_subject_bisac_metrics",
                            "create_oaebu_data_qa_jstor_isbn.dataset.jstor_country",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_sales",
                            "export_oaebu_table.book_product_subject_year_metrics",
                            "export_oaebu_table.book_product_author_metrics",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_traffic",
                            "export_oaebu_table.book_product_metrics_city",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.jstor_country",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.oapen_irus_uk",
                            "create_oaebu_data_qa_google_books_traffic_isbn",
                            "export_oaebu_table.book_product_metrics_country",
                            "export_oaebu_table.book_product_publisher_metrics",
                            "export_oaebu_table.book_product_subject_bic_metrics",
                            "export_oaebu_table.institution_list",
                            "create_oaebu_data_qa_oapen_irus_uk_isbn",
                            "export_oaebu_table.book_product_list",
                            "export_oaebu_table.book_product_subject_thema_metrics",
                            "create_oaebu_data_qa_google_books_sales_isbn",
                            "create_oaebu_data_qa_onix_aggregate",
                            "export_oaebu_table.book_product_metrics_events",
                            "export_oaebu_table.book_product_metrics",
                            "create_oaebu_data_qa_onix_isbn",
                            "export_oaebu_table.book_product_metrics_institution",
                        ],
                        "create_oaebu_data_qa_onix_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_onix_aggregate": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_jstor_isbn.dataset.jstor_country": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.jstor_country": [
                            "export_oaebu_qa_metrics"
                        ],
                        "create_oaebu_data_qa_oapen_irus_uk_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.oapen_irus_uk": [
                            "export_oaebu_qa_metrics"
                        ],
                        "create_oaebu_data_qa_google_books_sales_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_sales": [
                            "export_oaebu_qa_metrics"
                        ],
                        "create_oaebu_data_qa_google_books_traffic_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_traffic": [
                            "export_oaebu_qa_metrics"
                        ],
                        "export_oaebu_table.book_product_list": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_country": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_institution": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.institution_list": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_city": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_events": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_publisher_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_bic_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_bisac_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_thema_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_year_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_year_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_author_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_qa_metrics": ["create_oaebu_latest_views"],
                        "create_oaebu_latest_views": ["cleanup"],
                        "cleanup": ["add_new_dataset_releases"],
                        "add_new_dataset_releases": [],
                    },
                    dag,
                )

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    def test_dag_structure_with_google_analytics(self, mock_api):
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)
            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["jstor_country"].type_id,
                    dag_id_prefix="jstor",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="jstor_country",
                    isbn_field_name="isbn",
                    title_field_name="Book_Title",
                    sharded=False,
                ),
                OaebuPartner(
                    dataset_type_id=dataset_type_info["oapen_irus_uk"].type_id,
                    dag_id_prefix="oapen_irus_uk",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="oapen_irus_uk",
                    isbn_field_name="ISBN",
                    title_field_name="book_title",
                    sharded=False,
                ),
                OaebuPartner(
                    dataset_type_id=dataset_type_info["google_books_sales"].type_id,
                    dag_id_prefix="google_books",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="google_books_sales",
                    isbn_field_name="Primary_ISBN",
                    title_field_name="Title",
                    sharded=False,
                ),
                OaebuPartner(
                    dataset_type_id=dataset_type_info["google_books_traffic"].type_id,
                    dag_id_prefix="google_books",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="google_books_traffic",
                    isbn_field_name="Primary_ISBN",
                    title_field_name="Title",
                    sharded=False,
                ),
                OaebuPartner(
                    dataset_type_id=dataset_type_info["google_analytics"].type_id,
                    dag_id_prefix="google_analytics",
                    gcp_project_id="project",
                    gcp_dataset_id="dataset",
                    gcp_table_id="google_analytics",
                    isbn_field_name="publication_id",
                    title_field_name="title",
                    sharded=False,
                ),
            ]

            org_name = "ANU Press"
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=org_name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                dag = wf.make_dag()
                self.assert_dag_structure(
                    {
                        "google_analytics_anu_press_sensor": ["aggregate_works"],
                        "google_books_anu_press_sensor": ["aggregate_works"],
                        "jstor_anu_press_sensor": ["aggregate_works"],
                        "oapen_irus_uk_anu_press_sensor": ["aggregate_works"],
                        "onix_anu_press_sensor": ["aggregate_works"],
                        "aggregate_works": ["upload_aggregation_tables"],
                        "upload_aggregation_tables": ["bq_load_workid_lookup"],
                        "bq_load_workid_lookup": ["bq_load_workid_lookup_errors"],
                        "bq_load_workid_lookup_errors": ["bq_load_workfamilyid_lookup"],
                        "bq_load_workfamilyid_lookup": ["create_oaebu_crossref_metadata_table"],
                        "create_oaebu_crossref_metadata_table": ["create_oaebu_crossref_events_table"],
                        "create_oaebu_crossref_events_table": ["create_oaebu_book_table"],
                        "create_oaebu_book_table": [
                            "create_oaebu_intermediate_table.dataset.google_books_sales",
                            "create_oaebu_intermediate_table.dataset.oapen_irus_uk",
                            "create_oaebu_intermediate_table.dataset.google_analytics",
                            "create_oaebu_intermediate_table.dataset.google_books_traffic",
                            "create_oaebu_intermediate_table.dataset.jstor_country",
                        ],
                        "create_oaebu_intermediate_table.dataset.jstor_country": ["create_oaebu_book_product_table"],
                        "create_oaebu_intermediate_table.dataset.oapen_irus_uk": ["create_oaebu_book_product_table"],
                        "create_oaebu_intermediate_table.dataset.google_books_sales": [
                            "create_oaebu_book_product_table"
                        ],
                        "create_oaebu_intermediate_table.dataset.google_books_traffic": [
                            "create_oaebu_book_product_table"
                        ],
                        "create_oaebu_intermediate_table.dataset.google_analytics": ["create_oaebu_book_product_table"],
                        "create_oaebu_book_product_table": [
                            "export_oaebu_table.book_product_publisher_metrics",
                            "create_oaebu_data_qa_onix_isbn",
                            "create_oaebu_data_qa_jstor_isbn.dataset.jstor_country",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.jstor_country",
                            "export_oaebu_table.book_product_metrics_country",
                            "create_oaebu_data_qa_google_books_sales_isbn",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_analytics",
                            "export_oaebu_table.book_product_subject_year_metrics",
                            "export_oaebu_table.book_product_subject_thema_metrics",
                            "create_oaebu_data_qa_google_books_traffic_isbn",
                            "export_oaebu_table.book_product_subject_bic_metrics",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_sales",
                            "export_oaebu_table.book_product_list",
                            "export_oaebu_table.book_product_metrics_city",
                            "export_oaebu_table.book_product_author_metrics",
                            "export_oaebu_table.book_product_metrics_institution",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.oapen_irus_uk",
                            "create_oaebu_data_qa_google_analytics_isbn",
                            "export_oaebu_table.book_product_year_metrics",
                            "export_oaebu_table.book_product_subject_bisac_metrics",
                            "create_oaebu_data_qa_onix_aggregate",
                            "export_oaebu_table.book_product_metrics_events",
                            "export_oaebu_table.institution_list",
                            "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_traffic",
                            "create_oaebu_data_qa_oapen_irus_uk_isbn",
                            "export_oaebu_table.book_product_metrics",
                        ],
                        "create_oaebu_data_qa_onix_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_onix_aggregate": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_jstor_isbn.dataset.jstor_country": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.jstor_country": [
                            "export_oaebu_qa_metrics"
                        ],
                        "create_oaebu_data_qa_oapen_irus_uk_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.oapen_irus_uk": [
                            "export_oaebu_qa_metrics"
                        ],
                        "create_oaebu_data_qa_google_books_sales_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_sales": [
                            "export_oaebu_qa_metrics"
                        ],
                        "create_oaebu_data_qa_google_books_traffic_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_traffic": [
                            "export_oaebu_qa_metrics"
                        ],
                        "create_oaebu_data_qa_google_analytics_isbn": ["export_oaebu_qa_metrics"],
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_analytics": [
                            "export_oaebu_qa_metrics"
                        ],
                        "export_oaebu_table.book_product_list": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_country": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_institution": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.institution_list": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_city": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_metrics_events": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_publisher_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_bic_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_bisac_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_thema_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_year_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_subject_year_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_table.book_product_author_metrics": ["export_oaebu_qa_metrics"],
                        "export_oaebu_qa_metrics": ["create_oaebu_latest_views"],
                        "create_oaebu_latest_views": ["cleanup"],
                        "cleanup": ["add_new_dataset_releases"],
                        "add_new_dataset_releases": [],
                    },
                    dag,
                )

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
    @patch("oaebu_workflows.workflows.onix_workflow.run_bigquery_query")
    @patch("oaebu_workflows.workflows.onix_workflow.bq_load_shard")
    @patch("oaebu_workflows.workflows.onix_workflow.upload_files_to_cloud_storage")
    @patch("oaebu_workflows.workflows.onix_workflow.list_to_jsonl_gz")
    @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
    def test_create_and_upload_bq_isbn13_workid_lookup_table(
        self,
        mock_sel_table_suffixes,
        mock_write_to_file,
        mock_upload_files_from_list,
        mock_bq_load_lookup,
        mock_bq_query,
        mock_mr,
        mock_api,
    ):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_bq_query.return_value = TestOnixWorkflow.onix_data
        mock_api.return_value = self.api

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        with env.create():
            self.seed_db()
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                )

                mock_mr.return_value = OnixWorkflowRelease(
                    dag_id="onix_workflow_test",
                    release_date=pendulum.datetime(2021, 1, 1),
                    onix_release_date=pendulum.datetime(2021, 1, 1),
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    onix_dataset_id="onix",
                    onix_table_id="onix",
                )

                release = wf.make_release()

                # Test works aggregation
                wf.aggregate_works(release)
                self.assertEqual(mock_write_to_file.call_count, 3)
                call_args = mock_write_to_file.call_args_list
                self.assertEqual(len(call_args[0][0][1]), 3)
                lookup_table = {arg["isbn13"]: arg["work_id"] for arg in call_args[0][0][1]}

                self.assertTrue(
                    lookup_table["111"] == lookup_table["112"] and lookup_table["112"] != lookup_table["211"]
                )

                self.assertEqual(call_args[0][0][0], "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz")
                self.assertEqual(
                    call_args[1][0][1],
                    [
                        {
                            "Error": "Product ISBN13:111 is a manifestation of ISBN13:113, which is not given as a product identifier in any ONIX product record."
                        },
                    ],
                )
                self.assertEqual(call_args[1][0][0], "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz")

                # Test upload_aggregation_tables
                wf.upload_aggregation_tables(release)
                self.assertEqual(mock_upload_files_from_list.call_count, 1)
                _, call_args = mock_upload_files_from_list.call_args
                self.assertEqual(call_args["bucket_name"], "bucket_name")
                self.assertEqual(
                    call_args["blob_names"],
                    [
                        "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz",
                        "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz",
                        "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz",
                    ],
                )
                self.assertEqual(
                    call_args["file_paths"],
                    [
                        "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz",
                        "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz",
                        "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz",
                    ],
                )

                # Test bq_load_workid_lookup
                wf.bq_load_workid_lookup(release)
                self.assertEqual(mock_bq_load_lookup.call_count, 1)
                _, call_args = mock_bq_load_lookup.call_args
                self.assertTrue(call_args["schema_file_path"].endswith("onix_workid_isbn.json"))
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["transform_bucket"], "bucket_name")
                self.assertEqual(call_args["transform_blob"], "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz")
                self.assertEqual(call_args["dataset_id"], "onix_workflow")
                self.assertEqual(call_args["data_location"], "us")
                self.assertEqual(call_args["table_id"], "onix_workid_isbn")
                self.assertEqual(call_args["release_date"], pendulum.datetime(2021, 1, 1))
                self.assertEqual(call_args["source_format"], "NEWLINE_DELIMITED_JSON")
                self.assertEqual(call_args["dataset_description"], "ONIX workflow tables")

                # Test bq_load_workid_lookup_errors
                wf.bq_load_workid_lookup_errors(release)
                self.assertEqual(mock_bq_load_lookup.call_count, 2)
                _, call_args = mock_bq_load_lookup.call_args
                self.assertTrue(call_args["schema_file_path"].endswith("onix_workid_isbn_errors.json"))
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["transform_bucket"], "bucket_name")
                self.assertEqual(
                    call_args["transform_blob"], "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz"
                )
                self.assertEqual(call_args["dataset_id"], "onix_workflow")
                self.assertEqual(call_args["data_location"], "us")
                self.assertEqual(call_args["table_id"], "onix_workid_isbn_errors")
                self.assertEqual(call_args["release_date"], pendulum.datetime(2021, 1, 1))
                self.assertEqual(call_args["source_format"], "NEWLINE_DELIMITED_JSON")
                self.assertEqual(call_args["dataset_description"], "ONIX workflow tables")

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
    @patch("oaebu_workflows.workflows.onix_workflow.run_bigquery_query")
    @patch("oaebu_workflows.workflows.onix_workflow.bq_load_shard")
    @patch("oaebu_workflows.workflows.onix_workflow.upload_files_to_cloud_storage")
    @patch("oaebu_workflows.workflows.onix_workflow.list_to_jsonl_gz")
    @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
    def test_create_and_upload_bq_isbn13_workfamilyid_lookup_table(
        self,
        mock_sel_table_suffixes,
        mock_write_to_file,
        mock_upload_files_from_list,
        mock_bq_load_lookup,
        mock_bq_query,
        mock_mr,
        mock_api,
    ):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_bq_query.return_value = TestOnixWorkflow.onix_data
        mock_api.return_value = self.api

        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)
        with env.create():
            self.seed_db()
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                )

                mock_mr.return_value = OnixWorkflowRelease(
                    dag_id="onix_workflow_test",
                    release_date=pendulum.datetime(2021, 1, 1),
                    onix_release_date=pendulum.datetime(2021, 1, 1),
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    onix_dataset_id="onix",
                    onix_table_id="onix",
                )
                release = wf.make_release()

                # Test works family aggregation
                wf.aggregate_works(release)
                self.assertEqual(mock_write_to_file.call_count, 3)
                call_args = mock_write_to_file.call_args_list
                lookup_table = {arg["isbn13"]: arg["work_family_id"] for arg in call_args[2][0][1]}
                self.assertTrue(
                    lookup_table["112"] == lookup_table["111"] and lookup_table["111"] == lookup_table["211"]
                )

                self.assertEqual(call_args[0][0][0], "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz")

                # Test bq_load_workfamilyid_lookup
                wf.upload_aggregation_tables(release)
                wf.bq_load_workfamilyid_lookup(release)
                self.assertEqual(mock_bq_load_lookup.call_count, 1)
                _, call_args = mock_bq_load_lookup.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["transform_bucket"], "bucket_name")
                self.assertEqual(
                    call_args["transform_blob"], "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz"
                )

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    def test_create_oaebu_intermediate_table(self, mock_api):
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                )
                with patch(
                    "oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query", return_value=False
                ) as _:
                    with patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset") as _:
                        self.assertRaises(
                            AirflowException,
                            wf.create_oaebu_intermediate_table,
                            release=OnixWorkflowRelease(
                                dag_id="onix_workflow_test",
                                release_date=pendulum.datetime(2021, 1, 1),
                                onix_release_date=pendulum.datetime(2021, 1, 1),
                                gcp_project_id=self.telescope.organisation.project_id,
                                gcp_bucket_name=self.bucket_name,
                                onix_dataset_id="onix",
                                onix_table_id="onix",
                            ),
                            orig_project_id="project",
                            orig_dataset="dataset",
                            orig_table="table",
                            orig_isbn="isbn",
                            sharded=False,
                        )

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("oaebu_workflows.workflows.onix_workflow.OnixWorkflow.make_release")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    @patch("oaebu_workflows.workflows.onix_workflow.select_table_shard_dates")
    @patch("observatory.platform.utils.workflow_utils.select_table_shard_dates")
    def test_create_oaebu_intermediate_table_tasks(
        self,
        mock_select_table_shard_dates,
        mock_sel_table_suffixes,
        mock_create_bq_ds,
        mock_create_bq_table,
        mock_mr,
        mock_api,
    ):
        mock_api.return_value = self.api

        data_partners = [
            OaebuPartner(
                dataset_type_id="Test Partner",
                dag_id_prefix="test_dag",
                gcp_project_id="test_project",
                gcp_dataset_id="test_dataset",
                gcp_table_id="test_table",
                isbn_field_name="isbn",
                title_field_name="title",
                sharded=True,
            ),
            OaebuPartner(
                dataset_type_id="Test Partner",
                dag_id_prefix="test_dag",
                gcp_project_id="test_project",
                gcp_dataset_id="test_dataset",
                gcp_table_id="test_table2",
                isbn_field_name="isbn",
                title_field_name="title",
                sharded=True,
            ),
        ]

        mock_select_table_shard_dates.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_create_bq_table.return_value = True

        with self.env.create():
            self.seed_db()
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    dag_id="dagid",
                    data_partners=data_partners,
                )

                mock_mr.return_value = OnixWorkflowRelease(
                    dag_id="onix_workflow_test",
                    release_date=pendulum.datetime(2021, 1, 1),
                    onix_release_date=pendulum.datetime(2021, 1, 1),
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    onix_dataset_id="onix",
                    onix_table_id="onix",
                )

                # Spin up tasks
                oaebu_task1_op = wf.operators[9]
                self.assertEqual(oaebu_task1_op[0].task_id, "create_oaebu_intermediate_table.test_dataset.test_table")
                self.assertEqual(oaebu_task1_op[1].task_id, "create_oaebu_intermediate_table.test_dataset.test_table2")

                # Run tasks
                oaebu_task1_op[0].execute_callable()
                _, call_args = mock_create_bq_ds.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
                self.assertEqual(call_args["location"], "us")

                _, call_args = mock_create_bq_table.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
                self.assertEqual(call_args["table_id"], "test_dataset_test_table_matched20210101")
                self.assertEqual(call_args["location"], "us")
                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "1dabe0b435be583fdbae6ad4421f579b"
                self.assertEqual(sql_hash, expected_hash)

                oaebu_task1_op[1].execute_callable()
                _, call_args = mock_create_bq_ds.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
                self.assertEqual(call_args["location"], "us")

                _, call_args = mock_create_bq_table.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
                self.assertEqual(call_args["table_id"], "test_dataset_test_table2_matched20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "200d285a02bdc14b3229874f50d8a30d"
                self.assertEqual(sql_hash, expected_hash)

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_onix_isbn(self, mock_bq_ds, mock_bq_table_query, mock_api):
        mock_api.return_value = self.api

        with CliRunner().isolated_filesystem():
            with self.env.create():
                self.seed_db()
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_onix_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                )

                _, call_args = mock_bq_ds.call_args
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
                self.assertEqual(call_args["table_id"], "onix_invalid_isbn20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "98d0a2b99961e9c63c3ade6faf9997f9"
                self.assertEqual(sql_hash, expected_hash)

                mock_bq_table_query.return_value = False
                self.assertRaises(
                    AirflowException,
                    wf.create_oaebu_data_qa_onix_isbn,
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                )

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_onix_aggregate(self, mock_bq_ds, mock_bq_table_query, mock_api):
        mock_api.return_value = self.api

        with CliRunner().isolated_filesystem():
            with self.env.create():
                self.seed_db()
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_onix_aggregate(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                )

                _, call_args = mock_bq_ds.call_args
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
                self.assertEqual(call_args["table_id"], "onix_aggregate_metrics20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "e457bc4d32a3bbb75ef215009da917b3"
                self.assertEqual(sql_hash, expected_hash)

                mock_bq_table_query.return_value = False
                self.assertRaises(
                    AirflowException,
                    wf.create_oaebu_data_qa_onix_aggregate,
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                )

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("observatory.platform.utils.workflow_utils.select_table_shard_dates")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_jstor_isbn(self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes, mock_api):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)
            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["jstor_country"].type_id,
                    dag_id_prefix="jstor",
                    gcp_project_id="project",
                    gcp_dataset_id="jstor",
                    gcp_table_id="country",
                    isbn_field_name="ISBN",
                    title_field_name="Book_Title",
                    sharded=False,
                )
            ]
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_jstor_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="jstor",
                    orig_table="country",
                    sharded=True,
                )

                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                wf.create_oaebu_data_qa_jstor_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="jstor",
                    orig_table="country",
                    sharded=False,
                )
                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["project_id"], "project")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
                self.assertEqual(call_args["table_id"], "jstor_invalid_eisbn20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "7a9a66b5a0295ecdd53d245e659f3e85"
                self.assertEqual(sql_hash, expected_hash)

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("observatory.platform.utils.workflow_utils.select_table_shard_dates")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_google_books_sales_isbn(
        self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes, mock_api
    ):
        mock_api.return_value = self.api

        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)
            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["google_books_sales"].type_id,
                    dag_id_prefix="google_books",
                    gcp_project_id="project",
                    gcp_dataset_id="google_books",
                    gcp_table_id="sales",
                    isbn_field_name="Primary_ISBN",
                    title_field_name="Title",
                    sharded=False,
                )
            ]
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_google_books_sales_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="google_books",
                    orig_table="sales",
                    sharded=True,
                )

                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                wf.create_oaebu_data_qa_google_books_sales_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="google_books",
                    orig_table="sales",
                    sharded=False,
                )
                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["project_id"], "project")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
                self.assertEqual(call_args["table_id"], "google_books_sales_invalid_isbn20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "90bbe5c7fb00173d1a85f6ab13ab99b2"
                self.assertEqual(sql_hash, expected_hash)

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("observatory.platform.utils.workflow_utils.select_table_shard_dates")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_google_books_traffic_isbn(
        self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes, mock_api
    ):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)

            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["google_books_traffic"].type_id,
                    dag_id_prefix="google_books",
                    gcp_project_id="project",
                    gcp_dataset_id="google_books",
                    gcp_table_id="traffic",
                    isbn_field_name="Primary_ISBN",
                    title_field_name="Title",
                    sharded=False,
                )
            ]
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_google_books_traffic_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="google_books",
                    orig_table="traffic",
                    sharded=True,
                )

                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                wf.create_oaebu_data_qa_google_books_traffic_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="google_books",
                    orig_table="sales",
                    sharded=False,
                )
                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["project_id"], "project")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
                self.assertEqual(call_args["table_id"], "google_books_traffic_invalid_isbn20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "90bbe5c7fb00173d1a85f6ab13ab99b2"
                self.assertEqual(sql_hash, expected_hash)

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("observatory.platform.utils.workflow_utils.select_table_shard_dates")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_oapen_irus_uk_isbn(
        self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes, mock_api
    ):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)
            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["oapen_irus_uk"].type_id,
                    dag_id_prefix="oapen_irus_uk",
                    gcp_project_id="project",
                    gcp_dataset_id="irus_uk",
                    gcp_table_id="oapen_irus_uk",
                    isbn_field_name="ISBN",
                    title_field_name="book_title",
                    sharded=False,
                )
            ]
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_oapen_irus_uk_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="irus_uk",
                    orig_table="oapen_irus_uk",
                    sharded=True,
                )

                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                wf.create_oaebu_data_qa_oapen_irus_uk_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="irus_uk",
                    orig_table="oapen_irus_uk",
                    sharded=False,
                )
                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["project_id"], "project")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
                self.assertEqual(call_args["table_id"], "oapen_irus_uk_invalid_isbn20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "ae842fbf661d3a0c50b748dec8e1cd24"
                self.assertEqual(sql_hash, expected_hash)

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("observatory.platform.utils.workflow_utils.select_table_shard_dates")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_google_analytics_isbn(
        self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes, mock_api
    ):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)

            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["oapen_irus_uk"].type_id,
                    dag_id_prefix="google_analytics",
                    gcp_project_id="project",
                    gcp_dataset_id="google",
                    gcp_table_id="google_analytics",
                    isbn_field_name="publication_id",
                    title_field_name="book_title",
                    sharded=False,
                )
            ]
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_google_analytics_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="google",
                    orig_table="google_analytics",
                    sharded=True,
                )

                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                wf.create_oaebu_data_qa_google_analytics_isbn(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="google",
                    orig_table="google_analytics",
                    sharded=False,
                )
                self.assertEqual(mock_sel_table_suffixes.call_count, 1)

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["project_id"], "project")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
                self.assertEqual(call_args["table_id"], "google_analytics_invalid_isbn20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "4302ca70501b435561f0c81d04e314cd"
                self.assertEqual(sql_hash, expected_hash)

    @patch("oaebu_workflows.workflows.onix_workflow.make_observatory_api")
    @patch("observatory.platform.utils.gc_utils.select_table_shard_dates")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("oaebu_workflows.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_intermediate_unmatched_workid(
        self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes, mock_api
    ):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        mock_api.return_value = self.api

        with self.env.create():
            self.seed_db()
            dataset_type_info = get_dataset_type_info(self.api)
            data_partners = [
                OaebuPartner(
                    dataset_type_id=dataset_type_info["jstor_country"].type_id,
                    dag_id_prefix="jstor",
                    gcp_project_id="project",
                    gcp_dataset_id="jstor",
                    gcp_table_id="country",
                    isbn_field_name="ISBN",
                    title_field_name="Book_Title",
                    sharded=False,
                )
            ]
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.project_id,
                    gcp_bucket_name=self.bucket_name,
                    data_partners=data_partners,
                )
                mock_bq_table_query.return_value = True
                wf.create_oaebu_data_qa_intermediate_unmatched_workid(
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="jstor",
                    orig_table="country",
                    orig_isbn="isbn",
                    orig_title="Book_Title",
                )

                _, call_args = mock_bq_ds.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")

                _, call_args = mock_bq_table_query.call_args
                self.assertEqual(call_args["table_id"], "country_unmatched_isbn20210101")
                self.assertEqual(call_args["location"], "us")

                sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
                sql_hash = sql_hash.hexdigest()
                expected_hash = "71bc329dd609017504e91bc8fd8931fe"
                self.assertEqual(sql_hash, expected_hash)

                mock_bq_table_query.return_value = False
                self.assertRaises(
                    AirflowException,
                    wf.create_oaebu_data_qa_intermediate_unmatched_workid,
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.datetime(2021, 1, 1),
                        onix_release_date=pendulum.datetime(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    ),
                    project_id="project",
                    orig_dataset_id="jstor",
                    orig_table="country",
                    orig_isbn="isbn",
                    orig_title="Book_Title",
                )


class TestOnixWorkflowFunctional(ObservatoryTestCase):
    """Functionally test the workflow.  No Google Analytics."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.gcp_project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.timestamp = pendulum.now()
        self.schema_path = default_schema_folder()

        self.onix_table_id = "onix"
        self.test_onix_folder = random_id()  # "onix_workflow_test_onix_table"

        # vcrpy cassettes for http request mocking
        self.metadata_cassette = test_fixtures_folder("onix_workflow", "crossref_metadata_request.yaml")
        self.events_cassette = test_fixtures_folder("onix_workflow", "crossref_events_request.yaml")

        # API environment
        self.host = "localhost"
        self.port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "Curtin Press"

    def seed_db(self):
        table_type_info = get_table_type_info()
        seed_table_type(table_type_info=table_type_info, api=self.api)

        dataset_type_info = get_dataset_type_info(self.api)
        seed_dataset_type(api=self.api, dataset_type_info=dataset_type_info)

    def setup_api(self, orgs=None):
        dt = pendulum.now("UTC")

        name = "Onix Workflow"
        workflow_type = WorkflowType(name=name, type_id=OnixWorkflow.DAG_ID_PREFIX)
        self.api.put_workflow_type(workflow_type)

        workflow_type = WorkflowType(name="onix telescope", type_id="onix")
        self.api.put_workflow_type(workflow_type)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            type_id=OnixWorkflow.DAG_ID_PREFIX,
            name="ds type",
            extra={},
            table_type=TableType(id=1),
        )
        self.api.put_dataset_type(dataset_type)

        if orgs is None:
            orgs = [self.org_name]

        for org in orgs:
            organisation = Organisation(
                name=org,
                project_id="project",
                download_bucket="download_bucket",
                transform_bucket="transform_bucket",
            )
            organisation = self.api.put_organisation(organisation)

            telescope = Workflow(
                name=name,
                workflow_type=WorkflowType(id=2),  # onix telescope
                organisation=Organisation(id=organisation.id),
                extra={},
            )
            self.api.put_workflow(telescope)

            telescope = Workflow(
                name=name,
                workflow_type=WorkflowType(id=1),
                organisation=Organisation(id=organisation.id),
                extra={},
            )
            telescope = self.api.put_workflow(telescope)

            dataset = Dataset(
                name="Onix Workflow Example Dataset",
                address="project.dataset.table",
                service="bigquery",
                workflow=Workflow(id=telescope.id),
                dataset_type=DatasetType(id=1),
            )
            self.api.put_dataset(dataset)

    def setup_connections(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.port}")
        env.add_connection(conn)

    def test_dag_load(self):
        """Test that the DAG loads for each organisation"""

        dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "onix_workflow.py")
        org_names = ["ANU Press", "UCL Press", "University of Michigan Press"]

        env = ObservatoryEnvironment(self.gcp_project_id, self.data_location, api_host=self.host, api_port=self.port)
        with env.create():
            # Add Observatory API connection
            self.setup_connections(env)
            self.setup_api(orgs=org_names)
            self.seed_db()

            # Check that all DAGs load
            for org_name in org_names:
                dag_id = make_dag_id("onix_workflow", org_name)
                self.assert_dag_load(dag_id, dag_file)

    def test_crossref_API_calls(self):
        """
        Test the functions that query the crossref event and metadata APIs
        """
        # Test function calls with mocked HTTP responses
        good_test_doi = "10.5555/12345678"
        bad_test_doi = "10.1111111111111"
        good_test_isbn = "9780136019701"
        bad_test_isbn = "1111111111111"
        mailto = "agent@observatory.academy"

        with vcr.use_cassette(
            test_fixtures_folder("onix_workflow", "crossref_download_function_test.yaml"), record_mode="none"
        ):
            metadata_url = CROSSREF_METADATA_URL.format(isbn=good_test_isbn)
            metadata = download_crossref_isbn_metadata(metadata_url, i=0)
            assert metadata == [{"passed": True}], f"Metadata return incorrect. Got {metadata}"

            events_start = pendulum.date(2020, 1, 1)
            events_end = pendulum.date(2022, 1, 1)
            event_url_templates = [
                CROSSREF_EVENT_URL_TEMPLATE,
                CROSSREF_EDITED_EVENT_URL_TEMPLATE,
                CROSSREF_DELETED_EVENT_URL_TEMPLATE,
            ]
            for template in event_url_templates:
                event_url = template.format(
                    doi=good_test_doi,
                    mailto=mailto,
                    start_date=events_start.strftime("%Y-%m-%d"),
                    end_date=events_end.strftime("%Y-%m-%d"),
                )
                events = download_crossref_event_url(event_url)
                assert events == [{"passed": True}], f"Event return incorrect. Got {events}"

        # Test API calls end-to-end using the real server response
        good_meta = download_crossref_metadata([good_test_isbn])
        bad_meta = download_crossref_metadata([bad_test_isbn])
        assert good_meta, "Metadata should have returned something"
        assert not bad_meta, f"Metadata should have returned nothing, instead returned {bad_meta}"

        good_events = download_crossref_events([good_test_doi], events_start, events_end, mailto, max_threads=1)
        bad_events = download_crossref_events([bad_test_doi], events_start, events_end, mailto, max_threads=1)
        assert good_events, "Events should have returned something"
        assert len(good_events) == 4
        assert not bad_events, f"Events should have returned nothing, instead returned {bad_events}"

    def test_crossref_transform(self):
        # Test transformation functions
        input_metadata = [
            {
                "publisher": "University of Minnesota Press",
                "published-print": {"date-parts": [[2017, 10, 24]]},
                "DOI": "10.5749/j.ctt1pwt6tp",
                "type": "monograph",
                "is-referenced-by-count": 10,
                "title": ["Postcolonial Automobility"],
                "prefix": "10.5749",
                "author": [{"given": "Lindsey B.", "family": "Green-Simms", "sequence": "first", "affiliation": []}],
                "member": "3779",
                "issued": {"date-parts": [[]]},
                "ISBN": ["9780136019701"],
                "references-count": 0,
            }
        ]
        expected_transformed_metadata = [
            {
                "publisher": "University of Minnesota Press",
                "published_print": {"date_parts": [2017, 10, 24]},
                "DOI": "10.5749/j.ctt1pwt6tp",
                "type": "monograph",
                "is_referenced_by_count": 10,
                "title": ["Postcolonial Automobility"],
                "prefix": "10.5749",
                "author": [{"given": "Lindsey B.", "family": "Green-Simms", "sequence": "first", "affiliation": []}],
                "member": "3779",
                "issued": {"date_parts": []},
                "ISBN": ["9780136019701"],
                "references_count": 0,
            }
        ]

        # Standalone transform
        actual_transformed_metadata = transform_metadata_item(input_metadata[0])
        assert expected_transformed_metadata[0] == actual_transformed_metadata
        # List transform
        actual_transformed_metadata = transform_crossref_metadata(input_metadata)
        assert len(actual_transformed_metadata) == 1
        assert expected_transformed_metadata == actual_transformed_metadata

        input_events = [
            {
                "license": "https://creativecommons.org/publicdomain/zero/1.0/",
                "obj_id": "https://doi.org/10.5555/12345678",
                "source_token": "36c35e23-8757-4a9d-aacf-345e9b7eb50d",
                "occurred_at": "2021-07-21T13:27:50Z",
                "subj_id": "https://en.wikipedia.org/api/rest_v1/page/html/Josiah_S._Carberry/1034726541",
                "id": "c76bbfd9-5122-4859-82c8-505b1eb845fd",
                "evidence_record": "https://evidence.eventdata.crossref.org/evidence/20210721-wikipedia-83789066-1794-4c4f-bb2c-ed47ccd82264",
                "terms": "https://doi.org/10.13003/CED-terms-of-use",
                "action": "add",
                "subj": {
                    "pid": "https://en.wikipedia.org/wiki/Josiah_S._Carberry",
                    "url": "https://en.wikipedia.org/w/index.php?title=Josiah_S._Carberry&oldid=1034726541",
                    "title": "Josiah S. Carberry",
                    "work_type_id": "entry-encyclopedia",
                    "api-url": "https://en.wikipedia.org/api/rest_v1/page/html/Josiah_S._Carberry/1034726541",
                },
                "source_id": "wikipedia",
                "obj": {
                    "pid": "https://doi.org/10.5555/12345678",
                    "url": "http://psychoceramics.labs.crossref.org/10.5555-12345678.html",
                    "method": "landing-page-meta-tag",
                    "verification": "checked-url-exact",
                },
                "timestamp": "2021-07-21T14:00:58Z",
                "relation_type_id": "references",
            }
        ]
        expected_transformed_events = [
            {
                "license": "https://creativecommons.org/publicdomain/zero/1.0/",
                "obj_id": "https://doi.org/10.5555/12345678",
                "source_token": "36c35e23-8757-4a9d-aacf-345e9b7eb50d",
                "occurred_at": "2021-07-21T13:27:50+00:00",
                "subj_id": "https://en.wikipedia.org/api/rest_v1/page/html/Josiah_S._Carberry/1034726541",
                "id": "c76bbfd9-5122-4859-82c8-505b1eb845fd",
                "evidence_record": "https://evidence.eventdata.crossref.org/evidence/20210721-wikipedia-83789066-1794-4c4f-bb2c-ed47ccd82264",
                "terms": "https://doi.org/10.13003/CED-terms-of-use",
                "action": "add",
                "subj": {
                    "pid": "https://en.wikipedia.org/wiki/Josiah_S._Carberry",
                    "url": "https://en.wikipedia.org/w/index.php?title=Josiah_S._Carberry&oldid=1034726541",
                    "title": "Josiah S. Carberry",
                    "work_type_id": "entry-encyclopedia",
                    "api_url": "https://en.wikipedia.org/api/rest_v1/page/html/Josiah_S._Carberry/1034726541",
                },
                "source_id": "wikipedia",
                "obj": {
                    "pid": "https://doi.org/10.5555/12345678",
                    "url": "http://psychoceramics.labs.crossref.org/10.5555-12345678.html",
                    "method": "landing-page-meta-tag",
                    "verification": "checked-url-exact",
                },
                "timestamp": "2021-07-21T14:00:58+00:00",
                "relation_type_id": "references",
            }
        ]
        # Standalone transform
        actual_transformed_events = transform_event(input_events[0])
        assert expected_transformed_events[0] == actual_transformed_events
        # List transform
        actual_transformed_events = transform_crossref_events(input_events)
        assert len(actual_transformed_events) == 1
        assert expected_transformed_events == actual_transformed_events

    def test_utility_functions(self):
        """
        Test the standalone functions in the Onix workflow that aren't specifially tested in other classes
        """
        # Test Onix querying
        env = ObservatoryEnvironment(self.gcp_project_id, self.data_location, api_host=self.host, api_port=self.port)
        bucket_name = env.transform_bucket
        fake_onix_dataset_id = env.add_dataset(prefix="onix")
        fake_sharded_dataset = env.add_dataset(prefix="sharded_data")
        fake_view_dataset = env.add_dataset(prefix="views")
        with env.create():
            onix_table_file = test_fixtures_folder("onix_workflow", "onix_query_test.jsonl")
            blob = os.path.join(self.test_onix_folder, os.path.basename(onix_table_file))
            upload_file_to_cloud_storage(bucket_name=bucket_name, blob_name=blob, file_path=onix_table_file)

            fake_onix = load_jsonl(onix_table_file)
            tables = [
                Table(
                    "onix",
                    False,
                    fake_onix_dataset_id,
                    fake_onix,
                    find_schema(self.schema_path, "onix"),
                ),
            ]
            release_date = pendulum.datetime(2022, 6, 13)
            bucket_name = env.transform_bucket
            bq_load_tables(
                tables=tables,
                bucket_name=bucket_name,
                release_date=release_date,
                data_location=self.data_location,
                project_id=self.gcp_project_id,
            )

            actual_isbns = isbns_from_onix(self.gcp_project_id, fake_onix_dataset_id, "onix")
            actual_dois = dois_from_onix(self.gcp_project_id, fake_onix_dataset_id, "onix")
            fake_isbns = [entry["ISBN13"] for entry in fake_onix]
            fake_dois = [entry["DOI"] for entry in fake_onix]

            # Check there are no duplicates and the contents are the same
            assert len(actual_isbns) == len(set(fake_isbns))
            assert set(actual_isbns) == set(fake_isbns)
            assert len(actual_dois) == len(set(fake_dois))
            assert set(actual_dois) == set(fake_dois)

            # Test the creation of latest data views. We will use the onix.jsonl fixture for testing.
            previous_release_date = pendulum.datetime(2022, 6, 6)  # Add another release date
            data = [fake_onix, fake_onix[:2]]
            for release, table in zip([release_date, previous_release_date], data):
                test_table = Table(
                    "data_export",
                    True,
                    fake_sharded_dataset,
                    table,
                    find_schema(self.schema_path, "onix"),
                )
                bq_load_tables(
                    tables=[test_table],
                    bucket_name=bucket_name,
                    release_date=release,
                    data_location=self.data_location,
                    project_id=self.gcp_project_id,
                )

            # Now make the views from this dataset
            date_string = release_date.strftime("%Y%m%d")
            create_latest_views_from_dataset(
                project_id=self.gcp_project_id,
                from_dataset=fake_sharded_dataset,
                to_dataset=fake_view_dataset,
                date_match=date_string,
                data_location=self.data_location,
            )

            # Grab the data from the view and make assertions
            view_data = run_bigquery_query(f"SELECT * FROM {self.gcp_project_id}.{fake_view_dataset}.data_export")
            view_isbns = [entry["ISBN13"] for entry in view_data]
            view_dois = [entry["DOI"] for entry in view_data]
            actual_isbns = [entry["ISBN13"] for entry in fake_onix]
            actual_dois = [entry["DOI"] for entry in fake_onix]

            assert len(view_data) == len(fake_onix)
            assert len(actual_isbns) == len(view_isbns)
            assert set(actual_isbns) == set(view_isbns)
            assert len(actual_dois) == len(view_dois)
            assert set(actual_dois) == set(view_dois)

    def setup_fake_data_tables(
        self, dataset_id: str, settings_dataset_id: str, fixtures_dataset_id: str, release_date: pendulum.DateTime
    ):
        """Create a new onix and subject lookup data tables with their own dataset and table ids. Populate them with some fake data."""

        # Upload fixture to bucket

        files = [
            test_fixtures_folder("onix_workflow", "onix.jsonl"),
            test_fixtures_folder("onix_workflow", "bic_lookup.jsonl"),
            test_fixtures_folder("onix_workflow", "bisac_lookup.jsonl"),
            test_fixtures_folder("onix_workflow", "thema_lookup.jsonl"),
        ]
        blobs = [os.path.join(self.test_onix_folder, os.path.basename(file)) for file in files]
        upload_files_to_cloud_storage(bucket_name=self.gcp_bucket_name, blob_names=blobs, file_paths=files)

        # Load into bigquery
        table_ids = [
            table_ids_from_path("onix.jsonl")[0],
            table_ids_from_path("bic_lookup.jsonl")[0],
            table_ids_from_path("bisac_lookup.jsonl")[0],
            table_ids_from_path("thema_lookup.jsonl")[0],
        ]
        onix_test_schema_folder = test_fixtures_folder("onix_workflow", "schema")
        schema_file_paths = [
            find_schema(self.schema_path, "onix"),
            find_schema(onix_test_schema_folder, "bic_lookup"),
            find_schema(onix_test_schema_folder, "bisac_lookup"),
            find_schema(onix_test_schema_folder, "thema_lookup"),
        ]
        for blob, id, schema_file_path in zip(blobs, table_ids, schema_file_paths):
            bq_load_shard(
                schema_file_path=schema_file_path,
                project_id=self.gcp_project_id,
                transform_bucket=self.gcp_bucket_name,
                transform_blob=blob,
                dataset_id=dataset_id,
                data_location=self.data_location,
                table_id=id,
                release_date=release_date,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                **{},
            )

        # Create the table objects
        country = load_jsonl(test_fixtures_folder("onix_workflow", "country.jsonl"))
        bic_lookup = load_jsonl(test_fixtures_folder("onix_workflow", "bic_lookup.jsonl"))
        bisac_lookup = load_jsonl(test_fixtures_folder("onix_workflow", "bisac_lookup.jsonl"))
        thema_lookup = load_jsonl(test_fixtures_folder("onix_workflow", "thema_lookup.jsonl"))
        tables = [
            Table(
                "country",
                False,
                settings_dataset_id,
                country,
                find_schema(onix_test_schema_folder, "country", release_date=release_date),
            ),
            Table(
                "bic_lookup",
                False,
                fixtures_dataset_id,
                bic_lookup,
                find_schema(onix_test_schema_folder, "bic_lookup"),
            ),
            Table(
                "bisac_lookup",
                False,
                fixtures_dataset_id,
                bisac_lookup,
                find_schema(onix_test_schema_folder, "bisac_lookup"),
            ),
            Table(
                "thema_lookup",
                False,
                fixtures_dataset_id,
                thema_lookup,
                find_schema(onix_test_schema_folder, "thema_lookup"),
            ),
        ]

        # Load tables into bigquery
        bq_load_tables(
            tables=tables,
            bucket_name=self.gcp_bucket_name,
            release_date=release_date,
            data_location=self.data_location,
            project_id=self.gcp_project_id,
        )

    def setup_fake_partner_data(self, env, release_date: pendulum.DateTime):
        self.fake_partner_dataset = env.add_dataset()

        # Upload fixture to bucket
        path = test_fixtures_folder("onix_workflow")
        files = [
            os.path.join(path, "jstor_country.jsonl"),
            os.path.join(path, "jstor_institution.jsonl"),
            os.path.join(path, "google_books_sales.jsonl"),
            os.path.join(path, "google_books_traffic.jsonl"),
            os.path.join(path, "oapen_irus_uk.jsonl"),
            os.path.join(path, "google_analytics.jsonl"),
        ]
        blobs = [os.path.join(self.test_onix_folder, os.path.basename(file)) for file in files]
        upload_files_to_cloud_storage(bucket_name=self.gcp_bucket_name, blob_names=blobs, file_paths=files)

        # Load into bigquery
        table_ids = []
        for file_name, blob in zip(files, blobs):
            table_id, _ = table_ids_from_path(file_name)

            # set schema prefix to 'anu_press' for ANU press, custom dimensions are added in this schema.
            schema_prefix = "anu_press_" if table_id == "google_analytics" else ""

            schema_file_path = find_schema(self.schema_path, table_id, prefix=schema_prefix)
            bq_load_partition(
                schema_file_path=schema_file_path,
                project_id=self.gcp_project_id,
                transform_bucket=self.gcp_bucket_name,
                transform_blob=blob,
                dataset_id=self.fake_partner_dataset,
                data_location=self.data_location,
                table_id=table_id,
                release_date=release_date,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                partition_type=bigquery.table.TimePartitioningType.MONTH,
                dataset_description="Test Onix data for the workflow",
                partition_field="release_date",
                **{},
            )
            table_ids.append(table_id)

        self.seed_db()
        dataset_type_info = get_dataset_type_info(self.api)

        # Make partners
        partners = []
        for (dataset_type_id, isbn_field_name, title_field_name, dag_id_prefix), table_id in zip(
            [
                (dataset_type_info["jstor_country"].type_id, "ISBN", "Book_Title", "jstor"),
                (dataset_type_info["jstor_institution"].type_id, "ISBN", "Book_Title", "jstor"),
                (dataset_type_info["google_books_sales"].type_id, "Primary_ISBN", "Title", "google_books"),
                (dataset_type_info["google_books_traffic"].type_id, "Primary_ISBN", "Title", "google_books"),
                (dataset_type_info["oapen_irus_uk"].type_id, "ISBN", "book_title", "oapen_irus_uk"),
                (dataset_type_info["google_analytics"].type_id, "publication_id", "title", "google_analytics"),
            ],
            table_ids,
        ):
            partners.append(
                OaebuPartner(
                    dataset_type_id=dataset_type_id,
                    dag_id_prefix=dag_id_prefix,
                    gcp_project_id=self.gcp_project_id,
                    gcp_dataset_id=self.fake_partner_dataset,
                    gcp_table_id=table_id,
                    isbn_field_name=isbn_field_name,
                    title_field_name=title_field_name,
                    sharded=False,
                )
            )

        return partners

    def run_telescope_tests(self, *, org_name: str, include_google_analytics: bool = False):
        """Functional test of the ONIX workflow"""

        def vcr_ignore_condition(request):
            """This function is used by vcrpy to allow requests to sources not in the cassette file.
            At time of writing, the only mocked requests are the ones to crossref events and metadata."""
            allowed_domains = ["https://api.crossref.org", "https://api.eventdata.crossref.org"]
            allow_request = any([request.url.startswith(i) for i in allowed_domains])
            if not allow_request:
                return None
            return request

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.gcp_project_id, self.data_location, api_host=self.host, api_port=self.port)

        # Create datasets
        partner_release_date = pendulum.datetime(2022, 6, 13)
        onix_dataset_id = env.add_dataset(prefix="onix")
        oaebu_data_qa_dataset_id = env.add_dataset(prefix="oaebu_data_qa")
        oaebu_latest_data_qa_dataset_id = env.add_dataset(prefix="oaebu_data_qa_latest")
        onix_workflow_dataset_id = env.add_dataset(prefix="onix_workflow")
        oaebu_intermediate_dataset_id = env.add_dataset(prefix="oaebu_intermediate")
        oaebu_output_dataset_id = env.add_dataset(prefix="oaebu_output")
        oaebu_export_dataset_id = env.add_dataset(prefix="oaebu_export")
        oaebu_latest_export_dataset_id = env.add_dataset(prefix="oaebu_export_latest")
        oaebu_settings_dataset_id = env.add_dataset(prefix="settings")
        oaebu_fixtures_dataset_id = env.add_dataset(prefix="fixtures")
        oaebu_crossref_dataset_id = env.add_dataset(prefix="crossref")

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            self.gcp_bucket_name = env.transform_bucket
            self.setup_connections(env)
            self.setup_api(orgs=[org_name])
            # self.seed_db()

            # Setup data partners, remove Google Analytics (the last partner) from these tests
            data_partners = self.setup_fake_partner_data(env, partner_release_date)
            if not include_google_analytics:
                data_partners = data_partners[:-1]

            # Create fake data tables. There's no guarantee the data was deleted so clean it again just in case.
            self.setup_fake_data_tables(
                onix_dataset_id, oaebu_settings_dataset_id, oaebu_fixtures_dataset_id, partner_release_date
            )

            # Pull info from Observatory API
            gcp_bucket_name = env.transform_bucket
            gcp_project_id = self.gcp_project_id

            # Expected sensor dag_ids
            sensor_dag_ids = ["onix"] + list(set([partner.dag_id_prefix for partner in data_partners]))
            sensor_dag_ids.sort()

            # Setup telescope
            start_date = pendulum.datetime(year=2021, month=5, day=9)
            crossref_start_date = pendulum.datetime(year=2018, month=5, day=14)
            telescope = OnixWorkflow(
                org_name=org_name,
                gcp_project_id=gcp_project_id,
                gcp_bucket_name=gcp_bucket_name,
                country_project_id=gcp_project_id,
                country_dataset_id=oaebu_settings_dataset_id,
                crossref_dataset_id=oaebu_crossref_dataset_id,
                onix_dataset_id=onix_dataset_id,
                onix_table_id=self.onix_table_id,
                subject_project_id=gcp_project_id,
                subject_dataset_id=oaebu_fixtures_dataset_id,
                data_partners=data_partners,
                start_date=start_date,
                crossref_start_date=crossref_start_date,
                mailto="agent@observatory.academy",
                workflow_id=2,
                max_threads=1,
            )

            # Skip dag existence check in sensor.
            for sensor in telescope.operators[0]:
                sensor.check_exists = False
                sensor.grace_period = timedelta(seconds=1)

            # Make DAG
            workflow_dag = telescope.make_dag()

            # If there is no dag run in the search interval, sensor will return success.
            expected_state = "success"
            with env.create_dag_run(workflow_dag, start_date):
                for task_id in sensor_dag_ids:
                    ti = env.run_task(f"{make_dag_id(task_id, org_name)}_sensor")
                    self.assertEqual(expected_state, ti.state)

            # Run Dummy Dags
            expected_state = "success"
            execution_date = pendulum.datetime(year=2021, month=5, day=16)
            release_date = pendulum.datetime(year=2021, month=5, day=22)
            for dag_id in sensor_dag_ids:
                dag = make_dummy_dag(make_dag_id(dag_id, org_name), execution_date)
                with env.create_dag_run(dag, execution_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task("dummy_task")
                    self.assertEqual(expected_state, ti.state)

            # Run end to end tests for DOI DAG
            with env.create_dag_run(workflow_dag, execution_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                for task_id in sensor_dag_ids:
                    ti = env.run_task(f"{make_dag_id(task_id, org_name)}_sensor")
                    self.assertEqual(expected_state, ti.state)

                # Mock make_release
                telescope.make_release = MagicMock(
                    return_value=OnixWorkflowRelease(
                        dag_id=make_dag_id(OnixWorkflow.DAG_ID_PREFIX, org_name),
                        release_date=release_date,
                        onix_release_date=partner_release_date,
                        gcp_project_id=self.gcp_project_id,
                        gcp_bucket_name=self.gcp_bucket_name,
                        onix_dataset_id=onix_dataset_id,
                        onix_table_id=self.onix_table_id,
                        oaebu_data_qa_dataset=oaebu_data_qa_dataset_id,
                        oaebu_latest_data_qa_dataset=oaebu_latest_data_qa_dataset_id,
                        workflow_dataset=onix_workflow_dataset_id,
                        oaebu_intermediate_dataset=oaebu_intermediate_dataset_id,
                        oaebu_dataset=oaebu_output_dataset_id,
                        oaebu_export_dataset=oaebu_export_dataset_id,
                        oaebu_latest_export_dataset=oaebu_latest_export_dataset_id,
                    )
                )

                release_suffix = release_date.strftime("%Y%m%d")

                # Make the SQL queries for data QA
                data_qa_sql = {
                    "onix_invalid_isbn": f"SELECT ISBN13 from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.onix_invalid_isbn{release_suffix}",
                    "onix_aggregate_metrics": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.onix_aggregate_metrics{release_suffix}",
                    "jstor_invalid_isbn": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.jstor_invalid_isbn{release_suffix}",
                    "jstor_invalid_eisbn": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.jstor_invalid_eisbn{release_suffix}",
                    "jstor_country_unmatched_ISBN": f"SELECT ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.jstor_country_unmatched_ISBN{release_suffix}",
                    "google_books_sales_invalid_isbn": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_books_sales_invalid_isbn{release_suffix}",
                    "google_books_sales_unmatched_Primary_ISBN": f"SELECT Primary_ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_books_sales_unmatched_Primary_ISBN{release_suffix}",
                    "google_books_traffic_invalid_isbn": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_books_traffic_invalid_isbn{release_suffix}",
                    "google_books_traffic_unmatched_Primary_ISBN": f"SELECT Primary_ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_books_traffic_unmatched_Primary_ISBN{release_suffix}",
                    "oapen_irus_uk_invalid_isbn": f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.oapen_irus_uk_invalid_isbn{release_suffix}",
                    "oapen_irus_uk_unmatched_ISBN": f"SELECT ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.oapen_irus_uk_unmatched_ISBN{release_suffix}",
                }
                if include_google_analytics:
                    data_qa_sql[
                        "google_analytics_invalid_isbn"
                    ] = f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_analytics_invalid_isbn{release_suffix}"

                    data_qa_sql[
                        "google_analytics_unmatched_publication_id"
                    ] = f"SELECT publication_id from {self.gcp_project_id}.{oaebu_data_qa_dataset_id}.google_analytics_unmatched_publication_id{release_suffix}"

                # Create the latest data QA SQL as well
                latest_data_qa_sql = {}
                for k, sql in data_qa_sql.items():
                    latest_data_qa_sql[k] = sql.replace(
                        oaebu_data_qa_dataset_id, oaebu_latest_data_qa_dataset_id
                    ).replace(release_suffix, "")

                # Aggregate works
                ti = env.run_task(telescope.aggregate_works.__name__)
                self.assertEqual(expected_state, ti.state)

                # Upload aggregation tables
                ti = env.run_task(telescope.upload_aggregation_tables.__name__)
                self.assertEqual(expected_state, ti.state)

                # Load work id table into bigquery
                ti = env.run_task(telescope.bq_load_workid_lookup.__name__)
                self.assertEqual(expected_state, ti.state)
                self.assert_table_content(
                    f"{self.gcp_project_id}.{onix_workflow_dataset_id}.onix_workid_isbn{release_suffix}",
                    load_jsonl(test_fixtures_folder("onix_workflow", "onix_workid_isbn.jsonl")),
                )

                # Load work id errors table into bigquery
                ti = env.run_task(telescope.bq_load_workid_lookup_errors.__name__)
                self.assertEqual(expected_state, ti.state)
                self.assert_table_content(
                    f"{self.gcp_project_id}.{onix_workflow_dataset_id}.onix_workid_isbn_errors{release_suffix}",
                    load_jsonl(test_fixtures_folder("onix_workflow", "onix_workid_isbn_errors.jsonl")),
                )

                # Load work family id table into bigquery
                ti = env.run_task(telescope.bq_load_workfamilyid_lookup.__name__)
                self.assertEqual(expected_state, ti.state)
                self.assert_table_content(
                    f"{self.gcp_project_id}.{onix_workflow_dataset_id}.onix_workfamilyid_isbn{release_suffix}",
                    load_jsonl(test_fixtures_folder("onix_workflow", "onix_workfamilyid_isbn.jsonl")),
                )

                # Load crossref metadata table into bigquery
                metadata_vcr = vcr.VCR(record_mode="none", before_record_request=vcr_ignore_condition)
                with metadata_vcr.use_cassette(self.metadata_cassette):
                    ti = env.run_task(telescope.create_oaebu_crossref_metadata_table.__name__)
                # Assertions
                self.assertEqual(expected_state, ti.state)
                self.assert_table_content(
                    f"{gcp_project_id}.{oaebu_crossref_dataset_id}.crossref_metadata{release_suffix}",
                    load_jsonl(test_fixtures_folder("onix_workflow", "crossref_metadata.jsonl")),
                )

                # Load crossref event table into bigquery
                with vcr.use_cassette(
                    self.events_cassette, record_mode="none", before_record_request=vcr_ignore_condition
                ):
                    ti = env.run_task(telescope.create_oaebu_crossref_events_table.__name__)
                self.assertEqual(expected_state, ti.state)
                self.assert_table_content(
                    f"{gcp_project_id}.{oaebu_crossref_dataset_id}.crossref_events{release_suffix}",
                    load_jsonl(test_fixtures_folder("onix_workflow", "crossref_events.jsonl")),
                )

                # Create book table in bigquery
                ti = env.run_task(telescope.create_oaebu_book_table.__name__)
                self.assertEqual(expected_state, ti.state)
                self.assert_table_content(
                    f"{gcp_project_id}.{oaebu_output_dataset_id}.book{release_suffix}",
                    load_jsonl(test_fixtures_folder("onix_workflow", "book.jsonl")),
                )

                # Create oaebu intermediate tables
                for data_partner in data_partners:
                    oaebu_dataset = data_partner.gcp_dataset_id
                    oaebu_table = data_partner.gcp_table_id

                    ti = env.run_task(
                        f"{telescope.create_oaebu_intermediate_table.__name__}.{oaebu_dataset}.{oaebu_table}"
                    )
                    self.assertEqual(expected_state, ti.state)

                # Create oaebu output tables
                ti = env.run_task(telescope.create_oaebu_book_product_table.__name__)
                fixture_name = "book_product_ga.jsonl" if include_google_analytics else "book_product.jsonl"
                self.assertEqual(expected_state, ti.state)
                self.assert_table_content(
                    f"{gcp_project_id}.{oaebu_output_dataset_id}.book_product{release_suffix}",
                    load_jsonl(test_fixtures_folder("onix_workflow", fixture_name)),
                )

                # ONIX isbn check
                ti = env.run_task(telescope.create_oaebu_data_qa_onix_isbn.__name__)
                self.assertEqual(expected_state, ti.state)

                # Check invalid ISBN13s picked up in ONIX
                records = run_bigquery_query(data_qa_sql["onix_invalid_isbn"])
                isbns = set([record["ISBN13"] for record in records])
                self.assertEqual(len(isbns), 1)
                self.assertTrue("112" in isbns)

                # ONIX aggregate metrics
                ti = env.run_task(telescope.create_oaebu_data_qa_onix_aggregate.__name__)
                self.assertEqual(expected_state, ti.state)

                # Check ONIX aggregate metrics are correct
                records = run_bigquery_query(data_qa_sql["onix_aggregate_metrics"])
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0]["table_size"], 4)
                self.assertEqual(records[0]["no_isbns"], 0)
                self.assertEqual(records[0]["no_relatedworks"], 0)
                self.assertEqual(records[0]["no_relatedproducts"], 3)
                self.assertEqual(records[0]["no_doi"], 4)
                self.assertEqual(records[0]["no_productform"], 0)
                self.assertEqual(records[0]["no_contributors"], 4)
                self.assertEqual(records[0]["no_titledetails"], 4)
                self.assertEqual(records[0]["no_publisher_urls"], 4)

                # JSTOR country isbn check
                ti = env.run_task(
                    f"{telescope.create_oaebu_data_qa_jstor_isbn.__name__}.{data_partners[0].gcp_dataset_id}.{data_partners[0].gcp_table_id}"
                )
                self.assertEqual(expected_state, ti.state)
                # Check JSTOR ISBN are valid

                records = run_bigquery_query(data_qa_sql["jstor_invalid_isbn"])
                isbns = set([record["ISBN"] for record in records])
                self.assertEqual(len(isbns), 4)
                self.assertTrue("111" in isbns)
                self.assertTrue("211" in isbns)
                self.assertTrue("113" in isbns)
                self.assertTrue("112" in isbns)

                # Check JSTOR eISBN are valid
                records = run_bigquery_query(data_qa_sql["jstor_invalid_eisbn"])
                isbns = set([record["eISBN"] for record in records])
                self.assertEqual(len(isbns), 4)
                self.assertTrue("111" in isbns)
                self.assertTrue("113" in isbns)
                self.assertTrue("112" in isbns)
                self.assertTrue(None in isbns)

                # JSTOR country intermediate unmatched isbns
                ti = env.run_task(
                    f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[0].gcp_dataset_id}.{data_partners[0].gcp_table_id}"
                )
                self.assertEqual(expected_state, ti.state)

                # Check JSTOR unmatched ISBN picked up
                records = run_bigquery_query(data_qa_sql["jstor_country_unmatched_ISBN"])
                isbns = set([record["ISBN"] for record in records])
                self.assertEqual(len(isbns), 3)
                self.assertTrue("111" in isbns)
                self.assertTrue("113" in isbns)
                self.assertTrue("211" in isbns)

                # JSTOR institution isbn check
                ti = env.run_task(
                    f"{telescope.create_oaebu_data_qa_jstor_isbn.__name__}.{data_partners[1].gcp_dataset_id}.{data_partners[1].gcp_table_id}"
                )
                self.assertEqual(expected_state, ti.state)

                # JSTOR institution intermediate unmatched isbns
                ti = env.run_task(
                    f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[1].gcp_dataset_id}.{data_partners[1].gcp_table_id}"
                )
                self.assertEqual(expected_state, ti.state)

                # Google Books Sales isbn check
                ti = env.run_task(telescope.create_oaebu_data_qa_google_books_sales_isbn.__name__)
                self.assertEqual(expected_state, ti.state)

                # Check Google Books Sales ISBN are valid
                records = run_bigquery_query(data_qa_sql["google_books_sales_invalid_isbn"])
                isbns = set([record["Primary_ISBN"] for record in records])
                self.assertEqual(len(isbns), 4)
                self.assertTrue("111" in isbns)
                self.assertTrue("211" in isbns)
                self.assertTrue("113" in isbns)
                self.assertTrue("112" in isbns)

                # Google Books Sales intermediate unmatched isbns
                ti = env.run_task(
                    f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[2].gcp_dataset_id}.{data_partners[2].gcp_table_id}"
                )
                self.assertEqual(expected_state, ti.state)

                # Check Google Books Sales unmatched ISBN picked up
                records = run_bigquery_query(data_qa_sql["google_books_sales_unmatched_Primary_ISBN"])
                isbns = set([record["Primary_ISBN"] for record in records])
                self.assertEqual(len(isbns), 3)
                self.assertTrue("111" in isbns)
                self.assertTrue("113" in isbns)
                self.assertTrue("211" in isbns)

                # Google Books Traffic isbn check
                ti = env.run_task(telescope.create_oaebu_data_qa_google_books_traffic_isbn.__name__)
                self.assertEqual(expected_state, ti.state)
                # Check Google Books Traffic ISBN are valid
                records = run_bigquery_query(data_qa_sql["google_books_traffic_invalid_isbn"])
                isbns = set([record["Primary_ISBN"] for record in records])
                self.assertEqual(len(isbns), 4)
                self.assertTrue("111" in isbns)
                self.assertTrue("211" in isbns)
                self.assertTrue("113" in isbns)
                self.assertTrue("112" in isbns)

                # Google Books Traffic intermediate unmatched isbns
                ti = env.run_task(
                    f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[3].gcp_dataset_id}.{data_partners[3].gcp_table_id}"
                )
                self.assertEqual(expected_state, ti.state)

                # Check Google Books Traffic unmatched ISBN picked up
                records = run_bigquery_query(data_qa_sql["google_books_traffic_unmatched_Primary_ISBN"])
                isbns = set([record["Primary_ISBN"] for record in records])
                self.assertEqual(len(isbns), 3)
                self.assertTrue("111" in isbns)
                self.assertTrue("113" in isbns)
                self.assertTrue("211" in isbns)

                # OAPEN IRUS UK isbn check
                ti = env.run_task(telescope.create_oaebu_data_qa_oapen_irus_uk_isbn.__name__)
                self.assertEqual(expected_state, ti.state)

                # Check OAPEN IRUS UK ISBN are valid
                records = run_bigquery_query(data_qa_sql["oapen_irus_uk_invalid_isbn"])
                isbns = set([record["ISBN"] for record in records])
                self.assertEqual(len(isbns), 4)
                self.assertTrue("111" in isbns)
                self.assertTrue("113" in isbns)
                self.assertTrue("112" in isbns)
                self.assertTrue("211" in isbns)

                # OAPEN IRUS UK intermediate unmatched isbns
                ti = env.run_task(
                    f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[4].gcp_dataset_id}.{data_partners[4].gcp_table_id}"
                )
                self.assertEqual(expected_state, ti.state)

                # Check OAPEN IRUS UK unmatched ISBN picked up
                records = run_bigquery_query(data_qa_sql["oapen_irus_uk_unmatched_ISBN"])
                isbns = set([record["ISBN"] for record in records])
                self.assertEqual(len(isbns), 3)
                self.assertTrue("111" in isbns)
                self.assertTrue("113" in isbns)
                self.assertTrue("211" in isbns)

                if include_google_analytics:
                    # Google Analytics isbn check
                    env.run_task(telescope.create_oaebu_data_qa_google_analytics_isbn.__name__)

                    # Check Google Analytics ISBN are valid
                    records = run_bigquery_query(data_qa_sql["google_analytics_invalid_isbn"])
                    isbns = set([record["publication_id"] for record in records])
                    self.assertEqual(len(isbns), 1)
                    self.assertTrue("(none)" in isbns)

                    # Google Books Analytics unmatched isbns
                    print("---------------------------------------")
                    print(f"{data_partners[5].gcp_dataset_id}.{data_partners[5].gcp_table_id}")
                    print("---------------------------------------")
                    env.run_task(
                        f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[5].gcp_dataset_id}.{data_partners[5].gcp_table_id}"
                    )

                    # Check Google Analytics unmatched ISBN picked up
                    records = run_bigquery_query(data_qa_sql["google_analytics_unmatched_publication_id"])
                    isbns = set([record["publication_id"] for record in records])
                    self.assertEqual(len(isbns), 1)
                    self.assertTrue("(none)" in isbns)

                # Export oaebu elastic tables
                export_tables = [
                    ("book_product_list", 4),
                    ("book_product_metrics", 5),
                    ("book_product_metrics_country", 18),
                    ("book_product_metrics_institution", 1),
                    ("institution_list", 1),
                    ("book_product_metrics_city", 26),
                    ("book_product_metrics_events", 3),
                    ("book_product_publisher_metrics", 1),
                    ("book_product_subject_bic_metrics", 0),
                    ("book_product_subject_bisac_metrics", 0),
                    ("book_product_subject_thema_metrics", 0),
                    ("book_product_year_metrics", 1),
                    ("book_product_subject_year_metrics", 0),
                    ("book_product_author_metrics", 0),
                ]

                for table, exp_rows in export_tables:
                    ti = env.run_task(f"{telescope.export_oaebu_table.__name__}.{table}")
                    self.assertEqual(expected_state, ti.state, msg=f"table: {table}")
                    # Check that the data_export tables tables exist and have the correct number of rows
                    table_id = f"{self.gcp_project_id}.{oaebu_export_dataset_id}.{self.gcp_project_id.replace('-', '_')}_{table}{release_suffix}"
                    self.assert_table_integrity(table_id, expected_rows=exp_rows)

                # Book product list content assertion
                self.assert_table_content(
                    f"{gcp_project_id}.{oaebu_export_dataset_id}.{self.gcp_project_id.replace('-', '_')}_book_product_list{release_suffix}",
                    load_jsonl(test_fixtures_folder("onix_workflow", "book_product_list.jsonl")),
                )

                # Export oaebu elastic qa table
                ti = env.run_task(telescope.export_oaebu_qa_metrics.__name__)
                self.assertEqual(expected_state, ti.state)

                # Validate the joins worked
                # JSTOR
                sql = f"SELECT ISBN, work_id, work_family_id from {self.gcp_project_id}.{oaebu_intermediate_dataset_id}.{self.fake_partner_dataset}_jstor_country_matched{release_suffix}"
                records = run_bigquery_query(sql)
                oaebu_works = {record["ISBN"]: record["work_id"] for record in records}
                oaebu_wfam = {record["ISBN"]: record["work_family_id"] for record in records}

                self.assertTrue(
                    oaebu_works["112"] == oaebu_works["2222222222222"]
                    and oaebu_works["113"] is None
                    and oaebu_works["111"] is None
                    and oaebu_works["211"] is None
                )

                self.assertTrue(
                    oaebu_wfam["112"] == oaebu_wfam["2222222222222"]
                    and oaebu_wfam["113"] is None
                    and oaebu_wfam["111"] is None
                    and oaebu_wfam["211"] is None
                )

                # OAPEN IRUS UK
                sql = f"SELECT ISBN, work_id, work_family_id from {self.gcp_project_id}.{oaebu_intermediate_dataset_id}.{self.fake_partner_dataset}_oapen_irus_uk_matched{release_suffix}"
                records = run_bigquery_query(sql)
                oaebu_works = {record["ISBN"]: record["work_id"] for record in records}
                oaebu_wfam = {record["ISBN"]: record["work_family_id"] for record in records}

                self.assertTrue(
                    oaebu_works["112"] == oaebu_works["2222222222222"]
                    and oaebu_works["113"] is None
                    and oaebu_works["111"] is None
                    and oaebu_works["211"] is None
                )

                self.assertTrue(
                    oaebu_wfam["112"] == oaebu_wfam["2222222222222"]
                    and oaebu_wfam["113"] is None
                    and oaebu_wfam["111"] is None
                    and oaebu_wfam["211"] is None
                )

                # Create the views from the export and data QA tables
                env.run_task(telescope.create_oaebu_latest_views.__name__)

                # Check data QA views are the same as the tables
                for k in data_qa_sql.keys():
                    table_data = run_bigquery_query(data_qa_sql[k])
                    view_data = run_bigquery_query(latest_data_qa_sql[k])
                    self.assertEqual(table_data, view_data)

                for export_table, exp_rows in export_tables:
                    export_view = run_bigquery_query(
                        f"SELECT * FROM {self.gcp_project_id}.{oaebu_latest_export_dataset_id}.{self.gcp_project_id.replace('-', '_')}_{export_table}"
                    )
                    self.assertEqual(expected_state, ti.state, msg=f"table: {table}")
                    # Check that the data_export tables (views) has the correct number of rows
                    self.assertEqual(len(export_view), exp_rows)

                # Cleanup
                env.run_task(telescope.cleanup.__name__)

                # add_dataset_release_task
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 0)
                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)
                dataset_releases = get_dataset_releases(dataset_id=1)
                self.assertEqual(len(dataset_releases), 1)

    def test_telescope(self):
        """Test that ONIX Workflow runs when Google Analytics is not included"""

        self.run_telescope_tests(org_name="Curtin Press", include_google_analytics=False)

    def test_telescope_with_google_analytics(self):
        """Test that ONIX Workflow runs when Google Analytics is included"""

        self.run_telescope_tests(org_name="ANU Press", include_google_analytics=True)
