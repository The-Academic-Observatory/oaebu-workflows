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

# Author: Tuan Chien, Keegan Smith

import os
import shutil
from datetime import timedelta
from typing import List
import tempfile
from unittest.mock import MagicMock, patch

import pendulum
from airflow.models import DagBag

from oaebu_workflows.config import schema_folder as default_schema_folder, test_fixtures_folder
from oaebu_workflows.oaebu_partners import OAEBU_DATA_PARTNERS, OAEBU_METADATA_PARTNERS, OaebuPartner, partner_from_str
from oaebu_workflows.onix_workflow.onix_workflow import (
    copy_latest_export_tables,
    create_dag,
    dois_from_table,
    get_onix_records,
    insert_into_schema,
    _aggregate_works,
    _make_release,
    OnixWorkflowRelease,
)
from observatory_platform.airflow.workflow import Workflow, make_workflow_folder
from observatory_platform.config import module_file_path
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.files import load_jsonl
from observatory_platform.google.bigquery import bq_find_schema, bq_run_query, bq_sharded_table_id, bq_table_id
from observatory_platform.google.gcs import gcs_blob_name_from_path
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import (
    bq_load_tables,
    load_and_parse_json,
    make_dummy_dag,
    random_id,
    SandboxTestCase,
    Table,
)


class TestOnixWorkflow(SandboxTestCase):
    """Functionally test the workflow"""

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.onix_table_id = "onix"
        self.test_onix_folder = random_id()  # "onix_workflow_test_onix_table"

        # For telescope Initialisation
        self.snapshot_date = pendulum.datetime(2021, 1, 1)
        self.dag_id = "onix_workflow_test"
        self.gcp_project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.fake_onix_data_partner = OaebuPartner(  # Onix data partner to pass to the telescope for initialisation
            type_id="irus_oapen",
            bq_dataset_id="test_dataset",
            bq_table_name="test_table",
            isbn_field_name="isbn",
            title_field_name="title",
            sharded=True,
            schema_path=os.path.join(default_schema_folder(workflow_module="onix_telescope"), "onix.json"),
        )
        # Use all data partners except the jstor collections
        self.data_partner_list = [i for i in OAEBU_DATA_PARTNERS.keys()]
        self.data_partner_list.remove("jstor_country_collection")
        self.data_partner_list.remove("jstor_institution_collection")

        # fixtures folder location
        self.fixtures_folder = test_fixtures_folder(workflow_module="onix_workflow")
        self.maxDiff = None

    @patch("oaebu_workflows.onix_workflow.onix_workflow.bq_select_table_shard_dates")
    def test_make_release_sharded(self, mock_sel_table_suffixes):
        """Tests that _make_release works as intended for a sharded metadata partner"""

        onix_snapshot_date = self.snapshot_date.add(days=1)
        metadata_partner = OAEBU_METADATA_PARTNERS["onix"]
        expected_onix_table = bq_sharded_table_id(
            self.fake_cloud_workspace.project_id,
            metadata_partner.bq_dataset_id,
            metadata_partner.bq_table_name,
            onix_snapshot_date,
        )
        crossref_snapshot_date = self.snapshot_date

        env = SandboxEnvironment(self.gcp_project_id, self.data_location)
        with env.create():
            # Success case
            mock_sel_table_suffixes.side_effect = [[onix_snapshot_date], [crossref_snapshot_date]]
            release = _make_release(
                dag_id="test_make_release",
                run_id="test_run_id",
                snapshot_date=self.snapshot_date.add(days=7),
                metadata_partner=metadata_partner,
                cloud_workspace=self.fake_cloud_workspace,
                bq_master_crossref_project_id=self.fake_cloud_workspace.project_id,
                bq_master_crossref_dataset_id="crossref_metadata",
                bq_master_crossref_metadata_table_name="crossref_metadata",
                client=MagicMock(),
            )
            self.assertEqual(release.dag_id, "test_make_release")
            self.assertEqual(release.workslookup_path, os.path.join(release.transform_folder, "worksid.jsonl.gz"))
            self.assertEqual(
                release.workslookup_errors_path, os.path.join(release.transform_folder, "worksid_errors.jsonl.gz")
            )
            self.assertEqual(
                release.worksfamilylookup_path, os.path.join(release.transform_folder, "workfamilyid.jsonl.gz")
            )
            self.assertEqual(
                release.crossref_metadata_path, os.path.join(release.transform_folder, "crossref_metadata.jsonl.gz")
            )
            self.assertEqual(expected_onix_table, release.onix_table_id)
            self.assertEqual(crossref_snapshot_date, release.crossref_master_snapshot_date)

            # No ONIX releases found
            mock_sel_table_suffixes.side_effect = [[]]
            with self.assertRaisesRegex(RuntimeError, "ONIX"):
                _make_release(
                    dag_id="test_make_release",
                    run_id="test_run_id",
                    snapshot_date=self.snapshot_date.add(days=7),
                    metadata_partner=metadata_partner,
                    cloud_workspace=self.fake_cloud_workspace,
                    bq_master_crossref_project_id=self.fake_cloud_workspace.project_id,
                    bq_master_crossref_dataset_id="crossref_metadata",
                    bq_master_crossref_metadata_table_name="crossref_metadata",
                    client=MagicMock(),
                )

            # No Crossref releases found
            mock_sel_table_suffixes.side_effect = [[onix_snapshot_date], []]
            with self.assertRaisesRegex(RuntimeError, "Crossref"):
                _make_release(
                    dag_id="test_make_release",
                    run_id="test_run_id",
                    snapshot_date=self.snapshot_date.add(days=7),
                    metadata_partner=metadata_partner,
                    cloud_workspace=self.fake_cloud_workspace,
                    bq_master_crossref_project_id=self.fake_cloud_workspace.project_id,
                    bq_master_crossref_dataset_id="crossref_metadata",
                    bq_master_crossref_metadata_table_name="crossref_metadata",
                    client=MagicMock(),
                )

    @patch("oaebu_workflows.onix_workflow.onix_workflow.bq_select_table_shard_dates")
    def test_make_release_unsharded(self, mock_sel_table_suffixes):
        """Tests that _make_release works as intended for an unsharded metadata partner"""

        metadata_partner = OAEBU_METADATA_PARTNERS["onix_view"]  # Unsharded metadata partner
        expected_onix_table = bq_table_id(
            self.fake_cloud_workspace.project_id,
            metadata_partner.bq_dataset_id,
            metadata_partner.bq_table_name,
        )
        crossref_snapshot_date = self.snapshot_date

        # Success case
        env = SandboxEnvironment(self.gcp_project_id, self.data_location)
        with env.create():
            mock_sel_table_suffixes.return_value = [crossref_snapshot_date]
            release = _make_release(
                dag_id="test_make_release",
                run_id="test_run_id",
                snapshot_date=self.snapshot_date.add(days=7),
                metadata_partner=metadata_partner,
                cloud_workspace=self.fake_cloud_workspace,
                bq_master_crossref_project_id=self.fake_cloud_workspace.project_id,
                bq_master_crossref_dataset_id="crossref_metadata",
                bq_master_crossref_metadata_table_name="crossref_metadata",
                client=MagicMock(),
            )
            self.assertEqual(release.dag_id, "test_make_release")

            # Test release file names are as expected
            self.assertEqual(release.workslookup_path, os.path.join(release.transform_folder, "worksid.jsonl.gz"))
            self.assertEqual(
                release.workslookup_errors_path, os.path.join(release.transform_folder, "worksid_errors.jsonl.gz")
            )
            self.assertEqual(
                release.worksfamilylookup_path, os.path.join(release.transform_folder, "workfamilyid.jsonl.gz")
            )
            self.assertEqual(
                release.crossref_metadata_path, os.path.join(release.transform_folder, "crossref_metadata.jsonl.gz")
            )

            # Test that the onix table and crossref snapshots are as expected
            self.assertEqual(expected_onix_table, release.onix_table_id)
            self.assertEqual(crossref_snapshot_date, release.crossref_master_snapshot_date)

            # No Crossref releases found
            mock_sel_table_suffixes.return_value = []
            with self.assertRaisesRegex(RuntimeError, "Crossref"):
                _make_release(
                    dag_id="test_make_release",
                    run_id="test_run_id",
                    snapshot_date=self.snapshot_date.add(days=7),
                    metadata_partner=metadata_partner,
                    cloud_workspace=self.fake_cloud_workspace,
                    bq_master_crossref_project_id=self.fake_cloud_workspace.project_id,
                    bq_master_crossref_dataset_id="crossref_metadata",
                    bq_master_crossref_metadata_table_name="crossref_metadata",
                    client=MagicMock(),
                )

    def test_dag_load(self):
        """Test that the DAG loads"""

        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id="onix_workflow_test_dag_load",
                    name="Onix Workflow Test Dag Load",
                    class_name="oaebu_workflows.onix_workflow.onix_workflow",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(
                        sensor_dag_ids=[
                            "google_analytics3_test",
                            "google_books_sales_test",
                            "google_books_traffic_test",
                            "jstor_country_test",
                            "jstor_institution_test",
                            "irus_oapen_test",
                        ],
                        data_partners=self.data_partner_list + ["nonexistent_partner"],
                        metadata_partner="onix",
                    ),
                )
            ]
        )

        dag_file = os.path.join(module_file_path("dags"), "load_dags.py")
        with env.create():
            # This should raise one error for nonexistent partner
            with tempfile.TemporaryDirectory() as dag_folder:
                shutil.copy(dag_file, os.path.join(dag_folder, os.path.basename(dag_file)))
                dag_bag = DagBag(dag_folder=dag_folder)
                self.assertNotEqual({}, dag_bag.import_errors)
                self.assertEqual(len(dag_bag.import_errors), 1)

        # Remove the nonexistent partner and onix partner
        env.workflows[0].kwargs["data_partners"] = env.workflows[0].kwargs["data_partners"][:-1]
        with env.create():
            # Should not raise any errors
            self.assert_dag_load_from_config("onix_workflow_test_dag_load", dag_file)

    def test_dag_structure(self):
        """Tests that the dag structure is created as expected on dag load"""

        ## No data partners - dry run
        env = SandboxEnvironment(self.gcp_project_id, self.data_location)
        with env.create():
            dag = create_dag(
                dag_id=self.dag_id,
                cloud_workspace=self.fake_cloud_workspace,
                data_partners=[],
                metadata_partner="onix",
                sensor_dag_ids=[],
            )
            expected_dag_structure = {
                "check_dependencies": ["make_release"],
                "make_release": [
                    "export_tables.export_book_metrics_author",
                    "export_tables.export_book_list",
                    "export_tables.export_book_metrics_country",
                    "aggregate_works",
                    "export_tables.export_book_metrics",
                    "create_book_product_table",
                    "add_new_dataset_releases",
                    "create_crossref_metadata_table",
                    "create_book_table",
                    "update_latest_export_tables",
                    "cleanup_workflow",
                    "export_tables.export_book_metrics_subjects",
                ],
                "aggregate_works": ["create_crossref_metadata_table"],
                "create_crossref_metadata_table": ["create_book_table"],
                "create_book_table": ["create_book_product_table"],
                "create_book_product_table": [
                    "export_tables.export_book_metrics_author",
                    "export_tables.export_book_list",
                    "export_tables.export_book_metrics_country",
                    "export_tables.export_book_metrics",
                    "export_tables.export_book_metrics_subjects",
                ],
                "export_tables.export_book_list": ["update_latest_export_tables"],
                "export_tables.export_book_metrics": ["update_latest_export_tables"],
                "export_tables.export_book_metrics_country": ["update_latest_export_tables"],
                "export_tables.export_book_metrics_author": ["update_latest_export_tables"],
                "export_tables.export_book_metrics_subjects": ["update_latest_export_tables"],
                "update_latest_export_tables": ["add_new_dataset_releases"],
                "add_new_dataset_releases": ["cleanup_workflow"],
                "cleanup_workflow": [],
            }
            self.assert_dag_structure(expected_dag_structure, dag)

        ## All data partners
        env = SandboxEnvironment(self.gcp_project_id, self.data_location)
        with env.create():
            sensor_dag_ids = ["jstor", "irus_oapen", "google_books", "onix", "google_analytics3"]
            dag = create_dag(
                dag_id=self.dag_id,
                cloud_workspace=self.fake_cloud_workspace,
                data_partners=self.data_partner_list,
                metadata_partner="onix",
                sensor_dag_ids=sensor_dag_ids,
            )
            sensor_tasks = [f"sensors.{i}_sensor" for i in sensor_dag_ids]
            intermediate_tasks = [f"intermediate_tables.intermediate_{i}" for i in self.data_partner_list]
            expected_dag_structure = {
                # Everything from sensor_dag_ids should be in here
                "check_dependencies": sensor_tasks,
                **{task: ["make_release"] for task in sensor_tasks},  # e.g. "sensors.jstor_sensor": ["make_release"],
                "make_release": [
                    "aggregate_works",
                    "export_tables.export_book_metrics_author",
                    "export_tables.export_book_institution_list",
                    "export_tables.export_book_metrics",
                    "add_new_dataset_releases",
                    "create_book_table",
                    "export_tables.export_book_metrics_subjects",
                    "update_latest_export_tables",
                    "export_tables.export_book_list",
                    "cleanup_workflow",
                    "create_book_product_table",
                    "export_tables.export_book_metrics_country",
                    "create_crossref_metadata_table",
                    "export_tables.export_book_metrics_institution",
                    *intermediate_tasks,
                ],
                "aggregate_works": ["create_crossref_metadata_table"],
                "create_crossref_metadata_table": ["create_book_table"],
                "create_book_table": intermediate_tasks,
                **{
                    task: ["create_book_product_table"] for task in intermediate_tasks
                },  # e.g. "intermediate_tables.intermediate_google_books_sales": ["create_book_product_table"],
                "create_book_product_table": [
                    "export_tables.export_book_metrics_author",
                    "export_tables.export_book_institution_list",
                    "export_tables.export_book_metrics_country",
                    "export_tables.export_book_metrics_institution",
                    "export_tables.export_book_metrics_subjects",
                    "export_tables.export_book_list",
                    "export_tables.export_book_metrics",
                ],
                "export_tables.export_book_list": ["update_latest_export_tables"],
                "export_tables.export_book_institution_list": ["update_latest_export_tables"],
                "export_tables.export_book_metrics_institution": ["update_latest_export_tables"],
                "export_tables.export_book_metrics": ["update_latest_export_tables"],
                "export_tables.export_book_metrics_country": ["update_latest_export_tables"],
                "export_tables.export_book_metrics_author": ["update_latest_export_tables"],
                "export_tables.export_book_metrics_subjects": ["update_latest_export_tables"],
                "update_latest_export_tables": ["add_new_dataset_releases"],
                "add_new_dataset_releases": ["cleanup_workflow"],
                "cleanup_workflow": [],
            }
            self.assert_dag_structure(expected_dag_structure, dag)

    @patch("oaebu_workflows.onix_workflow.onix_workflow.bq_run_query")
    def test_create_and_load_aggregate_works_table(self, mock_bq_query):
        mock_bq_query.return_value = TestOnixWorkflow.onix_data
        workslookup_expected = [
            {"isbn13": "112", "work_id": "111"},
            {"isbn13": "111", "work_id": "111"},
            {"isbn13": "211", "work_id": "211"},
        ]
        workslookup_errors_expected = [
            {
                "Error": "Product ISBN13:111 is a manifestation of ISBN13:113, which is not given as a product identifier in any ONIX product record."
            }
        ]
        worksfamilylookup_expected = [
            {"isbn13": "112", "work_family_id": "111"},
            {"isbn13": "111", "work_family_id": "111"},
            {"isbn13": "211", "work_family_id": "111"},
        ]

        env = SandboxEnvironment(self.gcp_project_id, self.data_location)
        with env.create():
            bq_onix_workflow_dataset = env.add_dataset()
            bq_worksid_table_name = "onix_workid_isbn"
            bq_worksid_error_table_name = "onix_workid_isbn_errors"
            bq_workfamilyid_table_name = "onix_workfamilyid_isbn"

            release = OnixWorkflowRelease(
                dag_id=self.dag_id,
                run_id="test_run_id",
                snapshot_date=self.snapshot_date,
                crossref_master_snapshot_date=self.snapshot_date,
                onix_table_id="fake.onix.table_id",  # only passed through to get_onix_records, which is mocked via bq_run_query
            )

            status = _aggregate_works(
                release=release,
                cloud_workspace=env.cloud_workspace,
                bq_onix_workflow_dataset=bq_onix_workflow_dataset,
                bq_worksid_table_name=bq_worksid_table_name,
                bq_worksid_error_table_name=bq_worksid_error_table_name,
                bq_workfamilyid_table_name=bq_workfamilyid_table_name,
                schema_folder=default_schema_folder(workflow_module="onix_workflow"),
            )
            self.assertTrue(status)

            ### Make Assertions ###
            self.assertTrue(os.path.exists(release.workslookup_path))
            self.assertTrue(os.path.exists(release.workslookup_errors_path))
            self.assertTrue(os.path.exists(release.worksfamilylookup_path))

            self.assert_blob_integrity(
                env.transform_bucket, gcs_blob_name_from_path(release.workslookup_path), release.workslookup_path
            )
            self.assert_blob_integrity(
                env.transform_bucket,
                gcs_blob_name_from_path(release.workslookup_errors_path),
                release.workslookup_errors_path,
            )
            self.assert_blob_integrity(
                env.transform_bucket,
                gcs_blob_name_from_path(release.worksfamilylookup_path),
                release.worksfamilylookup_path,
            )

            table_id = bq_sharded_table_id(
                env.cloud_workspace.project_id, bq_onix_workflow_dataset, bq_worksid_table_name, release.snapshot_date
            )
            self.assert_table_integrity(table_id, len(workslookup_expected))
            self.assert_table_content(table_id, load_jsonl(release.workslookup_path), primary_key="isbn13")
            self.assert_table_content(table_id, workslookup_expected, primary_key="isbn13")

            table_id = bq_sharded_table_id(
                env.cloud_workspace.project_id,
                bq_onix_workflow_dataset,
                bq_worksid_error_table_name,
                release.snapshot_date,
            )
            self.assert_table_integrity(table_id, len(workslookup_errors_expected))
            self.assert_table_content(table_id, load_jsonl(release.workslookup_errors_path), primary_key="Error")
            self.assert_table_content(table_id, workslookup_errors_expected, primary_key="Error")

            table_id = bq_sharded_table_id(
                env.cloud_workspace.project_id,
                bq_onix_workflow_dataset,
                bq_workfamilyid_table_name,
                release.snapshot_date,
            )
            self.assert_table_integrity(table_id, len(worksfamilylookup_expected))
            self.assert_table_content(table_id, load_jsonl(release.worksfamilylookup_path), primary_key="isbn13")
            self.assert_table_content(table_id, worksfamilylookup_expected, primary_key="isbn13")

    @patch("oaebu_workflows.onix_workflow.onix_workflow.bq_run_query")
    def test_get_onix_records(self, mock_bq_query):
        mock_bq_query.return_value = TestOnixWorkflow.onix_data
        records = get_onix_records("test_table_id")
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0]["ISBN13"], "111")

    def test_insert_into_schema(self):
        """Tests the instert_into_schema function"""

        # Inserting a field into a schema with no existing fields
        schema_base = []
        insert_field = {"name": "field1", "type": "string"}
        result = insert_into_schema(schema_base, insert_field)
        self.assertEqual(result, [{"name": "field1", "type": "string"}])

        # Inserting a field into a schema with existing fields
        schema_base = [{"name": "field1", "type": "string"}]
        insert_field = {"name": "field2", "type": "integer"}
        result = insert_into_schema(schema_base, insert_field)
        self.assertEqual(result, [{"name": "field1", "type": "string"}, {"name": "field2", "type": "integer"}])

        # Inserting a field with no name into a schema
        schema_base = [{"name": "field1", "type": "string"}]
        insert_field = {"type": "integer"}
        with self.assertRaises(ValueError):
            insert_into_schema(schema_base, insert_field, "field2")

    def test_utility_functions(self):
        """
        Test the standalone functions in the Onix workflow that aren't specifially tested in other classes
        """

        ############################
        ### Test dois_from_table ###
        ############################

        env = SandboxEnvironment(self.gcp_project_id, self.data_location)
        fake_doi_isbn_dataset_id = env.add_dataset(prefix="doi_isbn_test")
        fake_sharded_dataset = env.add_dataset(prefix="sharded_data")
        fake_copied_export_dataset = env.add_dataset(prefix="copied_export")
        fake_table_schema = os.path.join(default_schema_folder(workflow_module="onix_telescope"), "onix.json")
        with env.create():
            doi_table_file = os.path.join(self.fixtures_folder, "doi_isbn_query_test.jsonl")

            fake_doi_isbn_table = load_jsonl(doi_table_file)
            test_table = Table(
                "doi_isbn_test",
                False,
                fake_doi_isbn_dataset_id,
                fake_doi_isbn_table,
                fake_table_schema,  # use onix schema just for ease uploading
            )

            release_date = pendulum.datetime(2022, 6, 13)
            bq_load_tables(
                tables=[test_table],
                bucket_name=env.transform_bucket,
                snapshot_date=release_date,
                project_id=self.gcp_project_id,
            )
            table_id = bq_table_id(self.gcp_project_id, fake_doi_isbn_dataset_id, "doi_isbn_test")
            actual_dois = dois_from_table(table_id, doi_column_name="DOI")
            fake_doi_isbns = {entry["DOI"] for entry in fake_doi_isbn_table}

            # Check there are no duplicates and the contents are the same
            self.assertEqual(len(actual_dois), len(fake_doi_isbns))
            self.assertEqual(actual_dois, fake_doi_isbns)

            #############################################
            ### Test copy_latest_export_tables ###
            #############################################

            # We will use the onix.jsonl fixture for testing.
            previous_release_date = pendulum.datetime(2022, 6, 6)  # Add another release date
            data = [fake_doi_isbn_table, fake_doi_isbn_table[:2]]
            for release, table in zip([release_date, previous_release_date], data):
                test_table = Table("data_export", True, fake_sharded_dataset, table, fake_table_schema)
                bq_load_tables(
                    tables=[test_table],
                    bucket_name=env.transform_bucket,
                    snapshot_date=release,
                    project_id=self.gcp_project_id,
                )

            # Now make the copied tables from this dataset
            copy_latest_export_tables(
                project_id=self.gcp_project_id,
                from_dataset=fake_sharded_dataset,
                to_dataset=fake_copied_export_dataset,
                date_match=release_date.strftime("%Y%m%d"),
                data_location=self.data_location,
            )

            # Grab the data from the copied tables and make assertions
            copied_data = bq_run_query(f"SELECT * FROM {self.gcp_project_id}.{fake_copied_export_dataset}.data_export")
            copied_isbns = [entry["ISBN13"] for entry in copied_data]
            copied_dois = [entry["DOI"] for entry in copied_data]
            actual_isbns = [entry["ISBN13"] for entry in fake_doi_isbn_table]
            actual_dois = [entry["DOI"] for entry in fake_doi_isbn_table]

            self.assertEqual(len(copied_data), len(fake_doi_isbn_table))
            self.assertEqual(len(actual_isbns), len(copied_isbns))
            self.assertEqual(sorted(actual_isbns), sorted(copied_isbns))
            self.assertEqual(len(actual_dois), len(copied_dois))
            self.assertEqual(sorted(actual_dois), sorted(copied_dois))

    def setup_fake_lookup_tables(
        self, settings_dataset_id: str, fixtures_dataset_id: str, release_date: pendulum.DateTime, bucket_name: str
    ):
        """Create a new onix and subject lookup and country tables with their own dataset and table ids.
        Populate them with some fake data.

        :param settings_dataset_id: The dataset to store the country table
        :param fixtures_dataset_id: The dataset to store the lookup tables
        :param release_date: The release/snapshot date
        :param bucket_name: The GCP bucket name to load the data to
        """

        table_and_dataset = [
            ("bic_lookup", fixtures_dataset_id),
            ("bisac_lookup", fixtures_dataset_id),
            ("thema_lookup", fixtures_dataset_id),
            ("country", settings_dataset_id),
        ]

        # Create the table objects
        onix_test_schema_folder = os.path.join(self.fixtures_folder, "schema")
        tables = []
        for table_name, dataset in table_and_dataset:
            tables.append(
                Table(
                    table_name,
                    False,
                    dataset,
                    load_jsonl(os.path.join(self.fixtures_folder, "e2e_inputs", f"{table_name}.jsonl")),
                    bq_find_schema(path=onix_test_schema_folder, table_name=table_name),
                )
            )

        # Load tables into bigquery
        bq_load_tables(
            tables=tables, bucket_name=bucket_name, snapshot_date=release_date, project_id=self.gcp_project_id
        )

    def setup_input_data(
        self,
        data_partners: List[str],
        settings_dataset_id: str,
        fixtures_dataset_id: str,
        crossref_master_dataset_id: str,
        partner_dataset: str,
        onix_dataset_id: str,
        release_date: pendulum.DateTime,
        bucket_name: str,
    ) -> List[OaebuPartner]:
        """Uploads the data partner fixtures to their respective GCP bucket and bigquery tables.
        Creates the partners based on the originals - but changes the dataset ids for tests.
        Create a new onix and subject lookup and country tables with their own dataset and table ids.

        :param data_partners: The list of data partners
        :param settings_dataset_id: The dataset to store the country table
        :param fixtures_dataset_id: The dataset to store the lookup tables
        :param partner_dataset: The bigquery dataset ID to load the data partner tables to
        :param crossref_master_dataset_id: The bigquery dataset ID of the master crossref table
        :param onix_dataaset: The Bigquery dataset ID to load the onix partner table to
        :param release_date: The release/snapshot date of sharded tables
        :param bucket_name: The name of the bucket to upload the jsonl files to
        :return: The resulting OaebuPartners
        """

        ############################################
        ### Upload lookups, country and crossref ###
        ############################################

        table_and_dataset = [
            ("bic_lookup", fixtures_dataset_id, False),
            ("bisac_lookup", fixtures_dataset_id, False),
            ("thema_lookup", fixtures_dataset_id, False),
            ("country", settings_dataset_id, False),
            ("crossref_metadata_master", crossref_master_dataset_id, True),
        ]

        # Create the table objects
        onix_test_schema_folder = os.path.join(self.fixtures_folder, "schema")
        tables = []
        for table_name, dataset, sharded in table_and_dataset:
            tables.append(
                Table(
                    table_name,
                    sharded,
                    dataset,
                    load_jsonl(os.path.join(self.fixtures_folder, "e2e_inputs", f"{table_name}.jsonl")),
                    bq_find_schema(path=onix_test_schema_folder, table_name=table_name),
                )
            )

        ###########################
        ### Upload partner data ###
        ###########################

        # Create metadata table
        metadata_partner = partner_from_str("onix", metadata_partner=True)
        metadata_partner.bq_dataset_id = onix_dataset_id
        tables.append(
            Table(
                "onix",
                metadata_partner.sharded,
                metadata_partner.bq_dataset_id,
                load_jsonl(os.path.join(self.fixtures_folder, "e2e_inputs", "onix.jsonl")),
                metadata_partner.schema_path,
            )
        )

        # Create partner objects define tables
        partners = []
        for partner_name in data_partners:
            partner = partner_from_str(partner_name)
            partner.bq_dataset_id = partner_dataset
            partners.append(partner)
            tables.append(
                Table(
                    partner_name,
                    partner.sharded,
                    partner.bq_dataset_id,
                    load_jsonl(os.path.join(self.fixtures_folder, "e2e_inputs", f"{partner_name}.jsonl")),
                    partner.schema_path,
                )
            )

        # Load tables into bigquery
        bq_load_tables(
            tables=tables, bucket_name=bucket_name, snapshot_date=release_date, project_id=self.gcp_project_id
        )

        return partners, metadata_partner

    def test_workflow(self):
        """End to end test of the ONIX workflow"""

        env = SandboxEnvironment(self.gcp_project_id, self.data_location)

        onix_workflow_dataset_id = env.add_dataset(prefix="onix_workflow")
        master_crossref_dataset_id = env.add_dataset(prefix="crossref_master")
        oaebu_intermediate_dataset_id = env.add_dataset(prefix="oaebu_intermediate")
        oaebu_output_dataset_id = env.add_dataset(prefix="oaebu_output")
        oaebu_export_dataset_id = env.add_dataset(prefix="oaebu_export")
        oaebu_latest_export_dataset_id = env.add_dataset(prefix="oaebu_export_latest")
        oaebu_settings_dataset_id = env.add_dataset(prefix="settings")
        oaebu_fixtures_dataset_id = env.add_dataset(prefix="fixtures")
        oaebu_crossref_dataset_id = env.add_dataset(prefix="crossref")
        onix_dataset_id = env.add_dataset(prefix="onix")
        partner_dataset_id = env.add_dataset(prefix="partner")

        with env.create(task_logging=True):
            partner_release_date = pendulum.datetime(2021, 5, 15)
            data_partners, metadata_partner = self.setup_input_data(
                data_partners=self.data_partner_list,
                settings_dataset_id=oaebu_settings_dataset_id,
                fixtures_dataset_id=oaebu_fixtures_dataset_id,
                crossref_master_dataset_id=master_crossref_dataset_id,
                partner_dataset=partner_dataset_id,
                onix_dataset_id=onix_dataset_id,
                release_date=partner_release_date,
                bucket_name=env.transform_bucket,
            )

            sensor_dag_ids = [
                "jstor",
                "google_books",
                "google_analytics3",
                "irus_oapen",
                "irus_fulcrum",
                "ucl_discovery",
                "onix",
            ]

            start_date = pendulum.datetime(year=2021, month=5, day=9)
            dag_id = "onix_workflow_test"
            bq_oaebu_crossref_metadata_table_name = "crossref_metadata"
            bq_book_table_name = "book"
            bq_book_product_table_name = "book_product"
            bq_worksid_table_name = "onix_workid_isbn"
            bq_worksid_error_table_name = "onix_workid_isbn_errors"
            bq_workfamilyid_table_name = "onix_workfamilyid_isbn"
            api_bq_dataset_id = env.add_dataset()
            dag = create_dag(
                dag_id=dag_id,
                cloud_workspace=env.cloud_workspace,
                metadata_partner=metadata_partner,
                bq_master_crossref_project_id=env.cloud_workspace.project_id,
                bq_master_crossref_dataset_id=master_crossref_dataset_id,
                bq_oaebu_crossref_dataset_id=oaebu_crossref_dataset_id,
                bq_oaebu_crossref_metadata_table_name=bq_oaebu_crossref_metadata_table_name,
                bq_master_crossref_metadata_table_name="crossref_metadata_master",
                bq_book_table_name=bq_book_table_name,
                bq_book_product_table_name=bq_book_product_table_name,
                bq_country_project_id=env.cloud_workspace.project_id,
                bq_country_dataset_id=oaebu_settings_dataset_id,
                bq_subject_project_id=env.cloud_workspace.project_id,
                bq_subject_dataset_id=oaebu_fixtures_dataset_id,
                bq_onix_workflow_dataset=onix_workflow_dataset_id,
                bq_oaebu_intermediate_dataset=oaebu_intermediate_dataset_id,
                bq_oaebu_dataset=oaebu_output_dataset_id,
                bq_worksid_table_name=bq_worksid_table_name,
                bq_worksid_error_table_name=bq_worksid_error_table_name,
                bq_workfamilyid_table_name=bq_workfamilyid_table_name,
                bq_oaebu_export_dataset=oaebu_export_dataset_id,
                bq_oaebu_latest_export_dataset=oaebu_latest_export_dataset_id,
                api_bq_dataset_id=api_bq_dataset_id,
                data_partners=data_partners,
                sensor_dag_ids=sensor_dag_ids,
                start_date=start_date,
                retries=0,
            )

            # Skip dag existence check in sensor.
            for sensor in [task for task in dag.tasks if task.node_id.startswith("sensors.")]:
                sensor.check_exists = False
                sensor.check_existence = False
                sensor.grace_period = timedelta(seconds=1)

            env.serialize_dag(dag)

            # Run Dummy Dags so sensors pass
            logical_date = pendulum.datetime(year=2021, month=5, day=17)
            for sensor_id in sensor_dag_ids:
                dummy_dag = make_dummy_dag(sensor_id, logical_date)
                env.serialize_dag(dummy_dag)
                dummy_dagrun = dummy_dag.test(logical_date=logical_date)
                self.assertEqual(dummy_dagrun.state, "success")

            now = pendulum.now("UTC")
            with patch("oaebu_workflows.onix_workflow.onix_workflow.bq_select_table_shard_dates") as mock_date, patch(
                "oaebu_workflows.onix_workflow.onix_workflow.pendulum.now"
            ) as mock_now:
                mock_date.return_value = [partner_release_date]
                mock_now.return_value = now
                dagrun = dag.test(logical_date=logical_date)

            self.assertEqual(dagrun.state, "success")

            # Get the release object
            release_ti = dagrun.get_task_instance(task_id="make_release")
            release_dict = release_ti.xcom_pull(task_ids="make_release", include_prior_dates=False)
            expected_onix_table_id = bq_sharded_table_id(
                env.cloud_workspace.project_id,
                metadata_partner.bq_dataset_id,
                metadata_partner.bq_table_name,
                partner_release_date,
            )
            expected_release_dict = {
                "dag_id": "onix_workflow_test",
                "run_id": dagrun.run_id,
                "snapshot_date": "2021-05-17",
                "onix_table_id": expected_onix_table_id,
                "crossref_master_snapshot_date": "2021-05-15",
            }
            self.assertEqual(release_dict, expected_release_dict)
            release = OnixWorkflowRelease.from_dict(release_dict)
            release_suffix = release.snapshot_date.strftime("%Y%m%d")

            # Aggregate works outputs
            table_id = bq_sharded_table_id(
                self.gcp_project_id, onix_workflow_dataset_id, bq_worksid_table_name, release.snapshot_date
            )
            self.assert_table_content(
                table_id,
                load_and_parse_json(os.path.join(self.fixtures_folder, "e2e_outputs", "onix_workid_isbn.json")),
                primary_key="isbn13",
            )
            table_id = bq_sharded_table_id(
                self.gcp_project_id, onix_workflow_dataset_id, bq_worksid_error_table_name, release.snapshot_date
            )
            self.assert_table_content(
                table_id,
                load_and_parse_json(os.path.join(self.fixtures_folder, "e2e_outputs", "onix_workid_isbn_errors.json")),
                primary_key="Error",
            )
            table_id = bq_sharded_table_id(
                self.gcp_project_id, onix_workflow_dataset_id, bq_workfamilyid_table_name, release.snapshot_date
            )
            self.assert_table_content(
                table_id,
                load_and_parse_json(os.path.join(self.fixtures_folder, "e2e_outputs", "onix_workfamilyid_isbn.json")),
                primary_key="isbn13",
            )

            # Crossref metadata table
            table_id = bq_sharded_table_id(
                self.gcp_project_id,
                oaebu_crossref_dataset_id,
                bq_oaebu_crossref_metadata_table_name,
                release.snapshot_date,
            )
            self.assert_table_content(
                table_id,
                load_and_parse_json(os.path.join(self.fixtures_folder, "e2e_outputs", "crossref_metadata.json")),
                primary_key="DOI",
            )

            # Book table
            table_id = bq_sharded_table_id(
                self.gcp_project_id, oaebu_output_dataset_id, bq_book_table_name, release.snapshot_date
            )
            self.assert_table_content(
                table_id,
                load_and_parse_json(os.path.join(self.fixtures_folder, "e2e_outputs", "book.json")),
                primary_key="isbn",
            )

            # Book product table
            table_id = bq_sharded_table_id(
                self.gcp_project_id, oaebu_output_dataset_id, bq_book_product_table_name, release.snapshot_date
            )

            # TODO: Remove
            rows = list(self.bigquery_client.list_rows(table_id))
            actual_content = [dict(row) for row in rows]
            print(actual_content)
            print(
                load_and_parse_json(
                    os.path.join(self.fixtures_folder, "e2e_outputs", "book_product.json"),
                    date_fields={"month", "published_date"},
                )
            )
            self.assert_table_content(
                table_id,
                load_and_parse_json(
                    os.path.join(self.fixtures_folder, "e2e_outputs", "book_product.json"),
                    date_fields={"month", "published_date"},
                ),
                primary_key="ISBN13",
            )

            # Export tables row counts
            export_tables = [
                ("book_list", 4),
                ("book_institution_list", 1),
                ("book_metrics", 3),
                ("book_metrics_country", 14),
                ("book_metrics_institution", 1),
                ("book_metrics_author", 1),
                # ("book_metrics_city", 39), # No data sources currently use city
                ("book_metrics_subject_bic", 1),
                ("book_metrics_subject_bisac", 0),
                ("book_metrics_subject_thema", 1),
            ]
            export_prefix = self.gcp_project_id.replace("-", "_")
            for table, exp_rows in export_tables:
                table_id = bq_sharded_table_id(
                    self.gcp_project_id, oaebu_export_dataset_id, f"{export_prefix}_{table}", release.snapshot_date
                )
                self.assert_table_integrity(table_id, expected_rows=exp_rows)

            table_id = bq_sharded_table_id(
                self.gcp_project_id, oaebu_export_dataset_id, f"{export_prefix}_book_list", release.snapshot_date
            )
            fixture_table = load_and_parse_json(
                os.path.join(self.fixtures_folder, "e2e_outputs", "book_list.json"),
                date_fields=["published_date"],
            )
            self.assert_table_content(table_id, fixture_table, primary_key="product_id")

            # Validate joins - JSTOR
            if "jstor_country" in [i.type_id for i in data_partners]:
                sql = f"SELECT ISBN, work_id, work_family_id from {self.gcp_project_id}.{oaebu_intermediate_dataset_id}.jstor_country_matched{release_suffix}"
                records = bq_run_query(sql)
                oaebu_works = {record["ISBN"]: record["work_id"] for record in records}
                oaebu_wfam = {record["ISBN"]: record["work_family_id"] for record in records}
                self.assertTrue(
                    oaebu_works["1111111111111"] == oaebu_works["2222222222222"]
                    and oaebu_works["113"] is None
                    and oaebu_works["111"] is None
                    and oaebu_works["211"] is None
                )
                self.assertTrue(
                    oaebu_wfam["1111111111111"] == oaebu_wfam["2222222222222"]
                    and oaebu_wfam["113"] is None
                    and oaebu_wfam["111"] is None
                    and oaebu_wfam["211"] is None
                )

            # Validate joins - IRUS OAPEN
            if "irus_oapen" in [i.type_id for i in data_partners]:
                sql = f"SELECT ISBN, work_id, work_family_id from {self.gcp_project_id}.{oaebu_intermediate_dataset_id}.irus_oapen_matched{release_suffix}"
                records = bq_run_query(sql)
                oaebu_works = {record["ISBN"]: record["work_id"] for record in records}
                oaebu_wfam = {record["ISBN"]: record["work_family_id"] for record in records}
                self.assertTrue(
                    oaebu_works["1111111111111"] == oaebu_works["2222222222222"]
                    and oaebu_works["113"] is None
                    and oaebu_works["111"] is None
                    and oaebu_works["211"] is None
                )
                self.assertTrue(
                    oaebu_wfam["1111111111111"] == oaebu_wfam["2222222222222"]
                    and oaebu_wfam["113"] is None
                    and oaebu_wfam["111"] is None
                    and oaebu_wfam["211"] is None
                )

            # Check latest-export copies
            for export_table, exp_rows in export_tables:
                export_copy = bq_run_query(
                    f"SELECT * FROM {self.gcp_project_id}.{oaebu_latest_export_dataset_id}.{export_prefix}_{export_table}"
                )
                self.assertEqual(len(export_copy), exp_rows)

            # Check API dataset release entry
            api = DatasetAPI(bq_project_id=self.gcp_project_id, bq_dataset_id=api_bq_dataset_id)
            dataset_releases = api.get_dataset_releases(dag_id=dag_id, entity_id="onix_workflow")
            self.assertEqual(len(dataset_releases), 1)
            expected_release = {
                "dag_id": dag_id,
                "entity_id": "onix_workflow",
                "dag_run_id": release.run_id,
                "created": now.to_iso8601_string(),
                "modified": now.to_iso8601_string(),
                "data_interval_start": "2021-05-17T00:00:00Z",
                "data_interval_end": "2021-05-17T00:00:00Z",
                "snapshot_date": "2021-05-17T00:00:00Z",
                "partition_date": None,
                "changefile_start_date": None,
                "changefile_end_date": None,
                "sequence_start": None,
                "sequence_end": None,
                "extra": {},
            }
            self.assertEqual(expected_release, dataset_releases[0].to_dict())

            # Check cleanup
            self.assert_cleanup(make_workflow_folder(dag_id, dagrun.run_id, make_dir=False))
