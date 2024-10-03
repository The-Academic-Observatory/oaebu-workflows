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
#
#
# Author: Tuan Chien, Richard Hosking, Keegan Smith

import json
import logging
import os
import re
from typing import Iterable, List, Optional, Union, Dict

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule
from airflow.timetables.base import Timetable
from google.cloud.bigquery import Client, SourceFormat
from jinja2 import Environment, FileSystemLoader

from oaebu_workflows.config import schema_folder as default_schema_folder, sql_folder
from oaebu_workflows.oaebu_partners import DataPartner, OaebuPartner, partner_from_str, create_bespoke_data_partners
from oaebu_workflows.onix_workflow.onix_work_aggregation import BookWorkAggregator, BookWorkFamilyAggregator
from onix_workflow_schedule import OnixWorkflowTimetable
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.release import make_snapshot_date, set_task_state, SnapshotRelease
from observatory_platform.airflow.sensors import DagCompleteSensor
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.airflow.workflow import cleanup, CloudWorkspace
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.files import save_jsonl_gz
from observatory_platform.google.bigquery import (
    bq_copy_table,
    bq_create_dataset,
    bq_create_table_from_query,
    bq_find_schema,
    bq_load_table,
    bq_run_query,
    bq_select_table_shard_dates,
    bq_sharded_table_id,
    bq_table_id,
)
from observatory_platform.google.gcs import gcs_blob_name_from_path, gcs_blob_uri, gcs_upload_files
from observatory_platform.jinja2_utils import render_template


class OnixWorkflowRelease(SnapshotRelease):
    """Release information for OnixWorkflow"""

    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
        crossref_master_snapshot_date: pendulum.DateTime,
        onix_table_id: str,
    ):
        """
        Construct the OnixWorkflow Release
        :param dag_id: DAG ID.
        :param release_date: The date of the partition/release
        :param crossref_master_snapshot_date: The release date/suffix of the crossref master table
        :param onix_table_id: The table ID of the onix table to use for querying
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)

        # Dates
        self.crossref_master_snapshot_date = crossref_master_snapshot_date

        # Onix table
        self.onix_table_id = onix_table_id

        # Files
        self.workslookup_file_name = "worksid.jsonl.gz"
        self.workslookup_errors_file_name = "worksid_errors.jsonl.gz"
        self.worksfamilylookup_file_name = "workfamilyid.jsonl.gz"
        self.crossref_metadata_file_name = "crossref_metadata.jsonl.gz"

        # Generated Schemas
        self.book_product_schema_file_name = "book_product_schema.json"
        self.author_metrics_schema_file_name = "author_metrics_schema.json"
        self.book_metrics_schema_file_name = "metrics_books_metrics_schema.json"
        self.country_metrics_schema_file_name = "country_metrics_schema.json"
        self.subject_metrics_bic_schema_file_name = "subject_metrics_bic_schema.json"
        self.subject_metrics_bisac_schema_file_name = "subject_metrics_bisac_schema.json"
        self.subject_metrics_thema_schema_file_name = "subject_metrics_thema_schema.json"

    ## File Paths ##
    @property
    def workslookup_path(self):
        return os.path.join(self.transform_folder, self.workslookup_file_name)

    @property
    def workslookup_errors_path(self):
        return os.path.join(self.transform_folder, self.workslookup_errors_file_name)

    @property
    def worksfamilylookup_path(self):
        return os.path.join(self.transform_folder, self.worksfamilylookup_file_name)

    @property
    def crossref_metadata_path(self):
        return os.path.join(self.transform_folder, self.crossref_metadata_file_name)

    @property
    def book_product_schema_path(self):
        return os.path.join(self.transform_folder, self.book_product_schema_file_name)

    @property
    def author_metrics_schema(self):
        return os.path.join(self.transform_folder, self.author_metrics_schema_file_name)

    @property
    def book_metrics_schema(self):
        return os.path.join(self.transform_folder, self.book_metrics_schema_file_name)

    @property
    def country_metrics_schema(self):
        return os.path.join(self.transform_folder, self.country_metrics_schema_file_name)

    @property
    def subject_metrics_bic_schema(self):
        return os.path.join(self.transform_folder, self.subject_metrics_bic_schema_file_name)

    @property
    def subject_metrics_bisac_schema(self):
        return os.path.join(self.transform_folder, self.subject_metrics_bisac_schema_file_name)

    @property
    def subject_metrics_thema_schema(self):
        return os.path.join(self.transform_folder, self.subject_metrics_thema_schema_file_name)

    ## Blob Names ##
    @property
    def workslookup_blob_name(self):
        return gcs_blob_name_from_path(self.workslookup_path)

    @property
    def workslookup_errors_blob_name(self):
        return gcs_blob_name_from_path(self.workslookup_errors_path)

    @property
    def worksfamilylookup_blob_name(self):
        return gcs_blob_name_from_path(self.worksfamilylookup_path)

    @property
    def crossref_metadata_blob_name(self):
        return gcs_blob_name_from_path(self.crossref_metadata_path)

    @staticmethod
    def from_dict(dict_: dict):
        return OnixWorkflowRelease(
            dag_id=dict_["dag_id"],
            run_id=dict_["run_id"],
            snapshot_date=pendulum.from_format(dict_["snapshot_date"], "YYYY-MM-DD"),
            crossref_master_snapshot_date=pendulum.from_format(dict_["crossref_master_snapshot_date"], "YYYY-MM-DD"),
            onix_table_id=dict_["onix_table_id"],
        )

    def to_dict(self):
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "snapshot_date": self.snapshot_date.to_date_string(),
            "crossref_master_snapshot_date": self.crossref_master_snapshot_date.to_date_string(),
            "onix_table_id": self.onix_table_id,
        }


def create_dag(
    *,
    dag_id: str,
    cloud_workspace: CloudWorkspace,
    metadata_partner: Union[str, OaebuPartner],
    # Bigquery parameters
    bq_master_crossref_project_id: str = "academic-observatory",
    bq_master_crossref_dataset_id: str = "crossref_metadata",
    bq_oaebu_crossref_dataset_id: str = "crossref",
    bq_master_crossref_metadata_table_name: str = "crossref_metadata",
    bq_oaebu_crossref_metadata_table_name: str = "crossref_metadata",
    bq_country_project_id: str = "oaebu-public-data",
    bq_country_dataset_id: str = "oaebu_reference",
    bq_subject_project_id: str = "oaebu-public-data",
    bq_subject_dataset_id: str = "oaebu_reference",
    bq_book_table_name: str = "book",
    bq_book_product_table_name: str = "book_product",
    bq_onix_workflow_dataset: str = "onix_workflow",
    bq_oaebu_intermediate_dataset: str = "oaebu_intermediate",
    bq_oaebu_dataset: str = "oaebu",
    bq_oaebu_export_dataset: str = "data_export",
    bq_oaebu_latest_export_dataset: str = "data_export_latest",
    bq_worksid_table_name: str = "onix_workid_isbn",
    bq_worksid_error_table_name: str = "onix_workid_isbn_errors",
    bq_workfamilyid_table_name: str = "onix_workfamilyid_isbn",
    # Run parameters
    data_partners: List[Union[str, OaebuPartner]] = None,
    bespoke_data_partners: List[Dict] = None,
    ga3_views_field="page_views",
    schema_folder: str = default_schema_folder(workflow_module="onix_workflow"),
    mailto: str = "agent@observatory.academy",
    crossref_start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
    api_bq_dataset_id: str = "dataset_api",
    # Ariflow parameters
    sensor_dag_ids: List[str] = None,
    catchup: Optional[bool] = False,
    start_date: Optional[pendulum.DateTime] = pendulum.datetime(2022, 8, 1),
    schedule: Union[str, Timetable] = OnixWorkflowTimetable(),  # Every Sunday and the 5th of the month
    max_active_runs: int = 1,
    retries: int = 3,
    retry_delay: Union[int, float] = 5,
):
    """
    Initialises the workflow object.

    :param dag_id: DAG ID.
    :param cloud_workspace: The CloudWorkspace object for this DAG
    :param metadata_partner: The Oaebu Metadata partner

    :param bq_master_crossref_project_id: GCP project ID of crossref master data
    :param bq_master_crossref_dataset_id: GCP dataset ID of crossref master data
    :param bq_oaebu_crossref_dataset_id: GCP dataset ID of crossref OAeBU data
    :param bq_master_crossref_metadata_table_name: The name of the master crossref metadata table
    :param bq_oaebu_crossref_metadata_table_name: The name of the OAeBU crossref metadata table
    :param bq_country_project_id: GCP project ID of the country table
    :param bq_country_dataset_id: GCP dataset containing the country table
    :param bq_subject_project_id: GCP project ID of the subject tables
    :param bq_subject_dataset_id: GCP dataset ID of the subject tables
    :param bq_book_table_name: The name of the book table
    :param bq_book_product_table_name: The name of the book product table
    :param bq_onix_workflow_dataset: Onix workflow dataset.
    :param bq_oaebu_intermediate_dataset: OAEBU intermediate dataset.
    :param bq_oaebu_dataset: OAEBU dataset.
    :param bq_oaebu_export_dataset: OAEBU data export dataset.
    :param bq_oaebu_latest_export_dataset: OAEBU data export dataset with the latest export tables
    :param bq_worksid_table_name: table ID of the worksid table
    :param bq_worksid_error_table_name: table ID of the worksid error table
    :param bq_workfamilyid_table_name: table ID of the workfamilyid table
    a previous version of the table.

    :param data_partners: OAEBU data sources.
    :param bespoke_data_partners: Dictionary-defined OAEBU Data Partners
    :param ga3_views_field: The name of the GA3 views field - should be either 'page_views' or 'unique_views'
    :param schema_folder: the SQL schema path.
    :param mailto: email address used to identify the user when sending requests to an API.
    :param crossref_start_date: The starting date of crossref's API calls
    :param api_bq_dataset_id: The name of the Bigquery dataset to store the API release(s)

    :param sensor_dag_ids: Dag IDs for dependent tasks
    :param catchup: Whether to catch up missed DAG runs.
    :param start_date: Start date of the DAG.
    :param schedule: Scheduled interval for running the DAG.
    :param max_active_runs: The maximum number of active DAG runs.
    :param retries: The number of times to retry failed tasks.
    :param retry_delay: The delay between retries in minutes.
    """

    if not sensor_dag_ids:
        sensor_dag_ids = []

    if data_partners is None:
        data_partners = list()

    metadata_partner = partner_from_str(metadata_partner, metadata_partner=True)
    data_partners = [partner_from_str(p) for p in data_partners]
    if bespoke_data_partners:
        data_partners.extend(create_bespoke_data_partners(bespoke_data_partners))

    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        tags=["oaebu"],
        max_active_runs=max_active_runs,
        default_args=dict(
            retries=retries, retry_delay=pendulum.duration(minutes=retry_delay), on_failure_callback=on_failure_callback
        ),
    )
    def onix_workflow():
        """Construct the DAG"""

        @task_group(group_id="sensors")
        def make_sensors():
            """Create the sensor tasks for the DAG. These check that the data partner dag runs are complete"""

            tasks = []
            for ext_dag_id in sensor_dag_ids:
                sensor = DagCompleteSensor(task_id=f"{ext_dag_id}_sensor", external_dag_id=ext_dag_id)
                tasks.append(sensor)
            chain(tasks)

        @task()
        def make_release(**context) -> dict:
            """Creates a release object.

            :param context: From Airflow. Contains the logical_date.
            :return: a dictionary representation of the OnixWorkflowRelease object.
            """

            snapshot_date = make_snapshot_date(**context)
            client = Client(project=cloud_workspace.project_id)

            # Get ONIX table ID
            if metadata_partner.sharded:
                onix_source_table_id = bq_table_id(
                    cloud_workspace.project_id, metadata_partner.bq_dataset_id, metadata_partner.bq_table_name
                )
                onix_snapshot_dates = bq_select_table_shard_dates(
                    table_id=onix_source_table_id, end_date=snapshot_date, client=client
                )

                if not len(onix_snapshot_dates):
                    raise RuntimeError("OnixWorkflow.make_release: no ONIX releases found")

                onix_snapshot_date = onix_snapshot_dates[0]  # Get most recent snapshot
                onix_table_id = bq_sharded_table_id(
                    cloud_workspace.project_id,
                    metadata_partner.bq_dataset_id,
                    metadata_partner.bq_table_name,
                    onix_snapshot_date,
                )
            else:
                onix_table_id = bq_table_id(
                    cloud_workspace.project_id, metadata_partner.bq_dataset_id, metadata_partner.bq_table_name
                )

            # Get Crossref Metadata release date
            crossref_table_id = bq_table_id(
                bq_master_crossref_project_id, bq_master_crossref_dataset_id, bq_master_crossref_metadata_table_name
            )
            crossref_metadata_snapshot_dates = bq_select_table_shard_dates(
                table_id=crossref_table_id, end_date=snapshot_date, client=client
            )

            if not len(crossref_metadata_snapshot_dates):
                raise RuntimeError("OnixWorkflow.make_release: no Crossref Metadata releases found")

            crossref_master_snapshot_date = crossref_metadata_snapshot_dates[0]  # Get most recent snapshot

            # Make the release object
            return OnixWorkflowRelease(
                dag_id=dag_id,
                run_id=context["run_id"],
                snapshot_date=snapshot_date,
                crossref_master_snapshot_date=crossref_master_snapshot_date,
                onix_table_id=onix_table_id,
            ).to_dict()

        @task()
        def aggregate_works(release: dict, **context) -> None:
            """Fetches the ONIX product records from our ONIX database, aggregates them into works, workfamilies,
            and outputs it into jsonl files.

            :param release: The onix workflow release object
            """

            release = OnixWorkflowRelease.from_dict(release)
            bq_create_dataset(
                project_id=cloud_workspace.project_id,
                dataset_id=bq_onix_workflow_dataset,
                location=cloud_workspace.data_location,
                description="Onix Workflow Aggregations",
            )

            # Fetch ONIX data
            client = Client(project=cloud_workspace.project_id)
            products = get_onix_records(release.onix_table_id, client=client)

            # Aggregate into works
            agg = BookWorkAggregator(products)
            works = agg.aggregate()
            lookup_table = agg.get_works_lookup_table()
            save_jsonl_gz(release.workslookup_path, lookup_table)

            # Save errors from aggregation
            error_table = [{"Error": error} for error in agg.errors]
            save_jsonl_gz(release.workslookup_errors_path, error_table)

            # Aggregate work families
            agg = BookWorkFamilyAggregator(works)
            agg.aggregate()
            lookup_table = agg.get_works_family_lookup_table()
            save_jsonl_gz(release.worksfamilylookup_path, lookup_table)

            # Upload the aggregation tables and error tables to a GCP bucket in preparation for BQ loading
            files = [release.workslookup_path, release.workslookup_errors_path, release.worksfamilylookup_path]
            gcs_upload_files(bucket_name=cloud_workspace.transform_bucket, file_paths=files)

            # Load the 'WorkID lookup', 'WorkID lookup table errors' and 'WorkFamilyID lookup' tables into BigQuery
            aggregation_blobs = [
                release.workslookup_blob_name,
                release.workslookup_errors_blob_name,
                release.worksfamilylookup_blob_name,
            ]
            aggregation_tables = [
                bq_worksid_table_name,
                bq_worksid_error_table_name,
                bq_workfamilyid_table_name,
            ]
            for blob, table_name in zip(aggregation_blobs, aggregation_tables):
                uri = gcs_blob_uri(cloud_workspace.transform_bucket, blob)
                table_id = bq_sharded_table_id(
                    cloud_workspace.project_id, bq_onix_workflow_dataset, table_name, release.snapshot_date
                )
                state = bq_load_table(
                    uri=uri,
                    table_id=table_id,
                    schema_file_path=bq_find_schema(path=schema_folder, table_name=table_name),
                    source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition="WRITE_TRUNCATE",
                    client=client,
                )
                set_task_state(state, context["ti"].task_id, release=release)

        @task()
        def create_crossref_metadata_table(release: dict, **context) -> None:
            """Creates the crossref metadata table by querying the AO master table and matching on this publisher's ISBNs"""

            release = OnixWorkflowRelease.from_dict(release)
            bq_create_dataset(
                project_id=cloud_workspace.project_id,
                dataset_id=bq_oaebu_crossref_dataset_id,
                location=cloud_workspace.data_location,
                description="Data from Crossref sources",
            )

            master_crossref_metadata_table_id = bq_sharded_table_id(
                bq_master_crossref_project_id,
                bq_master_crossref_dataset_id,
                bq_master_crossref_metadata_table_name,
                release.crossref_master_snapshot_date,
            )
            sql = render_template(
                os.path.join(sql_folder(workflow_module="onix_workflow"), "crossref_metadata_filter_isbn.sql.jinja2"),
                onix_table_id=release.onix_table_id,
                crossref_metadata_table_id=master_crossref_metadata_table_id,
            )
            logging.info("Creating crossref metadata table from master table")
            schema_file_path = bq_find_schema(path=schema_folder, table_name=bq_oaebu_crossref_metadata_table_name)
            oaebu_crossref_metadata_table_id = bq_sharded_table_id(
                cloud_workspace.project_id,
                bq_oaebu_crossref_dataset_id,
                bq_oaebu_crossref_metadata_table_name,
                release.snapshot_date,
            )
            client = Client(project=cloud_workspace.project_id)
            state = bq_create_table_from_query(
                sql=sql, table_id=oaebu_crossref_metadata_table_id, schema_file_path=schema_file_path, client=client
            )
            set_task_state(state, context["ti"].task_id, release=release)

        @task(pool="crossref_events_pool")
        def create_crossref_events_table(release: dict, **context) -> None:
            """Download, transform, upload and create a table for crossref events"""

            release = OnixWorkflowRelease.from_dict(release)

            if skip_downloading_crossref_events:
                raise AirflowSkipException("create_crossref_events_table: skipping fetching new Crossref Events data")

            # Get the unique dois from the metadata table
            metadata_table_id = bq_sharded_table_id(
                cloud_workspace.project_id,
                bq_oaebu_crossref_dataset_id,
                bq_oaebu_crossref_metadata_table_name,
                release.snapshot_date,
            )
            client = Client(project=cloud_workspace.project_id)
            dois = dois_from_table(metadata_table_id, doi_column_name="DOI", client=client)
            doi_prefixes = get_doi_prefixes(dois)
            logging.info(f"Found DOI prefixes: {doi_prefixes}")

            # Download and transform all events
            start_date = crossref_start_date
            end_date = release.snapshot_date
            input_path = release.download_crossref_events_path
            output_path = release.transformed_crossref_events_path
            download_crossref_events(input_path, doi_prefixes, start_date, end_date, mailto)
            transform_crossref_events(input_path, dois, output_path)

            # Upload to google cloud and load into BigQuery
            gcs_upload_files(
                bucket_name=cloud_workspace.transform_bucket, file_paths=[release.transformed_crossref_events_path]
            )
            table_id = bq_sharded_table_id(
                cloud_workspace.project_id,
                bq_oaebu_crossref_dataset_id,
                bq_crossref_events_table_name,
                release.snapshot_date,
            )
            state = bq_load_table(
                uri=gcs_blob_uri(cloud_workspace.transform_bucket, release.crossref_events_blob_name),
                table_id=table_id,
                schema_file_path=bq_find_schema(path=schema_folder, table_name=bq_crossref_events_table_name),
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition="WRITE_TRUNCATE",
                client=client,
            )
            set_task_state(state, context["ti"].task_id, release=release)

        @task(trigger_rule=TriggerRule.NONE_FAILED)
        def create_book_table(release: dict, **context) -> None:
            """Create the oaebu book table using the crossref metadata table"""

            release = OnixWorkflowRelease.from_dict(release)
            bq_create_dataset(
                project_id=cloud_workspace.project_id,
                dataset_id=bq_oaebu_dataset,
                location=cloud_workspace.data_location,
                description="OAEBU Tables",
            )
            book_table_id = bq_sharded_table_id(
                cloud_workspace.project_id, bq_oaebu_dataset, bq_book_table_name, release.snapshot_date
            )
            crossref_metadata_table_id = bq_sharded_table_id(
                cloud_workspace.project_id,
                bq_oaebu_crossref_dataset_id,
                bq_oaebu_crossref_metadata_table_name,
                release.snapshot_date,
            )
            client = Client(project=cloud_workspace.project_id)
            sql = render_template(
                os.path.join(sql_folder(workflow_module="onix_workflow"), "book.sql.jinja2"),
                crossref_metadata_table_id=crossref_metadata_table_id,
            )
            logging.info(sql)

            status = bq_create_table_from_query(
                sql=sql,
                table_id=book_table_id,
                schema_file_path=os.path.join(schema_folder, "book.json"),
                client=client,
            )
            set_task_state(status, context["ti"].task_id, release=release)

        @task_group(group_id="intermediate_tables")
        def _create_tasks_intermediate_tables(release: dict):
            tasks = []
            for data_partner in data_partners:
                task = _create_intermediate_table.override(task_id=f"intermediate_{data_partner.type_id}")(
                    release, data_partner=data_partner
                )
                tasks.append(task)
            chain(tasks)

        @task()
        def _create_intermediate_table(
            release: dict,
            *,
            data_partner: DataPartner,
            **context,
        ) -> None:
            """Create an intermediate oaebu table.  They are of the form type_id_matched<date>

            :param release: Onix workflow release information.
            :param data_partner: The data partner object for the data source.
            """

            release = OnixWorkflowRelease.from_dict(release)
            bq_create_dataset(
                project_id=cloud_workspace.project_id,
                dataset_id=bq_oaebu_intermediate_dataset,
                location=cloud_workspace.data_location,
                description="Intermediate OAEBU Tables",
            )
            bq_worksid_table_id = bq_sharded_table_id(
                cloud_workspace.project_id,
                bq_onix_workflow_dataset,
                bq_worksid_table_name,
                release.snapshot_date,
            )
            bq_workfamily_table_id = bq_sharded_table_id(
                cloud_workspace.project_id,
                bq_onix_workflow_dataset,
                bq_workfamilyid_table_name,
                release.snapshot_date,
            )

            status = create_intermediate_table(
                data_partner=data_partner,
                project_id=cloud_workspace.project_id,
                bq_output_dataset=bq_oaebu_intermediate_dataset,
                bq_worksid_table_id=bq_worksid_table_id,
                bq_workfamily_table_id=bq_workfamily_table_id,
                snapshot_date=release.snapshot_date,
            )
            set_task_state(status, context["ti"].task_id, release=release)

        @task()
        def create_book_product_table(release: dict, **context) -> None:
            """Create the Book Product Table"""

            release = OnixWorkflowRelease.from_dict(release)
            bq_create_dataset(
                project_id=cloud_workspace.project_id,
                dataset_id=bq_oaebu_dataset,
                location=cloud_workspace.data_location,
                description="OAEBU Tables",
            )

            # Data partner table names
            dp_tables = {
                f"{dp.type_id}_table_id": bq_intermediate_table_id(
                    project_id=cloud_workspace.project_id,
                    bq_intermediate_dataset=bq_oaebu_intermediate_dataset,
                    dp_type_id=dp.type_id,
                    snapshot_date=release.snapshot_date,
                )
                for dp in data_partners
            }

            # ONIX WF table names
            workid_table_id = bq_sharded_table_id(
                cloud_workspace.project_id,
                bq_onix_workflow_dataset,
                bq_worksid_table_name,
                release.snapshot_date,
            )
            workfamilyid_table_id = bq_sharded_table_id(
                cloud_workspace.project_id,
                bq_onix_workflow_dataset,
                bq_workfamilyid_table_name,
                release.snapshot_date,
            )
            country_table_id = bq_table_id(bq_country_project_id, bq_country_dataset_id, "country")
            book_table_id = bq_sharded_table_id(
                cloud_workspace.project_id, bq_oaebu_dataset, bq_book_table_name, release.snapshot_date
            )

            # Render the SQL
            bic_table_id = bq_table_id(bq_subject_project_id, bq_subject_dataset_id, "bic_lookup")
            thema_table_id = bq_table_id(bq_subject_project_id, bq_subject_dataset_id, "thema_lookup")
            bisac_table_id = bq_table_id(bq_subject_project_id, bq_subject_dataset_id, "bisac_lookup")
            env = create_data_partner_env(
                main_template=os.path.join(sql_folder(workflow_module="onix_workflow"), "book_product.sql.jinja2"),
                data_partners=data_partners,
            )
            sql = env.render(
                onix_table_id=release.onix_table_id,
                data_partners=data_partners,
                book_table_id=book_table_id,
                country_table_id=country_table_id,
                workid_table_id=workid_table_id,
                workfamilyid_table_id=workfamilyid_table_id,
                ga3_views_field=ga3_views_field,
                bic_table_id=bic_table_id,
                thema_table_id=thema_table_id,
                bisac_table_id=bisac_table_id,
                **dp_tables,
            )
            logging.info(f"Book Product SQL:\n{sql}")

            # Create the table
            with open(os.path.join(default_schema_folder("onix_workflow"), "book_product.json"), "r") as f:
                schema = json.load(f)

            # Create the schema
            for dp in data_partners:
                months_schema_file = os.path.join(dp.schema_directory, dp.files.book_product_metrics_schema)
                with open(months_schema_file, "r") as f:
                    months_schema = json.load(f)
                    schema = insert_into_schema(schema, insert_field=months_schema, schema_field_name="months")

                metadata_schema_file = os.path.join(dp.schema_directory, dp.files.book_product_metadata_schema)
                if dp.has_metadata:
                    with open(metadata_schema_file, "r") as f:
                        metadata_schema = json.load(f)
                        schema = insert_into_schema(schema, insert_field=metadata_schema, schema_field_name="metadata")

            table_id = bq_sharded_table_id(
                cloud_workspace.project_id,
                bq_oaebu_dataset,
                bq_book_product_table_name,
                release.snapshot_date,
            )

            # Run the query
            with open(release.book_product_schema_path, mode="w+") as f:
                json.dump(schema, f)
            client = Client(project=cloud_workspace.project_id)
            status = bq_create_table_from_query(
                sql=sql, table_id=table_id, schema_file_path=release.book_product_schema_path, client=client
            )
            set_task_state(status, context["ti"].task_id, release=release)

        @task_group(group_id="export_tables")
        def create_tasks_export_tables(release):
            """Create tasks for exporting final metrics from our OAEBU data.
            These are split into two categories: generic and custom.
            The custom exports change their schema depending on the data partners. Generic tables do not."""

            generic_export_tables = [
                {
                    "output_table": "book_list",
                    "query_template": os.path.join(sql_folder("onix_workflow"), "book_list.sql.jinja2"),
                    "schema": os.path.join(default_schema_folder("onix_workflow"), "book_list.json"),
                },
            ]
            if "jstor_institution" in [dp.type_id for dp in data_partners]:
                generic_export_tables.append(
                    {
                        "output_table": "book_institution_list",
                        "query_template": os.path.join(sql_folder("onix_workflow"), "book_institution_list.sql.jinja2"),
                        "schema": os.path.join(default_schema_folder("onix_workflow"), "book_institution_list.json"),
                    }
                )
                generic_export_tables.append(
                    {
                        "output_table": "book_metrics_institution",
                        "query_template": os.path.join(
                            sql_folder("onix_workflow"), "book_metrics_institution.sql.jinja2"
                        ),
                        "schema": os.path.join(default_schema_folder("onix_workflow"), "book_metrics_institution.json"),
                    },
                )
            if "irus_oapen" in [dp.type_id for dp in data_partners]:
                generic_export_tables.append(
                    {
                        "output_table": "book_metrics_city",
                        "query_template": os.path.join(sql_folder("onix_workflow"), "book_metrics_city.sql.jinja2"),
                        "schema": os.path.join(default_schema_folder("onix_workflow"), "book_metrics_city.json"),
                    }
                )

            # Create each export table in BiqQuery
            tasks = []

            for export_table in generic_export_tables:
                task = export_oaebu_table.override(task_id=f"export_{export_table['output_table']}")(
                    release,
                    output_table=export_table["output_table"],
                    query_template_path=export_table["query_template"],
                    schema_file_path=export_table["schema"],
                )
                tasks.append(task)

            tasks.append(export_book_metrics(release))
            tasks.append(export_book_metrics_country(release))
            tasks.append(export_book_metrics_author(release))
            tasks.append(export_book_metrics_subjects(release))
            chain(tasks)

        @task()
        def export_oaebu_table(
            release: dict, *, output_table, query_template_path, schema_file_path, **context
        ) -> bool:
            """Create an export table.

            Takes several kwargs:
            :param output_table: The name of the table to create
            :param query_template: The name of the template SQL file
            :param schema_file_path: The path to the schema
            """
            release = OnixWorkflowRelease.from_dict(release)
            _export_oaebu_table(release, output_table, query_template_path, schema_file_path)

        def _export_oaebu_table(
            release: OnixWorkflowRelease, output_table, query_template_path, schema_file_path
        ) -> bool:
            """Non-task version of export_oaebu_table()

            :return: Whether the table creation was a success"""
            bq_create_dataset(
                project_id=cloud_workspace.project_id,
                dataset_id=bq_oaebu_export_dataset,
                location=cloud_workspace.data_location,
                description="OAEBU Tables for Dashboarding",
            )

            output_table_name = f"{cloud_workspace.project_id.replace('-', '_')}_{output_table}"
            output_table_id = bq_sharded_table_id(
                cloud_workspace.project_id, bq_oaebu_export_dataset, output_table_name, release.snapshot_date
            )
            book_product_table_id = bq_sharded_table_id(
                cloud_workspace.project_id,
                bq_oaebu_dataset,
                bq_book_product_table_name,
                release.snapshot_date,
            )
            country_table_id = bq_table_id(bq_country_project_id, bq_country_dataset_id, "country")
            bic_table_id = bq_table_id(bq_subject_project_id, bq_subject_dataset_id, "bic_lookup")
            bisac_table_id = bq_table_id(bq_subject_project_id, bq_subject_dataset_id, "bisac_lookup")
            thema_table_id = bq_table_id(bq_subject_project_id, bq_subject_dataset_id, "thema_lookup")

            env = create_data_partner_env(main_template=query_template_path, data_partners=data_partners)
            sql = env.render(
                project_id=cloud_workspace.project_id,
                dataset_id=bq_oaebu_dataset,
                release=release.snapshot_date,
                data_partners=data_partners,
                book_product_table_id=book_product_table_id,
                country_table_id=country_table_id,
                bic_table_id=bic_table_id,
                bisac_table_id=bisac_table_id,
                thema_table_id=thema_table_id,
            )
            logging.info(f"{output_table} SQL:\n{sql}")

            client = Client(project=cloud_workspace.project_id)
            status = bq_create_table_from_query(
                sql=sql, table_id=output_table_id, schema_file_path=schema_file_path, client=client
            )
            return status

        @task()
        def export_book_metrics_country(release: dict, **context) -> None:
            """Create table for country metrics"""

            release = OnixWorkflowRelease.from_dict(release)
            country_schema_base = os.path.join(default_schema_folder("onix_workflow"), "book_metrics_country.json")
            with open(country_schema_base, "r") as f:
                country_schema = json.load(f)

            for dp in [dp for dp in data_partners if dp.export_country]:
                _file = dp.files.book_metrics_country_schema
                with open(os.path.join(dp.schema_directory, _file), "r") as f:
                    dp_schema = json.load(f)
                country_schema = insert_into_schema(country_schema, dp_schema)

            with open(release.country_metrics_schema, "w") as f:
                json.dump(country_schema, f)

            query_template_path = os.path.join(
                sql_folder(workflow_module="onix_workflow"), "book_metrics_country.sql.jinja2"
            )
            status = _export_oaebu_table(
                release=release,
                output_table="book_metrics_country",
                query_template_path=query_template_path,
                schema_file_path=release.country_metrics_schema,
            )
            set_task_state(status, context["ti"].task_id, release=release)

        @task()
        def export_book_metrics_author(release: dict, **context) -> None:
            """Create table for author metrics"""

            release = OnixWorkflowRelease.from_dict(release)
            author_schema_base = os.path.join(default_schema_folder("onix_workflow"), "book_metrics_author.json")
            with open(author_schema_base, "r") as f:
                author_schema = json.load(f)

            for dp in [dp for dp in data_partners if dp.export_author]:
                _file = dp.files.book_metrics_author_schema
                with open(os.path.join(dp.schema_directory, _file), "r") as f:
                    dp_schema = json.load(f)
                author_schema = insert_into_schema(author_schema, dp_schema)

            with open(release.author_metrics_schema, "w") as f:
                json.dump(author_schema, f)

            query_template_path = os.path.join(
                sql_folder(workflow_module="onix_workflow"), "book_metrics_author.sql.jinja2"
            )
            status = _export_oaebu_table(
                release=release,
                output_table="book_metrics_author",
                query_template_path=query_template_path,
                schema_file_path=release.author_metrics_schema,
            )
            set_task_state(status, context["ti"].task_id, release=release)

        @task()
        def export_book_metrics(release: dict, **context) -> None:
            """Create table for book metrics"""

            release = OnixWorkflowRelease.from_dict(release)
            book_schema_base = os.path.join(default_schema_folder("onix_workflow"), "book_metrics.json")
            with open(book_schema_base, "r") as f:
                book_schema = json.load(f)

            for dp in [dp for dp in data_partners if dp.export_book_metrics]:
                _file = dp.files.book_metrics_schema
                with open(os.path.join(dp.schema_directory, _file), "r") as f:
                    dp_schema = json.load(f)
                book_schema = insert_into_schema(book_schema, dp_schema)

            with open(release.book_metrics_schema, "w") as f:
                json.dump(book_schema, f)

            query_template_path = os.path.join(sql_folder(workflow_module="onix_workflow"), "book_metrics.sql.jinja2")
            status = _export_oaebu_table(
                release=release,
                output_table="book_metrics",
                query_template_path=query_template_path,
                schema_file_path=release.book_metrics_schema,
            )
            set_task_state(status, context["ti"].task_id, release=release)

        @task()
        def export_book_metrics_subjects(release: dict, **context) -> None:
            """Create tables for subject metrics"""

            release = OnixWorkflowRelease.from_dict(release)
            for sub, schema_dump in [
                ("bic", release.subject_metrics_bic_schema),
                ("bisac", release.subject_metrics_bisac_schema),
                ("thema", release.subject_metrics_thema_schema),
            ]:
                subject_schema_base = os.path.join(
                    default_schema_folder("onix_workflow"), f"book_metrics_subject_{sub}.json"
                )
                with open(subject_schema_base, "r") as f:
                    subject_schema = json.load(f)

                for dp in [dp for dp in data_partners if dp.export_subject]:
                    _file = dp.files.book_metrics_subject_schema
                    with open(os.path.join(dp.schema_directory, _file), "r") as f:
                        dp_schema = json.load(f)
                    subject_schema = insert_into_schema(subject_schema, dp_schema)

                with open(schema_dump, "w") as f:
                    json.dump(subject_schema, f)

                query_template_path = os.path.join(
                    sql_folder(workflow_module="onix_workflow"), f"book_metrics_subject_{sub}.sql.jinja2"
                )
                status = _export_oaebu_table(
                    release=release,
                    output_table=f"book_metrics_subject_{sub}",
                    query_template_path=query_template_path,
                    schema_file_path=schema_dump,
                )
                set_task_state(status, context["ti"].task_id, release=release)

        @task()
        def update_latest_export_tables(release: dict, **context) -> None:
            """Create copies of the latest data export tables in bigquery"""

            release = OnixWorkflowRelease.from_dict(release)
            copy_latest_export_tables(
                project_id=cloud_workspace.project_id,
                from_dataset=bq_oaebu_export_dataset,
                to_dataset=bq_oaebu_latest_export_dataset,
                date_match=release.snapshot_date.strftime("%Y%m%d"),
                data_location=cloud_workspace.data_location,
            )

        @task()
        def add_new_dataset_releases(release: dict, **context) -> None:
            """Adds release information to API."""

            release = OnixWorkflowRelease.from_dict(release)
            client = Client(project=cloud_workspace.project_id)
            api = DatasetAPI(bq_project_id=cloud_workspace.project_id, bq_dataset_id=api_bq_dataset_id, client=client)
            api.seed_db()
            dataset_release = DatasetRelease(
                dag_id=dag_id,
                entity_id="onix_workflow",
                dag_run_id=release.run_id,
                created=pendulum.now(),
                modified=pendulum.now(),
                snapshot_date=release.snapshot_date,
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
            )
            api.add_dataset_release(dataset_release)

        @task()
        def cleanup_workflow(release: dict, **context):
            """Cleanup temporary files."""

            release = OnixWorkflowRelease.from_dict(release)
            cleanup(dag_id=dag_id, workflow_folder=release.workflow_folder)

        # Define DAG tasks
        task_check_dependencies = check_dependencies()
        task_group_sensors = make_sensors()
        xcom_release = make_release()
        task_aggregate_works = aggregate_works(xcom_release)
        task_create_crossref_metadata_table = create_crossref_metadata_table(xcom_release)
        task_create_book_table = create_book_table(xcom_release)
        task_group_create_intermediate_tables = _create_tasks_intermediate_tables(xcom_release)
        task_create_book_product_table = create_book_product_table(xcom_release)
        task_group_create_export_tables = create_tasks_export_tables(xcom_release)
        task_update_latest_export_tables = update_latest_export_tables(xcom_release)
        task_add_release = add_new_dataset_releases(xcom_release)
        task_cleanup_workflow = cleanup_workflow(xcom_release)

        (
            task_check_dependencies
            >> task_group_sensors
            >> xcom_release
            >> task_aggregate_works
            >> task_create_crossref_metadata_table
            >> task_create_book_table
            >> task_group_create_intermediate_tables
            >> task_create_book_product_table
            >> task_group_create_export_tables
            >> task_update_latest_export_tables
            >> task_add_release
            >> task_cleanup_workflow
        )

    return onix_workflow()


def create_intermediate_table(
    data_partner: DataPartner,
    project_id: str,
    bq_output_dataset: str,
    bq_worksid_table_id: str,
    bq_workfamily_table_id: str,
    snapshot_date: str,
) -> bool:
    """Create an intermediate oaebu table from a data partner.

    :param data_partner: The data partner object for the data source.
    :param project_id: Project ID for the partner data.
    :param bq_worksid_table_id: The ID of the worksid table
    :param bq_workfamily_table_id: The ID of the workfamily table
    :param snapshot_date: The snapshot date to use
    :param bq_output_dataset: The name of the dataset to output the resulting table
    :return: Whether the operation was successful
    """

    if data_partner.sharded:
        orig_table_id = bq_sharded_table_id(
            project_id, data_partner.bq_dataset_id, data_partner.bq_table_name, snapshot_date
        )
    else:
        orig_table_id = bq_table_id(project_id, data_partner.bq_dataset_id, data_partner.bq_table_name)
    output_table_id = bq_intermediate_table_id(
        project_id=project_id,
        bq_intermediate_dataset=bq_output_dataset,
        dp_type_id=data_partner.type_id,
        snapshot_date=snapshot_date,
    )
    template_path = os.path.join(sql_folder(workflow_module="onix_workflow"), "assign_workid_workfamilyid.sql.jinja2")

    # Make the table from SQL query
    client = Client(project=project_id)
    sql = render_template(
        template_path,
        orig_table_id=orig_table_id,
        orig_isbn=data_partner.isbn_field_name,
        wid_table_id=bq_worksid_table_id,
        wfam_table_id=bq_workfamily_table_id,
    )
    status = bq_create_table_from_query(sql=sql, table_id=output_table_id, client=client)
    return status


def bq_intermediate_table_id(project_id: str, bq_intermediate_dataset: str, dp_type_id: str, snapshot_date: str) -> str:
    """
    Generates a table ID for a given data partner type ID

    :param project_id: The project ID
    :param dp_type_id: The type ID of the data partner
    :param snapshot_date: The stringified snapshot date
    :param bq_intermediate_dataset: The 'intermediate' dateset name
    :return: The data partner's intermediate table ID
    """
    return bq_sharded_table_id(project_id, bq_intermediate_dataset, f"{dp_type_id}_matched", snapshot_date)


def get_doi_prefixes(dois: Iterable[str]) -> set[str]:
    """Convert DOIs to a set of unique DOI prefixes.

    :param dois: the DOIs.
    :return: a set of unique prefixes
    """

    prefixes = set()
    for doi in dois:
        match = re.search(r"^(10\.\d{4,9})", doi)
        if match:
            doi_prefix = match.group(1)
            prefixes.add(doi_prefix)
        else:
            logging.warning(f"No DOI prefix found: {doi}")

    return prefixes


def dois_from_table(table_id: str, doi_column_name: str = "DOI", client: Client = None) -> set[str]:
    """
    Queries a metadata table to retrieve the distinct / unique DOIs. Provided the DOIs are not in a nested structure.

    :param table_id: The fully qualified ID of the metadata table on GCP
    :param doi_column_name: The name of the DOI column
    :return: All DOIs present in the metadata table
    """

    select_field = f"DISTINCT({doi_column_name})"
    sql = f"SELECT {select_field} FROM `{table_id}`"
    query_results = bq_run_query(sql, client=client)
    dois = {r["DOI"] for r in query_results}
    return dois


def clean_doi(doi: str) -> str:
    return re.sub(r"^https?://doi\.org/", "", doi).upper()


def copy_latest_export_tables(
    project_id: str, from_dataset: str, to_dataset: str, date_match: str, data_location: str, description: str = None
) -> None:
    """Creates copies of all sharded tables from a dataset with a matching a date string.

    :param project_id: The project id
    :param from_dataset: The dataset containing the sharded tables
    :param to_dataset: The dataset to contain the copied tables - will create if does not exist
    :param date_match: The date string to match. e.g. for a table named 'this_table20220101', this would be '20220101'
    :param data_location: The regional location of the data in google cloud
    :param description: The description for dataset housing the copied tables
    """

    if description is None:
        description = "OAEBU Export tables for Dashboarding"

    # Make to_dataset if it doesn't exist
    bq_create_dataset(
        project_id=project_id,
        dataset_id=to_dataset,
        location=data_location,
        description=description,
    )

    # Get the tables from the from_dataset
    client = Client(project_id)
    tables = [t.table_id for t in client.list_tables(from_dataset)]

    # Find the tables with specified date string
    regex_string = rf"^\w+{date_match}\b"
    matched_tables = [re.findall(regex_string, t) for t in tables]
    matched_tables = [t[0] for t in matched_tables if t]
    assert len(matched_tables), f"No tables matching date {date_match} in dataset {project_id}.{from_dataset}"

    # Copy all of the tables
    for table in matched_tables:
        table_id = bq_table_id(project_id, from_dataset, table)
        unsharded_name = re.match(r"(.*?)\d{8}$", table).group(1)  # Drop the date from the table for copied table
        copy_table_id = bq_table_id(project_id, to_dataset, unsharded_name)
        bq_copy_table(
            src_table_id=table_id, dst_table_id=copy_table_id, write_disposition="WRITE_TRUNCATE", client=client
        )


def get_onix_records(table_id: str, client: Client = None) -> List[dict]:
    """Fetch the latest onix snapshot from BigQuery.
    :param table_id: Fully qualified table ID.
    :return: List of onix product records.
    """

    sql = f"SELECT * FROM {table_id}"
    records = bq_run_query(sql, client=client)
    products = [{key: records[i][key] for key in records[i].keys()} for i in range(len(records))]
    return products


def get_isbn_utils_sql_string() -> str:
    """Load the ISBN utils sql functions.
    :return BQ SQL string.
    """

    isbn_utils_file = "isbn_utils.sql"
    isbn_utils_path = os.path.join(sql_folder(workflow_module="onix_workflow"), isbn_utils_file)
    with open(isbn_utils_path, "r") as f:
        isbn_utils_sql = f.read()

    return isbn_utils_sql


def create_data_partner_env(main_template: str, data_partners: Iterable[DataPartner]) -> Environment:
    """Creates a jinja2 environment for any number of data partners

    :param main_template: The name of the main jinja2 template
    :param data_partners: The data partners
    :return: Jinja2 environment with data partners sql folders loaded
    """

    directories = [dp.sql_directory for dp in data_partners]
    with open(main_template) as f:
        contents = f.read()
    loader = FileSystemLoader(directories)
    env = Environment(loader=loader).from_string(contents)
    return env


def insert_into_schema(schema_base: List[dict], insert_field: dict, schema_field_name: Optional[str] = None):
    """
    Inserts a given field into a schema.

    :param schema_base: (List[dict]): The base schema to insert the field into.
    :param insert_field: (dict): The field to be inserted into the schema.
    :param schema_field_name: (Optional[str], optional): The name of the field in the schema.
        If provided, the field will be inserted into the matching field.
        If not provided, the field will be appended to the end of the schema.
    :return: The updated schema with the field inserted.

    Raises ValueError If the provided schema_field_name is not found in the schema.
    """

    if schema_field_name:
        field_found = False
        for row in schema_base:
            if row["name"] == schema_field_name:
                field_found = True
                row["fields"].append(insert_field)
                break
        if not field_found:
            raise ValueError(f"Field {schema_field_name} not found in schema")
    else:
        schema_base.append(insert_field)

    return schema_base
