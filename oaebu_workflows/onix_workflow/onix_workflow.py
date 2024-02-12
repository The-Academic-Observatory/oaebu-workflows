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

import os
from datetime import timedelta
from typing import List, Optional, Tuple, Union, Iterable
import re
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import pendulum
from google.cloud.bigquery import SourceFormat, Client
from ratelimit import limits, sleep_and_retry
from tenacity import wait_exponential_jitter
from jinja2 import Environment, FileSystemLoader
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup

from oaebu_workflows.airflow_pools import CrossrefEventsPool
from oaebu_workflows.config import schema_folder as default_schema_folder
from oaebu_workflows.config import sql_folder
from oaebu_workflows.oaebu_partners import OaebuPartner, DataPartner, partner_from_str
from oaebu_workflows.onix_workflow.onix_work_aggregation import BookWorkAggregator, BookWorkFamilyAggregator
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.utils.dag_run_sensor import DagRunSensor
from observatory.platform.airflow import AirflowConns
from observatory.platform.files import save_jsonl_gz
from observatory.platform.utils.url_utils import get_user_agent, retry_get_url
from observatory.platform.gcs import gcs_upload_files, gcs_blob_uri, gcs_blob_name_from_path
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.api import make_observatory_api
from observatory.platform.bigquery import (
    bq_load_table,
    bq_table_id,
    bq_sharded_table_id,
    bq_table_id_parts,
    bq_create_dataset,
    bq_create_table_from_query,
    bq_run_query,
    bq_select_table_shard_dates,
    bq_copy_table,
    bq_find_schema,
)
from observatory.platform.workflows.workflow import (
    SnapshotRelease,
    Workflow,
    make_snapshot_date,
    cleanup,
    set_task_state,
    check_workflow_inputs,
)


CROSSREF_EVENT_URL_TEMPLATE = (
    "https://api.eventdata.crossref.org/v1/events?mailto={mailto}"
    "&from-collected-date={start_date}&until-collected-date={end_date}&rows=1000"
    "&obj-id={doi}"
)


class OnixWorkflowRelease(SnapshotRelease):
    """Release information for OnixWorkflow"""

    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
        onix_snapshot_date: pendulum.DateTime,
        crossref_master_snapshot_date: pendulum.DateTime,
    ):
        """
        Construct the OnixWorkflow Release
        :param dag_id: DAG ID.
        :param release_date: The date of the partition/release
        :param onix_snapshot_date: The ONIX snapshot/release date.
        :param crossref_master_snapshot_date: The release date/suffix of the crossref master table
        """

        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.onix_snapshot_date = onix_snapshot_date
        self.crossref_master_snapshot_date = crossref_master_snapshot_date

        # Files
        self.workslookup_path = os.path.join(self.transform_folder, "worksid.jsonl.gz")
        self.workslookup_errors_path = os.path.join(self.transform_folder, "worksid_errors.jsonl.gz")
        self.worksfamilylookup_path = os.path.join(self.transform_folder, "workfamilyid.jsonl.gz")
        self.crossref_metadata_path = os.path.join(self.transform_folder, "crossref_metadata.jsonl.gz")
        self.crossref_events_path = os.path.join(self.transform_folder, "crossref_events.jsonl.gz")

        # Generated Schemas
        self.book_product_schema_path = os.path.join(self.transform_folder, "book_product_schema.json")
        self.author_metrics_schema = os.path.join(self.transform_folder, "author_metrics_schema.json")
        self.book_metrics_schema = os.path.join(self.transform_folder, "metrics_books_metrics_schema.json")
        self.country_metrics_schema = os.path.join(self.transform_folder, "country_metrics_schema.json")
        self.subject_metrics_bic_schema = os.path.join(self.transform_folder, "subject_metrics_bic_schema.json")
        self.subject_metrics_bisac_schema = os.path.join(self.transform_folder, "subject_metrics_bisac_schema.json")
        self.subject_metrics_thema_schema = os.path.join(self.transform_folder, "subject_metrics_thema_schema.json")


class OnixWorkflow(Workflow):
    """Onix Workflow Instance"""

    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        metadata_partner: Union[str, OaebuPartner],
        # Bigquery parameters
        bq_master_crossref_project_id: str = "academic-observatory",
        bq_master_crossref_dataset_id: str = "crossref_metadata",
        bq_oaebu_crossref_dataset_id: str = "crossref",
        bq_master_crossref_metadata_table_name: str = "crossref_metadata",
        bq_oaebu_crossref_metadata_table_name: str = "crossref_metadata",
        bq_crossref_events_table_name: str = "crossref_events",
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
        bq_dataset_description: str = "ONIX workflow tables",
        oaebu_intermediate_match_suffix: str = "_matched",
        # Run parameters
        data_partners: List[Union[str, OaebuPartner]] = None,
        ga3_views_field="page_views",
        schema_folder: str = default_schema_folder(workflow_module="onix_workflow"),
        mailto: str = "agent@observatory.academy",
        crossref_start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
        api_dataset_id: str = "onix_workflow",
        max_threads: int = 2 * os.cpu_count() - 1,
        # Ariflow parameters
        observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API,
        sensor_dag_ids: List[str] = None,
        catchup: Optional[bool] = False,
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2022, 8, 1),
        schedule: Optional[str] = "@weekly",
    ):
        """
        Initialises the workflow object.

        :param dag_id: DAG ID.
        :param cloud_workspace: The CloudWorkspace object for this DAG

        :param bq_master_crossref_project_id: GCP project ID of crossref master data
        :param bq_master_crossref_dataset_id: GCP dataset ID of crossref master data
        :param bq_oaebu_crossref_dataset_id: GCP dataset ID of crossref OAeBU data
        :param bq_master_crossref_metadata_table_name: The name of the master crossref metadata table
        :param bq_oaebu_crossref_metadata_table_name: The name of the OAeBU crossref metadata table
        :param bq_crossref_events_table_name: The name of the crossref events table
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
        :param bq_dataset_description: Description to give to the workflow tables
        :param oaebu_intermediate_match_suffix: Suffix to append to intermediate tables

        :param data_partners: OAEBU data sources.
        :param ga3_views_field: The name of the GA3 views field - should be either 'page_views' or 'unique_views'
        :param schema_folder: the SQL schema path.
        :param mailto: email address used to identify the user when sending requests to an API.
        :param crossref_start_date: The starting date of crossref's API calls
        :param api_dataset_id: The ID to store the dataset release in the API
        :param max_threads: The maximum number of threads to use for parallel tasks.

        :param observatory_api_conn_id: The connection ID for the observatory API
        :param sensor_dag_ids: Dag IDs for dependent tasks
        :param catchup: Whether to catch up missed DAG runs.
        :param start_date: Start date of the DAG.
        :param schedule: Scheduled interval for running the DAG.
        """

        if not sensor_dag_ids:
            sensor_dag_ids = []

        if data_partners is None:
            data_partners = list()

        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.metadata_partner = partner_from_str(metadata_partner, metadata_partner=True)
        # Bigquery projects, datasets and tables
        self.bq_master_crossref_project_id = bq_master_crossref_project_id
        self.bq_master_crossref_dataset_id = bq_master_crossref_dataset_id
        self.bq_oaebu_crossref_dataset_id = bq_oaebu_crossref_dataset_id
        self.bq_master_crossref_metadata_table_name = bq_master_crossref_metadata_table_name
        self.bq_oaebu_crossref_metadata_table_name = bq_oaebu_crossref_metadata_table_name
        self.bq_crossref_events_table_name = bq_crossref_events_table_name
        self.bq_country_project_id = bq_country_project_id
        self.bq_country_dataset_id = bq_country_dataset_id
        self.bq_subject_project_id = bq_subject_project_id
        self.bq_subject_dataset_id = bq_subject_dataset_id
        self.bq_book_table_name = bq_book_table_name
        self.bq_book_product_table_name = bq_book_product_table_name
        self.bq_onix_workflow_dataset = bq_onix_workflow_dataset
        self.bq_oaebu_intermediate_dataset = bq_oaebu_intermediate_dataset
        self.bq_oaebu_dataset = bq_oaebu_dataset
        self.bq_oaebu_export_dataset = bq_oaebu_export_dataset
        self.bq_oaebu_latest_export_dataset = bq_oaebu_latest_export_dataset
        self.bq_worksid_table_name = bq_worksid_table_name
        self.bq_worksid_error_table_name = bq_worksid_error_table_name
        self.bq_workfamilyid_table_name = bq_workfamilyid_table_name
        self.bq_dataset_description = bq_dataset_description
        self.oaebu_intermediate_match_suffix = oaebu_intermediate_match_suffix
        # Run parameters
        self.data_partners = [partner_from_str(p) for p in data_partners]
        self.ga3_views_field = ga3_views_field
        self.schema_folder = schema_folder
        self.mailto = mailto
        self.crossref_start_date = crossref_start_date
        self.api_dataset_id = api_dataset_id
        self.max_threads = max_threads
        # Airflow Parameters
        self.observatory_api_conn_id = observatory_api_conn_id
        self.sensor_dag_ids = sensor_dag_ids
        self.catchup = catchup
        self.start_date = start_date
        self.schedule = schedule

        # Initialise Telesecope base class
        super().__init__(
            dag_id=self.dag_id,
            start_date=start_date,
            schedule=schedule,
            catchup=catchup,
            airflow_conns=[observatory_api_conn_id],
            tags=["oaebu"],
        )

        check_workflow_inputs(self)

    def make_dag(self) -> DAG:
        """Construct the DAG"""

        with self.dag:
            # DAG Sensors - Check the data partner dag runs are complete
            task_sensors = []
            with TaskGroup(group_id="sensors"):
                for ext_dag_id in self.sensor_dag_ids:
                    sensor = DagRunSensor(
                        task_id=f"{ext_dag_id}_sensor",
                        external_dag_id=ext_dag_id,
                        mode="reschedule",
                        duration=timedelta(days=7),  # Look back up to 7 days from execution date
                        poke_interval=int(
                            timedelta(hours=1).total_seconds()
                        ),  # Check at this interval if dag run is ready
                        timeout=int(timedelta(days=2).total_seconds()),  # Sensor will fail after 2 days of waiting
                    )
                    task_sensors.append(sensor)

            # Aggregate Works
            task_aggregate_works = self.make_python_operator(self.aggregate_works, "aggregate_works")

            # Create crossref metadata and event tables
            task_create_crossref_metadata_table = self.make_python_operator(
                self.create_crossref_metadata_table, "create_crossref_metadata_table"
            )
            # Create pool for crossref API calls (if they don't exist)
            # Pools are necessary to throttle the maxiumum number of requests we can make per second and avoid 429 errors
            crossref_events_pool = CrossrefEventsPool(pool_slots=15)
            crossref_events_pool.create_pool()
            task_create_crossref_events_table = self.make_python_operator(
                self.create_crossref_events_table,
                "create_crossref_events_table",
                op_kwargs=dict(
                    pool=crossref_events_pool.pool_name,
                    pool_slots=min(self.max_threads, crossref_events_pool.pool_slots),
                ),
            )

            # Create book table
            task_create_book_table = self.make_python_operator(self.create_book_table, "create_book_table")

            # Create OAEBU Intermediate tables for data partners
            task_create_intermediate_tables = []
            with TaskGroup(group_id="intermediate_tables"):
                for data_partner in self.data_partners:
                    task_id = f"intermediate_{data_partner.bq_table_name}"
                    intermediate = self.make_python_operator(
                        self.create_intermediate_table,
                        task_id,
                        op_kwargs=dict(
                            orig_project_id=self.cloud_workspace.project_id,
                            orig_dataset=data_partner.bq_dataset_id,
                            orig_table=data_partner.bq_table_name,
                            orig_isbn=data_partner.isbn_field_name,
                            sharded=data_partner.sharded,
                        ),
                    )
                    task_create_intermediate_tables.append(intermediate)

            # Book product table
            task_create_book_product_table = self.make_python_operator(
                self.create_book_product_table, "create_book_product_table"
            )

            # Create OAEBU Elastic Export tables
            task_create_export_tables = self.create_tasks_export_tables()

            # Create the (non-sharded) copies of the sharded tables
            task_update_latest_export_tables = self.make_python_operator(
                self.update_latest_export_tables, "update_latest_export_tables"
            )

            # Final tasks
            task_add_release = self.make_python_operator(self.add_new_dataset_releases, "add_new_dataset_releases")
            task_cleanup = self.make_python_operator(self.cleanup, "cleanup")

            chain(
                task_sensors,
                task_aggregate_works,
                task_create_crossref_metadata_table,
                task_create_crossref_events_table,
                task_create_book_table,
                task_create_intermediate_tables,
                task_create_book_product_table,
                task_create_export_tables,
                task_update_latest_export_tables,
                task_add_release,
                task_cleanup,
            )

        return self.dag

    def create_tasks_export_tables(self):
        """Create tasks for exporting final metrics from our OAEBU data.
        These are split into two categories: generic and custom.
        The custom exports change their schema depending on the data partners."""

        generic_export_tables = [
            {
                "output_table": "book_list",
                "query_template": os.path.join(sql_folder("onix_workflow"), "book_list.sql.jinja2"),
                "schema": os.path.join(default_schema_folder("onix_workflow"), "book_list.json"),
            },
            {
                "output_table": "book_metrics_events",
                "query_template": os.path.join(sql_folder("onix_workflow"), "book_metrics_events.sql.jinja2"),
                "schema": os.path.join(default_schema_folder("onix_workflow"), "book_metrics_events.json"),
            },
        ]
        if "jstor_institution" in [dp.type_id for dp in self.data_partners]:
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
                    "query_template": os.path.join(sql_folder("onix_workflow"), "book_metrics_institution.sql.jinja2"),
                    "schema": os.path.join(default_schema_folder("onix_workflow"), "book_metrics_institution.json"),
                },
            )
        if "irus_oapen" in [dp.type_id for dp in self.data_partners]:
            generic_export_tables.append(
                {
                    "output_table": "book_metrics_city",
                    "query_template": os.path.join(sql_folder("onix_workflow"), "book_metrics_city.sql.jinja2"),
                    "schema": os.path.join(default_schema_folder("onix_workflow"), "book_metrics_city.json"),
                }
            )

        # Create each export table in BiqQuery
        tasks = []
        with TaskGroup(group_id="export_tables"):
            for export_table in generic_export_tables:
                task_id = f"export_{export_table['output_table']}"
                tasks.append(
                    self.make_python_operator(
                        self.export_oaebu_table,
                        task_id,
                        op_kwargs=dict(
                            output_table=export_table["output_table"],
                            query_template_path=export_table["query_template"],
                            schema_file_path=export_table["schema"],
                        ),
                    )
                )
            tasks.append(self.make_python_operator(self.export_book_metrics, "export_book_metrics"))
            tasks.append(self.make_python_operator(self.export_book_metrics_country, "export_book_metrics_country"))
            tasks.append(self.make_python_operator(self.export_book_metrics_author, "export_book_metrics_author"))
            tasks.append(self.make_python_operator(self.export_book_metrics_subjects, "export_book_metrics_subjects"))
        return tasks

    def make_release(self, **kwargs) -> OnixWorkflowRelease:
        """Creates a release object.

        :param kwargs: From Airflow. Contains the execution_date.
        :return: an OnixWorkflowRelease object.
        """

        # Make snapshot date
        snapshot_date = make_snapshot_date(**kwargs)

        # Get ONIX release date
        onix_table_id = bq_table_id(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.metadata_partner.bq_dataset_id,
            table_id=self.metadata_partner.bq_table_name,
        )
        onix_snapshot_dates = bq_select_table_shard_dates(table_id=onix_table_id, end_date=snapshot_date)
        if not len(onix_snapshot_dates):
            raise RuntimeError("OnixWorkflow.make_release: no ONIX releases found")

        onix_snapshot_date = onix_snapshot_dates[0]  # Get most recent snapshot

        # Get Crossref Metadata release date
        crossref_table_id = bq_table_id(
            project_id=self.bq_master_crossref_project_id,
            dataset_id=self.bq_master_crossref_dataset_id,
            table_id=self.bq_master_crossref_metadata_table_name,
        )
        crossref_metadata_snapshot_dates = bq_select_table_shard_dates(
            table_id=crossref_table_id, end_date=snapshot_date
        )
        if not len(crossref_metadata_snapshot_dates):
            raise RuntimeError("OnixWorkflow.make_release: no Crossref Metadata releases found")
        crossref_master_snapshot_date = crossref_metadata_snapshot_dates[0]  # Get most recent snapshot

        # Make the release object
        return OnixWorkflowRelease(
            dag_id=self.dag_id,
            run_id=kwargs["run_id"],
            snapshot_date=snapshot_date,
            onix_snapshot_date=onix_snapshot_date,
            crossref_master_snapshot_date=crossref_master_snapshot_date,
        )

    def aggregate_works(self, release: OnixWorkflowRelease, **kwargs) -> None:
        """Fetches the ONIX product records from our ONIX database, aggregates them into works, workfamilies,
        and outputs it into jsonl files.

        :param release: The onix workflow release object
        """

        # Fetch ONIX data
        sharded_onix_table = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.metadata_partner.bq_dataset_id,
            self.metadata_partner.bq_table_name,
            release.onix_snapshot_date,
        )
        products = get_onix_records(sharded_onix_table)

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
        gcs_upload_files(bucket_name=self.cloud_workspace.transform_bucket, file_paths=files)

        # Load the 'WorkID lookup', 'WorkID lookup table errors' and 'WorkFamilyID lookup' tables into BigQuery
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_onix_workflow_dataset,
            location=self.cloud_workspace.data_location,
            description="Onix Workflow Aggregations",
        )

        aggregation_paths = [release.workslookup_path, release.workslookup_errors_path, release.worksfamilylookup_path]
        aggregation_tables = [
            self.bq_worksid_table_name,
            self.bq_worksid_error_table_name,
            self.bq_workfamilyid_table_name,
        ]
        for path, table_name in zip(aggregation_paths, aggregation_tables):
            uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, gcs_blob_name_from_path(path))
            table_id = bq_sharded_table_id(
                self.cloud_workspace.project_id, self.bq_onix_workflow_dataset, table_name, release.snapshot_date
            )
            state = bq_load_table(
                uri=uri,
                table_id=table_id,
                schema_file_path=bq_find_schema(path=self.schema_folder, table_name=table_name),
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition="WRITE_TRUNCATE",
            )
            set_task_state(state, kwargs["ti"].task_id, release=release)

    def create_crossref_metadata_table(self, release: OnixWorkflowRelease, **kwargs) -> None:
        """Creates the crossref metadata table by querying the AO master table and matching on this publisher's ISBNs"""

        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_crossref_dataset_id,
            location=self.cloud_workspace.data_location,
            description="Data from Crossref sources",
        )

        onix_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.metadata_partner.bq_dataset_id,
            self.metadata_partner.bq_table_name,
            release.onix_snapshot_date,
        )
        master_crossref_metadata_table_id = bq_sharded_table_id(
            self.bq_master_crossref_project_id,
            self.bq_master_crossref_dataset_id,
            self.bq_master_crossref_metadata_table_name,
            release.crossref_master_snapshot_date,
        )
        sql = render_template(
            os.path.join(sql_folder(workflow_module="onix_workflow"), "crossref_metadata_filter_isbn.sql.jinja2"),
            onix_table_id=onix_table_id,
            crossref_metadata_table_id=master_crossref_metadata_table_id,
        )
        logging.info("Creating crossref metadata table from master table")
        schema_file_path = bq_find_schema(
            path=self.schema_folder, table_name=self.bq_oaebu_crossref_metadata_table_name
        )
        oaebu_crossref_metadata_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_crossref_dataset_id,
            self.bq_oaebu_crossref_metadata_table_name,
            release.snapshot_date,
        )
        state = bq_create_table_from_query(
            sql=sql, table_id=oaebu_crossref_metadata_table_id, schema_file_path=schema_file_path
        )
        set_task_state(state, kwargs["ti"].task_id, release=release)

    def create_crossref_events_table(self, release: OnixWorkflowRelease, **kwargs) -> None:
        """Download, transform, upload and create a table for crossref events"""

        # Get the unique dois from the metadata table
        metadata_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_crossref_dataset_id,
            self.bq_oaebu_crossref_metadata_table_name,
            release.snapshot_date,
        )
        dois = dois_from_table(metadata_table_id, doi_column_name="DOI", distinct=True)

        # Download and transform all events
        start_date = self.crossref_start_date
        end_date = release.snapshot_date.subtract(days=1).date()
        events = download_crossref_events(dois, start_date, end_date, self.mailto, max_threads=self.max_threads)
        events = transform_crossref_events(events, max_threads=self.max_threads)

        # Zip and upload to google cloud
        save_jsonl_gz(release.crossref_events_path, events)
        gcs_upload_files(bucket_name=self.cloud_workspace.transform_bucket, file_paths=[release.crossref_events_path])
        uri = gcs_blob_uri(self.cloud_workspace.transform_bucket, gcs_blob_name_from_path(release.crossref_events_path))
        table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_crossref_dataset_id,
            self.bq_crossref_events_table_name,
            release.snapshot_date,
        )
        state = bq_load_table(
            uri=uri,
            table_id=table_id,
            schema_file_path=bq_find_schema(path=self.schema_folder, table_name=self.bq_crossref_events_table_name),
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_TRUNCATE",
        )
        set_task_state(state, kwargs["ti"].task_id, release=release)

    def create_book_table(self, release: OnixWorkflowRelease, **kwargs) -> None:
        """Create the oaebu book table using the crossref event and metadata tables"""

        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_dataset,
            location=self.cloud_workspace.data_location,
            description="OAEBU Tables",
        )
        book_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id, self.bq_oaebu_dataset, self.bq_book_table_name, release.snapshot_date
        )
        crossref_metadata_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_crossref_dataset_id,
            self.bq_oaebu_crossref_metadata_table_name,
            release.snapshot_date,
        )
        crossref_events_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_crossref_dataset_id,
            self.bq_crossref_events_table_name,
            release.snapshot_date,
        )
        sql = render_template(
            os.path.join(sql_folder(workflow_module="onix_workflow"), "book.sql.jinja2"),
            crossref_events_table_id=crossref_events_table_id,
            crossref_metadata_table_id=crossref_metadata_table_id,
        )
        logging.info(sql)

        status = bq_create_table_from_query(
            sql=sql,
            table_id=book_table_id,
            schema_file_path=os.path.join(self.schema_folder, "book.json"),
        )
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def create_intermediate_table(
        self,
        release: OnixWorkflowRelease,
        *,
        orig_project_id: str,
        orig_dataset: str,
        orig_table: str,
        orig_isbn: str,
        sharded: bool,
        **kwargs,
    ) -> None:
        """Create an intermediate oaebu table.  They are of the form datasource_matched<date>

        :param release: Onix workflow release information.
        :param orig_project_id: Project ID for the partner data.
        :param orig_dataset: Dataset ID for the partner data.
        :param orig_table: Table ID for the partner data.
        :param orig_isbn: Name of the ISBN field in the partner data table.
        :param sharded: Whether the data partner table is sharded
        """

        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_intermediate_dataset,
            location=self.cloud_workspace.data_location,
            description="Intermediate OAEBU Tables",
        )
        orig_table_id = (
            bq_sharded_table_id(orig_project_id, orig_dataset, orig_table, release.snapshot_date)
            if sharded
            else bq_table_id(orig_project_id, orig_dataset, orig_table)
        )
        output_table_name = f"{orig_table}{self.oaebu_intermediate_match_suffix}"
        template_path = os.path.join(
            sql_folder(workflow_module="onix_workflow"), "assign_workid_workfamilyid.sql.jinja2"
        )
        output_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_intermediate_dataset,
            output_table_name,
            release.snapshot_date,
        )
        wid_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_onix_workflow_dataset,
            self.bq_worksid_table_name,
            release.snapshot_date,
        )
        wfam_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_onix_workflow_dataset,
            self.bq_workfamilyid_table_name,
            release.snapshot_date,
        )

        # Make the table from SQL query
        sql = render_template(
            template_path,
            orig_table_id=orig_table_id,
            orig_isbn=orig_isbn,
            wid_table_id=wid_table_id,
            wfam_table_id=wfam_table_id,
        )
        status = bq_create_table_from_query(sql=sql, table_id=output_table_id)
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def create_book_product_table(
        self,
        release: OnixWorkflowRelease,
        **kwargs,
    ) -> None:
        """Create the Book Product Table"""

        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_dataset,
            location=self.cloud_workspace.data_location,
            description="OAEBU Tables",
        )

        # Data partner table names
        dp_tables = {
            f"{dp.type_id}_table_id": bq_sharded_table_id(
                self.cloud_workspace.project_id,
                self.bq_oaebu_intermediate_dataset,
                f"{dp.type_id}_matched",
                release.snapshot_date,
            )
            for dp in self.data_partners
        }

        # Metadata table name
        onix_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.metadata_partner.bq_dataset_id,
            self.metadata_partner.bq_table_name,
            release.onix_snapshot_date,
        )

        # ONIX WF table names
        workid_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_onix_workflow_dataset,
            self.bq_worksid_table_name,
            release.snapshot_date,
        )
        workfamilyid_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_onix_workflow_dataset,
            self.bq_workfamilyid_table_name,
            release.snapshot_date,
        )
        country_table_id = bq_table_id(self.bq_country_project_id, self.bq_country_dataset_id, "country")
        book_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id, self.bq_oaebu_dataset, self.bq_book_table_name, release.snapshot_date
        )

        # Render the SQL
        env = create_data_partner_env(
            main_template=os.path.join(sql_folder(workflow_module="onix_workflow"), "book_product.sql.jinja2"),
            data_partners=self.data_partners,
        )
        sql = env.render(
            onix_table_id=onix_table_id,
            data_partners=self.data_partners,
            book_table_id=book_table_id,
            country_table_id=country_table_id,
            workid_table_id=workid_table_id,
            workfamilyid_table_id=workfamilyid_table_id,
            ga3_views_field=self.ga3_views_field,
            **dp_tables,
        )
        logging.info(f"Book Product SQL:\n{sql}")

        # Create the table
        with open(os.path.join(default_schema_folder("onix_workflow"), "book_product.json"), "r") as f:
            schema = json.load(f)

        # Create the schema
        for dp in self.data_partners:
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
            self.cloud_workspace.project_id,
            self.bq_oaebu_dataset,
            self.bq_book_product_table_name,
            release.snapshot_date,
        )

        # Run the query
        with open(release.book_product_schema_path, mode="w+") as f:
            json.dump(schema, f)
        status = bq_create_table_from_query(
            sql=sql, table_id=table_id, schema_file_path=release.book_product_schema_path
        )
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def export_oaebu_table(self, release: OnixWorkflowRelease, **kwargs) -> bool:
        """Create an export table.

        Takes several kwargs:
        :param output_table: The name of the table to create
        :param query_template: The name of the template SQL file
        :param schema_file_path: The path to the schema
        :return: Whether the table creation was a success
        """

        output_table: str = kwargs["output_table"]
        query_template_path: str = kwargs["query_template_path"]
        schema_file_path: str = kwargs["schema_file_path"]
        bq_create_dataset(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_export_dataset,
            location=self.cloud_workspace.data_location,
            description="OAEBU Tables for Dashboarding",
        )
        output_table_name = f"{self.cloud_workspace.project_id.replace('-', '_')}_{output_table}"
        output_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id, self.bq_oaebu_export_dataset, output_table_name, release.snapshot_date
        )

        book_product_table_id = bq_sharded_table_id(
            self.cloud_workspace.project_id,
            self.bq_oaebu_dataset,
            self.bq_book_product_table_name,
            release.snapshot_date,
        )
        country_table_id = bq_table_id(self.bq_country_project_id, self.bq_country_dataset_id, "country")
        bic_table_id = bq_table_id(self.bq_subject_project_id, self.bq_subject_dataset_id, "bic_lookup")
        bisac_table_id = bq_table_id(self.bq_subject_project_id, self.bq_subject_dataset_id, "bisac_lookup")
        thema_table_id = bq_table_id(self.bq_subject_project_id, self.bq_subject_dataset_id, "thema_lookup")

        env = create_data_partner_env(main_template=query_template_path, data_partners=self.data_partners)
        sql = env.render(
            project_id=self.cloud_workspace.project_id,
            dataset_id=self.bq_oaebu_dataset,
            release=release.snapshot_date,
            data_partners=self.data_partners,
            book_product_table_id=book_product_table_id,
            country_table_id=country_table_id,
            bic_table_id=bic_table_id,
            bisac_table_id=bisac_table_id,
            thema_table_id=thema_table_id,
        )
        logging.info(f"{output_table} SQL:\n{sql}")

        status = bq_create_table_from_query(sql=sql, table_id=output_table_id, schema_file_path=schema_file_path)
        return status

    def export_book_metrics_country(self, release: OnixWorkflowRelease, **kwargs) -> None:
        """Create table for country metrics"""

        country_schema_base = os.path.join(default_schema_folder("onix_workflow"), "book_metrics_country.json")
        with open(country_schema_base, "r") as f:
            country_schema = json.load(f)

        for dp in [dp for dp in self.data_partners if dp.export_country]:
            _file = dp.files.book_metrics_country_schema
            with open(os.path.join(dp.schema_directory, _file), "r") as f:
                dp_schema = json.load(f)
            country_schema = insert_into_schema(country_schema, dp_schema)

        with open(release.country_metrics_schema, "w") as f:
            json.dump(country_schema, f)

        query_template_path = os.path.join(
            sql_folder(workflow_module="onix_workflow"), "book_metrics_country.sql.jinja2"
        )
        status = self.export_oaebu_table(
            release=release,
            output_table="book_metrics_country",
            query_template_path=query_template_path,
            schema_file_path=release.country_metrics_schema,
        )
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def export_book_metrics_author(self, release: OnixWorkflowRelease, **kwargs) -> None:
        """Create table for author metrics"""

        author_schema_base = os.path.join(default_schema_folder("onix_workflow"), "book_metrics_author.json")
        with open(author_schema_base, "r") as f:
            author_schema = json.load(f)

        for dp in [dp for dp in self.data_partners if dp.export_author]:
            _file = dp.files.book_metrics_author_schema
            with open(os.path.join(dp.schema_directory, _file), "r") as f:
                dp_schema = json.load(f)
            author_schema = insert_into_schema(author_schema, dp_schema)

        with open(release.author_metrics_schema, "w") as f:
            json.dump(author_schema, f)

        query_template_path = os.path.join(
            sql_folder(workflow_module="onix_workflow"), "book_metrics_author.sql.jinja2"
        )
        status = self.export_oaebu_table(
            release=release,
            output_table="book_metrics_author",
            query_template_path=query_template_path,
            schema_file_path=release.author_metrics_schema,
        )
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def export_book_metrics(self, release: OnixWorkflowRelease, **kwargs) -> None:
        """Create table for book metrics"""

        book_schema_base = os.path.join(default_schema_folder("onix_workflow"), "book_metrics.json")
        with open(book_schema_base, "r") as f:
            book_schema = json.load(f)

        for dp in [dp for dp in self.data_partners if dp.export_book_metrics]:
            _file = dp.files.book_metrics_schema
            with open(os.path.join(dp.schema_directory, _file), "r") as f:
                dp_schema = json.load(f)
            book_schema = insert_into_schema(book_schema, dp_schema)

        with open(release.book_metrics_schema, "w") as f:
            json.dump(book_schema, f)

        query_template_path = os.path.join(sql_folder(workflow_module="onix_workflow"), "book_metrics.sql.jinja2")
        status = self.export_oaebu_table(
            release=release,
            output_table="book_metrics",
            query_template_path=query_template_path,
            schema_file_path=release.book_metrics_schema,
        )
        set_task_state(status, kwargs["ti"].task_id, release=release)

    def export_book_metrics_subjects(self, release: OnixWorkflowRelease, **kwargs) -> None:
        """Create tables for subject metrics"""

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

            for dp in [dp for dp in self.data_partners if dp.export_subject]:
                _file = dp.files.book_metrics_subject_schema
                with open(os.path.join(dp.schema_directory, _file), "r") as f:
                    dp_schema = json.load(f)
                subject_schema = insert_into_schema(subject_schema, dp_schema)

            with open(schema_dump, "w") as f:
                json.dump(subject_schema, f)

            query_template_path = os.path.join(
                sql_folder(workflow_module="onix_workflow"), f"book_metrics_subject_{sub}.sql.jinja2"
            )
            status = self.export_oaebu_table(
                release=release,
                output_table=f"book_metrics_subject_{sub}",
                query_template_path=query_template_path,
                schema_file_path=schema_dump,
            )
            set_task_state(status, kwargs["ti"].task_id, release=release)

    def update_latest_export_tables(self, release: OnixWorkflowRelease, **kwargs) -> None:
        """Create copies of the latest data export tables in bigquery"""
        copy_latest_export_tables(
            project_id=self.cloud_workspace.project_id,
            from_dataset=self.bq_oaebu_export_dataset,
            to_dataset=self.bq_oaebu_latest_export_dataset,
            date_match=release.snapshot_date.strftime("%Y%m%d"),
            data_location=self.cloud_workspace.data_location,
        )

    def add_new_dataset_releases(self, release: OnixWorkflowRelease, **kwargs) -> None:
        """Adds release information to API."""

        api = make_observatory_api(observatory_api_conn_id=self.observatory_api_conn_id)
        dataset_release = DatasetRelease(
            dag_id=self.dag_id,
            dataset_id=self.api_dataset_id,
            dag_run_id=release.run_id,
            snapshot_date=release.snapshot_date,
            data_interval_start=kwargs["data_interval_start"],
            data_interval_end=kwargs["data_interval_end"],
        )
        api.post_dataset_release(dataset_release)

    def cleanup(self, release: OnixWorkflowRelease, **kwargs):
        """Cleanup temporary files."""
        cleanup(dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder)


def dois_from_table(table_id: str, doi_column_name: str = "DOI", distinct: str = True) -> List[str]:
    """
    Queries a metadata table to retrieve the unique DOIs. Provided the DOIs are not in a nested structure.

    :param metadata_table_id: The fully qualified ID of the metadata table on GCP
    :param doi_field_name: The name of the DOI column
    :param distinct: Whether to retrieve only unique DOIs
    :return: All DOIs present in the metadata table
    """

    select_field = f"DISTINCT({doi_column_name})" if distinct else doi_column_name
    sql = f"SELECT {select_field} FROM `{table_id}`"
    query_results = bq_run_query(sql)
    dois = [r["DOI"] for r in query_results]
    return dois


def download_crossref_events(
    dois: List[str],
    start_date: pendulum.DateTime,
    end_date: pendulum.DateTime,
    mailto: str,
    max_threads: int = 1,
) -> List[dict]:
    """
    Spawns multiple threads to download event data (DOI and publisher only) for each doi supplied.
    The url template was made with reference to the crossref event api:
    https://www.eventdata.crossref.org/guide/service/query-api/
    Note that the max_threads will cap at 15 because the events API will return a 429 if more than 15 requests are made
    per second. Each API request happens to take roughly 1 second. Having more threadsthan necessary slows down the
    download process as the retry script will wait a minimum of two seconds between each attempt.

    :param dois: The list of DOIs to download the events for
    :param start_date: The start date for events we're interested in
    :param end_date: The end date for events we're interested in
    :param mailto: The email to use as a reference for who is requesting the data
    :param max_threads: The maximum threads to spawn for the downloads.
    :return: All events for the input DOIs
    """

    event_url_template = CROSSREF_EVENT_URL_TEMPLATE
    url_start_date = start_date.strftime("%Y-%m-%d")
    url_end_date = end_date.strftime("%Y-%m-%d")
    max_threads = min(max_threads, 15)

    event_urls = [
        event_url_template.format(doi=doi, mailto=mailto, start_date=url_start_date, end_date=url_end_date)
        for doi in dois
    ]

    logging.info(f"Beginning crossref event data download from {len(event_urls)} URLs with {max_threads} workers")
    logging.info(
        f"Downloading DOI data using URL: {event_url_template.format(doi='***', mailto=mailto, start_date=url_start_date, end_date=url_end_date)}"
    )
    all_events = []
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for i, url in enumerate(event_urls):
            futures.append(executor.submit(download_crossref_event_url, url, i=i))
        for future in as_completed(futures):
            all_events.extend(future.result())

    return all_events


def download_crossref_event_url(url: str, i: int = 0) -> List[dict]:
    """
    Downloads all crossref events from a url, iterating through pages if there is more than one

    :param url: The url send the request to
    :param i: Worker number
    :return: The events from this URL
    """

    events = []
    headers = {"User-Agent": get_user_agent(package_name="oaebu_workflows")}
    next_cursor, page_counts, total_events, page_events = download_crossref_page_events(url, headers)
    events.extend(page_events)
    total_counts = page_counts
    while next_cursor:
        tmp_url = url + f"&cursor={next_cursor}"
        next_cursor, page_counts, _, page_events = download_crossref_page_events(tmp_url, headers)
        total_counts += page_counts
        events.extend(page_events)
    logging.info(f"{i + 1}: {url} successful")
    logging.info(f"{i + 1}: Total no. events: {total_events}, downloaded " f"events: {total_counts}")
    return events


def download_crossref_page_events(url: str, headers: dict) -> Tuple[str, int, int, List[dict]]:
    """
    Download crossref events from a single page

    :param url: The url to send the request to
    :param headers: Headers to send with the request
    :return: The cursor, event counter, total number of events and the events for the URL
    """

    crossref_events_limiter()
    response = retry_get_url(url, num_retries=5, wait=wait_exponential_jitter(initial=0.5, max=60), headers=headers)
    response_json = response.json()
    total_events = response_json["message"]["total-results"]
    events = response_json["message"]["events"]
    next_cursor = response_json["message"]["next-cursor"]
    counter = len(events)

    return next_cursor, counter, total_events, events


@sleep_and_retry
@limits(calls=15, period=1)
def crossref_events_limiter():
    """ "Task to throttle the calls to the crossref events API"""
    return


def transform_crossref_events(events: List[dict], max_threads: int = 1) -> List[dict]:
    """
    Spawns workers to transforms crossref events

    :param all_events: A list of the events to transform
    :param max_threads: The maximum number of threads to utilise for the transforming process
    :return: transformed events, the order of the events in the input list is not preserved
    """

    logging.info(f"Beginning crossref event transform with {max_threads} workers")
    transformed_events = []
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for event in events:
            futures.append(executor.submit(transform_event, event))
        for future in as_completed(futures):
            transformed_events.append(future.result())
    logging.info("Crossref event transformation complete")
    return transformed_events


def transform_event(event: dict) -> dict:
    """Transform the dictionary with event data by replacing '-' with '_' in key names, converting all int values to
    string except for the 'total' field and parsing datetime columns for a valid datetime.

    :param event: The event dictionary
    :return: The transformed event dictionary
    """

    if isinstance(event, (str, int, float)):
        return event
    if isinstance(event, dict):
        new = event.__class__()
        for k, v in event.items():
            if isinstance(v, int) and k != "total":
                v = str(v)
            if k in ["timestamp", "occurred_at", "issued", "dateModified", "updated_date"]:
                try:
                    v = str(pendulum.parse(v))
                except ValueError:
                    v = "0001-01-01T00:00:00Z"

            # Replace hyphens with underscores for BigQuery compatibility
            k = k.replace("-", "_")

            # Replace @ symbol in keys left by DataCite between the 15 and 22 March 2019
            k = k.replace("@", "")

            new[k] = transform_event(v)
        return new


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
        table_name = bq_table_id_parts(table_id)[2]  # Drop the date from the table for copied table
        unsharded_id = bq_table_id(project_id, to_dataset, table_name)
        bq_copy_table(src_table_id=table_id, dst_table_id=unsharded_id, write_disposition="WRITE_TRUNCATE")


def get_onix_records(table_id: str) -> List[dict]:
    """Fetch the latest onix snapshot from BigQuery.
    :param table_id: Fully qualified table ID.
    :return: List of onix product records.
    """

    sql = f"SELECT * FROM {table_id}"
    records = bq_run_query(sql)
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
