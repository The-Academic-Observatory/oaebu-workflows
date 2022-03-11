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
#
#
# Author: Richard Hosking

import os
from datetime import timedelta
from functools import partial, update_wrapper
from typing import List, Optional

import pendulum
from airflow.exceptions import AirflowException

from oaebu_workflows.config import sql_folder
from oaebu_workflows.workflows.onix_workflow import make_table_name
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.dag_run_sensor import DagRunSensor
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    create_bigquery_dataset,
    create_bigquery_table_from_query,
)
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.workflow_utils import make_dag_id, make_release_date
from observatory.platform.workflows.workflow import Workflow


class OapenWorkflowRelease:
    """
    Release information for OapenWorkflow.
    """

    def __init__(
        self,
        *,
        release_date: pendulum.DateTime,
        gcp_project_id: str,
    ):
        """
        :param release_date: The release date. It's the current execution date.
        :param oapen_release_date: the OAPEN release date.
        :param gcp_project_id: GCP Project ID.
        """

        self.release_date = release_date
        self.gcp_project_id = gcp_project_id

    def cleanup(self):
        """Delete all files and folders associated with this release.
        :return: None.
        """
        pass


class OapenWorkflow(Workflow):
    """
    Workflow for processing the OAPEN metadata and IRUS-UK metrics data
    """

    DAG_ID_PREFIX = "oapen_workflow"
    ORG_NAME = "OAPEN Press"

    def __init__(
        self,
        *,
        ao_gcp_project_id: str = "academic-observatory",
        oapen_gcp_project_id: str = "oaebu-oapen",
        oapen_metadata_dataset_id: str = "oapen",
        oapen_metadata_table_id: str = "metadata",
        public_book_metadata_dataset_id: str = "observatory",
        public_book_metadata_table_id: str = "book",
        irus_uk_dag_id_prefix: str = "oapen_irus_uk",
        irus_uk_dataset_id: str = "oapen",
        irus_uk_table_id: str = "oapen_irus_uk",
        oaebu_dataset: str = "oaebu",
        oaebu_onix_dataset: str = "oapen_onix",
        oaebu_intermediate_dataset: str = "oaebu_intermediate",
        oaebu_elastic_dataset: str = "data_export",
        country_project_id: str = "academic-observatory",
        country_dataset_id: str = "settings",
        dataset_location: str = "us",
        dataset_description: str = "Oapen workflow tables",
        dag_id: Optional[str] = None,
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2021, 3, 28),
        schedule_interval: Optional[str] = "@weekly",
        catchup: Optional[bool] = False,
        airflow_vars: List = None,
    ):
        """Initialises the workflow object.
        :param ao_gcp_project_id: GCP project ID for the Academic Observatory.
        :param oapen_metadata_dataset_id: GCP dataset ID for the oapen data.
        :param oapen_metadata_table_id: GCP table ID for the oapen data.
        :param public_book_dataset_id: GCP dataset ID for the public book data.
        :param public_book_table_id: GCP table ID for the public book data.
        :param irus_uk_dag_id_prefix: OAEBU IRUS_UK dag id prefix.
        :param irus_uk_dataset_id: OAEBU IRUS_UK dataset id.
        :param irus_uk_table_id: OAEBU IRUS_UK table id.
        :param oaebu_dataset: OAEBU dataset.
        :param oaebu_intermediate_dataset: OAEBU intermediate dataset.
        :param oaebu_elastic_dataset: OAEBU elastic dataset.
        :param dag_id: DAG ID.
        :param start_date: Start date of the DAG.
        :param schedule_interval: Scheduled interval for running the DAG.
        :param catchup: Whether to catch up missed DAG runs.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow.
        """

        self.dag_id = dag_id
        if dag_id is None:
            self.dag_id = make_dag_id(self.DAG_ID_PREFIX, self.ORG_NAME)

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
            ]

        self.org_name = self.ORG_NAME

        # GCP parameters for oaebu_oapen project
        self.dataset_location = dataset_location
        self.dataset_description = dataset_description

        self.oaebu_dataset = oaebu_dataset
        self.oaebu_onix_dataset = oaebu_onix_dataset
        self.oaebu_intermediate_dataset = oaebu_intermediate_dataset
        self.oaebu_elastic_dataset = oaebu_elastic_dataset

        # Academic Observatory Reference
        self.ao_gcp_project_id = ao_gcp_project_id
        self.oapen_gcp_project_id = oapen_gcp_project_id

        # OAPEN Metadata
        self.oapen_metadata_dataset_id = oapen_metadata_dataset_id
        self.oapen_metadata_table_id = oapen_metadata_table_id

        # Public Book Data
        self.public_book_metadata_dataset_id = public_book_metadata_dataset_id
        self.public_book_metadata_table_id = public_book_metadata_table_id

        # IRUS-UK
        self.irus_uk_dag_id_prefix = irus_uk_dag_id_prefix
        self.irus_uk_dataset_id = irus_uk_dataset_id
        self.irus_uk_table_id = irus_uk_table_id

        self.country_project_id = country_project_id
        self.country_dataset_id = country_dataset_id

        # Initialise Telesecope base class
        super().__init__(
            dag_id=self.dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=catchup,
            airflow_vars=airflow_vars,
        )

        with self.parallel_tasks():
            # Wait for irus_uk workflow to finish
            ext_dag_id = make_dag_id(irus_uk_dag_id_prefix, self.ORG_NAME)
            sensor = DagRunSensor(
                task_id=f"{ext_dag_id}_sensor",
                external_dag_id=ext_dag_id,
                mode="reschedule",
                duration=timedelta(days=7),  # Look back up to 7 days from execution date
                poke_interval=int(timedelta(hours=1).total_seconds()),  # Check at this interval if dag run is ready
                timeout=int(timedelta(days=2).total_seconds()),  # Sensor will fail after 2 days of waiting
            )
            self.add_operator(sensor)

            # Wait for OAPEN Metadata workflow to finish
            ext_dag_id = "oapen_metadata"
            sensor = DagRunSensor(
                task_id=f"{ext_dag_id}_sensor",
                external_dag_id=ext_dag_id,
                mode="reschedule",
                duration=timedelta(days=7),  # Look back up to 7 days from execution date
                poke_interval=int(timedelta(hours=1).total_seconds()),  # Check at this interval if dag run is ready
                timeout=int(timedelta(days=2).total_seconds()),  # Sensor will fail after 2 days of waiting
            )
            self.add_operator(sensor)

        # Setup tasks
        self.add_setup_task(self.check_dependencies)

        # Format OAPEN Metadata like ONIX to enable the next steps
        self.add_task(self.create_onix_formatted_metadata_output_tasks)

        # Create OAEBU book product table
        self.add_task(self.create_oaebu_book_product_table)

        # Create OAEBU Elastic Export tables
        self.create_oaebu_export_tasks()

        # Cleanup tasks
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> OapenWorkflowRelease:
        """Creates a release object.
        :param kwargs: From Airflow. Contains the execution_date.
        :return: an OapenWorkflowRelease object.
        """

        # Make release date
        release_date = make_release_date(**kwargs)
        project_id = self.oapen_gcp_project_id

        return OapenWorkflowRelease(
            release_date=release_date,
            gcp_project_id=project_id,
        )

    def cleanup(self, release: OapenWorkflowRelease, **kwargs):
        """Cleanup temporary files.

        :param release: Workflow release objects.
        :param kwargs: Unused.
        """

        release.cleanup()

    def create_onix_formatted_metadata_output_tasks(
        self,
        release: OapenWorkflowRelease,
        **kwargs,
    ):
        """Create the Book Product Table
        :param release: Oapen workflow release information.
        """

        output_dataset = self.oaebu_onix_dataset
        data_location = self.dataset_location
        project_id = release.gcp_project_id

        output_table = "onix"
        release_date = release.release_date
        table_id = bigquery_sharded_table_id(output_table, release_date)

        # SQL reference
        table_joining_template_file = "create_mock_onix_data.sql.jinja2"
        template_path = os.path.join(sql_folder(), table_joining_template_file)

        sql = render_template(
            template_path,
            project_id=self.ao_gcp_project_id,
            dataset_id=self.oapen_metadata_dataset_id,
            table_id=self.oapen_metadata_table_id,
        )

        create_bigquery_dataset(project_id=project_id, dataset_id=output_dataset, location=data_location)

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=output_dataset,
            table_id=table_id,
            location=data_location,
        )

        if not status:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {project_id}.{output_dataset}.{table_id}"
            )

    def create_oaebu_book_product_table(
        self,
        release: OapenWorkflowRelease,
        **kwargs,
    ):
        """Create the Book Product Table
        :param release: Oapen workflow release information.
        :param oapen_dataset: dataset_id if it is  a relevant data source for this publisher
        """

        output_table = "book_product"
        output_dataset = self.oaebu_dataset
        project_id = release.gcp_project_id

        data_location = self.dataset_location
        release_date = release.release_date

        table_joining_template_file = "create_book_products.sql.jinja2"
        template_path = os.path.join(sql_folder(), table_joining_template_file)

        table_id = bigquery_sharded_table_id(output_table, release_date)

        # Identify latest Book release from the Academic Observatory
        public_book_table_id = make_table_name(
            project_id=self.ao_gcp_project_id,
            dataset_id=self.public_book_metadata_dataset_id,
            table_id=self.public_book_metadata_table_id,
            end_date=release.release_date,
            sharded=True,
        )

        google_analytics_table_id = "empty_google_analytics"
        google_books_sales_table_id = "empty_google_books_sales"
        google_books_traffic_table_id = "empty_google_books_traffic"
        jstor_country_table_id = "empty_jstor_country"
        jstor_institution_table_id = "empty_jstor_institution"
        oapen_table_id = f"{project_id}.{self.irus_uk_dataset_id}.{self.irus_uk_table_id}"
        ucl_table_id = "empty_ucl_discovery"

        sql = render_template(
            template_path,
            project_id=project_id,
            onix_dataset_id=self.oaebu_onix_dataset,
            dataset_id=self.oaebu_intermediate_dataset,
            release_date=release_date,
            onix_release_date=release_date,
            onix_workflow=False,
            onix_workflow_dataset="",
            google_analytics_table_id=google_analytics_table_id,
            google_books_sales_table_id=google_books_sales_table_id,
            google_books_traffic_table_id=google_books_traffic_table_id,
            jstor_country_table_id=jstor_country_table_id,
            jstor_institution_table_id=jstor_institution_table_id,
            oapen_table_id=oapen_table_id,
            ucl_table_id=ucl_table_id,
            public_book_tabel_id=f"{self.ao_gcp_project_id}.{self.public_book_metadata_dataset_id}.{public_book_table_id}",
        )

        create_bigquery_dataset(project_id=project_id, dataset_id=output_dataset, location=data_location)

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=output_dataset,
            table_id=table_id,
            location=data_location,
        )

        if not status:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {project_id}.{output_dataset}.{table_id}"
            )

    def export_oaebu_table(
        self,
        release: OapenWorkflowRelease,
        *,
        output_table: str,
        query_template: str,
        **kwargs,
    ):
        """Create an exported oaebu table.
        :param release: Oapen workflow release information.
        """

        project_id = release.gcp_project_id
        output_dataset = self.oaebu_elastic_dataset
        data_location = self.dataset_location
        release_date = release.release_date

        create_bigquery_dataset(project_id=project_id, dataset_id=output_dataset, location=data_location)

        table_id = bigquery_sharded_table_id(f"{project_id.replace('-', '_')}_{output_table}", release_date)
        template_path = os.path.join(sql_folder(), query_template)

        sql = render_template(
            template_path,
            project_id=project_id,
            dataset_id=self.oaebu_dataset,
            release=release_date,
            country_project_id=self.country_project_id,
            country_dataset_id=self.country_dataset_id,
        )

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=output_dataset,
            table_id=table_id,
            location=data_location,
        )

        if not status:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {project_id}.{output_dataset}.{table_id}"
            )

    def create_oaebu_export_tasks(self):
        """Create tasks for exporting final metrics from our OAEBU data.  It will create output tables in the oaebu_elastic dataset."""

        export_tables = [
            {"output_table": "book_product_list", "query_template": "export_book_list.sql.jinja2", "file_type": "json"},
            {
                "output_table": "book_product_metrics",
                "query_template": "export_book_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_metrics_country",
                "query_template": "export_book_metrics_country.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_metrics_institution",
                "query_template": "export_book_metrics_institution.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "institution_list",
                "query_template": "export_institution_list.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_metrics_city",
                "query_template": "export_book_metrics_city.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_metrics_referrer",
                "query_template": "export_book_metrics_referrer.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_metrics_events",
                "query_template": "export_book_metrics_event.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_publisher_metrics",
                "query_template": "export_book_publisher_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_subject_bic_metrics",
                "query_template": "export_book_subject_bic_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_subject_bisac_metrics",
                "query_template": "export_book_subject_bisac_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_subject_thema_metrics",
                "query_template": "export_book_subject_thema_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_year_metrics",
                "query_template": "export_book_year_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_subject_year_metrics",
                "query_template": "export_book_subject_year_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_author_metrics",
                "query_template": "export_book_author_metrics.sql.jinja2",
                "file_type": "json",
            },
        ]

        # Create each export table in BiqQuery
        for export_table in export_tables:
            fn = partial(
                self.export_oaebu_table,
                output_table=export_table["output_table"],
                query_template=export_table["query_template"],
            )

            # Populate the __name__ attribute of the partial object (it lacks one by default).
            update_wrapper(fn, self.export_oaebu_table)
            fn.__name__ += f".{export_table['output_table']}"

            self.add_task(fn)
