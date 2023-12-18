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
#
#
# Author: Tuan Chien, Keegan Smith

import os
from typing import Union, Optional
from dataclasses import dataclass

from oaebu_workflows.config import schema_folder, sql_folder


@dataclass
class PartnerSQL:
    sql_directory: str
    book_product_functions: str
    book_product_body: str
    month_null: Optional[str] = None
    month_metrics_sum: Optional[str] = None
    export_country_body: Optional[str] = None
    export_country_struct: Optional[str] = None
    export_country_join: Optional[str] = None
    export_country_null: Optional[str] = None
    export_book_metrics: Optional[str] = None


@dataclass(kw_only=True)
class OaebuPartner:
    """Class for storing information about data sources we are using to produce oaebu intermediate tables for.

    :param type_id: The dataset type id. Should be the same as its dictionary key
    :param bq_dataset_id: The BigQuery dataset ID Bigquery Dataset ID.
    :param bq_table_name: The BigQuery table name Bigquery Table name
    :param isbn_field_name: Name of the field containing the ISBN.
    :param title_field_name: Name of the field containing the Title.
    :param sharded: whether the table is sharded or not.
    :param schema_path: The path of the partner's schema folder.
    """

    type_id: str
    bq_dataset_id: str
    bq_table_name: str
    isbn_field_name: str
    title_field_name: str
    sharded: bool
    schema_path: str

    def __str__(self):
        return self.type_id


@dataclass(kw_only=True)
class DataPartner(OaebuPartner):
    """Represents a data class for an oaebu data source.

    :param book_product_functions: The name of the book product temp functions file.
    :param book_product_body: The name of the book product body file.
    """

    sql: Optional[PartnerSQL] = None


OAEBU_METADATA_PARTNERS = dict(
    onix=OaebuPartner(
        type_id="onix",
        bq_dataset_id="onix",
        bq_table_name="onix",
        isbn_field_name="ISBN13",
        title_field_name="TitleDetails.TitleElements.TitleText",
        sharded=True,
        schema_path=os.path.join(schema_folder(workflow_module="onix_telescope"), "onix.json"),
    ),
    thoth=OaebuPartner(
        type_id="thoth",
        bq_dataset_id="onix",
        bq_table_name="onix",
        isbn_field_name="ISBN13",
        title_field_name="TitleDetails.TitleElements.TitleText",
        sharded=True,
        schema_path=os.path.join(schema_folder(workflow_module="onix_telescope"), "onix.json"),
    ),
    oapen_metadata=OaebuPartner(
        type_id="oapen_metadata",
        bq_dataset_id="onix",
        bq_table_name="onix",
        isbn_field_name="ISBN13",
        title_field_name="TitleDetails.TitleElements.TitleText",
        sharded=True,
        schema_path=os.path.join(schema_folder(workflow_module="onix_telescope"), "onix.json"),
    ),
)

OAEBU_DATA_PARTNERS = dict(
    google_analytics3=DataPartner(
        type_id="google_analytics3",
        bq_dataset_id="google",
        bq_table_name="google_analytics3",
        isbn_field_name="publication_id",
        title_field_name="title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="google_analytics3_telescope"), "google_analytics.json"),
        sql=PartnerSQL(
            sql_directory=os.path.join(sql_folder(workflow_module="google_analytics3_telescope")),
            book_product_functions="book_product_functions_google_analytics3.sql",
            book_product_body="book_product_body_google_analytics3.sql.jinja2",
            month_null="month_null_google_analytics3.sql",
            month_metrics_sum="month_metrics_sum_google_analytics3.sql",
            export_country_body="export_country_body_google_analytics3.sql.jinja2",
            export_country_struct="export_country_struct_google_analytics3.sql",
            export_country_join="export_country_join_google_analytics3.sql",
            export_country_null="export_country_null_google_analytics3.sql",
            export_book_metrics="export_book_metrics_google_analytics3.sql",
        ),
    ),
    google_books_sales=DataPartner(
        type_id="google_books_sales",
        bq_dataset_id="google",
        bq_table_name="google_books_sales",
        isbn_field_name="Primary_ISBN",
        title_field_name="Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="google_books_telescope"), "google_books_sales.json"),
        sql=PartnerSQL(
            sql_directory=os.path.join(sql_folder(workflow_module="google_books_telescope")),
            book_product_functions="book_product_functions_google_books_sales.sql",
            book_product_body="book_product_body_google_books_sales.sql.jinja2",
            month_null="month_null_google_books_sales.sql",
            month_metrics_sum="month_metrics_sum_google_books_sales.sql",
            export_country_body="export_country_body_google_books_sales.sql",
            export_country_struct="export_country_struct_google_books_sales.sql",
            export_country_join="export_country_join_google_books_sales.sql",
            export_country_null="export_country_null_google_books_sales.sql",
            export_book_metrics="export_book_metrics_google_books_sales.sql",
        ),
    ),
    google_books_traffic=DataPartner(
        type_id="google_books_traffic",
        bq_datasetsql_id="google",
        bq_table_name="google_books_traffic",
        isbn_field_name="Primary_ISBN",
        title_field_name="Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="google_books_telescope"), "google_books_traffic.json"),
        sql=PartnerSQL(
            sql_directory=os.path.join(sql_folder(workflow_module="google_books_telescope")),
            book_product_functions=None,
            book_product_body="book_product_body_google_books_traffic.sql.jinja2",
            month_null="month_null_google_books_traffic.sql",
            month_metrics_sum="month_metrics_sum_google_books_traffic.sql",
            export_country_body=None,
            export_country_struct=None,
            export_country_join=None,
            export_country_null=None,
            export_book_metrics="export_book_metrics_google_books_traffic.sql",
        ),
    ),
    jstor_country=DataPartner(
        type_id="jstor_country",
        bq_dataset_id="jstor",
        bq_table_name="jstor_country",
        isbn_field_name="ISBN",
        title_field_name="Book_Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="jstor_telescope"), "jstor_country.json"),
        sql=PartnerSQL(
            sql_directory=os.path.join(sql_folder(workflow_module="jstor_telescope")),
            book_product_functions="book_product_functions_jstor_country.sql",
            book_product_body="book_product_body_jstor_country.sql.jinja2",
            month_null="month_null_jstor_country.sql",
            month_metrics_sum=None,
            export_country_body="export_country_body_jstor_country.sql",
            export_country_struct="export_country_struct_jstor_country.sql",
            export_country_join="export_country_join_jstor_country.sql",
            export_country_null="export_country_null_jstor_country.sql",
            export_book_metrics="export_book_metrics_jstor_country.sql",
        ),
    ),
    jstor_institution=DataPartner(
        type_id="jstor_institution",
        bq_dataset_id="jstor",
        bq_table_name="jstor_institution",
        isbn_field_name="ISBN",
        title_field_name="Book_Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="jstor_telescope"), "jstor_institution.json"),
        sql=PartnerSQL(
            sql_directory=os.path.join(sql_folder(workflow_module="jstor_telescope")),
            book_product_functions="book_product_functions_jstor_institution.sql",
            book_product_body="book_product_body_jstor_institution.sql.jinja2",
            month_null=None,
            month_metrics_sum=None,
            export_country_body=None,
            export_country_struct=None,
            export_country_join=None,
            export_country_null=None,
            export_book_metrics=None,
        ),
    ),
    jstor_country_collection=DataPartner(
        type_id="jstor_country_collection",
        bq_dataset_id="jstor",
        bq_table_name="jstor_country",
        isbn_field_name="ISBN",
        title_field_name="Book_Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="jstor_telescope"), "jstor_country_collection.json"),
    ),
    jstor_institution_collection=DataPartner(
        type_id="jstor_institution_collection",
        bq_dataset_id="jstor",
        bq_table_name="jstor_institution",
        isbn_field_name="ISBN",
        title_field_name="Book_Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="jstor_telescope"), "jstor_institution_collection.json"),
    ),
    irus_oapen=DataPartner(
        type_id="irus_oapen",
        bq_dataset_id="irus",
        bq_table_name="irus_oapen",
        isbn_field_name="ISBN",
        title_field_name="book_title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="irus_oapen_telescope"), "irus_oapen.json"),
        sql=PartnerSQL(
            sql_directory=os.path.join(sql_folder(workflow_module="irus_oapen_telescope")),
            book_product_functions="book_product_functions_irus_oapen.sql",
            book_product_body="book_product_body_irus_oapen.sql.jinja2",
            month_null="month_null_irus_oapen.sql",
            month_metrics_sum="month_metrics_sum_irus_oapen.sql",
            export_country_body="export_country_body_irus_oapen.sql",
            export_country_struct="export_country_struct_irus_oapen.sql",
            export_country_join="export_country_join_irus_oapen.sql",
            export_country_null="export_country_null_irus_oapen.sql",
            export_book_metrics="export_book_metrics_irus_oapen.sql",
        ),
    ),
    irus_fulcrum=DataPartner(
        type_id="irus_fulcrum",
        bq_dataset_id="irus",
        bq_table_name="irus_fulcrum",
        isbn_field_name="ISBN",
        title_field_name="book_title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="irus_fulcrum_telescope"), "irus_fulcrum.json"),
        sql=PartnerSQL(
            sql_directory=os.path.join(sql_folder(workflow_module="irus_fulcrum_telescope")),
            book_product_functions="book_product_functions_irus_fulcrum.sql",
            book_product_body="book_product_body_irus_fulcrum.sql.jinja2",
            month_null="month_null_irus_fulcrum.sql",
            month_metrics_sum="month_metrics_sum_irus_fulcrum.sql",
            export_country_body="export_country_body_irus_fulcrum.sql",
            export_country_struct="export_country_struct_irus_fulcrum.sql",
            export_country_join="export_country_join_irus_fulcrum.sql",
            export_country_null="export_country_null_irus_fulcrum.sql",
            export_book_metrics="export_book_metrics_irus_fulcrum.sql",
        ),
    ),
    ucl_discovery=DataPartner(
        type_id="ucl_discovery",
        bq_dataset_id="ucl",
        bq_table_name="ucl_discovery",
        isbn_field_name="ISBN",
        title_field_name="title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="ucl_discovery_telescope"), "ucl_discovery.json"),
        sql=PartnerSQL(
            sql_directory=os.path.join(sql_folder(workflow_module="ucl_discovery_telescope")),
            book_product_functions=None,
            book_product_body="book_product_body_ucl_discovery.sql.jinja2",
            month_null="month_null_ucl_discovery.sql",
            month_metrics_sum=None,
            export_country_body="export_country_body_ucl_discovery.sql",
            export_country_struct="export_country_struct_ucl_discovery.sql",
            export_country_join="export_country_join_ucl_discovery.sql",
            export_country_null="export_country_null_ucl_discovery.sql",
            export_book_metrics="export_book_metrics_ucl_discovery.sql",
        ),
    ),
    internet_archive=DataPartner(
        type_id="internet_archive",
        bq_dataset_id="internet_archive",
        bq_table_name="internet_archive",
        isbn_field_name="ISBN13",
        title_field_name="title",
        sharded=False,
        schema_path=os.path.join(schema_folder(), "internet_archive", "internet_archive.json"),
        sql=PartnerSQL(
            sql_directory=os.path.join(sql_folder(), "internet_archive"),
            book_product_functions="book_product_functions_internet_archive.sql",
            book_product_body="book_product_body_internet_archive.sql.jinja2",
            month_null="month_null_internet_archive.sql",
            month_metrics_sum=None,
            export_country_body=None,
            export_country_struct=None,
            export_country_join=None,
            export_country_null=None,
            export_book_metrics="export_book_metrics_internet_archive.sql",
        ),
    ),
    worldreader=DataPartner(
        type_id="worldreader",
        bq_dataset_id="worldreader",
        bq_table_name="worldreader",
        isbn_field_name="ISBN13",
        title_field_name="title",
        sharded=False,
        schema_path=os.path.join(schema_folder(), "worldreader", "worldreader.json"),
        sql=PartnerSQL(
            sql_directory=os.path.join(sql_folder(), "worldreader"),
            book_product_functions="book_product_functions_worldreader.sql",
            book_product_body="book_product_body_worldreader.sql.jinja2",
            month_null="month_null_worldreader.sql",
            month_metrics_sum=None,
            export_country_body="export_country_body_worldreader.sql",
            export_country_struct="export_country_struct_worldreader.sql",
            export_country_join="export_country_join_worldreader.sql",
            export_country_null="export_country_null_worldreader.sql",
            export_book_metrics="export_book_metrics_worldreader.sql",
        ),
    ),
)


def partner_from_str(partner: Union[str, OaebuPartner], metadata_partner: bool = False) -> OaebuPartner:
    """Get the partner from a string.

    :param partner: The partner name.
    :param metadata_partner: If True, use the metadata partner dictionary; otherwise, use the data partners dictionary
    :raises Exception: Raised if the partner name is not found
    :return: The OaebuPartner
    """

    if isinstance(partner, OaebuPartner):
        return partner

    partners_dict = OAEBU_METADATA_PARTNERS if metadata_partner else OAEBU_DATA_PARTNERS

    try:
        partner = partners_dict[str(partner)]
    except KeyError as e:
        raise KeyError(f"Partner not found: {partner}").with_traceback(e.__traceback__)

    return partner
