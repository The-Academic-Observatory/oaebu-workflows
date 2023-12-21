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
class DataPart:
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


class DataPartner(OaebuPartner):
    """Represents a data class for an oaebu data source.

    :param book_product_functions: The name of the book product temp functions file.
    :param book_product_body: The name of the book product body file.
    """

    def __init__(
        self,
        *,
        type_id: str,
        bq_dataset_id: str,
        bq_table_name: str,
        isbn_field_name: str,
        title_field_name: str,
        sharded: bool,
        schema_path: str,
        schema_directory: str,
        sql_directory: str,
        export_author: bool,
        export_book_metrics: bool,
        export_country: bool,
        export_subject: bool,
    ):
        super().__init__(
            type_id=type_id,
            bq_dataset_id=bq_dataset_id,
            bq_table_name=bq_table_name,
            isbn_field_name=isbn_field_name,
            title_field_name=title_field_name,
            sharded=sharded,
            schema_path=schema_path,
        )
        self.schema_directory = schema_directory
        self.sql_directory = sql_directory
        self.export_author = export_author
        self.export_book_metrics = export_book_metrics
        self.export_country = export_country
        self.export_subject = export_subject
        self.files = DataPartnerFiles(partner_name=self.type_id)


class DataPartnerFiles:
    def __init__(self, *, partner_name: str):
        self.partner_name = partner_name

        # Schema files
        self.book_product_metrics_schema = f"book_product_metrics_{self.partner_name}.json"
        self.book_product_metadata_schema = f"book_product_metadata_{self.partner_name}.json"

        # SQL files
        self.book_product_body = f"book_product_body_{self.partner_name}.sql.jinja2"
        self.book_product_functions = f"book_product_functions_{self.partner_name}.sql"
        self.export_author_struct = f"export_author_struct_{self.partner_name}.sql"
        self.export_book_metrics = f"export_book_metrics_{self.partner_name}.sql"
        self.export_country_metrics = f"export_country_metrics_{self.partner_name}.sql.jinja2"
        self.export_country_join = f"export_country_join_{self.partner_name}.sql"
        self.export_country_null = f"export_country_null_{self.partner_name}.sql"
        self.export_country_struct = f"export_country_struct_{self.partner_name}.sql"
        self.month_metrics_sum = f"month_metrics_sum_{self.partner_name}.sql"
        self.month_null = f"month_null_{self.partner_name}.sql"


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
        schema_directory=os.path.join(schema_folder(workflow_module="google_analytics3_telescope")),
        sql_directory=os.path.join(sql_folder(workflow_module="google_analytics3_telescope")),
        export_author=True,
        export_book_metrics=True,
        export_country=True,
        export_subject=True,
    ),
    google_books_sales=DataPartner(
        type_id="google_books_sales",
        bq_dataset_id="google",
        bq_table_name="google_books_sales",
        isbn_field_name="Primary_ISBN",
        title_field_name="Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="google_books_telescope"), "google_books_sales.json"),
        schema_directory=os.path.join(schema_folder(workflow_module="google_books_telescope")),
        sql_directory=os.path.join(sql_folder(workflow_module="google_books_telescope")),
        export_author=True,
        export_book_metrics=True,
        export_country=True,
        export_subject=True,
    ),
    google_books_traffic=DataPartner(
        type_id="google_books_traffic",
        bq_datasetsql_id="google",
        bq_table_name="google_books_traffic",
        isbn_field_name="Primary_ISBN",
        title_field_name="Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="google_books_telescope"), "google_books_traffic.json"),
        schema_directory=os.path.join(schema_folder(workflow_module="google_books_telescope")),
        sql_directory=os.path.join(sql_folder(workflow_module="google_books_telescope")),
        export_author=True,
        export_book_metrics=True,
        export_country=True,
        export_subject=True,
    ),
    jstor_country=DataPartner(
        type_id="jstor_country",
        bq_dataset_id="jstor",
        bq_table_name="jstor_country",
        isbn_field_name="ISBN",
        title_field_name="Book_Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="jstor_telescope"), "jstor_country.json"),
        schema_directory=os.path.join(schema_folder(workflow_module="jstor_telescope")),
        sql_directory=os.path.join(sql_folder(workflow_module="jstor_telescope")),
        export_author=False,
        export_book_metrics=True,
        export_country=True,
        export_subject=False,
    ),
    jstor_institution=DataPartner(
        type_id="jstor_institution",
        bq_dataset_id="jstor",
        bq_table_name="jstor_institution",
        isbn_field_name="ISBN",
        title_field_name="Book_Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="jstor_telescope"), "jstor_institution.json"),
        schema_directory=os.path.join(schema_folder(workflow_module="jstor_telescope")),
        sql_directory=os.path.join(sql_folder(workflow_module="jstor_telescope")),
        export_author=False,
        export_book_metrics=False,
        export_country=False,
        export_subject=False,
    ),
    jstor_country_collection=DataPartner(
        type_id="jstor_country_collection",
        bq_dataset_id="jstor",
        bq_table_name="jstor_country",
        isbn_field_name="ISBN",
        title_field_name="Book_Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="jstor_telescope"), "jstor_country_collection.json"),
        schema_directory=os.path.join(schema_folder(workflow_module="jstor_telescope")),
        sql_directory=os.path.join(sql_folder(workflow_module="jstor_telescope")),
        export_author=True,
        export_book_metrics=False,
        export_country=True,
        export_subject=True,
    ),
    jstor_institution_collection=DataPartner(
        type_id="jstor_institution_collection",
        bq_dataset_id="jstor",
        bq_table_name="jstor_institution",
        isbn_field_name="ISBN",
        title_field_name="Book_Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="jstor_telescope"), "jstor_institution_collection.json"),
        schema_directory=os.path.join(schema_folder(workflow_module="jstor_telescope")),
        sql_directory=os.path.join(sql_folder(workflow_module="jstor_telescope")),
        export_author=False,
        export_book_metrics=False,
        export_country=False,
        export_subject=False,
    ),
    irus_oapen=DataPartner(
        type_id="irus_oapen",
        bq_dataset_id="irus",
        bq_table_name="irus_oapen",
        isbn_field_name="ISBN",
        title_field_name="book_title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="irus_oapen_telescope"), "irus_oapen.json"),
        schema_directory=os.path.join(schema_folder(workflow_module="irus_oapen_telescope")),
        sql_directory=os.path.join(sql_folder(workflow_module="irus_oapen_telescope")),
        export_author=True,
        export_book_metrics=True,
        export_country=True,
        export_subject=True,
    ),
    irus_fulcrum=DataPartner(
        type_id="irus_fulcrum",
        bq_dataset_id="irus",
        bq_table_name="irus_fulcrum",
        isbn_field_name="ISBN",
        title_field_name="book_title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="irus_fulcrum_telescope"), "irus_fulcrum.json"),
        schema_directory=os.path.join(schema_folder(workflow_module="irus_fulcrum_telescope")),
        sql_directory=os.path.join(sql_folder(workflow_module="irus_fulcrum_telescope")),
        export_author=True,
        export_book_metrics=True,
        export_country=True,
        export_subject=True,
    ),
    ucl_discovery=DataPartner(
        type_id="ucl_discovery",
        bq_dataset_id="ucl",
        bq_table_name="ucl_discovery",
        isbn_field_name="ISBN",
        title_field_name="title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="ucl_discovery_telescope"), "ucl_discovery.json"),
        schema_directory=os.path.join(schema_folder(workflow_module="ucl_discovery_telescope")),
        sql_directory=os.path.join(sql_folder(workflow_module="ucl_discovery_telescope")),
        export_author=False,
        export_book_metrics=True,
        export_country=True,
        export_subject=False,
    ),
    internet_archive=DataPartner(
        type_id="internet_archive",
        bq_dataset_id="internet_archive",
        bq_table_name="internet_archive",
        isbn_field_name="ISBN13",
        title_field_name="title",
        sharded=False,
        schema_path=os.path.join(schema_folder(), "internet_archive", "internet_archive.json"),
        schema_directory=os.path.join(schema_folder(), "internet_archive"),
        sql_directory=os.path.join(sql_folder(), "internet_archive"),
        export_author=False,
        export_book_metrics=True,
        export_country=False,
        export_subject=False,
    ),
    worldreader=DataPartner(
        type_id="worldreader",
        bq_dataset_id="worldreader",
        bq_table_name="worldreader",
        isbn_field_name="ISBN13",
        title_field_name="title",
        sharded=False,
        schema_path=os.path.join(schema_folder(), "worldreader", "worldreader.json"),
        schema_directory=os.path.join(schema_folder(), "worldreader"),
        sql_directory=os.path.join(sql_folder(), "worldreader"),
        export_author=False,
        export_book_metrics=True,
        export_country=True,
        export_subject=False,
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
