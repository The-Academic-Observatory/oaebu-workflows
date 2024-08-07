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
import inspect
from typing import Union, List, Dict
from dataclasses import dataclass

from oaebu_workflows.config import schema_folder, sql_folder


class DataPartnerFiles:
    def __init__(self, *, partner_name: str):
        """
        Generates the file names of the data partner. These files may or may not exist depending on whether the
        partner uses them.

        :param partner_name: The name of the partner. The type_id from the OaebuPartner class.

        Generates the following file names:
        - book_product_metrics_schema: The schema for the metrics field in the book_products table.
        - book_product_metadata_schema: The schema for the metadata field in the book_products table.
        - book_metrics_schema: The schema for the book metrics export table.
        - book_metrics_author_schema: The schema for the author metrics table.
        - book_metrics_country_schema: The schema for the country metrics table.
        - book_metrics_subject_schema: The schema for the subject metrics export tables.
        - book_product_body_sql: The SQL template for the body of the book_product table.
        - book_product_functions_sql: The SQL for the functions in the book_product table.
        - book_metrics_sql: The SQL for the book metrics export table.
        - book_metrics_country_body_sql: The SQL template for metrics extion of the country export table.
        - book_metrics_country_join_sql: The SQL for the join of the country export table.
        - book_metrics_country_null_sql: The SQL for the null value assertion in the country export table.
        - book_metrics_country_struct_sql: The SQL for the struct in the country export table.
        - month_metrics_sum_sql: The SQL for the sum of the data partner's month metrics.
        - month_null_sql: The SQL for the null value assertion in the data partner's month metrics.
        """
        self.partner_name = partner_name

        # Schema files
        self.book_product_metrics_schema = f"book_product_metrics_{self.partner_name}.json"
        self.book_product_metadata_schema = f"book_product_metadata_{self.partner_name}.json"
        self.book_metrics_schema = f"book_metrics_{self.partner_name}.json"
        self.book_metrics_author_schema = f"book_metrics_author_{self.partner_name}.json"
        self.book_metrics_country_schema = f"book_metrics_country_{self.partner_name}.json"
        self.book_metrics_subject_schema = f"book_metrics_subject_{self.partner_name}.json"

        # SQL files
        self.book_product_body_sql = f"book_product_body_{self.partner_name}.sql.jinja2"
        self.book_product_functions_sql = f"book_product_functions_{self.partner_name}.sql"
        self.book_metrics_sql = f"book_metrics_{self.partner_name}.sql"
        self.book_metrics_country_body_sql = f"book_metrics_country_body_{self.partner_name}.sql.jinja2"
        self.book_metrics_country_join_sql = f"book_metrics_country_join_{self.partner_name}.sql"
        self.book_metrics_country_null_sql = f"book_metrics_country_null_{self.partner_name}.sql"
        self.book_metrics_country_struct_sql = f"book_metrics_country_struct_{self.partner_name}.sql"
        self.month_metrics_sum_sql = f"month_metrics_sum_{self.partner_name}.sql"
        self.month_null_sql = f"month_null_{self.partner_name}.sql"


@dataclass(kw_only=True)
class OaebuPartner:
    """Class for storing information about data sources we are using to produce oaebu intermediate tables for.

    :param type_id: The dataset type id. Should be the same as its dictionary key.
    :param bq_dataset_id: The BigQuery dataset ID Bigquery Dataset ID.
    :param bq_table_name: The BigQuery table name.
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
    """Represents a data partner.

    :param OaebuPartner: The parent class
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
        book_product_functions: str,
        export_author: bool = False,
        export_book_metrics: bool = False,
        export_country: bool = False,
        export_subject: bool = False,
        has_metadata: bool = True,
    ):
        """
        Initialises the class. Also uses the DataPartnerFiles class to set up the file names.

        :param schema_directory: The directory containing the schema files.
        :param sql_directory: The directory containing the SQL files/templates.
        :param book_product_functions: Whether the partner uses functions in the book product table creation.
        :param export_author: Indicates if the partner will use the author export table.
        :param export_book_metrics: Indicates if the partner will use the book metrics export table.
        :param export_country: Indicates if the partner will use the country export table.
        :param export_subject: Indicates if the partner will use the subject export tables (bic, bisac, thema).
        :param has_metadata: Whether the partner has book metadata records
        """
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
        self.book_product_functions = book_product_functions
        self.export_author = export_author
        self.export_book_metrics = export_book_metrics
        self.export_country = export_country
        self.export_subject = export_subject
        self.has_metadata = has_metadata
        self.files = DataPartnerFiles(partner_name=self.type_id)


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
    onix_view=OaebuPartner(
        type_id="onix_view",
        bq_dataset_id="onix",
        bq_table_name="onix",
        isbn_field_name="ISBN13",
        title_field_name="TitleDetails.TitleElements.TitleText",
        sharded=False,
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
        book_product_functions=True,
        export_author=True,
        export_book_metrics=True,
        export_country=True,
        export_subject=True,
        has_metadata=False,
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
        book_product_functions=True,
        export_author=True,
        export_book_metrics=True,
        export_country=True,
        export_subject=True,
    ),
    google_books_traffic=DataPartner(
        type_id="google_books_traffic",
        bq_dataset_id="google",
        bq_table_name="google_books_traffic",
        isbn_field_name="Primary_ISBN",
        title_field_name="Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="google_books_telescope"), "google_books_traffic.json"),
        schema_directory=os.path.join(schema_folder(workflow_module="google_books_telescope")),
        sql_directory=os.path.join(sql_folder(workflow_module="google_books_telescope")),
        book_product_functions=False,
        export_author=True,
        export_book_metrics=True,
        export_country=False,
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
        book_product_functions=True,
        export_author=True,
        export_book_metrics=True,
        export_country=True,
        export_subject=True,
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
        book_product_functions=True,
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
        book_product_functions=True,
        export_author=False,
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
        book_product_functions=True,
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
        book_product_functions=True,
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
        book_product_functions=True,
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
        book_product_functions=False,
        export_author=False,
        export_book_metrics=True,
        export_country=True,
        export_subject=False,
    ),
    ucl_sales=DataPartner(
        type_id="ucl_sales",
        bq_dataset_id="ucl",
        bq_table_name="ucl_sales",
        isbn_field_name="ISBN13",
        title_field_name="Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="ucl_sales_telescope"), "ucl_sales.json"),
        schema_directory=os.path.join(schema_folder(workflow_module="ucl_sales_telescope")),
        sql_directory=os.path.join(sql_folder(workflow_module="ucl_sales_telescope")),
        book_product_functions=False,
        export_author=False,
        export_book_metrics=False,
        export_country=False,
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
        book_product_functions=False,
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
        book_product_functions=False,
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

    if not isinstance(partner, str):
        return partner

    partners_dict = OAEBU_METADATA_PARTNERS if metadata_partner else OAEBU_DATA_PARTNERS

    try:
        partner = partners_dict[str(partner)]
    except KeyError as e:
        raise KeyError(f"Partner not found: {partner}").with_traceback(e.__traceback__)

    return partner


def create_bespoke_data_partners(partners: List[Dict]) -> List[DataPartner]:
    return [create_bespoke_data_partner(p) for p in partners]


def create_bespoke_data_partner(partner_dict: dict) -> DataPartner:
    """Converts a dictionary description of a partner to a DataPartner object

    :param partner: The dictionary description of the partner. Should have all required fields as per the DataPartner
        object description
    """

    # Get the required parameters of the DataPartner class
    # https://docs.python.org/3/library/inspect.html#inspect.Parameter.kind
    required_params = []
    dp_params = inspect.signature(DataPartner.__init__).parameters
    for k, v in dp_params.items():
        if k == "self":  # Ignore the "self" parameter
            continue
        if v.default is v.empty:
            required_params.append(k)

    present_params = list(partner_dict.keys())
    missing_params = []
    for p in required_params:
        if p not in present_params:
            missing_params.append(p)
    if missing_params:
        raise NameError(f"Missing required parameters for DataPartner class: {missing_params}")

    undefined_params = []
    all_params = list(k for k in dp_params.keys() if k != "self")
    print(all_params)
    for p in partner_dict.keys():
        if p not in all_params:
            undefined_params.append(p)
    if undefined_params:
        raise NameError(f"Unrecognised arguments supplied to DataPartner class: {undefined_params}")

    return DataPartner(**partner_dict)
