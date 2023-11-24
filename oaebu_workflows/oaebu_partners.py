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
from typing import Union
from dataclasses import dataclass

from oaebu_workflows.config import schema_folder


@dataclass
class OaebuPartner:
    """Class for storing information about data sources we are using to produce oaebu intermediate tables for.

    :param type_id: The dataset type id. Should be the same as its dictionary key
    :param bq_dataset_id: The BigQuery dataset ID Bigquery Dataset ID.
    :param bq_table_name: The BigQuery table name Bigquery Table name
    :param isbn_field_name: Name of the field containing the ISBN.
    :param title_field_name: Name of the field containing the Title.
    :param sharded: whether the table is sharded or not.
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
    google_analytics3=OaebuPartner(
        type_id="google_analytics3",
        bq_dataset_id="google",
        bq_table_name="google_analytics3",
        isbn_field_name="publication_id",
        title_field_name="title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="google_analytics3_telescope"), "google_analytics.json"),
    ),
    google_books_sales=OaebuPartner(
        type_id="google_books_sales",
        bq_dataset_id="google",
        bq_table_name="google_books_sales",
        isbn_field_name="Primary_ISBN",
        title_field_name="Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="google_books_telescope"), "google_books_sales.json"),
    ),
    google_books_traffic=OaebuPartner(
        type_id="google_books_traffic",
        bq_dataset_id="google",
        bq_table_name="google_books_traffic",
        isbn_field_name="Primary_ISBN",
        title_field_name="Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="google_books_telescope"), "google_books_traffic.json"),
    ),
    jstor_country=OaebuPartner(
        type_id="jstor_country",
        bq_dataset_id="jstor",
        bq_table_name="jstor_country",
        isbn_field_name="ISBN",
        title_field_name="Book_Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="jstor_telescope"), "jstor_country.json"),
    ),
    jstor_institution=OaebuPartner(
        type_id="jstor_institution",
        bq_dataset_id="jstor",
        bq_table_name="jstor_institution",
        isbn_field_name="ISBN",
        title_field_name="Book_Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="jstor_telescope"), "jstor_institution.json"),
    ),
    jstor_country_collection=OaebuPartner(
        type_id="jstor_country_collection",
        bq_dataset_id="jstor",
        bq_table_name="jstor_country",
        isbn_field_name="ISBN",
        title_field_name="Book_Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="jstor_telescope"), "jstor_country_collection.json"),
    ),
    jstor_institution_collection=OaebuPartner(
        type_id="jstor_institution_collection",
        bq_dataset_id="jstor",
        bq_table_name="jstor_institution",
        isbn_field_name="ISBN",
        title_field_name="Book_Title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="jstor_telescope"), "jstor_institution_collection.json"),
    ),
    irus_oapen=OaebuPartner(
        type_id="irus_oapen",
        bq_dataset_id="irus",
        bq_table_name="irus_oapen",
        isbn_field_name="ISBN",
        title_field_name="book_title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="irus_oapen_telescope"), "irus_oapen.json"),
    ),
    irus_fulcrum=OaebuPartner(
        type_id="irus_fulcrum",
        bq_dataset_id="irus",
        bq_table_name="irus_fulcrum",
        isbn_field_name="ISBN",
        title_field_name="book_title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="irus_fulcrum_telescope"), "irus_fulcrum.json"),
    ),
    ucl_discovery=OaebuPartner(
        type_id="ucl_discovery",
        bq_dataset_id="ucl",
        bq_table_name="ucl_discovery",
        isbn_field_name="ISBN",
        title_field_name="title",
        sharded=False,
        schema_path=os.path.join(schema_folder(workflow_module="ucl_discovery_telescope"), "ucl_discovery.json"),
    ),
    internet_archive=OaebuPartner(
        type_id="internet_archive",
        bq_dataset_id="internet_archive",
        bq_table_name="internet_archive",
        isbn_field_name="ISBN13",
        title_field_name="title",
        sharded=False,
        schema_path=os.path.join(schema_folder(), "internet_archive", "internet_archive.json"),
    ),
    worldreader=OaebuPartner(
        type_id="worldreader",
        bq_dataset_id="worldreader",
        bq_table_name="worldreader",
        isbn_field_name="ISBN13",
        title_field_name="title",
        sharded=False,
        schema_path=os.path.join(schema_folder(), "worldreader", "worldreader.json"),
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
