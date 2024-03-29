# Copyright 2023-2024 Curtin University
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
# Author: Keegan Smith

import unittest
from unittest.mock import patch

from oaebu_workflows.oaebu_partners import (
    OaebuPartner,
    DataPartner,
    partner_from_str,
    OAEBU_DATA_PARTNERS,
    OAEBU_METADATA_PARTNERS,
)

MOCK_DATA_PARTNERS = {
    "data_partner": DataPartner(
        type_id="data_partner",
        bq_dataset_id="dataset1",
        bq_table_name="table1",
        isbn_field_name="isbn1",
        title_field_name="title1",
        sharded=False,
        schema_path="schema_folder1",
        schema_directory="path/to/schema",
        sql_directory="path/to/sql",
        book_product_functions=True,
        export_author=True,
        export_book_metrics=True,
        export_country=True,
        export_subject=True,
        has_metdata=True,
    )
}
MOCK_METADATA_PARTNERS = {
    "md_partner": OaebuPartner(
        type_id="md_partner",
        bq_dataset_id="dataset1",
        bq_table_name="table1",
        isbn_field_name="isbn1",
        title_field_name="title1",
        sharded=False,
        schema_path="schema_folder1",
    )
}


class TestPartnerFromStr(unittest.TestCase):
    @patch.dict("oaebu_workflows.oaebu_partners.OAEBU_METADATA_PARTNERS", MOCK_METADATA_PARTNERS)
    @patch.dict("oaebu_workflows.oaebu_partners.OAEBU_DATA_PARTNERS", MOCK_DATA_PARTNERS)
    def test_valid_partner_name_string(self):
        # Call the function with a valid partner name string
        result = partner_from_str("data_partner")
        self.assertIsInstance(result, OaebuPartner)
        result = partner_from_str("md_partner", metadata_partner=True)
        self.assertIsInstance(result, OaebuPartner)
        result = partner_from_str(OAEBU_DATA_PARTNERS["data_partner"])
        self.assertEqual(result, OAEBU_DATA_PARTNERS["data_partner"])
        result = partner_from_str(OAEBU_METADATA_PARTNERS["md_partner"], metadata_partner=True)
        self.assertEqual(result, OAEBU_METADATA_PARTNERS["md_partner"])

    @patch.dict("oaebu_workflows.oaebu_partners.OAEBU_METADATA_PARTNERS", MOCK_METADATA_PARTNERS)
    @patch.dict("oaebu_workflows.oaebu_partners.OAEBU_DATA_PARTNERS", MOCK_DATA_PARTNERS)
    def test_invalid_partner_name_string(self):
        # Call the function with an invalid partner name string
        with self.assertRaisesRegex(KeyError, "Partner not found: invalid_partner"):
            partner_from_str("invalid_partner")
        with self.assertRaisesRegex(KeyError, "Partner not found: invalid_partner"):
            partner_from_str("invalid_partner", metadata_partner=True)
        # Call the function with a valid metadata partner but without the flag
        with self.assertRaisesRegex(KeyError, "Partner not found: md_partner"):
            partner_from_str("md_partner", metadata_partner=False)
