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
# Author: Tuan Chien


from collections import OrderedDict
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.utils import get_api_client, seed_dataset_type, get_table_type_ids
from observatory.api.client.model.table_type import TableType
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501


def get_dataset_type_info(api: ObservatoryApi):
    ttids = get_table_type_ids(api)
    dataset_type_info = OrderedDict()
    dataset_type_info["doab"] = DatasetType(type_id="doab", name="DOAB", table_type=TableType(id=ttids["regular"]))
    dataset_type_info["oapen_metadata"] = DatasetType(type_id="oapen_metadata", name="OAPEN Metadata", table_type=TableType(id=ttids["regular"]))
    dataset_type_info["onix"] = DatasetType(type_id="onix", name="Onix", table_type=TableType(id=ttids["sharded"]))
    dataset_type_info["google_analytics"] = DatasetType(type_id="google_analytics", name="Google Analytics", extra={"isbn_field_name": "publication_id", "title_field_name": "title"}, table_type=TableType(id=ttids["partitioned"]))
    dataset_type_info["google_books_sales"] = DatasetType(type_id="google_books_sales", name="Google Books Sales", extra={"isbn_field_name": "Primary_ISBN", "title_field_name": "Title"}, table_type=TableType(id=ttids["partitioned"]))
    dataset_type_info["google_books_traffic"] = DatasetType(type_id="google_books_traffic", name="Googl Books Traffic", extra={"isbn_field_name": "Primary_ISBN", "title_field_name": "Title"}, table_type=TableType(id=ttids["partitioned"]))
    dataset_type_info["jstor_country"] = DatasetType(type_id="jstor_country", name="JSTOR Country", extra={"isbn_field_name": "eISBN", "title_field_name": "Book_Title"}, table_type=TableType(id=ttids["partitioned"]))
    dataset_type_info["jstor_institution"] = DatasetType(type_id="jstor_institution", name="JSTOR Institution", extra={"isbn_field_name": "eISBN", "title_field_name": "Book_Title"}, table_type=TableType(id=ttids["partitioned"]))
    dataset_type_info["oapen_irus_uk"] = DatasetType(type_id="oapen_irus_uk", name="OAPEN IRUS UK", extra={"isbn_field_name": "ISBN", "title_field_name": "book_title"}, table_type=TableType(id=ttids["partitioned"]))
    dataset_type_info["ucl_discovery"] = DatasetType(type_id="ucl_discovery", name="UCL Discovery", table_type=TableType(id=ttids["partitioned"]))
    dataset_type_info["onix_workflow"] = DatasetType(type_id="onix_workflow", name="ONIX Workflow", table_type=TableType(id=ttids["partitioned"]))
    dataset_type_info["oapen_workflow"] = DatasetType(type_id="oapen_workflow", name="OAPEN Workflow", table_type=TableType(id=ttids["partitioned"]))
    return dataset_type_info

if __name__ == "__main__":
    api = get_api_client()
    dataset_type_info = get_dataset_type_info()
    seed_dataset_type(api=api, dataset_type_info=dataset_type_info)
