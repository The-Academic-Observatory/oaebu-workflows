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
from oaebu_workflows.api_type_ids import DatasetTypeId, TableTypeId


def get_dataset_type_info(api: ObservatoryApi):
    ttids = get_table_type_ids(api)
    dataset_type_info = OrderedDict()
    dataset_type_info[DatasetTypeId.doab] = DatasetType(
        type_id=DatasetTypeId.doab,
        name="DOAB",
        table_type=TableType(id=ttids[TableTypeId.regular]),
    )
    dataset_type_info[DatasetTypeId.oapen_metadata] = DatasetType(
        type_id=DatasetTypeId.oapen_metadata,
        name="OAPEN Metadata",
        table_type=TableType(id=ttids[TableTypeId.regular]),
    )
    dataset_type_info[DatasetTypeId.onix] = DatasetType(
        type_id="onix",
        name="ONIX",
        table_type=TableType(id=ttids[TableTypeId.sharded]),
    )
    dataset_type_info[DatasetTypeId.google_analytics] = DatasetType(
        type_id=DatasetTypeId.google_analytics,
        name="Google Analytics",
        extra={"isbn_field_name": "publication_id", "title_field_name": "title"},
        table_type=TableType(id=ttids[TableTypeId.partitioned]),
    )
    dataset_type_info[DatasetTypeId.google_books_sales] = DatasetType(
        type_id=DatasetTypeId.google_books_sales,
        name="Google Books Sales",
        extra={"isbn_field_name": "Primary_ISBN", "title_field_name": "Title"},
        table_type=TableType(id=ttids[TableTypeId.partitioned]),
    )
    dataset_type_info[DatasetTypeId.google_books_traffic] = DatasetType(
        type_id=DatasetTypeId.google_books_traffic,
        name="Googl Books Traffic",
        extra={"isbn_field_name": "Primary_ISBN", "title_field_name": "Title"},
        table_type=TableType(id=ttids[TableTypeId.partitioned]),
    )
    dataset_type_info[DatasetTypeId.jstor_country] = DatasetType(
        type_id=DatasetTypeId.jstor_country,
        name="JSTOR Country",
        extra={"isbn_field_name": "eISBN", "title_field_name": "Book_Title"},
        table_type=TableType(id=ttids[TableTypeId.partitioned]),
    )
    dataset_type_info[DatasetTypeId.jstor_institution] = DatasetType(
        type_id=DatasetTypeId.jstor_institution,
        name="JSTOR Institution",
        extra={"isbn_field_name": "eISBN", "title_field_name": "Book_Title"},
        table_type=TableType(id=ttids[TableTypeId.partitioned]),
    )
    dataset_type_info[DatasetTypeId.oapen_irus_uk] = DatasetType(
        type_id=DatasetTypeId.oapen_irus_uk,
        name="OAPEN IRUS UK",
        extra={"isbn_field_name": "ISBN", "title_field_name": "book_title"},
        table_type=TableType(id=ttids[TableTypeId.partitioned]),
    )
    dataset_type_info[DatasetTypeId.ucl_discovery] = DatasetType(
        type_id=DatasetTypeId.ucl_discovery,
        name="UCL Discovery",
        extra={"isbn_field_name": "ISBN", "title_field_name": "book_title"},
        table_type=TableType(id=ttids[TableTypeId.partitioned]),
    )
    dataset_type_info[DatasetTypeId.onix_workflow] = DatasetType(
        type_id=DatasetTypeId.onix_workflow,
        name="ONIX Workflow",
        table_type=TableType(id=ttids[TableTypeId.partitioned]),
    )
    dataset_type_info[DatasetTypeId.oapen_workflow] = DatasetType(
        type_id=DatasetTypeId.oapen_workflow,
        name="OAPEN Workflow",
        table_type=TableType(id=ttids[TableTypeId.partitioned]),
    )
    return dataset_type_info


if __name__ == "__main__":
    api = get_api_client()
    dataset_type_info = get_dataset_type_info(api)
    seed_dataset_type(api=api, dataset_type_info=dataset_type_info)
