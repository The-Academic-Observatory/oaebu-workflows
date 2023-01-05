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

from oaebu_workflows.api_type_ids import DatasetTypeId
from observatory.api.client.api.observatory_api import ObservatoryApi
from observatory.api.client.model.dataset import Dataset
from observatory.api.utils import get_api_client, get_workflows, seed_dataset, get_dataset_type


def get_dataset_info(api: ObservatoryApi):
    workflows = get_workflows(api=api)

    dataset_info = OrderedDict()
    name = "DOAB Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="academic-observatory.doab.doab",
        workflow=workflows["DOAB Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.doab),
    )
    name = "OAPEN Metadata Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="academic-observatory.oapen.metadata",
        workflow=workflows["OAPEN Metadata"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.oapen_metadata),
    )
    name = "OAPEN Press Onix Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-oapen.onix.onix",
        workflow=workflows["OAPEN Press ONIX Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.onix),
    )
    name = "ANU Press Onix Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-anu-press.onix.onix",
        workflow=workflows["ANU Press ONIX Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.onix),
    )
    name = "UCL Press Onix Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-ucl-press.onix.onix",
        workflow=workflows["UCL Press ONIX Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.onix),
    )
    name = "Wits Press Onix Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-witts-press.onix.onix",
        workflow=workflows["Wits Press ONIX Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.onix),
    )
    name = "UoMP Onix Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-umich-press.onix.onix",
        workflow=workflows["UoMP ONIX Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.onix),
    )
    name = "OBP Thoth Onix Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-obp.onix.onix",
        workflow=workflows["OBP Thoth Onix Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.onix),
    )
    name = "CEU Press Onix Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-ceu-press.onix.onix",
        workflow=workflows["CEU Press ONIX Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.onix),
    )
    name = "ANU Press Google Analytics Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-anu-press.google.google_analytics",
        workflow=workflows["ANU Press Google Analytics Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.google_analytics),
    )
    name = "ANU Press Google Books Sales Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-anu-press.google.google_books_sales",
        workflow=workflows["ANU Press Google Books Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.google_books_sales),
    )
    name = "ANU Press Google Books Traffic Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-anu-press.google.google_books_traffic",
        workflow=workflows["ANU Press Google Books Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.google_books_traffic),
    )
    name = "UCL Press Google Books Sales Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-ucl-press.google.google_books_sales",
        workflow=workflows["UCL Press Google Books Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.google_books_sales),
    )
    name = "UCL Press Google Books Traffic Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-ucl-press.google.google_books_traffic",
        workflow=workflows["UCL Press Google Books Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.google_books_traffic),
    )
    name = "UoMP Google Books Sales Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-umich-press.google.google_books_sales",
        workflow=workflows["UoMP Google Books Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.google_books_sales),
    )
    name = "UoMP Google Books Traffic Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-umich-press.google.google_books_traffic",
        workflow=workflows["UoMP Google Books Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.google_books_traffic),
    )
    name = "CEU Press Google Books Sales Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-ceu-press.google.google_books_sales",
        workflow=workflows["CEU Press Google Books Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.google_books_sales),
    )
    name = "CEU Press Google Books Traffic Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-ceu-press.google.google_books_traffic",
        workflow=workflows["CEU Press Google Books Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.google_books_traffic),
    )
    name = "ANU Press JSTOR Country Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-anu-press.jstor.jstor_country",
        workflow=workflows["ANU Press JSTOR Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.jstor_country),
    )
    name = "ANU Press JSTOR Institution Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-anu-press.jstor.jstor_institution",
        workflow=workflows["ANU Press JSTOR Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.jstor_institution),
    )
    name = "UoMP JSTOR Country Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-umich-press.jstor.jstor_country",
        workflow=workflows["UoMP JSTOR Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.jstor_country),
    )
    name = "UoMP JSTOR Institution Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-umich-press.jstor.jstor_institution",
        workflow=workflows["UoMP JSTOR Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.jstor_institution),
    )
    name = "UCL Press JSTOR Country Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-ucl-press.jstor.jstor_country",
        workflow=workflows["UCL Press JSTOR Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.jstor_country),
    )
    name = "UCL Press JSTOR Institution Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-ucl-press.jstor.jstor_institution",
        workflow=workflows["UCL Press JSTOR Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.jstor_institution),
    )
    name = "Wits Press JSTOR Country Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-witts-press.jstor.jstor_country",
        workflow=workflows["Wits Press JSTOR Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.jstor_country),
    )
    name = "Wits Press JSTOR Institution Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-witts-press.jstor.jstor_institution",
        workflow=workflows["Wits Press JSTOR Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.jstor_institution),
    )
    name = "CEU Press JSTOR Country Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-ceu-press.jstor.jstor_country",
        workflow=workflows["CEU Press JSTOR Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.jstor_country),
    )
    name = "CEU Press JSTOR Institution Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-ceu-press.jstor.jstor_institution",
        workflow=workflows["CEU Press JSTOR Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.jstor_institution),
    )
    name = "UCL Press OAPEN IRUS UK Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-ucl-press.oapen.oapen_irus_uk",
        workflow=workflows["UCL Press OAPEN IRUS-UK Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.oapen_irus_uk),
    )
    name = "ANU Press OAPEN IRUS UK Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-anu-press.oapen.oapen_irus_uk",
        workflow=workflows["ANU Press OAPEN IRUS-UK Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.oapen_irus_uk),
    )
    name = "UoMP OAPEN IRUS UK Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-umich-press.oapen.oapen_irus_uk",
        workflow=workflows["UoMP OAPEN IRUS-UK Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.oapen_irus_uk),
    )
    name = "OAPEN Press OAPEN IRUS UK Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-oapen.oapen.oapen_irus_uk",
        workflow=workflows["OAPEN Press OAPEN IRUS-UK Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.oapen_irus_uk),
    )
    name = "Wits Press OAPEN IRUS UK Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-witts-press.oapen.oapen_irus_uk",
        workflow=workflows["Wits Press OAPEN IRUS-UK Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.oapen_irus_uk),
    )
    name = "UCL Press UCL Discovery Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-ucl-press.ucl.ucl_discovery",
        workflow=workflows["UCL Press UCL Discovery Telescope"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.ucl_discovery),
    )
    name = "OAPEN Press ONIX Workflow Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-oapen.oaebu.book_product",
        workflow=workflows["OAPEN Press ONIX Workflow"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.onix_workflow),
    )
    name = "ANU Press ONIX Workflow Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-anu-press.oaebu.book_product",
        workflow=workflows["ANU Press ONIX Workflow"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.onix_workflow),
    )
    name = "UCL Press ONIX Workflow Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-ucl-press.oaebu.book_product",
        workflow=workflows["UCL Press ONIX Workflow"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.onix_workflow),
    )
    name = "Wits Press ONIX Workflow Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-witts-press.oaebu.book_product",
        workflow=workflows["Wits Press ONIX Workflow"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.onix_workflow),
    )
    name = "UoMP ONIX Workflow Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-umich-press.oaebu.book_product",
        workflow=workflows["UoMP ONIX Workflow"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.onix_workflow),
    )
    name = "CEU Press ONIX Workflow Dataset"
    dataset_info[name] = Dataset(
        name=name,
        service="google",
        address="oaebu-ceu-press.oaebu.book_product",
        workflow=workflows["CEU Press ONIX Workflow"],
        dataset_type=get_dataset_type(api=api, type_id=DatasetTypeId.onix_workflow),
    )
    return dataset_info


if __name__ == "__main__":
    api = get_api_client()
    dataset_info = get_dataset_info(api)
    seed_dataset(api=api, dataset_info=dataset_info)
