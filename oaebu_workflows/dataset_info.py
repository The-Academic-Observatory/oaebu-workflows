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
from observatory.api.client.model.dataset import Dataset
from observatory.api.utils import get_api_client, get_workflows, seed_dataset, get_dataset_type
from observatory.api.client.api.observatory_api import ObservatoryApi


def get_dataset_info(api: ObservatoryApi):
    workflows = get_workflows(api=api)

    dataset_info = OrderedDict()
    dataset_info["DOAB Dataset"] = Dataset(name="DOAB Dataset", service="google", address="academic-observatory.doab.doab", workflow=workflows["DOAB Telescope"], dataset_type=get_dataset_type(api=api, type_id="doab"))
    dataset_info["OAPEN Metadata Dataset"] = Dataset(name="OAPEN Metadata Dataset", service="google", address="academic-observatory.oapen.metadata", workflow=workflows["OAPEN Metadata Telescope"], dataset_type=get_dataset_type(api=api, type_id="oapen_metadata"))
    dataset_info["ANU Press Onix Dataset"] = Dataset(name="ANU Press Onix Dataset", service="google", address="oaebu-anu-press.onix.onix", workflow=workflows["ANU Press ONIX Telescope"], dataset_type=get_dataset_type(api=api, type_id="onix"))
    dataset_info["UCL Press Onix Dataset"] = Dataset(name="UCL Press Onix Dataset", service="google", address="oaebu-ucl-press.onix.onix", workflow=workflows["UCL Press ONIX Telescope"], dataset_type=get_dataset_type(api=api, type_id="onix"))
    dataset_info["Wits Press Onix Dataset"] = Dataset(name="Wits Press Onix Dataset", service="google", address="oaebu-witts-press.onix.onix", workflow=workflows["Wits University Press ONIX Telescope"], dataset_type=get_dataset_type(api=api, type_id="onix"))
    dataset_info["UoMP Onix Dataset"] = Dataset(name="UoMP Onix Dataset", service="google", address="oaebu-umich-press.onix.onix", workflow=workflows["UoMP ONIX Telescope"], dataset_type=get_dataset_type(api=api, type_id="onix"))
    dataset_info["ANU Press Google Analytics Dataset"] = Dataset(name="ANU Press Google Analytics Dataset", service="google", address="oaebu-anu-press.google.google_analytics", workflow=workflows["ANU Press Google Analytics Telescope"], dataset_type=get_dataset_type(api=api, type_id="google_analytics"))
    dataset_info["ANU Press Google Books Sales Dataset"] = Dataset(name="ANU Press Google Books Sales Dataset", service="google", address="oaebu-anu-press.google.google_books_sales", workflow=workflows["ANU Press Google Books Telescope"], dataset_type=get_dataset_type(api=api, type_id="google_books_sales"))
    dataset_info["ANU Press Google Books Traffic Dataset"] = Dataset(name="ANU Press Google Books Traffic Dataset", service="google", address="oaebu-anu-press.google.google_books_traffic", workflow=workflows["ANU Press Google Books Telescope"], dataset_type=get_dataset_type(api=api, type_id="google_books_traffic"))
    dataset_info["UCL Press Google Books Sales Dataset"] = Dataset(name="UCL Press Google Books Sales Dataset", service="google", address="oaebu-ucl-press.google.google_books_sales", workflow=workflows["UCL Press Google Books Telescope"], dataset_type=get_dataset_type(api=api, type_id="google_books_sales"))
    dataset_info["UCL Press Google Books Traffic Dataset"] = Dataset(name="UCL Press Google Books Traffic Dataset", service="google", address="oaebu-ucl-press.google.google_books_traffic", workflow=workflows["UCL Press Google Books Telescope"], dataset_type=get_dataset_type(api=api, type_id="google_books_traffic"))
    dataset_info["UoMP Google Books Sales Dataset"] = Dataset(name="UoMP Google Books Sales Dataset", service="google", address="oaebu-umich-press.google.google_books_sales", workflow=workflows["UoMP Google Books Telescope"], dataset_type=get_dataset_type(api=api, type_id="google_books_sales"))
    dataset_info["UoMP Google Books Traffic Dataset"] = Dataset(name="UoMP Google Books Traffic Dataset", service="google", address="oaebu-umich-press.google.google_books_traffic", workflow=workflows["UoMP Google Books Telescope"], dataset_type=get_dataset_type(api=api, type_id="google_books_traffic"))
    dataset_info["ANU Press JSTOR Country Dataset"] = Dataset(name="ANU Press JSTOR Country Dataset", service="google", address="oaebu-anu-press.jstor.jstor_country", workflow=workflows["ANU Press JSTOR Telescope"], dataset_type=get_dataset_type(api=api, type_id="jstor_country"))
    dataset_info["ANU Press JSTOR Institution Dataset"] = Dataset(name="ANU Press JSTOR Institution Dataset", service="google", address="oaebu-anu-press.jstor.jstor_institution", workflow=workflows["ANU Press JSTOR Telescope"], dataset_type=get_dataset_type(api=api, type_id="jstor_institution"))
    dataset_info["UoMP JSTOR Country Dataset"] = Dataset(name="UoMP JSTOR Country Dataset", service="google", address="oaebu-umich-press.jstor.jstor_country", workflow=workflows["UoMP JSTOR Telescope"], dataset_type=get_dataset_type(api=api, type_id="jstor_country"))
    dataset_info["UoMP JSTOR Institution Dataset"] = Dataset(name="UoMP JSTOR Institution Dataset", service="google", address="oaebu-umich-press.jstor.jstor_institution", workflow=workflows["UoMP JSTOR Telescope"], dataset_type=get_dataset_type(api=api, type_id="jstor_institution"))
    dataset_info["UCL Press JSTOR Country Dataset"] = Dataset(name="UCL Press JSTOR Country Dataset", service="google", address="oaebu-ucl-press.jstor.jstor_country", workflow=workflows["UCL Press JSTOR Telescope"], dataset_type=get_dataset_type(api=api, type_id="jstor_country"))
    dataset_info["UCL Press JSTOR Institution Dataset"] = Dataset(name="UCL Press JSTOR Institution Dataset", service="google", address="oaebu-ucl-press.jstor.jstor_institution", workflow=workflows["UCL Press JSTOR Telescope"], dataset_type=get_dataset_type(api=api, type_id="jstor_institution"))
    dataset_info["Wits Press JSTOR Country Dataset"] = Dataset(name="Wits Press JSTOR Country Dataset", service="google", address="oaebu-witts-press.jstor.jstor_country", workflow=workflows["Wits Press JSTOR Telescope"], dataset_type=get_dataset_type(api=api, type_id="jstor_country"))
    dataset_info["Wits Press JSTOR Institution Dataset"] = Dataset(name="Wits Press JSTOR Institution Dataset", service="google", address="oaebu-witts-press.jstor.jstor_institution", workflow=workflows["Wits Press JSTOR Telescope"], dataset_type=get_dataset_type(api=api, type_id="jstor_institution"))
    dataset_info["UCL Press OAPEN IRUS UK Dataset"] = Dataset(name="UCL Press OAPEN IRUS UK Dataset", service="google", address="oaebu-ucl-press.oapen.oapen_irus_uk", workflow=workflows["UCL Press OAPEN IRUS-UK Telescope"], dataset_type=get_dataset_type(api=api, type_id="oapen_irus_uk"))
    dataset_info["ANU Press OAPEN IRUS UK Dataset"] = Dataset(name="ANU Press OAPEN IRUS UK Dataset", service="google", address="oaebu-anu-press.oapen.oapen_irus_uk", workflow=workflows["ANU Press OAPEN IRUS-UK Telescope"], dataset_type=get_dataset_type(api=api, type_id="oapen_irus_uk"))
    dataset_info["UoMP OAPEN IRUS UK Dataset"] = Dataset(name="UoMP OAPEN IRUS UK Dataset", service="google", address="oaebu-umich-press.oapen.oapen_irus_uk", workflow=workflows["UoMP OAPEN IRUS-UK Telescope"], dataset_type=get_dataset_type(api=api, type_id="oapen_irus_uk"))
    dataset_info["OAPEN Press OAPEN IRUS UK Dataset"] = Dataset(name="OAPEN Press OAPEN IRUS UK Dataset", service="google", address="oaebu-oapen.oapen.oapen_irus_uk", workflow=workflows["OAPEN Press OAPEN IRUS-UK Telescope"], dataset_type=get_dataset_type(api=api, type_id="oapen_irus_uk"))
    dataset_info["Wits Press OAPEN IRUS UK Dataset"] = Dataset(name="Wits Press OAPEN IRUS UK Dataset", service="google", address="oaebu-witts-press.oapen.oapen_irus_uk", workflow=workflows["Wits Press OAPEN IRUS-UK Telescope"], dataset_type=get_dataset_type(api=api, type_id="oapen_irus_uk"))
    dataset_info["UCL Press UCL Discovery Dataset"] = Dataset(name="UCL Press UCL Discovery Dataset", service="google", address="oaebu-ucl-press.ucl.ucl_discovery", workflow=workflows["UCL Press UCL Discovery Telescope"], dataset_type=get_dataset_type(api=api, type_id="ucl_discovery"))
    dataset_info["ANU Press ONIX Workflow Dataset"] = Dataset(name="ANU Press ONIX Workflow Dataset", service="google", address="oaebu-anu-press.oaebu.book_product", workflow=workflows["ANU Press ONIX Workflow"], dataset_type=get_dataset_type(api=api, type_id="onix_workflow"))
    dataset_info["UCL Press ONIX Workflow Dataset"] = Dataset(name="UCL Press ONIX Workflow Dataset", service="google", address="oaebu-ucl-press.oaebu.book_product", workflow=workflows["UCL Press ONIX Workflow"], dataset_type=get_dataset_type(api=api, type_id="onix_workflow"))
    dataset_info["Wits Press ONIX Workflow Dataset"] = Dataset(name="Wits Press ONIX Workflow Dataset", service="google", address="oaebu-witts-press.oaebu.book_product", workflow=workflows["Wits Press ONIX Workflow"], dataset_type=get_dataset_type(api=api, type_id="onix_workflow"))
    dataset_info["UoMP ONIX Workflow Dataset"] = Dataset(name="UoMP ONIX Workflow Dataset", service="google", address="oaebu-umich-press.oaebu.book_product", workflow=workflows["UoMP ONIX Workflow"], dataset_type=get_dataset_type(api=api, type_id="onix_workflow"))
    dataset_info["OAPEN Workflow Dataset"] = Dataset(name="OAPEN Workflow Dataset", service="google", address="oaebu-oapen.oaebu.book_product", workflow=workflows["OAPEN Workflow"], dataset_type=get_dataset_type(api=api, type_id="oapen_workflow"))
    return dataset_info


if __name__ == "__main__":
    api = get_api_client()
    dataset_info = get_dataset_info(api)
    seed_dataset(api=api, dataset_info=dataset_info)
