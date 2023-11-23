# Copyright 2023 Curtin University
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

# Author: Keegan Smith

import os
import re
import json
import shutil
import subprocess
import logging
import xmltodict
import tempfile
from tempfile import TemporaryDirectory
from glob import glob
from typing import List, Tuple, Literal, Union, Mapping, Any
from dataclasses import dataclass
from copy import deepcopy

from onixcheck import validate as validate_onix

from oaebu_workflows.config import schema_folder
from observatory.platform.config import observatory_home
from observatory.platform.utils.http_download import download_file
from observatory.platform.utils.proc_utils import wait_for_process
from observatory.platform.files import save_jsonl_gz, load_jsonl


@dataclass
class OnixParser:
    """Class for storing infromation on the java ONIX parser

    :param filename: The name of the java ONIX parser file
    :param url: The url to use for downloading the parser
    :param bash template: The path to the bash template "onix_parser.sh.jinja2"
    """

    filename = "coki-onix-parser-1.2-SNAPSHOT-shaded.jar"
    url = "https://github.com/The-Academic-Observatory/onix-parser/releases/download/v1.3.0/coki-onix-parser-1.2-SNAPSHOT-shaded.jar"
    cmd = "java -jar {parser_path} {input_dir} {output_dir}"


class OnixTransformer:
    def __init__(
        self,
        *,
        input_path: str,
        output_dir: str,
        filter_products: bool = False,
        error_removal: bool = False,
        normalise_related_products: bool = False,
        deduplicate_related_products: bool = False,
        elevate_related_products: bool = False,
        add_name_fields: bool = False,
        collapse_subjects: bool = False,
        filter_schema: str = os.path.join(schema_folder(workflow_module="oapen_metadata_telescope"), "oapen_metadata_filter.json"),
        invalid_products_name: str = "invalid_products.xml",
        save_format: Literal["json", "jsonl", "jsonl.gz"] = "jsonl.gz",
        keep_intermediate: bool = False,
    ) -> None:
        """
        Constructor for the MetadataTransformer class.

        :param input_path: The path to the metadata file
        :param output_dir: The directory to output the transformed metadata
        :param filter_products: Filter the metadata through a filter schema
        :param error_removal: Remove products containing errors
        :param normalise_related_products: Fix imporperly formatted related products
        :param deduplicate_related_products: Deduplicate related products
        :param add_name_fields: Add the Contributor.PersonName and Contributor.InvertedPersonName fields where possible
        :param collapse_subjects: Collapse subjects into semicolon-separated strings
        :param filter_schema: The filter schema to use. Required if filter_products is True
        :param invalid_products_name: The name of the invalid products file.
        :param save_format: The format to save the transformed metadata in - json, jsonl, or jsonl.gz
        :param keep_intermediate: Keep the intermediate files
        """
        self.input_path = input_path
        self.output_dir = output_dir
        self.filter_products = filter_products
        self.error_removal = error_removal
        self.normalise_related_products = normalise_related_products
        self.deduplicate_related_products = deduplicate_related_products
        self.elevate_related_products = elevate_related_products
        self.add_name_fields = add_name_fields
        self.collapse_subjects = collapse_subjects
        self.filter_schema = filter_schema
        self.invalid_products_name = invalid_products_name
        self.save_format = save_format
        self.keep_intermediate = keep_intermediate

        if self.filter_products and not self.filter_schema:
            raise ValueError("Product filtering requires a provided filter_schema")

        # Use this attribute to keep track of the last modified metadata file
        self._current_md_path = input_path

        # Temp directory for working in
        self._work_dir = tempfile.mkdtemp()

    @property
    def current_metadata(self) -> Union[List[dict], Mapping[str, Any]]:
        return self._load_metadata(self._current_md_path)

    def __del__(self):
        # Remove the temporary directory
        if hasattr(self, "_work_dir") and self._work_dir:
            shutil.rmtree(self._work_dir, ignore_errors=True)

    def transform(self):
        """
        Transform the oapen metadata XML file based on the supplied options.

        The transformations will be done in the following order. Transforms not included will be skipped:
        1) Filter the XML metadata using a schema to keep the desired fields only
        2) Remove remaining products containing errors
        3) Fix incorrectly formatted related products
        4) Elevate related products to the product level
        5) Construct the Contributor.PersonName and Contributor.InvertedPersonName fields where possible
        6) Parse through the java parser to return .jsonl format - This is always done
        7) Collapse subjects into semicolon-separated strings
        """
        settings = []
        if self.filter_products:
            settings.append("Filter Products")
        if self.error_removal:
            settings.append("Error Removal")
        if self.normalise_related_products:
            settings.append("Normalise Related Products")
        if self.deduplicate_related_products:
            settings.append("Deduplicate Related Products")
        if self.elevate_related_products:
            settings.append("Elevate Related Products")
        settings.append("Parse ONIX")
        if self.add_name_fields:
            settings.append("Add Name Fields")
        if self.collapse_subjects:
            settings.append("Collapse Subjects")
        logging.info("Applying transformation in the following order:")
        logging.info(" | ".join(settings))

        if self.filter_products:
            self._filter_products()
        if self.error_removal:
            self._remove_errors()
        if self.normalise_related_products:
            self._normalise_related_products()
        if self.deduplicate_related_products:
            self._deduplicate_related_products()
        if self.elevate_related_products:
            self._elevate_related_products()
        self._apply_parser()
        if self.add_name_fields:
            self._apply_name_fields()
        if self.collapse_subjects:
            self._collapse_subjects()
        self._save_metadata(self.current_metadata, os.path.join(self.output_dir, f"transformed.{self.save_format}"))
        logging.info(f"Saved transformed metadata to {self._current_md_path}")
        return self._current_md_path

    def _intermediate_file_path(self, file_name):
        dir_ = self.output_dir if self.keep_intermediate else self._work_dir
        return os.path.join(dir_, file_name)

    def _save_metadata(self, metadata: Union[List[dict], Mapping[str, Any]], file_path: str):
        save_path = os.path.join(self._work_dir, file_path)
        format = re.search(r"\.(.*)$", file_path).group(1)
        if format == "xml":
            if not isinstance(metadata, Mapping):
                raise TypeError(f"Metadata must be of type Mapping, instead got type {type(metadata)}")
            with open(save_path, "w") as f:
                xmltodict.unparse(metadata, output=f, pretty=True)
        elif format == "json":
            with open(save_path, "w") as f:
                json.dump(metadata, f)
        elif format == "jsonl":
            with open(save_path, "w") as f:
                for m in metadata:
                    json.dump(m, f)
                    f.write("\n")
        elif format == "jsonl.gz":
            if not type(metadata) == list:
                raise TypeError(f"Metadata must be of type list, instead got type {type(metadata)}")
            save_jsonl_gz(save_path, metadata)
        else:
            raise ValueError(f"Unsupported format: {format}")
        self._current_md_path = save_path

    def _load_metadata(self, file_path: str):
        format = re.search(r"\.(.*)$", file_path).group(1)
        if format == "xml":
            with open(file_path, "rb") as f:
                metadata = xmltodict.parse(f)
        elif format == "json":
            with open(file_path, "r") as f:
                metadata = json.load(f)
        elif format == "jsonl" or format == "jsonl.gz":
            metadata = load_jsonl(file_path)
        else:
            raise ValueError(f"Unsupported format: {format}")
        return metadata

    def _filter_products(self):
        with open(self.filter_schema, "r") as f:
            schema = json.load(f)
        filtered_metadata = filter_through_schema(self.current_metadata, schema)
        self._save_metadata(filtered_metadata, self._intermediate_file_path("filtered.xml"))

    def _remove_errors(self):
        invalid_products_path = os.path.join(self.output_dir, self.invalid_products_name)
        save_path = self._intermediate_file_path("errors_removed.xml")
        remove_invalid_products(self._current_md_path, save_path, invalid_products_path)
        self._current_md_path = save_path

    def _normalise_related_products(self):
        metadata = deepcopy(self.current_metadata)
        if not isinstance(metadata, Mapping):
            raise TypeError(f"Metadata must be of type Mapping, instead is type {type(metadata)}")
        fixed_products = normalise_related_products(metadata["ONIXMessage"]["Product"])
        metadata["ONIXMessage"]["Product"] = fixed_products

        self._save_metadata(metadata, self._intermediate_file_path("normalised.xml"))

    def _deduplicate_related_products(self):
        metadata = deepcopy(self.current_metadata)
        if not isinstance(metadata, Mapping):
            raise TypeError(f"Metadata must be of type Mapping, instead is type {type(metadata)}")
        deduplicated_products = deduplicate_related_products(metadata["ONIXMessage"]["Product"])
        metadata["ONIXMessage"]["Product"] = deduplicated_products
        self._save_metadata(metadata, self._intermediate_file_path("deduplicated.xml"))

    def _elevate_related_products(self):
        metadata = deepcopy(self.current_metadata)
        if not isinstance(metadata, Mapping):
            raise TypeError(f"Metadata must be of type Mapping, instead is type {type(metadata)}")
        logging.info(f"Original product count: {len(metadata['ONIXMessage']['Product'])}")
        elevated = elevate_related_products(metadata["ONIXMessage"]["Product"])
        logging.info(f"Product count after elevating related products: {len(elevated)}")
        metadata["ONIXMessage"]["Product"] = elevated
        self._save_metadata(metadata, self._intermediate_file_path("elevated.xml"))

    def _apply_parser(self):
        success, parser_path = onix_parser_download()
        if not success:
            raise RuntimeError("Failed to download parser")

        with TemporaryDirectory() as tmp_dir:
            shutil.copy(self._current_md_path, os.path.join(tmp_dir, "input.xml"))
            success = onix_parser_execute(parser_path, input_dir=tmp_dir, output_dir=self.output_dir)
        if not success:
            raise RuntimeError("Failed to execute parser")

        self._current_md_path = os.path.join(self.output_dir, "full.jsonl")

    def _apply_name_fields(self):
        metadata = deepcopy(self.current_metadata)
        if not isinstance(metadata, list):
            raise TypeError(f"Metadata must be of type list, instead is type {type(metadata)}")
        metadata = create_personname_fields(metadata)
        self._save_metadata(metadata, self._intermediate_file_path("name_applied.jsonl"))

    def _collapse_subjects(self):
        metadata = deepcopy(self.current_metadata)
        if not isinstance(metadata, list):
            raise TypeError(f"Metadata must be of type list, instead is type {type(metadata)}")
        metadata = collapse_subjects(metadata)
        self._save_metadata(metadata, self._intermediate_file_path("collapsed.jsonl"))


def onix_parser_download(download_dir: str = observatory_home("bin")) -> Tuple[bool, str]:
    """Downloads the ONIX parser from Github

    :param download_dir: The directory to download the file to
    :return: (Whether the download operation was a success, The (expected) location of the downloaded file)
    """
    success, _ = download_file(url=OnixParser.url, prefix_dir=download_dir, filename=OnixParser.filename)
    parser_path = os.path.join(download_dir, OnixParser.filename)
    success = os.path.isfile(parser_path) if success else success  # Second check for file existence
    return success, parser_path


def onix_parser_execute(parser_path: str, input_dir: str, output_dir: str) -> bool:
    """Executes the Java ONIX parser. Requires a .xml file in the input directory.

    :param parser_path: Filepath of the parser
    :param input_dir: The input directory - first argument of the parser
    :param output_dir: The output directory - second argument of the parser
    :return: Whether the task succeeded or not (return code 0 means success)
    """
    if not glob(os.path.join(input_dir, "*.xml")):
        logging.error(f"No .xml file found in input directory: {input_dir}")
        return False

    cmd = OnixParser.cmd.format(parser_path=parser_path, input_dir=input_dir, output_dir=output_dir)
    process = subprocess.Popen(cmd.split(" "), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = wait_for_process(process)
    if stdout:
        logging.info(stdout)
    if stderr:
        logging.info(stderr)
    if process.returncode != 0:
        logging.error(f"Bash command failed `{cmd}`: {stderr}")
        return False
    if not os.path.exists(os.path.join(output_dir, "full.jsonl")):  # May fail to produce file even with returncode=0
        logging.error(f"No .jsonl file found in output directory: {output_dir}")
        return False

    return True


def collapse_subjects(onix: List[dict]) -> List[dict]:  # TODO: alter this to work with pre-parsed onix (.xml)
    """The book product table creation requires the keywords (under Subjects.SubjectHeadingText) to occur only once
    Some ONIX feeds return all keywords as separate entires. This function finds and collapses each keyword into a
    semi-colon separated string. Other common separators will be replaced with semi-colons.

    :param onix: The onix feed
    :return: The onix feed after collapsing the keywords of each row
    """
    for row in onix:
        # Create the joined keywords in this row
        keywords = []
        for subject in row["Subjects"]:
            if subject["SubjectSchemeIdentifier"] != "Keywords":
                continue
            subject_heading_text = [i for i in subject["SubjectHeadingText"] if i is not None]  # Remove Nones
            if not subject_heading_text:  # Empty list
                continue
            keywords.append("; ".join(subject_heading_text))
        # Enforce a split by semicolon
        keywords = "; ".join(keywords)
        keywords = keywords.replace(",", ";")
        keywords = keywords.replace(":", ";")

        # Replace one of the subrows with the new keywords string
        keywords_replaced = False
        remove_indexes = []
        for i, subject in enumerate(row["Subjects"]):
            if subject["SubjectSchemeIdentifier"] == "Keywords":
                if not keywords_replaced:
                    subject["SubjectHeadingText"] = [keywords]
                    keywords_replaced = True
                else:
                    remove_indexes.append(i)

        # Remove additional "keywords" subrows
        for i in sorted(remove_indexes, reverse=True):
            del row["Subjects"][i]

    return onix


def create_personname_fields(onix_products: List[dict]) -> List[dict]:
    """Given an ONIX product list, attempts to populate the Contributors.PersonName and/or Contributors.PersonNameInverted
    fields by concatenating the Contributors.NamesBeforeKey and Contributors.KeyNames fields where possible

    :param onix: The input onix feed
    :return: The onix feed with the additional fields populated where possible
    """

    def _can_make_personname(contributor: dict) -> Union[bool, None]:
        return contributor.get("KeyNames") and contributor.get("NamesBeforeKey")

    def _can_make_personname_inverted(contributor: dict) -> Union[bool, None]:
        return contributor.get("NamesBeforeKey") and contributor.get("KeyNames")

    return_products = deepcopy(onix_products)
    for product in return_products:
        try:
            contributors = product["Contributors"]
        except KeyError:
            continue

        if not isinstance(contributors, list):
            contributors = [contributors]

        for c in contributors:
            if _can_make_personname(c) and not c.get("PersonName"):
                c["PersonName"] = f"{c['NamesBeforeKey']} {c['KeyNames']}"
            if _can_make_personname_inverted(c) and not c.get("PersonNameInverted"):
                c["PersonNameInverted"] = f"{c['KeyNames']}, {c['NamesBeforeKey']}"
    return return_products


def elevate_product_identifiers(related_product: dict) -> List[dict]:
    """Given a single <RelatedProduct>, returns a list of <RelatedProduct> elements by elevating
    <ProductIdentifier> elements that shouldn't be there.
    A <ProductIdentifier> element should only appear once in the <RelatedProduct> unless it has the same
    <IDValue> and a different <ProductIDType>

    <RelatedProduct>
        <ProductRelationCode></ProductRelationCode>
        <ProductIdentifier>
            <IDValue></IDValue>
            <ProductIDType></ProductIDType>
        </ProductIdentifier>
    </RelatedProduct>

    :param related_product: The single <RelatedProduct> element with product identifiers to elevate
    :return: A list of <RelatedProduct> elements
    """
    product_identifiers = _get_product_identifiers(related_product)

    # Find all unique <ProductIdentifier> elements. Elements with same ID and different ID types will be put in the
    # same <RelatedProduct> element.
    product_contains = {}  # key : id_value, value : set(id_types)
    for product_identifier in product_identifiers:
        id_type = product_identifier["ProductIDType"]
        id_value = product_identifier["IDValue"]
        if id_value not in product_contains.keys():
            product_contains[id_value] = set()
            product_contains[id_value].add(id_type)
        else:
            product_contains[id_value].add(id_type)

    new_related_products = []
    product_relation_code = related_product["ProductRelationCode"]
    for product_identifier, id_types in product_contains.items():
        identifier_list = [{"ProductIDType": id_type, "IDValue": product_identifier} for id_type in id_types]
        product = {"ProductRelationCode": product_relation_code, "ProductIdentifier": identifier_list}
        new_related_products.append(product)
    return new_related_products


def normalise_related_products(onix_products: List[dict]) -> List[dict]:
    """Many products have incorrect related products. The related products should be a list of <RelatedProduct> but is
    instead a single <RelatedProduct> with many <ProductIdentifier> elements.
    This function fixes this issue by creating a new <RelatedProduct> for each <ProductIdentifier> where necessary.
    Note that a <RelatedProduct> can still have more than one <ProductIdentifier> if the <IDValue> elements are the
    same, but the <ProductIDType> is different.

    :param product: The list of onix products to fix
    :return: The amended products
    """
    return_products = deepcopy(onix_products)
    for product in return_products:
        related_products = _get_related_products(product)
        if not related_products:
            continue

        new_related_products = []
        for related_product in related_products:
            new_related_products.extend(elevate_product_identifiers(related_product))
        product["RelatedMaterial"]["RelatedProduct"] = deepcopy(new_related_products)
    return return_products


def deduplicate_related_products(onix_products: List[dict]) -> List[dict]:
    """Removes any duplicated <RelatedProduct> elements. Will also remove parent ISBNs present in the <RelatedProduct>
    as they are effectively duplicates

    :param product: The list of onix products to fix
    :return: The amended products
    """
    return_products = deepcopy(onix_products)
    for product in return_products:
        seen_ids = {}
        id_relation_codes = {}
        product_isbn = _get_product_isbn(product)
        related_products = _get_related_products(product)
        if not related_products:
            continue

        # Find all unique <RelatedProduct> elements
        for related_product in related_products:
            for product_identifier in _get_product_identifiers(related_product):
                rp_id = product_identifier["IDValue"]
                rp_id_type = product_identifier["ProductIDType"]
                if rp_id == product_isbn and rp_id_type == "15":  # Ignore parent ISBN in related products
                    continue
                if rp_id not in seen_ids.keys():
                    seen_ids[rp_id] = set()
                seen_ids[rp_id].add(rp_id_type)
                id_relation_codes[rp_id] = related_product["ProductRelationCode"]

        # Rebuild the related products
        new_related_products = []
        for id, id_types in seen_ids.items():
            related_product = {"ProductRelationCode": id_relation_codes[id], "ProductIdentifier": []}
            for id_type in id_types:
                related_product["ProductIdentifier"].append({"ProductIDType": id_type, "IDValue": id})
            new_related_products.append(related_product)
        product["RelatedMaterial"]["RelatedProduct"] = deepcopy(new_related_products)

    return return_products


def elevate_related_products(onix_products: List[dict]) -> List[dict]:
    """Makes a copy of an ONIX product for each of its unique related products with an ISBN.
    The copies will swap the original ISBN with their own ISBN and make a unique record reference.
    This "elevates" all related products to the product level.
    Note that if the product identifier for the related product is not an ISBN, it will not be elevated

    Related Product Structure:
    <RelatedProduct>
        <ProductRelationCode></ProductRelationCode>
        <ProductIdentifier>
            <IDValue></IDValue>
            <ProductIDType></ProductIDType>
        </ProductIdentifier>
        <ProductIdentifier>
            <IDValue></IDValue>
            <ProductIDType></ProductIDType>
        </ProductIdentifier>
    </RelatedProduct>

    :param product: The ONIX product list
    :return: A list containing the original ONIX product and its related children products.
    """

    return_products = deepcopy(onix_products)

    # Get a list of product ISBNS
    parent_isbns = [_get_product_isbn(product) for product in return_products]

    # Get the original product ISBN
    added_products = []
    for product in return_products:
        related_products = _get_related_products(product)
        product_isbn = _get_product_isbn(product)

        if not product_isbn:
            continue  # Only elevate if parent product has an ISBN

        for related_product in related_products:
            relation_code = related_product["ProductRelationCode"]
            if relation_code != "06":
                continue  # We expect only "Alternative Format" relations - https://onix-codelists.io/codelist/51

            # Find the ISBN for the related product
            rp_isbn = _get_product_isbn(related_product)
            if not rp_isbn:
                continue  # No ISBNs in related product
            if rp_isbn in parent_isbns:
                continue  # No need to elevate as this is a duplicate of one of the original ISBNs

            # Copy & make modifications to product
            new_product = deepcopy(product)
            new_product["ProductIdentifier"] = {"ProductIDType": "15", "IDValue": rp_isbn}
            new_product["RecordReference"] += f"_{rp_isbn}"
            # Find and replace the original related product with the parent's ISBN
            for rp in _get_related_products(new_product):
                relation_code = rp["ProductRelationCode"]
                if relation_code != "06":
                    continue
                if _get_product_isbn(rp) == rp_isbn:
                    rp["ProductIdentifier"] = {"ProductIDType": "15", "IDValue": product_isbn}

            added_products.append(new_product)
            parent_isbns.append(rp_isbn)
    return return_products + added_products


def _get_product_isbn(product: dict) -> Union[str, None]:
    """Finds the ISBN of a product or relatedproduct"""
    product_identifiers = product["ProductIdentifier"]
    if not isinstance(product_identifiers, list):
        product_identifiers = [product_identifiers]
    for product_identifier in product_identifiers:
        if str(product_identifier["ProductIDType"]) == "15":
            return product_identifier["IDValue"]


def _get_related_products(product: dict):
    """Finds the related products of a product"""
    try:
        related_products = product["RelatedMaterial"]["RelatedProduct"]
    except (KeyError, TypeError):  # Product has no related products
        return []
    # If there's only one related product, it's not a list
    if not isinstance(related_products, list):
        related_products = [related_products]
    return related_products


def _get_product_identifiers(product: dict):
    """Finds the product identifiers of a product"""
    try:
        product_identifiers = product["ProductIdentifier"]
    except KeyError:  # Product has no product identifiers
        return []
    # If there's only one product identifier, it's not a list
    if not isinstance(product_identifiers, list):
        product_identifiers = [product_identifiers]
    return product_identifiers


@dataclass
class OnixProduct:
    """Represents a single ONIX product and its identifying reference for simplicity"""

    product: dict
    record_reference: str


def find_onix_product(all_lines: list, line_index: int) -> OnixProduct:
    """Finds the range of lines encompassing a <Product> tag, given a line_number that is contained in the product

    :param all_lines: All lines in the onix file
    :param line_number: The line number associated with the product
    :return: A two-tuple of the start and end line numbers of the product
    :raises ValueError: Raised if the return would encompass a negative index, indicating the input line was not in a product
    """
    if line_index < 0 or line_index >= len(all_lines):
        raise IndexError(f"Supplied line index {line_index} is not within the length of the file: {len(all_lines)}")

    # Go up until we find <Product>
    begin_index = line_index
    begin_content = all_lines[begin_index]
    while not "<Product>" in begin_content:
        begin_index -= 1
        if begin_index < 0:
            raise ValueError(f"Product not found surrounding line {line_index}")
        begin_content = all_lines[begin_index]

    # Go up until we find </Product>
    finish_index = line_index
    finish_content = all_lines[finish_index]
    while not "</Product>" in finish_content:
        finish_index += 1
        if finish_index >= len(all_lines):
            raise ValueError(f"Product not found surrounding line {line_index}")
        finish_content = all_lines[finish_index]

    product_str = "".join(all_lines[begin_index : finish_index + 1])
    product = xmltodict.parse(product_str)["Product"]
    if not product:
        raise ValueError(f"Product field is empty for product at line {begin_index}")
    record_reference = product.get("RecordReference")
    if not record_reference:
        raise KeyError(f"No RecordReference for product: {product_str}")

    return OnixProduct(product, record_reference)


def remove_invalid_products(input_xml: str, output_xml: str, invalid_products_file: str = None) -> None:
    """Attempts to validate the input xml as an ONIX file. Will remove any products that contain errors.

    :param input_xml: The filepath of the xml file to validate
    :param output_xml: The output filepath
    :param invalid_products_file: The filepath to write the invalid products to. Ignored if unsupplied.
    """
    # Get the line numbers of any errors
    errors = validate_onix(input_xml)
    error_lines = [int(e.location.split(":")[-2]) - 1 for e in errors]

    # Ingest the file into a list and find onix products with errors
    with open(input_xml, "r") as f:
        metadata = f.readlines()

    invalid_products = [find_onix_product(metadata, line_number) for line_number in error_lines]
    invalid_references = set([product.record_reference for product in invalid_products])
    logging.info(
        f"Metadata feed has been trimmed and {len(invalid_references)} errors remain. Products with errors will be removed"
    )

    # Parse the xml to dictionary
    with open(input_xml, "rb") as f:
        metadata = xmltodict.parse(f)

    # Remove products matching the record references
    metadata["ONIXMessage"]["Product"] = [
        p for p in metadata["ONIXMessage"]["Product"] if p["RecordReference"] not in invalid_references
    ]

    # Create the clean XML file
    with open(output_xml, "w") as f:
        xmltodict.unparse(metadata, output=f, pretty=True)

    # Create the invalid product file
    if invalid_products_file:
        with open(invalid_products_file, "w") as f:
            metadata["ONIXMessage"]["Product"] = [p.product for p in invalid_products]
            xmltodict.unparse(metadata, output=f, pretty=True)
        logging.info(f"Invalid products written to {invalid_products_file}")


def filter_through_schema(input: Union[dict, list], schema: dict):
    """
    This function recursively traverses the input dictionary and compares it to the provided schema.
    It retains only the fields and values that exist in the schema structure, and discards
    any fields that do not match the schema.

    # Example usage with a dictionary and schema:
        input_dict = {
            "name": "John",
            "age": 30,
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "zip": "10001"
            }
        }
        schema = {
            "name": null,
            "age": null,
            "address": {
                "street": null,
                "city": null
            }
        }
        filtered_dict = filter_dict_by_schema(input_dict, schema)
        filtered_dict will be:
        {
            "name": "John",
            "age": 30,
            "address": {
                "street": "123 Main St",
                "city": "New York"
            }
        }

    :param input: The dictionary to filter
    :param schema: The schema describing the desired structure of the dictionary
    """
    if isinstance(input, dict) and isinstance(schema, dict):
        return {key: filter_through_schema(value, schema.get(key)) for key, value in input.items() if key in schema}
    elif isinstance(input, list) and isinstance(schema, list):
        return [filter_through_schema(item, schema[0]) for item in input]
    else:
        return input
