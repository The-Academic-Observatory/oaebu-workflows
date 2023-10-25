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
import subprocess
import logging
import xmltodict
from glob import glob
from typing import List, Tuple
from dataclasses import dataclass
from copy import deepcopy

from onixcheck import validate as validate_onix

from observatory.platform.config import observatory_home
from observatory.platform.utils.http_download import download_file
from observatory.platform.utils.proc_utils import wait_for_process


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
    if process.returncode != 0:
        logging.error(f"Bash command failed `{cmd}`: {stderr}")
        return False

    return True


def onix_collapse_subjects(onix: List[dict]) -> List[dict]:
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


def onix_create_personname_fields(products: List[dict]) -> List[dict]:
    """Given an ONIX product list, attempts to populate the Contributors.PersonName and/or Contributors.PersonNameInverted
    fields by concatenating the Contributors.NamesBeforeKey and Contributors.KeyNames fields where possible

    :param onix: The input onix feed
    :return: The onix feed with the additional fields populated where possible
    """

    def _can_make_personname(contributor: dict) -> bool:
        return contributor.get("KeyNames") and contributor.get("NamesBeforeKey")

    def _can_make_personname_inverted(contributor: dict) -> bool:
        return contributor.get("NamesBeforeKey") and contributor.get("KeyNames")

    for entry in [i for i in products if i.get("Contributors")]:
        for c in entry["Contributors"]:
            if _can_make_personname(c) and not c.get("PersonName"):
                c["PersonName"] = f"{c['NamesBeforeKey']} {c['KeyNames']}"
            if _can_make_personname_inverted(c) and not c.get("PersonNameInverted"):
                c["PersonNameInverted"] = f"{c['KeyNames']}, {c['NamesBeforeKey']}"
    return products


def elevate_product_identifiers(related_product: dict) -> List[dict]:
    """Given a single <RelatedProduct>, returns a list of <RelatedProduct> elements by elevating
    <ProductIdentifier> elements that shouldn't be there.

    :param related_product: The single <RelatedProduct> element to elevate
    :return: A list of <RelatedProduct> elements
    """
    # If only one exists, it's stored as a single ordered dict
    product_identifiers = related_product["ProductIdentifier"]
    if not isinstance(product_identifiers, list):
        product_identifiers = [product_identifiers]

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


def fix_related_products(products: List[dict]) -> List[dict]:
    """Many products have incorrect related products. The related products should be a list of <RelatedProduct> but is
    instead a single <RelatedProduct> with many <ProductIdentifier> elements.
    This function fixes this issue by creating a new <RelatedProduct> for each <ProductIdentifier> where necessary.
    Note that a <RelatedProduct> can still have more than one <ProductIdentifier> if the <IDValue> elements are the
    same, but the <ProductIDType> is different.

    :param product: The list of products to fix
    :return: The amended products
    """

    for product in products:
        try:
            related_products = product["RelatedMaterial"]["RelatedProduct"]
        except KeyError:  # No related products for this product
            continue

        # If there is only one <RelatedProduct> element, it returns as an ordered dict rather than a list
        if not isinstance(related_products, list):
            related_products = [related_products]

        new_related_products = []
        for related_product in related_products:  # each <RelatedProduct>
            new_related_products.extend(elevate_product_identifiers(related_product))
        product["RelatedMaterial"]["RelatedProduct"] = deepcopy(new_related_products)
    return products


def elevate_related_products(products: dict) -> List[dict]:
    """Makes a copy of an ONIX product for each of its unique related products with an ISBN.
    The copies will swap the original ISBN with their own ISBN and make a unique record reference.
    This elevates all related products to the product level.
    Note that if the product identifier is not an ISBN, will not elevate related products.

    Example:
    Input:
        {"id": "1", "related_ids": ["5", "10", "15"]}
    Output:[
        {"id": "1", "related_ids": ["5", "10", "15"]},
        {"id": "5", "related_ids": ["1", "10", "15"]},
        {"id": "10", "related_ids": ["5", "1", "15"]},
        {"id": "15", "related_ids": ["5", "10", "1"]}
    ]

    Note: the above input format is for demonstration purposes. ONIX products have a different format.


    :param product: The ONIX product
    :return: A list containing the original ONIX product and its related children products.
    """

    def _remove_duplicates(input_list: List[dict], key: str) -> List[dict]:
        """Remove duplicates from a list of dictionaries based on a specified key

        :param input_list: The original list of dictionaries.
        :param key: The key in the dictionaries to identify duplicates.
        :return: A new list containing unique dictionaries based on the specified key.
        """
        seen = set()
        output_list = []

        for item in input_list:
            if item[key] not in seen:
                seen.add(item[key])
                output_list.append(item)

        return output_list

    return_products = []
    # Get the original product ISBN
    for product in products:
        if isinstance(product["ProductIdentifier"], list):
            isbn = [p["IDValue"] for p in product["ProductIdentifier"] if str(p["ProductIDType"]) == "15"][0]
        elif str(product["ProductIdentifier"]["ProductIDType"]) == "15":
            # Happens when only one ProductIdentifier exists, it's not stored as a list
            isbn = product["ProductIdentifier"]["IDValue"]
        else:
            return return_products

        # Get the related products
        try:
            related_products = product["RelatedMaterial"]["RelatedProduct"]["ProductIdentifier"]
        except KeyError:
            # There are no related products for this product
            return return_products

        if isinstance(related_products, list):
            rp_isbns = [rp["IDValue"] for rp in related_products if str(rp["ProductIDType"]) == "15"]
        else:
            rp_isbns = [related_products["IDValue"]] if str(related_products["ProductIDType"]) == "15" else []

        # Elevate all related products in this product
        elevated_isbns = []
        for rp_isbn in rp_isbns:
            # Duplicates of isbn or other related products can exist. Do not elevate these
            if rp_isbn == isbn or rp_isbn in elevated_isbns:
                continue

            # Deepcopy the original product. Update record reference. Swap original ISBN with related ISBN.
            new_product = deepcopy(product)
            new_product["ProductIdentifier"] = {"ProductIDType": "15", "IDValue": rp_isbn}
            new_product["RecordReference"] += f"_{rp_isbn}"
            for rp in new_product["RelatedMaterial"]["RelatedProduct"]["ProductIdentifier"]:
                if str(rp["ProductIDType"]) == "15" and rp["IDValue"] == rp_isbn:
                    rp["IDValue"] = isbn

            # Remove duplicate related_products entries
            new_product["RelatedMaterial"]["RelatedProduct"]["ProductIdentifier"] = _remove_duplicates(
                new_product["RelatedMaterial"]["RelatedProduct"]["ProductIdentifier"], "IDValue"
            )

            return_products.append(new_product)
            elevated_isbns.append(rp_isbn)

    return return_products


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


def filter_through_schema(input: dict, schema: dict):
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
