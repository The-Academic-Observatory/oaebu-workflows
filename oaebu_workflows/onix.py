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
from glob import glob
from typing import List, Tuple
from dataclasses import dataclass

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


def onix_create_personname_fields(onix: List[dict]) -> List[dict]:
    """Given an ONIX feed, attempts to populate the Contributors.PersonName and/or Contributors.PersonNameInverted
    fields by concatenating the Contributors.NamesBeforeKey and Contributors.KeyNames fields where possible

    :param onix: The input onix feed
    :return: The onix feed with the additional fields populated where possible
    """

    def _can_make_personname(contributor: dict) -> bool:
        return contributor.get("KeyNames") and contributor.get("NamesBeforeKey")

    def _can_make_personname_inverted(contributor: dict) -> bool:
        return contributor.get("NamesBeforeKey") and contributor.get("KeyNames")

    for entry in [i for i in onix if i.get("Contributors")]:
        for c in entry["Contributors"]:
            if _can_make_personname(c) and not c.get("PersonName"):
                c["PersonName"] = f"{c['NamesBeforeKey']} {c['KeyNames']}"
            if _can_make_personname_inverted(c) and not c.get("PersonNameInverted"):
                c["PersonNameInverted"] = f"{c['KeyNames']}, {c['NamesBeforeKey']}"
    return onix


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
