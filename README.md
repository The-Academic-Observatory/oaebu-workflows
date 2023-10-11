# Book Usage Data Workflows

Book Usage Data Workflows provides Apache Airflow workflows for fetching, processing and analysing data about Open Access Books.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/python-3.10-blue)](https://img.shields.io/badge/python-3.10-blue)
![Python package](https://github.com/The-Academic-Observatory/oaebu-workflows/workflows/Unit%20Tests/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/oaebu-workflows/badge/?version=latest)](https://oaebu-workflows.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/The-Academic-Observatory/oaebu-workflows/branch/develop/graph/badge.svg?token=YEB00O777L)](https://codecov.io/gh/The-Academic-Observatory/oaebu-workflows)
[![DOI](https://zenodo.org/badge/401298548.svg)](https://zenodo.org/badge/latestdoi/401298548)

## Telescope Workflows
A telescope a type of workflow used to ingest data from different data sources, and to run workflows that process and
output data to other places. Workflows are built on top of Apache Airflow's DAGs.

The workflows include: Google Analytics, Google Books, JSTOR, IRUS Fulcrum, IRUS OAPEN,
Onix, UCL Discovery and an Onix Workflow for combining all of this data.

| Telescope Workflow  | Description |
| ------------- | ------------- |
| <img src="docs/logos/crossref-events.svg" alt="Crossref Events" width="150" /> | Crossref Event Data captures discussion on scholarly content and acts as a hub for the storage and distribution of this data. An event may be a citation in a dataset or patent, a mention in a news article, Wikipedia page or on a blog, or discussion and comment on social media.  |
| <img src="docs/logos/crossref-metadata.svg" alt="Crossref Metadata" width="150" />  | Crossref is a non-for-profit membership organisation working on making scholarly communications better. It is an official Digital Object Identifier (DOI) Registration Agency of the International DOI Foundation. They provide metadata for every DOI that is registered with Crossref.  |
| <img src="docs/logos/google_analytics.svg" alt="Google Analytics" width="150" /> | Google Analytics is a web-based service that allows groups to track usage of their web properties. It offers vistor counts, statistics, and other breakdowns such as country or origin for visitors. If publishers or partners already have Google Analytics already setup of their website, this usage data is able to be ingested  |
| <img src="docs/logos/google_books.svg" alt="Google Books" width="150" /> | The Google Books Partner program enables selling books through the Google Play store and offering a preview on Google books. As a publisher it is possible to download reports on Google Books data, currently there are 3 report types available (sales summary, sales transaction and traffic) of which we use the latter 2  |
| <img src="docs/logos/fulcrum.png" alt="IRUS Fulcrum" width="150" /> | IRUS provides COUNTER standard access reports for books hosted on the Fulcrum platform. The reports show access figures for each month and the country of usage |
| <img src="docs/logos/irus.png" alt="IRUS OAPEN" width="150" /> | IRUS provides COUNTER standard access reports for books hosted on the OAPEN platform. Almost all books on OAPEN are provided as a whole book PDF file. The reports show access figures for each month as well as the location of the access. Since the location info includes an IP-address, the original data is handled only from within the OAPEN Google Cloud project |
| <img src="docs/logos/jstor.svg" alt="JSTOR" width="150" /> | JSTOR provides publisher usage reports, the reports offer details about the use of journal or book content by institution, and country. Journal reports also include usage by issue and article.  Usage is aligned with the COUNTER 5 standard of Item Requests (views + downloads) | 
| <img src="docs/logos/oapen.png" alt="OAPEN Metadata" width="150" /> | The OAPEN Library hosts more than 18,000 Open Access books. OAPEN enables libraries and aggregators to use the metadata of all available titles in the OAPEN Library, made available under a CC0 1.0 license. The metadata is available in different formats and the OAPEN metadata telescope harvests the data in XML format |
| <img src="docs/logos/onix.svg" alt="Onix" width="150" /> | ONIX is a standard format that book publishers use to share information about the books that they have published. Publishers that have ONIX feeds are given credentials and access to their own upload folder on the Mellon SFTP server. The publisher uploads their ONIX feed to their upload folder on a weekly, fortnightly or monthly basis. The ONIX telescope downloads, transforms (with the ONIX parser Java command line tool) and then loads the ONIX data into BigQuery for further processing |
| <img src="docs/logos/thoth.png" alt="Thoth" width="150" /> | Thoth is a free, open metadata service that publishers can choose to utilise as a solution for metadata storage. Thoth can provide metadata upon request in a number of formats. The Thoth telescope uses the Thoth Export API to download metadata in an ONIX format. |
| <img src="docs/logos/ucl.svg" alt="UCL Discovery" width="150" /> | UCL Discovery is UCL's open access repository, showcasing and providing access to the full texts of UCL research publications.The metadata for all eprints is obtained from their publicly available CSV file (https://discovery.ucl.ac.uk/cgi/search/advanced)  |


## Documentation
For detailed documentation about the Book Usage Data Workflows see the Read the Docs website  [https://oaebu-workflows.readthedocs.io](https://oaebu-workflows.readthedocs.io)

## Other requirements to create the Book Usage Datasets
The Observatory Platform, an environment for fetching, processing and analysing data, see the Repository  [https://github.com/The-Academic-Observatory/observatory-platform](https://github.com/The-Academic-Observatory/observatory-platform)

The Academic Observatory Workflows, which provides Apache Airflow workflows for fetching, processing and analysing data about academic institutions, see the Repository  [https://github.com/The-Academic-Observatory/academic-observatory-workflows](https://github.com/The-Academic-Observatory/academic-observatory-workflows)

The Onix Parser, a command line tool that transforms ONIX files into a format suitable for loading into BigQuery, see the Repository  [https://github.com/The-Academic-Observatory/onix-parser](https://github.com/The-Academic-Observatory/onix-parser)

