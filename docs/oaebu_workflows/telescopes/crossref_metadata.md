# Crossref metadata

Crossref is a non-for-profit membership organisation working on making scholarly communications better. 
It is an official Digital Object Identifier (DOI) Registration Agency of the International DOI Foundation. 
They provide metadata for every DOI that is registered with Crossref.

Crossref Members send Crossref scholarly metadata on research which is collated and 
standardised into the Crossref metadata dataset. This dataset is made available through 
services and tools for manuscript tracking, searching, bibliographic management, 
library systems, author profiling, specialist subject databases, scholarly sharing networks
. _- source: [Crossref Metadata](https://www.crossref.org/services/metadata-retrieval/)_ 
and [schema details](https://github.com/Crossref/rest-api-doc/blob/master/api_format.md).

The BigQuery table created by the Crossref Metadata telescope from the [Academic Observatory workflows](https://academic-observatory-workflows.readthedocs.io/en/latest/telescopes/crossref_metadata.html) is queried with the list of ISBNs from a publisher's Onix feed to create a filtered table in BigQuery called `crossref.crossref_metadataYYYYMMDD`. This table is created during the [Onix workflow](../workflows/onix_workflow_step_1.md).


## Latest schema

``` eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/crossref_metadata_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```