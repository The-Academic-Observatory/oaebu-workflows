ONIX workflow Step 1 - Mapping Book Products
=============================================

The ONIX workflow uses the ONIX table created by the ONIX telescope to do the following:
  1. Aggregate book product records into works records. Works are equivalence classes of products, where each product in the class is a manifestation of each other. For example, a PDF and a paperback of the same work.
  2. Aggregate work records into work family records. A work family is an equivalence class of works where each work in the class is just a different edition.
  3. Produce intermediate lookup tables mapping ISBN13 -> WorkID and ISBN13 -> WorkFamilyID.
  4. Produce intermediate tables that append work_id and work_family_id columns to different data tables with ISBN keys.
  
[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/workflows/onix_workflow.py)

## Definitions - Product, Work and Work Families

A **Product**: A product is a manifestation of a work, and will have its own ISBN. There may be several DOIs linked to a single product though (or sometimes none at all).

A **Work**: Can be a collection of products, which are each different manifestation of the same work. Some datasets have unique IDs assigned to the concept of a work, but these are not as clear as the usage of ISBN for a product.

An **Edition**: Is a new Work, but is derived as a revision from an existing work as opposed to being entirely new.

A **Work Family** is a collection of works which are different editions of each other.

## Dependencies
The ONIX workflow is dependent on the ONIX telescope.  It waits for the ONIX telescope to finish before it starts executing.  This requires an ONIX telescope to be present and scheduled.

[Link to Code](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/workflows/onix_work_aggregation.py)

## Work ID
The Work ID will be an arbitrary ISBN representative from a product in the equivalence class.

``` eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/onix_workid_isbn_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

## Work Family ID
The Work Family ID will be an arbitrary Work ID (ISBN) representative from a work in the equivalence class.

``` eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/onix_workfamilyid_isbn_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

## ONIX Work ID ISBN Errors

``` eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/onix_workid_isbn_errors_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```
## Create Crossref metadata table
Crossref Metadata is required to proceed. The ISBNs for each work is obtained from the publisher's Onix table. For each of these ISBNs, the Crossref Metadata table produced by the [Academic Observatory workflows](https://github.com/The-Academic-Observatory/academic-observatory-workflows/tree/develop) is queried. Refer to the [Crossref Metadata telescope](../telescopes/crossref_metadata.md).  

## Create Crossref events table
Similarly to Crossref Metadata, Crossref Event Data is retrieved through Crossref's dedicated [event REST API](https://www.eventdata.crossref.org/guide/service/query-api/) through the [Crossref Event Data telescope](../telescopes/crossref_events.md). The API accepts queries based on DOI only, which we retrieve by matching the appropriate ISBN13 from the metadata.  

## Create book table
The book table is a collection of works and their relevant details for the relative publisher. The table accommodates a title's Crossref metadata, events and separate chapters.

## Create intermediate tables
For each data partner's tables containing ISBN, create new "matched" tables which extend the original data with new "work_id" and "work_family_id" columns.

The schemas for these tables are identical to the raw Telescope's schemas, with the addition of work_ids and work_family_ids.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/assign_workid_workfamilyid.sql.jinja2)

``` eval_rst
.. image:: ../static/onix_workflow_1.png
   :width: 650
```

## Create QA tables
For each data source, including the intermediate tables, we perform basic quality assurance checks on the data, and output the results to tables that are easy to export for analysis by the publisher (e.g. to CSV). For example we verify if the provided ISBNs are valid, or if there are unmatched ISBNs indicating that there are missing ONIX product records.


### ONIX Aggregate Metrics

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/onix_aggregate_metrics.sql.jinja2)

``` eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/onix_aggregate_metrics_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```
### ONIX Invalid ISBN

Details ISBN13s in the ONIX feed that are not valid.

``` eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/onix_invalid_isbn_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Data Platform Invalid ISBN

Details ISBN13s in the data source that are not valid. An example schema is below, as data platforms may use different name fields (e.g, 'ISBN', 'publication_id', 'Primary_ISBN').

``` eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/platform_invalid_isbn_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Data Platform Unmatched ISBN

Details ISBN-13s in the data source that were not matched to ISBN-13s in the ONIX feed.

``` eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/platform_unmatched_isbn_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```
