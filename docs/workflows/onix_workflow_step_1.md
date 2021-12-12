# ONIX workflow Step 1 - Mapping Book Products

The ONIX workflow uses the ONIX table created by the ONIX telescope to do the following:
  1. Aggregate book product records into works records. Works are equivalence classes of products, where each product in the class is a manifestation of each other. For example, a PDF and a paperback of the same work.
  2. Aggregate work records into work family records. A work family is an equivalence class of works where each work in the class is just a different edition.
  3. Produce intermediate lookup tables mapping ISBN13 -> WorkID and ISBN13 -> WorkFamilyID.
  4. Produce oaebu_intermediate tables that append work_id and work_family_id columns to different data tables with ISBN keys.

## Definitions - Product, Work and Work Families

A **Product**: A product is a manifestation of a work, and will have its own ISBN. There may be several DOIs linked to a single product though (or sometimes none at all).

A **Work**: Can be a collection of products, which are each different manifestation of the same work. Some datasets have unique IDs assigned to the concept of a work, but these are not as clear as the usage of ISBN for a product.

An **Edition**: Is a new Work, but is derived as a revision from an existing work as opposed to being entirely new.

A **Work Family** is a collection of works which are different editions of each other.

## Dependencies
The ONIX workflow is dependent on the ONIX telescope.  It waits for the ONIX telescope to finish before it starts executing.  This requires an ONIX telescope to be present and scheduled.

## Work ID
The Work ID will be an arbitrary ISBN representative from a product in the equivalence class.

``` eval_rst
.. csv-table::
   :file: ../schemas/onix_workid_isbn_latest.csv
   :width: 100%
   :header-rows: 1
```

## Work Family ID
The Work Family ID will be an arbitrary Work ID (ISBN) representative from a work in the equivalence class.

``` eval_rst
.. csv-table::
   :file: ../schemas/onix_workfamilyid_isbn_latest.csv
   :width: 100%
   :header-rows: 1
```

## Create OAEBU intermediate tables
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
.. csv-table::
   :file: ../schemas/onix_aggregate_metrics_latest.csv
   :width: 100%
   :header-rows: 1
```

### ONIX Work ID ISBN Errors

``` eval_rst
.. csv-table::
   :file: ../schemas/onix_workid_isbn_errors_latest.csv
   :width: 100%
   :header-rows: 1
```