# Thoth

The Thoth Telescope downloads, transforms and loads publisher ONIX feeds from [Thoth](https://thoth.pub/) into BigQuery. [ONIX](https://www.editeur.org/83/Overview/) is a standard format that book publishers use to share information about the books that they have published.

Thoth is a free, open metadata service that publishers can choose to utilise as a solution for metadata storage. Thoth can provide metadata upon request in a number of formats. The Thoth Telescope used the [Thoth Export API](https://export.thoth.pub/#get-/formats/-format_id-) to download metadata in an ONIX format. This API provides a snapshot of a specified publisher's metadata at the time of request. It requires the publisher's ID as part of the URL, which can be found using the [GraphiQL API](https://api.thoth.pub/graphiql).

The Thoth telescope downloads the ONIX metadata files and then transforms the data into a format suitable for loading into BigQuery with the [ONIX parser](https://github.com/The-Academic-Observatory/onix-parser) Java command line tool. This is a near-identical process to how the [ONIX telescope's](onix.md) data-transformation step is executed. The transformed data is loaded into BigQuery, where it can be picked up and used by the [ONIX Workflow](../workflows/onix_workflow_intro.md).

The corresponding table in BigQuery is `onix.onixYYYYMMDD`. 

```eval_rst
+------------------------------+--------------+
| Summary                      |              |
+==============================+==============+
| Average runtime              | 5-10 mins    |
+------------------------------+--------------+
| Average download size        | 1-10 MB      |
+------------------------------+--------------+
| Harvest Type                 | URL          |
+------------------------------+--------------+
| Harvest Frequency            | Weekly       |
+------------------------------+--------------+
| Runs on remote worker        | False        |
+------------------------------+--------------+
| Catchup missed runs          | False        |
+------------------------------+--------------+
| Credentials Required         | No           |
+------------------------------+--------------+
| Uses Telescope Template      | None         |
+------------------------------+--------------+
| Each shard includes all data | Yes          |
+------------------------------+--------------+
```

## Configuration

The following settings need to be configured for the Thoth telescope.

### Telescope API Instance

A Thoth Telescope API instance needs to be created. Unlike the ONIX telescope, it does not require any 'extra' fields.

### Airflow Connections

The Thoth telescope does not require any airflow connections to run, as the Thoth API is freely usable.

## Latest schema

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/onix_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable 
```
