# IRUS Fulcrum

The IRUS Fulcrum telescope collects usage statistics for titles accessed via the [Fulcrum Platform](https://www.fulcrum.org/). Usage data is accessible through [IRUS](https://irus.jisc.ac.uk/r5/) in much the same way as the [IRUS OAPEN](./irus_oapen.md) telescope. Unlike IRUS OAPEN, IRUS Fulcrum does not record sensitive IP address information. This makes dealing with the data much simpler.

The earliest available data for the Fulcrum platform is April 2022. It follows that all data is of [COUNTER](https://www.projectcounter.org/) 5 standard.

The corresponding table created in BigQuery is `irus.irus_fulcrumYYYYMMDD`.

```eval_rst
+------------------------------+--------------+
| Summary                      |              |
+==============================+==============+
| Average runtime              | 5-10 mins    |
+------------------------------+--------------+
| Average download size        | 1-10 MB      |
+------------------------------+--------------+
| Harvest Type                 | API          |
+------------------------------+--------------+
| Harvest Frequency            | Monthly      |
+------------------------------+--------------+
| Runs on remote worker        | False        |
+------------------------------+--------------+
| Catchup missed runs          | True         |
+------------------------------+--------------+
| Credentials Required         | Yes          |
+------------------------------+--------------+
| Uses Telescope Template      | None         |
+------------------------------+--------------+
| Each shard includes all data | No           |
+------------------------------+--------------+
```

## Airflow connections

Note that all values need to be urlencoded.
In the config.yaml file, the following airflow connections are required:

### Irus_api

The IRUS requestor_id/api_key is required to access the IRUS platform.

## Data Download

The download is done via an API call to IRUS:

```
https://irus.jisc.ac.uk/api/v3/irus/reports/irus_ir/?platform=235&requestor_id={requestor_id}&begin_date={start_date}&end_date={end_date}
```

Where the requestor ID is the API key for the IRUS API. The telescope will use the same begin and end dates (YYYY-MM) in order to retrieve data on a per-month basis.

A second call to the API is made with the following appended to the above URL:

```
&attributes_to_show=Country
```

Which splits the data by country, leaving us with two datasets. These datasets will be referred to as the _total_ and _country_ datasets.

Before making any changes to the data, these datasets are uploaded to a Google storage bucket

## Data Transform

The transform step has a few things to achieve:

-   Collate the _total_ and _country_ datasets into a single object
-   Remove columns that are not of interest to us
-   Add the release month to each row as a partitioning column
-   Remove rows from the data that do not relate to the publisher of interest

The result of points 1 -> 3 are evident in the [schema](#latest-schema). The final point requires some communication with the publisher. This is because a single publisher may have published titles under more than one name. For example, University of Michigan has 10 associated publishing names. These names are listed as part of a dictionary in the telescope.

The resulting transformed file is uploaded to a Google Cloud bucket

## BigQuery Load

The transformed data is loaded from the Google Cloud bucket into a partitioned BigQuery table. The table is in the respective publisher's Project and a _fulcrum_ dataset will be created if it does not exist. Since the data is partitioned on the release month, there will only be a single table.

## Latest schema

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/irus_fulcrum_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```