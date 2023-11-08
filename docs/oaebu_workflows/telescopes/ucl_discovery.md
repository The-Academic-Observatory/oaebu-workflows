# UCL Discovery

UCL Discovery is UCL's open access repository, showcasing and providing access to the full texts of UCL research publications.

## The Google Sheet
UCL's titles are referenced via their identifier - the eprint ID. Their metadata maps the eprint ID to an ISBN13, but not consistently. For this reason, we forgo the use of their metadata and instead employ a semi-manual process to reliably map the two identifiers. 
The telescope references a Google sheet that contains all of the titles available in the UCL Discovery repository under the following headings: 

| Heading             | Description                      |
| ------------------- | -------------------------------- |
| ISBN13              | The title's ISBN13               |
| date                | The date of publication          |
| title_list_title    | The title of the publication     |
| discovery_eprint_id | The eprint ID of the publication |

Some notes :
- These headings are hardcoded into the telescope. Any change in the sheet will break the telescope without prior intervention.
- Entries without a publication date or with a publication date in the future (where the current time is determined by the airflow scheduler) will be ignored.
- Entries missing either an ISBN13 or eprint ID will be ignored.

For the aforementioned reasons, it is important that **the google sheet remain up to date**. Otherwise, the usage for a title may be missed and require a rerun.

### Access
Access to the sheet can be granted using the sheet UI (*Share* at the top right of the page). The telescope will access the sheet via a service account, which will need to be given read access (*Viewer*) by supplying the account's email address.

## Usage API
UCL Discovery provides free and open access to their usage REST API. Unfortunately, I can't find any documentation on its use and design. We utilise two endpoints:
- **Countries URI** = https://discovery.ucl.ac.uk/cgi/stats/get?from=[YYYYMMDD]&to=[YYYYMMDD]&irs2report=eprint&set_name=eprint&set_value=[EPRINT_ID]&datatype=countries&top=countries&view=Table&limit=all&export=JSON
- **Totals URI** = https://discovery.ucl.ac.uk/cgi/stats/get?from=[YYYYMMDD]&to=[YYYYMMDD]&irs2report=eprint&set_name=eprint&set_value=[EPRINT_ID]&datatype=downloads&graph_type=column&view=Google%3A%3AGraph&date_resolution=month&title=Download+activity+-+last+12+months&export=JSON

Where *from*, *to* and *set_value* are appropriately set.
The countries URI returns statistics pertaining to the number of downloads of the provided eprint ID broken down by country.
The totals URI returns statistics pertaining to the number of downloads of the provided eprint ID aggregated over all regions.
It should be noted that the *totals* data is not necessarily a simply aggregation of the *countries* data. This is because country data is omitted for downloads that are not attributed to a region. It is therefore not uncommon to have a total download count (derived from the totals URI) that is greater than the sum of all downloads from all listed countries (from the countries URI).

## Telescope Workflow
The telescope's workflow can be broken down as such:

### Download
Acquires the eprint IDs and publication dates from [the Google Sheet](#the-google-sheet). For each ID that has a publication date that is before the current scheduled run date, download the country and totals data. Then upload to GCS download bucket.

### Transform
Acquires the eprint IDs, ISBN13s and titles from [the Google Sheet](#the-google-sheet). For each ID, load the downloaded data (both coutried and totals) into a single data structure and include the title (whether it is empty or not does not matter - the title exists for completeness only). Add an additional field to each row - the *release_date* which is determined by the scheduled runtime. Upload this transformed structure to GCS transform bucket.

### BQ Load
Load the table into BigQuery and partition on the *release_date*.


### Run Summary

The corresponding table in BigQuery is `ucl.ucl_discoveryYYYYMMDD`. 

```eval_rst
+------------------------------+---------+
| Summary                      |         |
+==============================+=========+
| Average runtime              | 2 min   |
+------------------------------+---------+
| Average download size        | 1.5 MB  |
+------------------------------+---------+
| Harvest Type                 |   API   |
+------------------------------+---------+
| Harvest Frequency            | Monthly |
+------------------------------+---------+
| Runs on remote worker        | False   |
+------------------------------+---------+
| Catchup missed runs          | True    |
+------------------------------+---------+
| Table Write Disposition      | Append  |
+------------------------------+---------+
| Update Frequency             | Daily   |
+------------------------------+---------+
| Credentials Required         | No      |
+------------------------------+---------+
| Each shard includes all data | No      |
+------------------------------+---------+
```

## Latest schema
``` eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/ucl_discovery_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```