# OAPEN Metadata

The OAPEN Metadata telescope collects data from the OAPEN Metadata feed.
OAPEN enables libraries and aggregators to use the metadata of all available titles in the OAPEN Library.  
The metadata is available in different formats and this telescope harvests the data in the XML format.  
See the [OAPEN Metadata webpage](https://www.oapen.org/resources/15635975-metadata) for more information.

```eval_rst
+------------------------------+------------+
| Summary                      |            |
+==============================+============+
| Average runtime              |  5min      |
+------------------------------+------------+
| Average download size        |  150-200MB |
+------------------------------+------------+
| Harvest Type                 |  URL       |
+------------------------------+------------+
| Harvest Frequency            | Weekly     |
+------------------------------+------------+
| Runs on remote worker        | True       |
+------------------------------+------------+
| Catchup missed runs          | False      |
+------------------------------+------------+
| Table Write Disposition      | Append     |
+------------------------------+------------+
| Update Frequency             | Daily      |
+------------------------------+------------+
| Credentials Required         | No         |
+------------------------------+------------+
| Uses Telescope Template      | Stream     |
+------------------------------+------------+
```

## Configuration

### Airflow Connections

The OAPEN metadata is freely accessible, so no credentials are required for it. However, the telescope will upload the XML to the SFTP server once finished processing.

The (url-encoded) ssh username, password and host key to connect to the SFTP server:

```bash
sftp_service: ssh://user-name:password@host-name:port?host_key=host-key
```

## Schedule

The XML file containing metadata is updated daily at +0000GMT. This telescope is scheduled to harvest the metadata weekly.

## Results

There are no resulting tables from this telescope. Its purpose is to transfrom the OAPEN Metadata into a valid ONIX file. This can then be picked up and loaded by the [ONIX Telescope](onix.md).

## Tasks

### Download

This is where the metadata is downloaded. The XML file containing metadata is downloaded using the XML URL that is
available on the OAPEN Metadata webpage mentioned above.

Note that if the metadata file is part-way through an update (occurring daily at +0000GMT and taking upwards of one hour), the XML file will be incomplete and invalid. The telescope has a failesafe to attempt to resolve this during runtime, which can lead to much longer than normal 'download' times.

### Transform

The transform step modifies the downloaded metadata into a valid ONIX format. This is done in two steps:

1. The XML is loaded and all unnecessary fields are removed. The fields deemed necessary are described by the header and product schema files below
2. The resulting XML is parsed through the Python [onixcheck](https://pypi.org/project/onixcheck/). This reveals any remaining invalid products. These products are removed from the file. The removed products are saved to a separate file and uploaded to the transform bucket for storage.

```eval_rst
.. csv-table::
   :file: ../../schemas/OAPEN_ONIX_header_fields_latest.csv
   :width: 100%
   :header-rows: 1
```

```eval_rst
.. csv-table::
   :file: ../../schemas/OAPEN_ONIX_product_fields_latest.csv
   :width: 100%
   :header-rows: 1
```

### Upload to SFTP Server

After checking that the metadata is now in a valid ONIX format, the ONIX XML is uploaded to the SFTP server. This can then be picked up by the [ONIX telescope](onix.md) and integrated into the proceeding workflows as normal.
