# ONIX
The ONIX telescope downloads, transforms and loads publisher ONIX feeds into BigQuery. [ONIX](https://www.editeur.org/83/Overview/)
is a standard format that book publishers use to share information about the books that they have published.

Book publishers with ONIX feeds are given credentials and access to their own upload folder on the OAeBU SFTP server.
They then configure [ONIX Suite](http://www.onixsuite.com/) to upload their ONIX feeds to the SFTP server on a weekly
basis. The ONIX feeds need to be full dumps every time, not incremental updates.

The ONIX telescope downloads the ONIX files from the SFTP server. It then transforms the data into a format suitable
for loading into BigQuery with the [ONIX parser](https://github.com/The-Academic-Observatory/onix-parser) Java command 
line tool. The data is loaded into BigQuery and then used by the ONIX Workflow.

```eval_rst
+------------------------------+--------------+
| Summary                      |              |
+==============================+==============+
| Average runtime              | 10-20 mins   |
+------------------------------+--------------+
| Average download size        | 10-100 MB    |
+------------------------------+--------------+
| Harvest Type                 | SFTP server  |
+------------------------------+--------------+
| Harvest Frequency            | Weekly       |
+------------------------------+--------------+
| Runs on remote worker        | False        |
+------------------------------+--------------+
| Catchup missed runs          | False        |
+------------------------------+--------------+
| Credentials Required         | Yes          |
+------------------------------+--------------+
| Uses Telescope Template      | Snapshot     |
+------------------------------+--------------+
| Each shard includes all data | Yes          |
+------------------------------+--------------+
```
## Configuration
The following settings need to be configured for the ONIX telescope.

### Telescope API instance
An ONIX telescope API instance needs to be created, making sure to add the below settings to the extra field.

#### Telescope API 'extra' field
The `date_regex` field must be added to the telescope extra field, it is used to extract the date from the ONIX
feed file name. For example, the regex `\\d{8}` will extract the date from the file name `20220301_CURTINPRESS_ONIX.xml`.
```json
"extra": {
  "date_regex": "\\d{8}"
}
```

### Airflow connections
In the config.yaml file, the below Airflow connection is required. Note that all values need to be urlencoded. 

#### sftp_service
The ssh username, password and host key to connect to the SFTP server.
```bash
sftp_service: ssh://user-name:password@host-name:port?host_key=host-key
```

## Latest schema
``` eval_rst
.. csv-table::
   :file: ../schemas/onix_latest.csv
   :width: 100%
   :header-rows: 1
```