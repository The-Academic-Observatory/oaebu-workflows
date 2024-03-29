# IRUS OAPEN

IRUS provides OAPEN COUNTER standard access reports. Almost all books on OAPEN are provided as a whole book PDF file. The reports show access figures for each month as well as the location of the access. 

Since the location info includes an IP-address, the original data is handled only from within the OAPEN Google Cloud project.

Using a Cloud Function, the original data is downloaded and IP-addresses are replaced with geographical information, such as city and country.  
After this transformation, the data without IP-addresses is uploaded to a Google Cloud Storage Bucket.

This is all done from within the OAPEN Google Cloud project.
The Cloud Function is created and called from the telescope, when the Cloud Function has finished the data is copied from the Storage Bucket inside the OAPEN project, to a Bucket inside the main airflow project.

The corresponding table created in BigQuery is `irus.irus_oapenYYYYMMDD`.

```eval_rst
+------------------------------+---------+
| Summary                      |         |
+==============================+=========+
| Average runtime              | 5 min   |
+------------------------------+---------+
| Average download size        | 5 MB    |
+------------------------------+---------+
| Harvest Type                 |  API    |
+------------------------------+---------+
| Harvest Frequency            | Monthly |
+------------------------------+---------+
| Runs on remote worker        | False   |
+------------------------------+---------+
| Catchup missed runs          | True    |
+------------------------------+---------+
| Table Write Disposition      | Truncate|
+------------------------------+---------+
| Update Frequency             | Daily   |
+------------------------------+---------+
| Credentials Required         | Yes     |
+------------------------------+---------+
| Uses Telescope Template      | Snapshot|
+------------------------------+---------+
| Each shard includes all data | No      |
+------------------------------+---------+
```

## Telescope object 'extra'

This telescope is created using the Observatory API. There are two 'extra' fields that are required for the
corresponding Telescope object, namely the 'publisher_name_v4' and 'publisher_uuid_v5'.  
A mapping is required between the OAPEN publisher name and the organisation name obtained from the observatory API.
The OAPEN publisher name is used directly for the older counter 4 platform, for the newer counter 5 platform the
publisher UUID is used.

### publisher_name_v4

The publisher_name_v4 can be found by going to the OAPEN [page to manually create reports](https://irus.jisc.ac.uk/IRUSConsult/irus-oapen/v2/br1b/).
On this page there is a drop down list with publisher names, to get the publisher name simply url encode the publisher
name from this list.

Note that occasionally there are multiple publisher names for one publisher.  
For example to get all data from Edinburgh University Press, you need data from both publishers
`Edinburgh University Press` and `Edinburgh University Press,`.
Multiple publisher names can be passed on by delimiting them with a '|' character.

### publisher_uuid_v5

The publisher_uuid_v5 can be found by querying the OAPEN API and creating a list of unique Publisher names and UUIDs.

This API request will return all items including their Publisher name and UUID:
https://irus.jisc.ac.uk/api/oapen/reports/oapen_ir/?platform=215&requestor_id=<requestor_id>&api_key=<api_key>&granularity=totals&begin_date=2020-04&end_date=2021-11

To get a file with mappings between Publisher Name and UUID, use the following Python snippet:

```python
import requests
import pandas as pd

# Set up your credentials, the start & end date and path to output file
requestor_id = "YOUR_REQUESTOR_ID"
api_key = "YOUR_API_KEY"
start_date = "2020-04"
end_date = "2021-11"
out_file = "/path/to/output_mapping.csv"

# Query the OAPEN API
url = f"https://irus.jisc.ac.uk/api/oapen/reports/oapen_ir/?platform=215&requestor_id={requestor_id}&api_key={api_key}&granularity=totals&begin_date={start_date}&end_date={end_date}"
response = requests.get(url)
response_json = response.json()

# Store result in dataframe, get unique publisher values and sort
df = pd.DataFrame(response_json['Report_Items'])
result = df.drop_duplicates(["Publisher", "Publisher_ID"])[["Publisher", "Publisher_ID"]].sort_values(["Publisher", "Publisher_ID"])

# Save result to csv file
result.to_csv(out_file, index=False)
```

From this file look up the publisher UUIDs of interest.
Similar to the publisher names described above, multiple publisher UUIDs can be passed on by delimiting them with a
'|' character.

## Cloud Function
The IRUS OAPEN telescope makes use of a Google Cloud Function that resides in the OAPEN Google project. 

There is a specific airflow task that will create the Cloud Function if it does not exist yet, or update it if the source code has changed.  

The source code for the Cloud Function can be found inside a separate repository that is part of the same organization (https://github.com/The-Academic-Observatory/oapen-irus-uk-cloud-function).

### Download access stats data
The Cloud Function downloads IRUS OAPEN access stats data for 1 month and for a single publisher. Usage data after April 2020 is hosted on a new platform.  

The newer data is obtained by using their API, this requires a `requestor_id` and an `api_key`.  
Data before April 2020 is obtained from an URL, this requires an `email` and a `password`.

The required values for either the newer or older way of downloading data are passed on as a `username` and `password` to the Cloud Function.
The `username` and `password` are obtained from an airflow connection, which should be set in the config file (see below).

### Replace IP addresses

Once the data is downloaded, the IP addresses are replaced with geographical information (corresponding city and country).  
This is done using the GeoIp database, which is downloaded from inside the Cloud Function. The license key for this database is passed on as a parameter as well, `geoip_license_key`.  
The `geoip_license_key` is also obtained from an airflow connection, which should be set in the config file (see below).

### Upload data to storage bucket

Next, the data without the IP addresses is upload to a bucket inside the OAPEN project. All files in this bucket are deleted after 1 day.
In the next airflow task, the data can then be copied from this bucket to the appropriate bucket in the project where airflow is hosted.

## Set-up OAPEN Google Cloud project

To make use of the Cloud Function described above it is required to enable two APIs and set up permissions for the Google service account that airflow is using.

See the [Google support answer](https://support.google.com/googleapi/answer/6158841?hl=en) for info on how to enable an API. The API's that need to be enabled are:

-   Cloud Functions API
-   Cloud build API
-   Cloud Run Admin API
-   Artifact Registry API

Inside the OAPEN Google project, add the airflow Google service account (<airflow_project_id>@<airflow_project_id>.iam.gserviceaccount.com, where airflow_project_id is the project where airflow is hosted).
This can be done from the 'IAM & Admin' menu and 'IAM' tab. Then, assign the following permissions to this account:

-   Cloud Functions Developer (to create or update the Cloud Function)
-   Cloud Functions Invoker (to call/invoke the Cloud Function)
-   Storage Admin (to create a bucket)
-   Storage Object Admin (to list and get a blob from the storage bucket)

Additionally, it is required to assign the role of service account user to the service account of the Cloud Function, with the airflow service account as a member.
The Cloud SDK command for this is:  
`gcloud iam service-accounts add-iam-policy-binding <OAPEN_project_id>-compute@developer.gserviceaccount.com 
--member=<airflow_project_id@airflow_project_id.iam.gserviceaccount.com> --role=roles/iam.serviceAccountUser`

Alternatively, it can be done with the Google Cloud console, from the 'IAM & Admin' menu and 'Service Accounts' tab.  
Click on the service account of the Cloud Function: `<OAPEN_project_id>-compute@developer.gserviceaccount.com`.  
In the 'permissions' tab, click 'Grant Access', add the airflow service account as a member `<airflow_project_id@airflow_project_id.iam.gserviceaccount.com>` and assign the role 'Service Account User'.

## Airflow connections

Note that all values need to be urlencoded.
In the config.yaml file, the following airflow connections are required:

### irus_oapen_login
To get the email address/password combination, contact IRUS.

### irus_oapen_api
To get the requestor_id/api_key, contact IRUS.

### geoip_license_key

To get the user*id/license_key, first sign up for geolite2 at https://www.maxmind.com/en/geolite2/signup.  
From your account, in the 'Services' section, click on 'Manage License Keys'. The user_id is displayed on this page.  
Then, click on 'Generate new license key', this can be used for the 'license_key'.  
Answer \_No* for the question: "Old versions of our GeoIP Update program use a different license key format. Will this key be used for GeoIP Update?"

```yaml
irus_oapen_login: mysql://email_address:password@
irus_oapen_api: mysql://requestor_id:api_key@
geoip_license_key: mysql://user_id:license_key@
```

## Latest schema
```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}|
.. csv-table::
   :file: ../../schemas/irus_oapen.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```
