# Google Analytics Universal

Google Analytics was a web analytics service offered by Google that tracks and reports website traffic (now replaced with Google Analytics 4).  
This telescope gets data from Google Analytics for 1 view id per publisher and for several combinations of metrics and dimensions.  
It is possible to add a regex expression to filter on pagepaths, so only data on relevant pagepaths is collected.  
Note that Google Analytics data is only available for the last 26 months, see 
[Data retention - Analytics Help](https://support.google.com/analytics/answer/7667196?hl=en) for more info.

To get access to the analytics data a publisher needs to add the relevant google service account as a user.

The corresponding table created in BigQuery is `google.google_analyticsYYYYMMDD`.

```eval_rst
+------------------------------+---------+
| Summary                      |         |
+==============================+=========+
| Average runtime              | 5 min   |
+------------------------------+---------+
| Average download size        | 1 MB    |
+------------------------------+---------+
| Harvest Type                 | API     |
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
## Custom dimensions for ANU Press
ANU Press is using custom dimensions in their google analytics data. To ensure that the telescope processes these
 custom dimensions, the organisation name needs to be set to exactly 'ANU Press'.  
The organisation name is used directly inside the telescope and if it matches 'ANU Press' additional dimensions will
 be added and a different BigQuery schema is used.  

## A note on the API metrics
We use the python client for the The Google Analytics API in order to retrieve the data on several metrics (such as page views) per country. It appears as though the API does not return a result for every country. We would have expected any data without a country field to be labelled with a country name of **not set**, however this does not appear to be the case.  
At this time, we have no other way of retrieving country-level data on the desired metrics, so we must acknowledge that the numbers returned by the API are slightly different to those found on the Google Analytics web page.  
A [ticket](https://issuetracker.google.com/issues/237421688) has been created with google in the hope of resolving this issue.

## Telescope object 'extra'
This telescope is created using the Observatory API. There are two 'extra' fields that are required for the
 corresponding Telescope object.  
These are the 'view_id' and the 'pagepath_regex'.   

### view_id
The view_id points to the specific view on which Google Analytics data is collected.  
See [the google support page](https://support.google.com/analytics/answer/1009618?hl=en) for more information on the hierarchy of the Analytics account.
Below is more information on how to list the view_ids which a service account has access to.

### pagepath_regex
This is a regular expression that is used to filter on pagepaths for which analytics data is collected.  
The regular expression can be set to an empty string if no filtering is required.  
Note that the Google Analytics API uses 're2', so it is not possible to use e.g. negative lookaheads.  
See [the google support page](https://support.google.com/analytics/answer/1034324?hl=en) and [github wiki](https://github.com/google/re2/wiki/Syntax) for more information.  

## Setting up service account
* Create a service account from IAM & Admin - Service Accounts
* Create a JSON key and download the file with key
* For each organisation/publisher of interest, ask them to add this service account as a user for the correct view id

## Getting the view ID (after given access)
```python
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

scopes = ['https://www.googleapis.com/auth/analytics.readonly']
credentials_path = '/path/to/service_account_credentials.json'

creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scopes=scopes)

# Build the service object.
service = build('analytics', 'v3', credentials=creds)

account_summaries = service.management().accountSummaries().list().execute()
view_ids = []
for account in account_summaries['items']:
    account_name = account['name']
    profiles = account['webProperties'][0]['profiles']
    website_url = account['webProperties'][0]['websiteUrl']
    for profile in profiles:
        view_id_info = {'account': account_name, 'websiteUrl': website_url, 'view_id': profile['id'], 
                        'view_name': profile['name']}
        view_ids.append(view_id_info)
```

## Airflow connections
Note that all values need to be urlencoded.
In the config.yaml file, the following airflow connections are required:  

### oaebu_service_account
After creating the JSON key file as described above, open the JSON file and use the information to create the connection.  
URL encode each of the fields 'private_key_id', 'private_key', 'client_email' and 'client_id'.
```yaml
oaebu_service_account: google-cloud-platform://?type=service_account&private_key_id=<private_key_id>&private_key=<private_key>&client_email=<client_email>&client_id=<client_id>
```

## Latest schema
``` eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/google_analytics_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```