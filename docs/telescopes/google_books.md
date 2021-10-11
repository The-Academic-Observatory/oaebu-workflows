# Google Books
The Google Books Partner program enables selling books through the Google Play store and offering a preview on Google books.  
The program makes books discoverable to Google users around the world on Google books. When readers find a book on Google Books, they can preview a limited number of pages to decide if they're interested in it. 
Readers can also follow links to buy the book or borrow or download it when applicable.

As a publisher you can download reports on Google Books data from https://play.google.com/books/publish/.  

Currently there are 3 report types available:
- Google Play sales summary report
- Google Play sales transaction report
- Google Books Traffic Report

In this telescope we collect data from the last 2 reports.  
The corresponding tables created in BigQuery are `google.google_books_salesYYYYMMDD` and `google.google_books_trafficYYYYMMDD`.

```eval_rst
+------------------------------+---------+
| Summary                      |         |
+==============================+=========+
| Average runtime              | ? min   |
+------------------------------+---------+
| Average download size        |  ? MB   |
+------------------------------+---------+
| Harvest Type                 |  SFTP   |
+------------------------------+---------+
| Harvest Frequency            | Monthly |
+------------------------------+---------+
| Runs on remote worker        | True    |
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
This telescope is created using the Observatory API. There is one 'extra' field that is optional for the
 corresponding Telescope object, namely the 'accounts' field.   

### accounts
This field is only required if a publisher uses more than 1 Google Books account. 
If there are multiple accounts for 1 publisher, the reports of these accounts (for the same report type and month
) are combined in the 'transform' step of the telescope.  
To distinguish the reports of the same type and date, but from different accounts, a file suffix is used. 
When uploading the reports to the SFTP server, this file suffix should be included in the file name. 
There are instructions both on how to download and correctly name the reports manually as well as how to do it semi
-automatically using Selenium.

A list of the file suffixes described above should be passed on to the Telescope 'extra' object.

## Authentication
The reports are downloaded from https://play.google.com/books/publish/. To get access to the reports the publisher needs to give access to a google service account.  
This service account can then be used to login on this webpage and download each report manually.

## Setting up a service account  
* Create a service account from IAM & Admin - Service Accounts  
* Create a JSON key and download the file with key  
* For each organisation/publisher of interest, ask them to add this service account for Google Books  

## Downloading Reports Manually
There is no API available to download the Google Books report and it is quite challenging to automate the Google login process through tools such as Selenium, because of Google's bot detection triggering a reCAPTCHA.  
Until this step can be automated, the reports need to be downloaded manually.  
For each publisher and for both the sales transaction report and the traffic report:   
*  A report should be created for exactly 1 month (e.g. starting 2021-01-01 and ending 2021-01-31). 
*  All titles should be selected.
*  All countries should be selected.
*  The traffic report is organised by 'Book'.
*  It is important to save the file with the right name, this should be in the following format (<file_suffix> is optional):  
    *  `GoogleSalesTransactionReport_<file_suffix>YYYY_MM.csv` or
    *  `GoogleBooksTrafficReport_<file_suffix>YYYY_MM.csv`
*  Upload each report to the SFTP server at https://oaebu.exavault.com/
    *   Add it to the folder `/telescopes/google_books/<publisher>/upload`
    *   Files are automatically moved between folders, please do not move files between folders manually

### Using Selenium to help download reports
When downloading many reports it might be faster to use the script below that helps to download the reports.  
It is required to run the script in debug mode, so a breakpoint can be set at the right spot (marked in the code) and
 you can manually login with your Google account.  
From there on, the reports are automatically downloaded on a monthly basis between the given start and end date, for
 the given publisher account numbers.  
To use Selenium you need the chrome webdriver, this can be downloaded from [here](https://chromedriver.chromium.org/downloads)

<details>
    <summary> Click to expand and see the full script </summary>
    
```python
import os
import shutil
import time

import pendulum
from selenium import webdriver


def main():
    """Download Google Books traffic and sales report using Selenium.
    Needs to be run in debug mode, because it requires manual sign in at breakpoint (to avoid bot detection).

    Reports are downloaded at a monthly granularity between the start_date and end_date.
    They are downloaded for each publisher in the 'account_numbers' dict and moved to the corresponding subdirectory
    in the download directory.

    If a publisher has more than 1 account linked a tuple should be used with the publisher name and a file suffix.
    The file suffix will be added to the filepath and is used to distinguish reports from different accounts for
    the same publisher.
    The file suffixes that are used here should be passed on to the telescope 'extra' information as described in the
    docs.

    The traffic report is organised by 'Book'.

    :return: None.
    """

    """ Customise values """
    download_dir = "/path/to/download/dir"
    driver_path = "/path/to/chromedriver"
    # Account numbers can be found in the page path when you are signed in to the google books partner center
    account_numbers = {
        "account_number1": "publisher_name1",
        "account_number2": "publisher_name2",
        "account_number3": ("publisher_name3", "suffix1"),
        "account_number4": ("publisher_name3", "suffix2"),
    }
    start_date = pendulum.datetime(2018, 1, 1)
    end_date = pendulum.now()
    """ Customise values """

    # Set download dir for webdriver
    chrome_options = webdriver.ChromeOptions()
    prefs = {"download.default_directory": download_dir}
    chrome_options.add_experimental_option("prefs", prefs)

    # Initialise webdriver and go to books url to login
    driver = webdriver.Chrome(executable_path=driver_path, chrome_options=chrome_options)
    driver.get("https://play.google.com/books/publish/")

    fmt = "%Y,%-m,%-d"  # <-------- set breakpoint here and manually sign in

    # Create download dir
    if not os.path.exists(download_dir):
        os.mkdir(download_dir)

    # Loop through publishers
    for account_number, publisher in account_numbers.items():
        # Get publisher name and file suffix if given
        if isinstance(publisher, tuple):
            name = publisher[0]
            file_suffix = publisher[1]
        else:
            name = publisher
            file_suffix = ""

        # Create publisher dir
        publisher_dir = os.path.join(download_dir, name)
        if not os.path.exists(publisher_dir):
            os.mkdir(publisher_dir)

        # Loop through months
        period = pendulum.period(start_date, end_date)
        for dt in period.range("months"):
            # Skip month if month is not finished yet
            if dt.end_of("month") >= pendulum.now():
                continue

            # Get start and end date in correct string format
            start = dt.strftime(fmt)
            end = dt.end_of("month").strftime(fmt)

            # Download traffic report
            traffic_report_src = os.path.join(download_dir, "GoogleBooksTrafficReport.csv")
            traffic_report_dst = os.path.join(
                publisher_dir, f'GoogleBooksTrafficReport_{file_suffix}{dt.strftime("%Y_%m")}.csv'
            )
            url = (
                f"https://play.google.com/books/publish/u/2/a/{account_number}/downloadTrafficReport?"
                f"f.req=[[null,{start}],[null,{end}],2,false]"
            )
            download_report(driver, url, traffic_report_src, traffic_report_dst)

            # Download sales report
            sales_report_src = os.path.join(download_dir, "GoogleSalesTransactionReport.csv")
            sales_report_dst = os.path.join(
                publisher_dir,
                f'GoogleSalesTransactionReport_{file_suffix}{dt.strftime("%Y_%m")}.csv',
            )
            url = (
                f"https://play.google.com/books/publish/a/{account_number}/downloadSalesTransactionReport?"
                f"f.req=[[null,{start}],[null,{end}],[],null,null,null,[],[]]"
            )
            download_report(driver, url, sales_report_src, sales_report_dst)


def download_report(driver: webdriver, url: str, src_path: str, dst_path: str):
    """Download a traffic or sales report from url and move report to a different location.

    :param driver: The chrome webdriver
    :param url: Download url
    :param src_path: File path where file is automatically downloaded to
    :param dst_path: File path where file is moved to
    :return: None.
    """
    # Check if report already exists
    if os.path.exists(dst_path):
        return
    # Download from url
    driver.get(url)
    while not os.path.exists(src_path):
        time.sleep(2)
    # Move to correct dir and add date to filename
    shutil.move(src_path, dst_path)
    print(f"Downloaded: {dst_path}")


if __name__ == "__main__":
    main()
```

</details>

## Airflow connections
Note that all values need to be urlencoded.  
In the config.yaml file, the following airflow connection is required:  

### sftp_service
The sftp_service airflow connection is used to connect to the sftp_service and download the reports.  
The username and password are created by the sftp service and the host is e.g. `oaebu.exavault.com`.  
The host key is optional, you can get it by running ssh-keyscan, e.g.:
```
ssh-keyscan oaebu.exavault.com
```

```yaml
sftp_service: ssh://<username>:<password>@<host>:<port>?host_key=<host_key>
```

## Latest schema

### Google Books Sales

``` eval_rst
.. csv-table::
   :file: ../schemas/google_books_sales_latest.csv
   :width: 100%
   :header-rows: 1
```

### Google Books Traffic

``` eval_rst
.. csv-table::
   :file: ../schemas/google_books_traffic_latest.csv
   :width: 100%
   :header-rows: 1
```
