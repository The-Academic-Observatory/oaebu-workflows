# Frequently Asked Questions

This section of the help guide contains frequently asking questions about using the pilot project dashboards:
* [I’m having trouble seeing some visualisations](#im-having-trouble-accessing-the-dashboards-or-viewing-some-visualisations)
* [Some of the usage data is missing for a specific book title](#some-of-the-usage-data-is-missing-for-a-specific-book-title)
* [I can’t find a specific title](#i-cant-find-a-specific-title)
* [The title I am looking for is in some dashboards but not others](#the-title-i-am-looking-for-is-in-some-dashboards-but-not-others)
* [I can’t find a specific author in the ‘Select Author’ dropdown](#i-cant-find-a-specific-author-in-the-select-author-dropdown)
* [Why is there a relatively low number of ISBNs with usage data?](#why-is-there-a-relatively-low-number-of-isbns-with-usage-data)
* [I can only see data for one title/author/country/institution](#i-can-only-see-data-for-one-title-author-country-or-institution)
* [I’m trying to use ‘Options’ on a visualisation and getting an error](#im-trying-to-use-options-on-a-visualisation-and-getting-an-error)

## I’m having trouble accessing the dashboards or viewing some visualisations

If you are unable to access the dashboards, please check your web browser settings. if you have a setting in your web browser that 'blocks all cookies’, you will not be able to see the dashboards. To enable the cookies:
* Navigate to the ‘cookies in use’ section in your web browser and check for any blocked cookies. For example for the Chrome web browser, navigate to the 'padlock' icon next to the URL window, and click 'cookies' and then ‘Blocked’. If the cookies from 'es.io' or the link you use to access the dashboard are blocked, set them to 'allow'.

Please use an up-to-date web browser. The dashboards should be compatible with the latest versions of Firefox, Chrome, Safari and Edge Chromium (see <https://www.elastic.co/support/matrix#matrix_browsers> for specific versions of each browser).

Please send any bug reports to: coki@curtin.edu.au

## Some of the usage data is missing for a specific book title

Some of the reasons why you may not be able to see usage data about a book title are:
* The book may not have been published as an open access eBook during that time period
* An ISBN may have been assigned, but the open access version may not yet have been released.  
* Data sources have usage data for different time periods. For example, the University of Michigan Press has data sources for the following time periods:
    * Usage data is from September 2020 for Google Books
    * Usage data is from January 2018 onwards for JSTOR
    * Usage data is from January 2018 to March 2020 for OAPEN (Counter 4), and from April 2020 for OAPEN (Counter 5) 
    * Event data is from May 2018 onwards for Crossref Events
    * For publishers using Google Analytics data, data is only available for the previous 26 months from the activation of data ingestion [see the Google Analytic telescope documentation for more details](../../oaebu_workflows/telescopes/google_analytics).

## I can’t find a specific title

``` eval_rst
.. tip::  Try searching by author or ISBN if you can’t find a specific title
```   

Data about open access eBooks comes from different sources, and sometimes there are inconsistencies in this data. You may not be able to find a specific title for some of the following reasons:
* A book has not yet been released, so the Title and ISBN might not be in the data sources yet. 
* A book is released but very recently, so the Title and ISBN might not be in the most recent data source. 
* Titles and ISBNs presented in the dashboards (aside from the manual data upload dashboards) are obtained from the publisher's ONIX feed. A book may have a slightly different title, for example with the word ‘The’ in a title from one data source, but not in the ONIX feed. In this case, try leaving out the word ‘The’ to see if the title you are looking for is returned. 
* A book has a shorter title across different data sources. For example one data source has the title “Sounding Together”, while a different data source has the title “Sounding Together: Collaborative Perspectives on U.S. Music in the 21st Century”

For publishers, the [Data Quality](../user_guides/data_qual_db.md) and [Overview](../user_guides/overview_db) dashboards may be helpful for identifying issues with data quality, and discrepancies between data from different sources.

## The title I am looking for is in some dashboards but not others
The dashboards are built on usage data from different data sources. Sometimes a title is in one data source but not another, hence it may not appear in all Title/ISBN filters across all dashboards. 

## I can’t find a specific author in the ‘Select Author’ dropdown
If a book is not yet released, the Title and ISBN may be in the pilot project dashboards, but without the author details. 

## Why is there a relatively low number of ISBNs with usage data?
This can happen when only partial data is available for the current month, or for the last few months. Data is transferred from our data sources at different frequencies, with Crossref events data and  ONIX feeds  generally updated weekly, with usage data updated approximately monthly. Find out more in the user documentation about [Book Usage Data Workflows](../../oaebu_workflows/index).

## I can only see data for one title, author, country or institution
There could be filters applied in the dashboard. First try clearing all filters applied in the filter dropdowns. For example, to remove all authors from the ‘Select Author(s)/Volume Editor(s)’ filter, click on the X in a circle icon. 

``` eval_rst
.. image:: ../images/faq1.png
    :width: 400
```   

Another way to remove applied filters in a dashboard is to use the filter bar at the top left of the dashboard. Click on the X to remove the filter. 

``` eval_rst
.. image:: ../images/faq2.png
    :width: 500
``` 

If filters are still active, reload the dashboard by clicking on its link in the navigation menu at the top right of any dashboard. 

``` eval_rst
.. image:: ../images/faq3.png
    :width: 200
``` 

## I’m trying to use ‘Options’ on a visualisation and getting an error
Some of the header text in the dashboards display the ‘Options’ symbol (three dots). However, these options are redundant because they are on a header rather than a visualisation itself. 

You may also see the following error ‘No requests logged. The element hasn't logged any requests (yet). This usually means that there was no need to fetch any data or that the element has not yet started fetching data.’

To see the options for a visualisation itself, click on the ‘Options’ symbol (three dots) beside the visualisation rather than beside the header. 

Click here:

``` eval_rst
.. image:: ../images/faq4.png
    :width: 500
``` 

Don’t click here:

``` eval_rst
.. image:: ../images/faq5.png
    :width: 500
``` 
