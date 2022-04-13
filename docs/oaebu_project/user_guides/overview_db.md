# Overview Dashboard

The ‘Overview’ dashboard presents a summary of all open access eBooks published by the University of Michigan Press. To look at other dashboards showing usage of specific titles and authors by country, institution, or subject, use the navigation menu at the top right of this dashboard.

``` eval_rst
.. image:: ../images/overview_intro1.png
    :width: 200
```    

For the pilot project, data sources of usage data cover different time periods. For example, the University of Michigan Press has data sources for the following time periods:
* Usage data is from September 2020 for Google Books
* Usage data is from January 2018 onwards for JSTOR
* Usage data is from January 2018 to March 2020 for OAPEN (Counter 4), and from April 2020 for OAPEN (Counter 5) 
* Event data is from May 2018 onwards for Crossref Events

The following sections are included in the ‘Overview’ dashboard:
* [Summary ONIX data](#summary-onix-data)
* [Summary Usage data](#summary-usage-data)

## Summary ONIX data
The ‘Summary - ONIX data’ section shows a summary of the ONIX feed from the University of Michigan Press. ONIX for Books (ONline Information eXchange) is a standard format that book publishers use to share information about the books that they have published (<https://bisg.org/general/custom.asp?page=ONIXforBooks>).

This section contains:
* Number of unique ISBNs in a bar graph and table - ‘# unique ISBNs in the ONIX feed’
* List of all titles in the ONIX feed as a table - ‘All titles in the ONIX feed’

### Number of unique ISBNs
Hover over a year in the ‘# unique ISBNs in the ONIX feed’ bar graph to see the number of ISBNs published in that year. 

Click on any bar in the bar graph, and the dashboard will be updated to show ONIX and usage data for the specified year, including a list of titles published that year in the ‘All titles in the ONIX feed’ table. Note that the ‘Summary - Usage data’ section will show the years after the publication year, as usage of an eBook generally occurs after the publication year. 

To remove the year filter, use the filter bar at the top left of the dashboard. Click on the X to remove the year filter. 

``` eval_rst
.. image:: ../images/overview_summary1.png
    :width: 200
```    

The data contained in the ‘# unique ISBNs in the ONIX feed’ table can be exported to CSV by using the ‘Options’ popup. Hover to the right of the ‘ISBNs’ column to show three dots.  

``` eval_rst
.. image:: ../images/overview_summary2.png
    :width: 300
```    

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder.

### List of all titles in the ONIX feed
Navigate the ‘All titles in the ONIX feed’ table to view more Titles, by using the scrollbar and page numbers. You can also change the number of rows displayed via ‘Rows per page’.

``` eval_rst
.. image:: ../images/overview_summary3.png
    :width: 600
```    

The data contained in the ‘All titles in the ONIX feed’ table can be exported to CSV by using the ‘Options’ popup. Hover to the right of the ‘Book ISBNs’ column to show three dots.  

``` eval_rst
.. image:: ../images/overview_summary4.png
    :width: 600
```    

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder.

## Summary Usage data
The ‘Summary - Usage data’ section usage data comes from different sources (OAPEN, Google Books, JSTOR and Crossref):
* ISBN summary
* Downloads from all sources
* Views from all sources
* Events from all sources

### ISBN summary
The start of the ‘Summary - Usage data’ section shows a summary of the number of ISBNs with usage data for each data source (OAPEN, Google Books, JSTOR and Crossref). 

The two bar graphs show the number of ISBNs with usage data from Google Books, JSTOR, OAPEN and Crossref Events combined:
* ‘# ISBNs per month with usage data’ 
* ‘# ISBNs per year with usage data’ - note that this data is not the same as the individual months added together, as it shows the number of ISBNs across a whole year that had usage data recorded about them.

Hover over any of the months/years in these bar graphs to see the number of ISBNs in that month/year that have usage data. 

Click on any bar in the bar graphs, the dashboard will be updated to show only the ONIX and usage data for the specified month/year, including a list of titles published that month/year in the ‘All titles in the ONIX feed’ table. Note that the ‘Summary - Usage data’ section will show only the years after the publication year, where usage of an eBook occurred after the publication year. 

To remove the month/year filter, reload the dashboard by clicking on the ‘Overview’ link in the navigation menu at the top right of this dashboard.

``` eval_rst
.. image:: ../images/overview_intro1.png
    :width: 200
```    

The data contained in the ‘Summary - Usage data’ bar graphs can be exported to CSV by using the ‘Options’ popup. Hover to the right of the bar graph title to show three dots e.g. for ‘# ISBNs per year with usage data’:  

``` eval_rst
.. image:: ../images/overview_summary5.png
    :width: 600
```    

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder.

### Downloads from all sources
This section shows downloads from the following sources combined together - book downloads from OAPEN and GoogleBooks, and chapter downloads from JSTOR. 

Hover over any of the months in the bar graph to see the number of downloads for that month. 

Click on any bar in the bar graph, the dashboard will be updated to show the downloads, views and events data for the specified month. 

To remove the month filter, reload the dashboard by clicking on the ‘Overview’ link in the navigation menu at the top right of this dashboard.

The data contained in this bar graph can be exported to CSV by using the ‘Options’ popup. Hover in the top right corner of the bar graph to show three dots.  

``` eval_rst
.. image:: ../images/overview_summary6.png
    :width: 600
```    

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder.

### Views from all sources
This section shows views from Google Books. See the ‘Downloads from all sources’ section on this help page for how to add and remove a month filter, and export data to CSV. 

### Events from all sources
This section shows events and mentions from Crossref Events. Crossref Event Data captures online discussion about research outputs including eBooks, such as ‘a citation in a dataset or patent, a mention in a news article, Wikipedia page or on a blog, or discussion and comment on social media’ (<https://www.crossref.org/services/event-data/>).

See the [‘Downloads from all sources’](#downloads-from-all-sources) section on this help page for how to add and remove a month filter, and export data to CSV. 
