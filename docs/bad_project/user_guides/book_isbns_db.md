# Book ISBNs Dashboard
The ‘Book ISBNs’ dashboard shows usage data for all eBooks. This usage data comes from Google Books, JSTOR, OAPEN and Crossref Events. For more information about these data sources, see the [partner data](../overview/partner_data) and [public data](../overview/public_data) sections of this help guide. 

Data sources have usage data for different time periods. For example, the University of Michigan Press has data sources for the following time periods:
* Usage data is from September 2020 for Google Books
* Usage data is from January 2018 onwards for JSTOR
* Usage data is from January 2018 to March 2020 for OAPEN (Counter 4), and from April 2020 for OAPEN (Counter 5) 
* Event data is from May 2018 onwards for Crossref Events

The sections of this dashboard are:
* [Filters](#filters)
* [Summary](#summary)
* [Downloads](#downloads)
* [Views](#views) 
* [Mentions and Events](#mentions-and-events)

## Filters
A filter is a way to narrow a search and look for more specific information. 

``` eval_rst
.. tip::  Only one filter can be used at a time – ‘Select Title(s)’ OR ‘Select ISBN(s)’ OR ‘Select Author’.  
```    
If you select a filter then add a second filter, the first filter will be automatically removed. For example, if you use ‘Select Title(s)’, then ‘Select ISBN(s)’, the ‘Select Title(s)’ filter will be automatically removed. Note that on this ‘Book ISBNs’ dashboard only one author can be selected, not multiple authors.

To look at the downloads, views and events for multiple University of Michigan Press authors or volume editors, go to the [Author/Volume Editor dashboard](./authors_db).      

### Select one value in a filter
Use the dropdown in any filter field to choose a value e.g. a Title in the ‘Select Title(s)’ field, or start typing in a filter field to search all values by specific text. There is no search button to click, and the rest of the dashboard will automatically update.

### Select multiple values in a filter
To select multiple values in a specific filter field, keep selecting values one by one. This can be either using the dropdown OR by typing directly in the field and selecting from the matches. 

### Remove value/values from a filter
Selected value(s) of a filter can be removed one at a time, or all removed at the same time. 

For example, to clear one Title, click on the X next to the Title:

``` eval_rst
.. image:: ../images/book_isbns_filter1.png
    :width: 400
```    

To remove multiple titles at the same time, click on the grey circle icon with a white X inside it:

``` eval_rst
.. image:: ../images/book_isbns_filter2.png
    :width: 400
```    

Another way to remove applied filters in a dashboard, is to use the filter bar at the top left of the dashboard. Click on the X to remove the filter. 

``` eval_rst
.. image:: ../images/book_isbns_filter3.png
    :width: 600
```    

## Summary 
The ‘Summary’ section shows the number of ISBNs with usage data from Google Books, JSTOR, OAPEN and Crossref Events in two bar graphs:
* ‘# ISBNs per month with usage data’ - this is not the same as the number of downloads across ISBNs/Titles or Authors, for this data see the ‘Downloads’ section in this dashboard.    
* ‘# ISBNs per year with usage data’ - this data is not the same as the individual months added together, as it shows the number of ISBNs across a whole year that had usage data recorded about them.

Hover over any of the months/years in these bar graphs to see the number of ISBNs in that month/year that have usage data. 

### View month or year in detail
To look at a specific month/year in more detail in the Summary bar graphs, click on a month/year in the bar graph to add a time filter. The bar graph visualisation will now display the number of ISBNs for which there is usage data, for one month/year. The rest of the dashboard will automatically update to use this time filter. 

To remove a time filter in the ‘Summary’ section, force the dashboard to reload by clicking on the Book ISBNs link at the top right of this dashboard. 

``` eval_rst
.. image:: ../images/bisac_summary5.png
    :width: 300
```    
The dashboard will automatically refresh to show usage data for all ISBNs. 

## Options popup
The Options popup contains the following features:
* Customize time range
* Inspect
* Maximise panel
* Download as CSV

To open the ‘Options’ popup, hover over and click on the three dots on the right of a graph or table name. For example, in the ‘Summary’ section of the ‘Book ISBNs’ dashboard, hover over and click on the three dots on the right of ‘# ISBNs per month with usage data’ (or ‘# ISBNs per year with usage data):

``` eval_rst
.. image:: ../images/book_isbns_summary2.png
    :width: 400
```    

#### Customize time range
In the ‘Options’ popup, click on ‘Customize time range’. 

``` eval_rst
.. image:: ../images/book_isbns_summary3.png
    :width: 400
```    

A ‘Customize panel time range’ popup will be shown, with a ‘Time range’ of ‘All Years Usage data’ by default. 

``` eval_rst
.. image:: ../images/book_isbns_summary4.png
    :width: 400
```    

Click on ‘Show dates’ to see the current date range. 

``` eval_rst
.. image:: ../images/book_isbns_summary5.png
    :width: 400
```    

To change the time span of the range, click on the first date. The calendar popup will be shown, with a blue line under the ‘From date’. There are three ways to choose a ‘From’ date:
* Absolute
* Relative
* Now 

``` eval_rst
.. image:: ../images/book_isbns_summary6.png
    :width: 400
```    

For example, to change the time range to 2018 – 2019 only, click in the ‘Time range’ field to change the dates to this range: 

``` eval_rst
.. image:: ../images/book_isbns_summary7.png
    :width: 400
```  

Select ‘Add to panel’ to apply the new time range to the bar graph. 

To clear the time range in the bar graph, go back into ‘Options’ and then ‘Customize time range’. Click on ‘Remove’. 

``` eval_rst
.. image:: ../images/book_isbns_summary8.png
    :width: 400
```  

The time range will be removed, and the bar graph will return to the default of showing ISBNs with usage data after January 2018

If the entered time range is not possible or has no data, for example ‘in 2 months to in 3 months’, the message ‘No results found’ will be displayed:

``` eval_rst
.. image:: ../images/book_isbns_summary9.png
    :width: 400
```  

#### Inspect

In the ‘Options’ popup, click on ‘Inspect’:

``` eval_rst
.. image:: ../images/book_isbns_summary10.png
    :width: 400
```  

A table of the data used in the bar graph (data visualisation) will be displayed. For example, for ‘# ISBNs per month with usage data’:

``` eval_rst
.. image:: ../images/book_isbns_summary12.png
    :width: 400
```  

You can change the number of rows displayed via ‘Rows per page’ (10, 20 or 50), and navigating to other rows. 

``` eval_rst
.. image:: ../images/book_isbns_summary13.png
    :width: 400
```  

##### Downloading the data using Inspect

Data can be exported in two ways:
* ‘Options’ -> ‘Inspect -> Download CSV’ (this section) or
* ‘Options’ -> [‘Download as CSV’](#download-as-csv)

While still in the ‘Inspect’ popup, Select ‘Download CSV’ to download the displayed data either as a formatted CSV file (data already in table format), or as a raw CSV file (data that has not been formatted). 

``` eval_rst
.. image:: ../images/book_isbns_summary14.png
    :width: 400
```  

In this example, the raw CSV contains the following format, where month is expressed as an epoch timestamp in milliseconds. Epoch time is ‘the number of seconds that have elapsed since the Unix epoch’ (<https://en.wikipedia.org/wiki/Unix_time>)

```
"Month ","# ISBNs"
1514736000000,27
1517414400000,28
```

Because the CSV file format does not require specific software, you can view, or import it in many other programs including LibreOffice’s Calc (<https://www.libreoffice.org/discover/calc/>) and Microsoft Excel.

##### Using filters in Inspect

It is possible to use any of these months as a filter across the whole ‘Book ISBNs’ dashboard. To do this, hover beside the month you would like to filter on, and click on the plus in a circle icon to ‘Filter for value’. In this example, we are adding a filter for December 2021 in the ‘Book ISBNs’ dashboard. 

``` eval_rst
.. image:: ../images/book_isbns_summary15.png
    :width: 400
```  

Each section of the whole ‘Book ISBNs’ dashboard (‘Summary’, ‘Downloads’, ‘Views’, and ‘Mentions and Events’) will automatically update to only show data for December 2021. 

``` eval_rst
.. image:: ../images/book_isbns_summary16.png
    :width: 400
```  

This filter is also displayed at in the filter bar at the top left of the of ‘Book ISBNs’ dashboard. 

``` eval_rst
.. image:: ../images/book_isbns_summary17.png
    :width: 400
```  

To remove this filter, go to the filter bar at the top left of the ‘Book ISBNs’ dashboard, and click on the X. 

``` eval_rst
.. image:: ../images/book_isbns_summary18.png
    :width: 400
```  

The filter will be removed, and the ‘Book ISBNs’ dashboard will return to showing the data for all ISBNs for this publisher.


##### View Requests via Inspect

By default, the data is displayed when ‘Options – Inspect’ is selected in the ‘Book ISBNs’ dashboard. The requests used to collect the data can also be viewed, by clicking on ‘View: Data’ and selecting ‘Requests’. Note, the documents here refer to how the data is stored in Elasticsearch, and is not a summary of the usage metrics that are visualised in the dashboards. The Requests menu may be of interest if you wish to view details of the Elasticsearch queries that power the visualisations. 

``` eval_rst
.. image:: ../images/book_isbns_summary19.png
    :width: 400
```  

The three tabs are:
* Statistics
* Request
* Response

``` eval_rst
.. image:: ../images/book_isbns_summary20.png
    :width: 400
```  

Here the word ‘document’ means the number of months where there is usage data for the specified ISBN(s), title(s) or author.
The ‘Statistics’ tab shows:
* Hits = The number of documents returned by the query
* Hits (total) = The number of documents that match the query
* Index pattern = The index pattern that connected to the Elasticsearch indices
* Index pattern ID = The ID in the .kibana index
* Query time = The time it took to process the query. Does not include the time to send the request or parse it in the browser
* Request timestamp = Time when the start of the request has been logged

The ‘Request’ tab shows the Kibana Query Language (KQL) query that was used in Elasticsearch to find the number of ISBNs per month with usage data. You can find out more about KQL at <https://www.elastic.co/guide/en/kibana/current/kuery-query.html> 

The ‘Response’ tab shows the response to the KQL request. 

To return to viewing the data, click on ‘View: Requests’ and select ‘Data’. 


``` eval_rst
.. image:: ../images/book_isbns_summary21.png
    :width: 400
```  

#### Maximise panel
Use ‘Maximise panel’ to expand a visualisation to full screen view. In the ‘Options’ popup, click on ‘Maximise panel’:

``` eval_rst
.. image:: ../images/book_isbns_summary22.png
    :width: 400
```  

The bar graph visualisation will then be shown as full screen within your browser.

``` eval_rst
.. image:: ../images/book_isbns_summary23.png
    :width: 400
```  

To minimise the panel and return to the default view, hover over and click on the three dots on the right of ‘# ISBNs per month with usage data’, and select ‘Minimize’. 

``` eval_rst
.. image:: ../images/book_isbns_summary24.png
    :width: 400
```  

#### Download as CSV

To export the data from the bar graph or table to a .CSV file, go to the ‘Options’ popup and click on ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder. 

## Downloads

The ‘Downloads’ section of the ‘Book ISBNs’ dashboard shows the total number of downloads for the selected ISBN(s) from OAPEN, Google Books and JSTOR combined. The separate totals are also presented for each data source - OAPEN, Google Books and JSTOR. 

``` eval_rst
.. tip::  To select specific ISBNs, only one filter should be used at a time – ‘Select Title(s)’ OR ‘Select ISBN(s)’ OR ‘Select Author’.
```    

There are three monthly downloads bar graphs, and three top titles tables in the ‘Downloads’ section:
* ‘# monthly downloads OAPEN’
* ‘# monthly downloads GoogleBooks’
* ‘# monthly downloads JSTOR’
* ‘Top titles by # downloads from OAPEN from Counter 4 and 5’
* ‘Top titles by # downloads from GoogleBooks’
* ‘Top titles by # chapter downloads from JSTOR’

### Monthly downloads bar graphs

For example, when the title ‘Embodied Archive’ is selected in the ‘Select Title(s)’ filter, no downloads were reported in the OAPEN data:

``` eval_rst
.. image:: ../images/book_isbns_downloads1.png
    :width: 300
```  

For the same title, ‘Embodied Archive’ (a single ISBN), two downloads were reported in the Google Books data – one download in April 2021, and one download in October 2021. 

``` eval_rst
.. image:: ../images/book_isbns_downloads2.png
    :width: 300
```  

For the same title, ‘Embodied Archive’ (a single ISBN), 285 chapter downloads were reported in the JSTOR data, spread between April 2021 and December 2021. 

``` eval_rst
.. image:: ../images/book_isbns_downloads3.png
    :width: 300
```  

To open the ‘Options’ popup for the OAPEN, Google Books or JSTOR bar graphs, hover over and click on the three dots on the right of ‘# monthly downloads OAPEN’, OR ‘# monthly downloads GoogleBooks’ OR ‘# monthly downloads JSTOR’. 

``` eval_rst
.. image:: ../images/book_isbns_downloads4.png
    :width: 500
```  

See the [‘Summary’](#summary) section in this help page for how to use the [‘Options’ popup](#options-popup) functions including exporting data from graphs and tables. 

### Top titles tables

This table shows the top titles reported by OAPEN, Google Books and JSTOR from the specified title(s)/ISBN(s). By default these are top titles for all books, but it will also show top titles for whatever subset of books you have selected via the Filter section of the top of the ‘Books ISBN’ dashboard.

Filters at the top of the dashboard can also be used to view a specific author’s top titles in this table, for example see the following screenshot for all eBooks published by the University of Michigan Press by the author Robert E. Cole. 

``` eval_rst
.. image:: ../images/book_isbns_top_titles_1.png
    :width: 600
```  

Note that if only one ISBN is selected via the filters at the top of the ‘Book ISBNs’ dashboard, it will be the top title of one ISBN. For example, see the following screenshot for ‘Embodied Archive’.

``` eval_rst
.. image:: ../images/book_isbns_top_titles_2.png
    :width: 600
```  

Note: The data shown in this section is from the following variables: OAPEN (title_requests, then total_item_requests from April 2020), Google Books (google_books_sales.qty) and JSTOR chapters (Total_Item_Requests). 

## Views

The ‘Views’ section shows the number of views from Google Books for the selected ISBNs. The ‘Select Title(s)’ OR ‘Select ISBN(s)’ OR ‘Select Author’ filters at the top of the dashboard can be used to select specific ISBNs. By default the views across all ISBNs from this publisher will be shown. 

To open the ‘Options’ popup in the ‘Views’ section, hover over and click on the three dots on the right of ‘# monthly views from GoogleBooks’ or ‘Top ISBNs by # views from Google Books’.

See the [‘Summary’](#summary) section in this help page for how to use the [‘Options’ popup](#options-popup) functions including exporting data from graphs and tables. 

### Monthly views 

Hover over any of the months in the ‘# monthly views from GoogleBooks’  bar graph to see the number of views from Google Books for the selected ISBN(s) in that month. 

See the section [‘View month or year in detail’](#view-month-or-year-in-detail) for how to use a time filter in this bar graph. 

### Top ISBNs by number of views

The table ‘Top ISBNs by # views from Google Books’ shows which ISBNs had the most views from Google Books, for the ISBN(s), Title(s) or author that were selected at the top of the ‘Book ISBNs’ dashboard. This list is ranked in descending order of views. Note that ordering in this list is just based on data from Google Books, and other titles may have higher downloads (see the ‘Downloads’ section), or higher mentions and events (see the ‘Mentions and Events’ section). 

Any of the ‘ISBN’, ‘Title’ and ‘# views’ columns can be hidden. Click on this symbol in the column header: ̌

``` eval_rst
.. image:: ../images/book_isbns_views1.png
    :width: 400
```  

In the popup menu, click on ‘Hide’ to hide this column. 

``` eval_rst
.. image:: ../images/book_isbns_views2.png
    :width: 400
```  

Note that you cannot hide all columns, the last column remaining will not have a ‘Hide’ option. 

``` eval_rst
.. image:: ../images/book_isbns_views3.png
    :width: 400
```  

To restore the hidden columns, reload the dashboard by clicking on the ‘Book ISBNs’ link in the navigation menu at the top right of this dashboard.

Note: The data shown in this section is from the following variable: Google Books (BV_with_Pages_Viewed).

## Mentions and events

Crossref Event Data captures online discussion about research outputs including eBooks, such as ‘a citation in a dataset or patent, a mention in a news article, Wikipedia page or on a blog, or discussion and comment on social media’ (<https://www.crossref.org/services/event-data/>).

The ‘Mentions and Events’ section shows Crossref Event data for the ISBN(s), Title(s) or Author selected at the top of the ‘Book ISBNs’ dashboard. By default the mentions and events from across all ISBNs from this publisher will be shown. 

See the [‘Summary’](#summary) section in this help page for how to use the [‘Options’ popup](#options-popup) functions including exporting data from graphs and tables. 

### Number of Crossref events

The bar graph ‘# Crossref events’ shows the number of Crossref events per month. By default this data will be shown for all ISBNs, if filters are selected the data will be displayed for those filters. 

Hover on a bar in the bar graph to show the number of Crossref events for a specific month. To apply a time filter by clicking on a bar in the bar graph, see the section ‘View month or year in detail’ on this help page. 

### Top ISBNs

The table ‘Top ISBNs by # Crossref events’ shows which ISBNs had the most mentions and events from Crossref, for the ISBN(s), Title(s) or Author that were selected at the top of the ‘Book ISBNs’ dashboard. This list is ranked in descending order of mentions and events. Note that the ordering in this list is based on data from Crossref Events, and other titles may have higher downloads (see the [‘Downloads’](#downloads) section), or higher views (see the [‘Views’](#views) section). 

Any of the ‘ISBN’, ‘Title’ and ‘# events’ columns can be hidden. See the instructions on how to hide and restore table columns in the [‘Options’ popup](#options-popup) section of this help page. 

Note: The data shown in this section is from the following variable: crossref_events.count.
