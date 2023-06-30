# Fulcrum Dashboard

Fulcrum is a “community-developed, open-source platform for digital scholarship” which provides “users the ability to read books with associated digital enhancements, such as: 3-D models, embedded audio, video, and databases; zoomable online images, and interactive media.” (<https://www.press.umich.edu/librarians>). 

In Fulcrum, DOIs are allocated to any digital content within eBooks, for example images and audio files. The Parent Title and Parent DOI are used for the eBook, and one Parent DOI can include multiple DOIs. In the default dashboard displayed, the number of downloads is the sum of downloads across all DOIs in Fulcrum, not just the parent DOIs. 

For the pilot project, the Fulcrum data source is a manual data upload for the University of Michigan Press, and usage data is available from April 2020 for both paid and open access eBooks. 

The University of Michigan Press Ebook Collection can be accessed at: 
<https://www.fulcrum.org/michigan> 

The sections of this dashboard are:
* [Filters](#fulcrum-filters)
* [Fulcrum - Summary](#fulcrum-summary)
* [Fulcrum - Books](#fulcrum-books)
* [Fulcrum - Usage](#fulcrum-usage)
* [Fulcrum - Institutions](#fulcrum-institutions)
* [Fulcrum - Publishers](#fulcrum-publishers)

## Fulcrum - Filters

A filter is a way to narrow a search and look for more specific information. 

``` eval_rst
.. tip::  Only one filter can be used at a time – ‘Select Parent Title(s)’ OR ‘Select Authors(s)’ OR ‘Select Parent ISBN(s)’ OR ‘Select Parent DOI(s)’ OR ‘Select ISBN(s)’ OR ‘Select DOI Handle(s)’ OR ‘Select Access type(s). 
```    
If you select a filter then add a second filter, the first filter will be automatically removed. For example, if you ‘Select Parent Title(s)’, then ‘Select Author(s)’, the ‘Select Parent Title(s)’ filter will be automatically removed. 

``` eval_rst
.. note:: A warning message may appear on the filters: ‘Terms list might be incomplete because the request is taking too long ...’. This is expected, and these filters can still be used but the full list of options may not be displayed. 
```    

### Select one value in a filter

Use the dropdown in any filter field to choose a value e.g. an Author in the ‘Select Author(s)’ field, or start typing in a filter field to search all values by specific text. There is no search button to click, and the rest of the dashboard will automatically update.

### Select multiple values in a filter

To select multiple values in a specific filter field, keep selecting values one by one. This can be either using the dropdown OR by typing directly in the field and selecting from the matches. 

### Remove value/values from a filter

Selected value(s) of a filter can be removed one at a time, or all removed at the same time. 
For example, to clear one author, click on the X next to the author:

``` eval_rst
.. image:: ../images/fulcrum_select_authors1.png
    :width: 400
```    

To remove multiple authors at the same time, click on the grey circle icon with a white X inside it:

``` eval_rst
.. image:: ../images/fulcrum_select_authors2.png
    :width: 400
```

Another way to remove applied filters in a dashboard, is to use the filter field in the top left of the dashboard. Click on the grey x to remove the filter. 

``` eval_rst
.. image:: ../images/fulcrum_select_authors3.png
    :width: 400
```

## Fulcrum - Summary 

The ‘Fulcrum - Summary’ section shows the number of Parent Titles, Parent DOIs, DOIs and downloads from Fulcrum for any selected filters. When no filters are selected, a summary of all titles and associated DOIs in the Fulcrum data source for the University of Michigan Press is displayed in the dashboard. 

There may be more DOIs than Titles because DOIs can be allocated to extra digital content associated with a single eBook, for example images and audio files. The number of downloads is across all University of Michigan Press DOIs in Fulcrum, not just the parent DOIs.

Note: The data shown in this section is from the following variables: Total_Item_Requests, parent_title, parent_doi, doi.

## Fulcrum - Books

In the ‘Fulcrum - Books' section, the ‘# publications published each year’ bar graph shows the number of titles published by the University of Michigan Press per year (from the Fulcrum data). Hover over any of the months in this bar graph to see the number of titles published in that month.

The ‘# Parent Titles per year’ table shows the same data as the ‘# publications published each year’ bar graph, and can be scrolled to view all years. 

``` eval_rst
.. image:: ../images/fulcrum_books1.png
    :width: 400
```    

### Exporting the data

To export the data from the bar graph or table to a .CSV file, hover in the top right corner of the graph or table to find three dots: 

``` eval_rst
.. image:: ../images/fulcrum_books2.png
    :width: 500
```    

``` eval_rst
.. image:: ../images/fulcrum_books3.png
    :width: 400
```    

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’:

``` eval_rst
.. image:: ../images/fulcrum_books4.png
    :width: 400
```    

Depending on your internet browser, the CSV file may be saved to the Downloads folder. 

### Using time filters

To look at a specific month in the bar graph ‘# publications published each year’ in more detail, click on this month in the bar graph to add a time filter. The bar graph visualisation will now display the number of publications for one year. The rest of the dashboard will automatically update to use this time filter. 

To remove the time filter of a specific month, click on the grey x in the top left corner: 

``` eval_rst
.. image:: ../images/fulcrum_books5.png
    :width: 400
```  

The dashboard will automatically refresh to show all titles and associated DOIs.

Note: The data shown in this section is from the following variable: parent_title.

## Fulcrum - Usage

Usage is the total number of times that a Title has been requested from Fulcrum. The ‘Fulcrum - Usage' section shows the usage per month (in a graph and a table), and top authors by usage. 

All data in this section can be exported to CSV by following the ['Exporting the data'](#exporting-the-data) instructions in the ‘Fulcrum - Books’ section above:

* ‘Monthly usage’ bar graph (hover over any month to see the number of downloads in that month)
* ‘Monthly usage’ table (usage per month in chronological order)
* ‘Top Authors by usage’ table (all authors, ranked by highest usage first)
* ‘Top Parent Titles by usage’ table (all titles, ranked by highest usage first)

To look at a specific month in a bar graph in more detail, see the section above on [‘Using time filters’](#using-time-filters). 

``` eval_rst
.. tip::  In the ‘Top Parent Titles by usage’ table, click on any DOI in the Parent DOI column to go to the Fulcrum website where that open access eBook title can be viewed and downloaded.  
```    

Note: The data shown in this section is from the following variable: Total_Item_Requests.

## Fulcrum - Institutions

The ‘Fulcrum - Institutions’ section shows downloads by institution as a word cloud and table. 

In the ‘Top Institutions by usage’ word cloud – click on any of the institutions to apply a filter to the whole dashboard. To remove this filter by institution, click on the grey x in the top left corner of the dashboard:

``` eval_rst
.. image:: ../images/fulcrum_institutions.png
    :width: 400
```    
Note that some usage in the Fulcrum data is for ‘Unknown Institution’. 

The ‘Top Institutions by usage’ table can be exported to .CSV, see the ['Exporting the data'](#exporting-the-data) instructions in the ‘Fulcrum - Books’ section above. 

Note: The data shown in this section is from the following variable: institution. 

## Fulcrum - Publishers

The ‘Fulcrum - Institutions’ section shows downloads for different publishers which make up the University of Michigan Press, as a donut graph and a table. 

In the ‘Top Publishers by usage’ donut graph, hover over a publisher to view the number and % of downloads per publisher. Click on a publisher section of the donut graph to apply this as a filter to the whole dashboards. To remove this filter by publisher, click on the grey x in the top left corner of the dashboard. 

``` eval_rst
.. image:: ../images/fulcrum_publishers.png
    :width: 400
```    

The ‘Top Publishers by usage’ table can be exported to .CSV by following the ['Exporting the data'](#exporting-the-data) instructions in the ‘Fulcrum - Books’ section above. 

Note: The data shown in this section is from the following variable: publisher.