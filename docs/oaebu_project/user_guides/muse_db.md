# MUSE Dashboard
Project MUSE provides a platform which hosts journals and books from multiple publishers including University of Michigan Press and University College London. Some of the MUSE offerings are open access eBooks (<https://about.muse.jhu.edu/muse/open-access-overview/>). To see open access book titles on Project MUSE, go to <https://muse.jhu.edu/search?action=oa_browse>

For the OAeBU pilot, the MUSE data source is a manual data upload for the University of Michigan Press, and usage data is available from January 2020. 

The sections of this dashboard are:
* [Filters](#filters)
* [MUSE Summary](#muse-summary)
* [MUSE Books and Items](#muse-books-and-items)
* [MUSE Usage](#muse-usage)
* [MUSE Access](#muse-access)
* [MUSE Country](#muse-country)
* [MUSE Institutions](#muse-institutions)
* [MUSE Format](#muse-format)

## Filters
A filter is a way to narrow a search and look for more specific information. 

``` eval_rst
.. tip::  Only one filter can be used at a time – ‘Select Book Title(s)’ OR ‘Select ISSN/ISBN(s)’ OR ‘Select Item Title(s)’ OR ‘Select Resource_ID(s)’ OR ‘Select Authors(s)’ OR ‘Select Access type(s)'. 
```    

If you select a filter then add a second filter, the first filter will be automatically removed. For example, if you ‘Select Book Title(s)’, then ‘Select Author(s)’, the ‘Select Book Title(s)’ filter will be automatically removed. 

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
.. image:: ../images/muse_filters1.png
    :width: 300
```    

To remove multiple authors at the same time, click on the grey circle icon with a white X inside it:

``` eval_rst
.. image:: ../images/muse_filters2.png
    :width: 300
```

Another way to remove applied filters in a dashboard, is to use the filter bar at the top left of the dashboard. Click on the grey X to remove the filter. 

``` eval_rst
.. image:: ../images/muse_filters3.png
    :width: 400
```

## MUSE Summary 
The ‘MUSE - Summary’ section shows the number of Book titles, Resource IDs, ISSN/ISBNs, Item titles and requests from MUSE for any selected filters. When no filters are selected, a summary in the MUSE data source for all University of Michigan Press titles is displayed in the dashboard. Note that there can be multiple ISBNs per title for different formats, for example pdf, html or hard cover. 

Note: The data shown in this section is from the following variables: resource, resource_id, issn_isbn, fulltext_title.

## MUSE Books and items
In the ‘MUSE – Books and Items' section, the number of books and the number of items per launch year are shown in two bar graphs and a table: 
* ‘# Book titles per launch year’ (book titles only)
* ‘# Item titles per launch year’ (item titles only)
* ‘# Books and Items per launch year’ (books and items combined)

Hover over a year in the bar graphs to see the number of book or item titles per launch year. 
Note: The data shown in this section is from the following variables: resource, resource_id, issn_isbn, fulltext_title.

### Using time filters
To look at a specific launch year in more detail, click on a year in the bar graph. The dashboard will be updated to show usage data from MUSE for that year only. 

To remove the launch year filter, go to the filter bar at the top left of the dashboard. Click on the X to remove the launch filter. 

``` eval_rst
.. image:: ../images/muse_books1.png
    :width: 300
```    

### Exporting the data
To export the data from the bar graphs or table to a .CSV file, hover in the top right corner of the graph or table to find three dots: 

``` eval_rst
.. image:: ../images/muse_books2.png
    :width: 600
```    

``` eval_rst
.. image:: ../images/muse_books3.png
    :width: 600
```    

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder. 

## MUSE Usage
Usage is the total number of times that a title or item has been requested from MUSE. The ‘MUSE - Usage' section shows the usage per month (in a graph and a table), and top book and item titles by usage. 

All data in this section can be exported to CSV by following the ‘Exporting the data’ instructions in the ‘MUSE – Books and Items’ section above:
* ‘Monthly usage’ bar graph (hover over any month to see the number of downloads in that month)
* ‘Monthly usage’ table (usage per month in chronological order)
* ‘Top 10 Book titles by usage’ table (all titles ranked by highest usage first)
* ‘Top 10 Item titles by usage’ table (all items ranked by highest usage first)

To look at a specific month in the bar graph in more detail, click on a bar in the bar graph. The dashboard will automatically update to use this time filter. To remove this time filter, reload the dashboard by clicking on the ‘MUSE’ link in the navigation menu at the top right of this dashboard. 

``` eval_rst
.. tip::  Tip: In the ‘Top 10 Book titles by usage’ and ‘Top 10 Item titles by usage’ table, click on any URL in the [resource_url] or [fulltext_url] column to go to the MUSE website where that open access eBook title/item can be viewed and downloaded.  
```    

Note: The data shown in this section is from the following variable: requests

## MUSE Access
Access is how the title or item was requested from MUSE, and can be one of the following types: ‘open_access’ or ‘gated’. The ‘MUSE - Access’ section shows the top access types in a donut graph and table. 

All data in this section can be exported to CSV by following the ‘Exporting the data’ instructions in the ‘MUSE - Books and Items’ section above. 

Note: The data shown in this section is from the following variable: access

## MUSE Country
Country is the source of the request for a title or item from MUSE. The ‘MUSE - Country’ section shows the top countries in a donut graph ‘Top Countries by usage’ and table ‘Top Countries by usage’. 

Click on a country in the donut graph to select a specific country. All sections in this dashboard will automatically update, for example the ‘MUSE - Institutions’ section will show all institutions in the selected country only. 

Alternatively, scroll to a country in the table ‘Top Countries by usage’, and hover beside the ‘Country’ name to show the +. Click on the + to apply a country filter to the ‘MUSE’ dashboard. 

``` eval_rst
.. image:: ../images/muse_country1.png
    :width: 400
```    

To remove the Country filter, go to the filter bar at the top left of the dashboard. Click on the X to remove the Country filter. 

``` eval_rst
.. image:: ../images/muse_country2.png
    :width: 200
```    

All data in this section can be exported to CSV by following the ‘Exporting the data’ instructions in the ‘MUSE - Books and Items’ section above. 

Note: The data shown in this section is from the following variable: country 

## MUSE Institutions
Institution refers to the institution where the request for a title or item from MUSE originated. The ‘MUSE - Institution’ section shows the top institutions in a donut graph and table. 

Click on an institution in the donut graph to select a specific institution. All sections in this dashboard will automatically update. 

Alternatively, scroll to an institution in the table ‘Top Institutions by usage’, and hover beside the institution name to show the +. Click on the + to apply an Institution filter to the ‘MUSE’ dashboard. 

To remove the Institution filter, go to the filter bar at the top left of the dashboard. Click on the X to remove the Institution filter. 

``` eval_rst
.. image:: ../images/muse_institutions1.png
    :width: 250
```    

All data in this section can be exported to CSV by following the ‘Exporting the data’ instructions in the ‘MUSE - Books and Items’ section above. 

Note: The data shown in this section is from the following variable: institution

## MUSE Format
Format is the format of how the title or item was downloaded from MUSE, and can be of the types ‘pdf’ or ‘html’. The ‘MUSE - Format’ section shows the top format types in a donut graph and table. 

All data in this section can be exported to CSV by following the ‘Exporting the data’ instructions in the ‘MUSE - Books and Items’ section above. 

Note: The data shown in this section is from the following variable: format
