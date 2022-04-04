# Countries and Territories Dashboard

The ‘Countries and Territories’ dashboard shows usage data or downloads for eBooks by countries and territories. By default, this dashboard will automatically show usage data for all eBooks from this publisher. This data comes from Google Books, JSTOR, OAPEN and Crossref Events. For more information about these data sources, see the [partner data](../overview/partner_data) and [public data](../overview/public_data) sections of this help guide. 

* Usage data is available from September 2020 for Google Books
* Usage data is available from January 2018 onwards for JSTOR
* Usage data is available from January 2018 to March 2020 for OAPEN (Counter 4), and from April 2020 for OAPEN (Counter 5) 
* Event data is from May 2018 onwards for Crossref Events

The sections of this dashboard are:
* [Filters](#filters)
* [Downloads](#downloads)

## Filters 
A filter is a way to narrow a search and look for more specific information. 

``` eval_rst
.. tip::  Only one filter can be used at a time – ‘Select Country(s)/Territory(s)’ OR ‘Select Title(s)’ OR ‘Select ISBN(s)’.   
```    

If you select a filter then and add a second filter, the first filter will be automatically removed. For example, if you ‘Select Country(s)/Territory(s)’, then ‘Select Title(s)’, the ‘Select Country(s)/Territory(s)’ filter will be automatically removed. 

Three filters can be used in this dashboard:
* ‘Select Country(s)/Territory(s)’ - filter for downloads from one or multiple countries and territories OR
* ‘Select Title(s)’ - filter for downloads of one or multiple Titles across countries and territories OR
* ‘Select ISBN(s)’ - filter for downloads for one or multiple ISBNs across countries and territories

### Select one value in a filter
Use the dropdown option in any filter field to choose a value e.g. a Title in the ‘Select Title(s)’ field, or start typing in a filter field to search all values by specific text. There is no search button to click, and the rest of the dashboard will automatically update.

### Select multiple values in a filter
To select multiple values in a specific filter field, keep selecting values one by one. This can be either via the dropdown OR by typing directly in the field and selecting from the matches. 

### Remove value/values from a filter
Selected value(s) of a filter can be removed one at a time, or all removed at the same time. 

For example, to clear one Title, click on the X next to the Title: 

``` eval_rst
.. image:: ../images/countries_filters1.png
    :width: 400
```    

To remove multiple titles at the same time, click on the grey circle icon with a white X inside it:

``` eval_rst
.. image:: ../images/countries_filters2.png
    :width: 400
```    

Another way to remove applied filters in a dashboard, is to use the filter bar at the top left of the dashboard. Click on the X to remove the filter. 

``` eval_rst
.. image:: ../images/countries_filters3.png
    :width: 600
```    

## Downloads
In the ‘Downloads’ section, data is shown for the three data sources – OAPEN, JSTOR and Google Books. For each data source, there are four ways to view the download data: on a map, in a donut graph, in a bar graph and in a table. 
* Downloads map - ‘Downloads by country or territory’
* Top countries/territories (donut graph) e.g. ‘Top countries/territories for chapter downloads from JSTOR’
* Number of monthly downloads (bar graph) e.g. ‘# monthly downloads from OAPEN by country/territory’
* All countries/territories (table) e.g. ‘All countries/territories for downloads from GoogleBooks’

By default, the dashboard shows download data for all open access eBooks by this publisher. If any of the country/territory, title(s) or ISBN(s) filters are used, the map, donut graphs, bar graphs and tables will automatically update to show downloads for the specified filters. For example a specific Title can be added in the ‘Select Title(s)’ filter at the top of the dashboard. The downloads donut graphs, bar charts and tables will automatically update to display downloads for that title only. 

Note: The data shown in this section is from the following variables: OAPEN (total_item_requests), JSTOR chapters (Total_Item_Requests), Google Books (google_books_sales.qty). 

### Downloads map
In the map ‘Downloads by country or territory’, you can hover over each country to view information on the number of downloads for the titles specified in the filters at the top of the ‘Countries and Territories’ dashboard. If no filters are selected, the map will default to showing downloads for all titles from this publisher.  

``` eval_rst
.. image:: ../images/countries_downloads1.png
    :width: 400
```

The downloads map uses different shades of grey to show different download numbers. See the section [Layers popup](#layers-popup) (Show/hide downloads layer details) for more about this map feature.

#### Layers popup
To open the ‘Layers’ popup, click on the layers panel icon in the top right corner of the map:

``` eval_rst
.. image:: ../images/countries_downloads2.png
    :width: 400
```

The ‘Layers’ popup will be displayed:

``` eval_rst
.. image:: ../images/countries_downloads3.png
    :width: 300
```

##### Show or hide downloads layer

In the ‘Layers’ popup, click on the eye icon near ‘Downloads’ to show/hide the downloads layer of the map.

``` eval_rst
.. image:: ../images/countries_downloads4.png
    :width: 300
```

##### Show or hide country labels layer

In the ‘Layers’ popup, click on the eye icon near ‘Country labels’ to show/hide the country labels layer of the map.

``` eval_rst
.. image:: ../images/countries_downloads5.png
    :width: 300
```

##### Show or hide downloads layer details

In the ‘Layers’ popup, click on the expand symbol in between ‘Downloads’ and ‘Country Labels’ to show/hide the details of the downloads layer. 

``` eval_rst
.. image:: ../images/countries_downloads6.png
    :width: 300
```

The key will be displayed for the different shades of grey used to show different download numbers. 

``` eval_rst
.. image:: ../images/countries_downloads7.png
    :width: 400
```

### Downloads donut graphs
There are three donut graphs showing the top countries/territories for downloads from different sources:
* ‘Top countries/territories for downloads from OAPEN (C5)’
* ‘Top countries/territories for chapter downloads from JSTOR’
* ‘Top countries/territories for downloads from GoogleBooks’

Hover over a country/territory in a donut graph to see the number of downloads per country/territory from that data source. 

``` eval_rst
.. image:: ../images/countries_downloads8.png
    :width: 500
```

Click on any country/territory in a donut graph to update the dashboard to show downloads for just that country/territory. 

To clear a country/territory that was selected in a donut graph, go to the filter bar at the top left of the dashboard. Click on the X to remove the filter.


``` eval_rst
.. image:: ../images/countries_downloads9.png
    :width: 400
```

The data contained in the donut graphs can be exported to CSV by using the ‘Options’ popup. Hover to the right of one of the donut graph titles e.g. ‘Top countries/territories for downloads from OAPEN (C5)’. Three dots will appear. 


``` eval_rst
.. image:: ../images/countries_downloads10.png
    :width: 400
```

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder.

### Downloads bar graphs

There are three bar graphs showing the number of monthly downloads by country/territory:
* ‘# monthly downloads from OAPEN by country/territory’
* ‘# monthly chapter downloads from JSTOR by country/territory’
* ‘# monthly downloads from GoogleBooks by country/territory’

Hover over a country/territory in the bar graphs to see the number of downloads per month, from different countries/territories from that data source. 


``` eval_rst
.. image:: ../images/countries_downloads11.png
    :width: 400
```

To look at a specific month/year in more detail, click on a month in the bar graph to add time and country/territory filters. In the ‘Select filters to apply’ popup, select ‘Apply’. 

``` eval_rst
.. image:: ../images/countries_downloads12.png
    :width: 400
```

The bar graph visualisation will now display the number of downloads by country/territory for the selected month. The rest of the dashboard will automatically update to use these filters. 

To remove the Country filter, go to the filter bar at the top left of the dashboard. Click on the X to remove the Country filter. 

``` eval_rst
.. image:: ../images/countries_downloads13.png
    :width: 400
```

To remove the time filter, reload the dashboard by clicking on the ‘Countries and Territories’ link in the navigation menu at the top right of this dashboard.

``` eval_rst
.. image:: ../images/countries_downloads14.png
    :width: 300
```

The dashboard will automatically refresh to show usage data by country for all ISBNs from this publisher. 

The data contained in the bar graphs can be exported to CSV by using the ‘Options’ popup. Hover to the right of one of the bar graph titles e.g. ‘# monthly downloads from OAPEN by country/territory’. Three dots will appear. 

``` eval_rst
.. image:: ../images/countries_downloads15.png
    :width: 400
```

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder.

### Downloads tables
There are three tables showing the number of downloads by country/territory:
* ‘All countries/territories for downloads from OAPEN (C5)’
* ‘All countries/territories for chapter downloads from JSTOR’
* ‘All countries/territories for downloads from GoogleBooks’

Use the slider bar on the right of a table to scroll through the countries and territories.

``` eval_rst
.. image:: ../images/countries_downloads16.png
    :width: 400
```

The data contained in the tables can be exported to CSV by using the ‘Options’ popup. Hover to the right of one of the table titles e.g. ‘All countries/territories for downloads from OAPEN (C5)’. Three dots will appear. 

``` eval_rst
.. image:: ../images/countries_downloads17.png
    :width: 400
```

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder.
