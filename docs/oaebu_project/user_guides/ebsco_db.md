# EBSCO Dashboard

EBSCO hosts collections of different publications including eBooks, some of which are open access. They provide data about the usage of these eBooks to publishers, such as University of Michigan Press. The eISBN used in the EBSCO data source is one of the identifiers for eBooks used by some publishers and platforms. To access EBSCO open access book titles, libraries can sign up here:
<https://more.ebsco.com/ebooks-open-access-2021.html>

For the pilot project, the EBSCO data source is a manual data upload that was developed specifically for the University of Michigan Press. Usage data is available from January 2021.

The sections of this dashboard are:
* [Filters](#filters)
* [EBSCO - Summary](#ebsco-summary)
* [EBSCO - Usage](#ebsco-usage)
* [EBSCO - Imprints](#ebsco-imprints)
* [EBSCO - Subjects](#ebsco-subjects)
* [EBSCO - Customers](#ebsco-customers)
* [EBSCO - Markets](#ebsco-markets)

## Filters
A filter is a way to narrow a search and look for more specific information. 

``` eval_rst
.. tip::  Only one filter can be used at a time – ‘Select Book Title(s)’ OR ‘Select ISBN(s)’ OR ‘Select EISBN(s)’.   
```    

If you select a filter then add a second filter, the first filter will be automatically removed. For example, if you ‘Select Book Title(s)’, then ‘Select ISBN(s)’, the ‘Select Book Title(s)’ filter will be automatically removed.  

``` eval_rst
.. note::  A warning message may appear on the filters: ‘Terms list might be incomplete because the request is taking too long ...’. This is expected, and these filters can still be used but the full list of options may not be displayed.   
```   

### Select one value in a filter
Use the dropdown option in any filter field to choose a value e.g. an ISBN in the ‘Select ISBN(s)’ field, or start typing in a filter field to search all values by specific text. There is no search button to click, and the rest of the dashboard will automatically update.

### Select multiple values in a filter
To select multiple values in a specific filter field, keep selecting values one by one. This can be either via the dropdown OR by typing directly in the field and selecting from the matches. 

### Remove value/values from a filter
Selected value(s) of a filter can be removed one at a time, or all removed at the same time. 

For example, to clear one ISBN, click on the X next to the ISBN:

``` eval_rst
.. image:: ../images/ebsco_filters1.png
    :width: 400
```    

To remove multiple ISBNs at the same time, click on the grey circle icon with a white X inside it:

``` eval_rst
.. image:: ../images/ebsco_filters2.png
    :width: 400
```    

Another way to remove applied filters in a dashboard, is to use the filter bar at the top left of the dashboard. Click on the grey X to remove the filter.

``` eval_rst
.. image:: ../images/ebsco_filters3.png
    :width: 400
```    

## EBSCO Summary 
The ‘EBSCO - Summary’ section shows the number of Titles, ISBNs, EISBNs and ‘RetrievalCount’ from EBSCO for any selected filters. When no filters are selected, a summary from the EBSCO data source for all University of Michigan Press titles is displayed in the dashboard. 

The ‘Book Titles’ table shows all book titles from the University of Michigan Press that are available from EBSCO. 

To look at a specific Title, scroll to it in the ‘Book Titles’ table, and hover beside the Title to show the +. Click on the + to apply a Title filter to the ‘EBSCO’ dashboard. All sections in this dashboard will automatically update to show data about this Title. 


``` eval_rst
.. image:: ../images/ebsco_summary1.png
    :width: 500
```    

To remove the Title filter, go to the filter bar at the top left of the dashboard. Click on the X to remove the title filter. 

``` eval_rst
.. image:: ../images/ebsco_summary2.png
    :width: 400
```    

Note: The data shown in this section is from the following variables: title, isbn, eisbn, RetrievalCount. 

### Exporting the data
To export the data from the table to a CSV file, hover in the top right corner of the graph or table to find three dots: 

``` eval_rst
.. image:: ../images/ebsco_summary3.png
    :width: 500
```    

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder. 

## EBSCO Usage
Usage is the total number of times that a title has been requested from EBSCO. The ‘EBSCO - Usage' section shows the usage per month (in a graph and a table). 

All data in this section can be exported to CSV by following the ‘Exporting the data’ instructions in the ‘EBSCO - Summary’ section above.

To look at a specific month in the bar graph in more detail, click on a bar in the bar graph. The dashboard will automatically update to use this time filter. To remove this time filter, reload the dashboard by clicking on the ‘EBSCO’ link in the navigation menu at the top right of this dashboard. 

Note: The data shown in this section is from the following variable: RetrievalCount.

## EBSCO Imprints
The ‘EBSCO - Imprints' section shows the top imprints by usage in a donut graph and a table. 

All data in this section can be exported to CSV by following the ‘Exporting the data’ instructions in the ‘EBSCO - Summary’ section above.

Note: The data shown in this section is from the following variable: ImprintPublisher.

## EBSCO Subjects
The ‘EBSCO - Subjects' section shows the top imprints by usage in a donut graph and a table. 

Click on a subject in the donut graph to select a specific subject. All sections in this dashboard will automatically update. 

Alternatively, scroll to a subject in the table ‘Top Subjects by usage’, and hover beside the ‘Subject’ to show the +. Click on the + to apply a subject filter to the whole ‘EBSCO’ dashboard. 

``` eval_rst
.. image:: ../images/ebsco_subjects1.png
    :width: 500
```    

To remove the Subject filter, go to the filter bar at the top left of the dashboard. Click on the X to remove the Subject filter. 

``` eval_rst
.. image:: ../images/ebsco_subjects2.png
    :width: 400
```  

All data in this section can be exported to CSV by following the ‘Exporting the data’ instructions in the ‘EBSCO - Summary’ section above.

Note: The data shown in this section is from the following variable: Subjects.


## EBSCO Customers
The ‘EBSCO – Customers’ section shows downloads by EBSCO customers as a donut graph and table. 
In the ‘Top Countries of Customers by usage’ donut graph, click on any of the EBSCO customers to apply a filter to the whole dashboard. 

Alternatively, scroll to a customer in the table ‘Top Customers by usage’, and hover beside the ‘Customer’ to show the +. Click on the + to apply a customer filter to the whole ‘EBSCO’ dashboard. 

To remove this filter by Customer, go to the filter bar at the top left of the dashboard. Click on the X to remove the Customer filter. 

Note that some Customer usage in the EBSCO data is for ‘Other’ customers. 

The donut graph and table can be exported to .CSV, see the ‘Exporting the data’ instructions in the ‘EBSCO -Summary’ section above. 

Note: The data shown in this section is from the following variables: Cust_Name, CustId, CountryName, Cust_StateProv. 

## EBSCO Markets
The ‘EBSCO – Markets’ section shows top markets by usage from the EBSCO data, as a donut graph and a table. 

In the ‘Top Markets by usage’ donut graph, click on any of the EBSCO markets to apply a filter to the whole dashboard. 

Alternatively, scroll to a market in the table ‘Top Markets by usage’, and hover beside the ‘Market’ to show the +. Click on the + to apply a market filter to the whole ‘EBSCO’ dashboard. 

To remove this filter by Market, go to the filter bar at the top left of the dashboard. Click on the X to remove the Market filter. 

The donut graph and table can be exported to .CSV, see the ‘Exporting the data’ instructions in the ‘EBSCO -Summary’ section above. 

Note: The data shown in this section is from the following variable: Market. 
