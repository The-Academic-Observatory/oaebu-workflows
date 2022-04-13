# Institutions Dashboard
The ‘Institutions’ dashboard shows usage data (or downloads of eBook chapters) by institutions. For the pilot project, this data comes from JSTOR, and the data is available from January 2018 onwards. For more information about JSTOR
see the [partner data](../overview/partner_data) section of this help guide. 

The sections of this dashboard are:
* [Filters](#filters)
* [Downloads](#downloads)

## Filters 
A filter is a way to narrow a search and look for more specific information. 

``` eval_rst
.. tip::  Only one filter can be used at a time – ‘Select Institution(s)’ OR ‘Select Title(s’) OR ‘Select ISBN(s)’.   
```    

If you use select a filter then and add a second filter, the first filter will be automatically removed. For example, if you ‘Select Institution(s)’, then ‘Select Title(s)’, the ‘Select Institution(s)’ filter will be automatically removed. 

The ‘Downloads’ section will automatically show the number of chapter downloads by institutions for all eBook chapters available from this publisher. Filters can be used to look at:
* Chapter downloads by institution(s) OR
* Chapter downloads for one or multiple Titles across institution(s) OR
* Chapter downloads for one or multiple ISBNs across institution(s).

### Select one value in a filter
Use the dropdown option in any filter field to choose a value e.g. a Title in the ‘Select Title(s)’ field, or start typing in a filter field to search all values by specific text. There is no search button to click, and the rest of the dashboard will automatically update.

### Select multiple values in a filter
To select multiple values in a specific filter field, keep selecting values one by one. This can be either via the dropdown OR by typing directly in the field and selecting from the matches. 

### Remove value/values from a filter
Selected value(s) of a filter can be removed one at a time, or all removed at the same time. 

For example, to clear one Title, click on the X next to the Title: 

``` eval_rst
.. image:: ../images/institutions_filters1.png
    :width: 400
```  

To remove multiple titles at the same time, click on the grey circle icon with a white X inside it:

``` eval_rst
.. image:: ../images/institutions_filters2.png
    :width: 400
```  

Another way to remove applied filters in a dashboard, is to use the filter bar at the top left of the dashboard. Click on the X to remove the filter. 

``` eval_rst
.. image:: ../images/institutions_filters3.png
    :width: 500
```  

## Downloads
In the ‘Downloads’ section, data about chapter downloads and institutions is presented in different ways:
* Monthly chapter downloads bar graph - ‘# monthly chapter downloads from JSTOR by the top 10 institutions’
* Top 1000 institutions table - ‘Top 1000 institutions for # chapter downloads from JSTOR’
* Top 20 institutions word cloud - ‘Top 20 institutions for # chapter downloads from JSTOR from Jan 2018’
* Top institutions and titles table - ‘Top institutions and Titles by # chapter downloads JSTOR’

Note: The data shown in this section is from the following variable: JSTOR chapters (Total_Item_Requests).

### Monthly chapter downloads bar graph
The bar graph ‘# monthly chapter downloads from JSTOR by the top 10 institutions’ shows the number of chapter downloads per institution by month, for the 10 institutions with the highest total of chapter downloads from JSTOR since January 2018. 

Hover over a month bar in this bar graph to see the number of chapter downloads for each of the top 10 institutions.

``` eval_rst
.. image:: ../images/institutions_downloads1.png
    :width: 400
```  

The data contained in the graph can be exported to CSV by using the ‘Options’ popup. Hover to the right of the bar graph title ‘# monthly chapter downloads from JSTOR by the top 10 institutions’. Three dots will appear. 

``` eval_rst
.. image:: ../images/institutions_downloads2.png
    :width: 600
```  

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder.

To look at a specific month and institution in more detail, click on a month and institution in the bar graph. In the example in the next screenshot, the ‘Macquarie University’ part of the 2021-08-01 bar was clicked on. 

``` eval_rst
.. image:: ../images/institutions_downloads3.png
    :width: 400
```  

In the ‘Select filters to apply’ popup, select ‘Apply’. The dashboard will update to show data for this month and institution. 

``` eval_rst
.. image:: ../images/institutions_downloads4.png
    :width: 400
```  

``` eval_rst
.. note::  Clicking on the ‘Other’ section of a bar graph will cause incorrect filters to be applied, as ‘Other’ represents combined data from multiple institutions.   
```   

To remove the institution filter, go to the filter bar at the top left of the dashboard. Click on the X to remove the filter.

``` eval_rst
.. image:: ../images/institutions_downloads5.png
    :width: 400
```  

To remove the time filter, reload the dashboard by clicking on the ‘Institutions’ link in the navigation menu at the top right of this dashboard.

``` eval_rst
.. image:: ../images/institutions_downloads6.png
    :width: 300
```  

The dashboard will automatically refresh to show downloads for all institutions from this publisher. 

### Top 1000 institutions table
The ‘Top 1000 institutions for # chapter downloads from JSTOR’ table shows the top institutions ranked in order of highest downloads for this publisher. By default this shows downloads for all titles from this publisher, but it will also show top institutions for whatever subset of books you have selected via the Filter section of the top of the ‘Institutions’ dashboard.

In this table, you can select one institution to see downloads for just that institution. Click on the + sign in a circle beside an institution. 

``` eval_rst
.. image:: ../images/institutions_downloads7.png
    :width: 500
```  

To clear an institution that was selected in the table, go to the filter bar at the top left of the dashboard. Click on the X to remove the filter.

``` eval_rst
.. image:: ../images/institutions_downloads8.png
    :width: 400
```  

Do not use the - sign in a circle to try to remove the filter. This will add another filter that excludes the selected institution.

``` eval_rst
.. image:: ../images/institutions_downloads9.png
    :width: 500
```  

The data contained in the table can be exported to CSV by using the ‘Options’ popup. Hover to the right of the table title ‘Top 1000 institutions for # chapter downloads from JSTOR’. Three dots will appear. 

``` eval_rst
.. image:: ../images/institutions_downloads10.png
    :width: 500
```  

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder. 

### Top 20 institutions word cloud 
The word cloud ‘Top 20 institutions for # chapter downloads from JSTOR’ shows the top 20 institutions that had the highest number of chapter downloads, using JSTOR as a data source. Note that this data source starts from January 2018.

If specific institutions have been selected in the filters at the top of the dashboard, then this section will reflect only those institutions. 

Click on any of the institution names to see downloads for just that single institution applied across this ‘Institutions’ dashboard. For example, click on ‘University of Edinburgh’ in the word cloud:

``` eval_rst
.. image:: ../images/institutions_downloads11.png
    :width: 600
```  

The dashboard will update to show data for only the selected institution, including monthly chapter downloads from JSTOR and the top titles.

To return to seeing data for all institutions, go to the top of the dashboard. In the filter bar at the top left of the dashboard, click on X to remove the filter being ‘University of Edinburgh’ in this example.

``` eval_rst
.. image:: ../images/institutions_downloads12.png
    :width: 400
```  

### Top institutions and titles table
The ‘Top institutions and Titles by # chapter downloads JSTOR’ table shows the Titles with the highest chapter downloads per institution. This means that there are duplicate ISBNs and titles in the ‘ISBN’ and ‘Title’ columns of this table that show downloads for different institutions. 

In this table, you can select one institution to see downloads for just that institution. Click on the + sign in a circle beside an institution. 

``` eval_rst
.. image:: ../images/institutions_downloads13.png
    :width: 500
```  

It is also possible to select a particular ISBN or title by clicking on the + sign in a circle beside an ISBN or Title. 

To clear an institution, ISBN or title that was selected in the table, go to the filter bar at the top left of the dashboard. Click on the X to remove the filter.

``` eval_rst
.. image:: ../images/institutions_downloads14.png
    :width: 400
```  

Do not use the - sign in a circle to try to remove the filter. This will add another filter that excludes the selected institution, ISBN or title.

``` eval_rst
.. image:: ../images/institutions_downloads15.png
    :width: 400
```  

The data contained in the table can be exported to CSV by using the ‘Options’ popup. Hover to the right of the table title ‘Top institutions and Titles by # chapter downloads JSTOR’. Three dots will appear. 

``` eval_rst
.. image:: ../images/institutions_downloads16.png
    :width: 600
```  

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder.
