# Data Quality Dashboard

The ‘Data Quality’ dashboard can be used by a publisher to identify issues with data quality, and discrepancies between data from different sources. 

This dashboard has two sections:
* ISBNs with no usage data
* ISBNs with usage data that are not in ONIX

## ISBNs with no usage data

This section shows a table containing any ISBNs from the University of Michigan Press that are in ONIX, but which have no usage data. The table is scrollable by using the scrollbar on the right-hand side. 

``` eval_rst
.. image:: ../images/quality_isbns1.png
    :width: 500
```   

The data in the table can be exported. Click on the three dots beside the ‘Title’ column. 

``` eval_rst
.. image:: ../images/quality_isbns2.png
    :width: 500
```  

Select ‘Download as CSV’ from the ‘Options’ popup menu. Depending on your internet browser, the CSV file may be saved to the Downloads folder. 

``` eval_rst
.. tip::  If you open the CSV in Excel, the ISBN columns may have odd values of something similar to ‘9.78047E+12’. To correct this and show the actual ISBNs in Excel: 1. highlight the ISBN column in Excel, 2. right-click and select ‘Format Cells’ 3. change the ‘Category’ to ‘Number’ with 0 decimal places. The ISBNs will now be displayed as numbers. 
```  

## ISBNs with usage data that are not in ONIX
A comparison of ONIX data to three data sources is provided so that publishers can review their data:
* Number of ISBNs in the OAPEN data source but not in ONIX - ‘# ISBNs in OAPEN that are not in ONIX’
* Number of ISBNs in the JSTOR data source but not in ONIX - ‘# ISBNs in JSTOR that are not in ONIX’
* Number of ISBNs in the Google Books data source but not in ONIX - '# ISBNs in Google Books that are not in ONIX.’

Note, as data sources such as JSTOR and Google Books are not restricted to only open access titles, ISBNs present in this section (that are not present in the Open Access ONIX feed) may belong to gated or closed titles. The OAPEN metadata source also contains both chapters and books, and there may be some inconsistencies in titles between Counter 4 and Counter 5 schemas that cause a title to be presented twice in the data with the same ISBN.

These three tables are scrollable by using the scrollbar on the right-hand side of each table: 

``` eval_rst
.. image:: ../images/quality_isbns3.png
    :width: 400
```  

The three tables can also be exported. Click on the three dots beside the ISBN column in any of the tables. 

``` eval_rst
.. image:: ../images/quality_isbns4.png
    :width: 400
```  

Select ‘Download as CSV’ from the ‘Options’ popup menu:

``` eval_rst
.. image:: ../images/quality_isbns5.png
    :width: 400
```  

Depending on your internet browser, the CSV file may be saved to the Downloads folder. 