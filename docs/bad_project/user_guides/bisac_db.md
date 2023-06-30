# BISAC Subjects Dashboard

This dashboard shows usage data by Book Industry Standards and Communications (BISAC) subject code, where the BISAC subject code is a way of classifying what a book is about. Examples of BISAC subject codes are ‘Education’ and ‘Mathematics’.  

More information about the BISAC Subject Codes is available at these links:
* BISAC Subject Codes description - <https://bisg.org/general/custom.asp?page=BISACSubjectCodes>  
* BISAC Subject Codes FAQ - <https://bisg.org/page/BISACFaQ> 
* List of codes - <https://bisg.org/page/BISACEdition>

The sections of this dashboard are:
* [Filter](#filter) 
* [Summary](#summary)
* [Downloads by BISAC subject](#downloads-by-bisac-subject)
* [Views by BISAC subject](#views-by-bisac-subject)
* [Mentions and Events](#mentions-and-events)

## Filter
There is one filter in this dashboard – ‘Select BISAC Subject(s)’. By default, data will be shown for all BISAC subjects where usage data is available. 

### Select one value in a filter
Use the dropdown in the filter to choose a value, or start typing in a filter field to search all values. There is no search button, the rest of the page will automatically update.

### Select multiple values in a filter
To select multiple values in the filter, keep selecting values one by one. This can be either using the dropdown OR by typing directly in the field’s text box and selecting from the matches. 

### Remove value/values from a filter
Selected values of a filter can be removed one at a time, or all removed at the same time. 
For example, to clear one author, click on the X next to the author:

``` eval_rst
.. image:: ../images/bisac_filters1.png
    :width: 400
```    

To remove multiple authors at the same time, click on the grey circle icon with a white X inside it:

``` eval_rst
.. image:: ../images/bisac_filters2.png
    :width: 400
```    

Another way to remove applied filters in a dashboard, is to use the filter field in the top left of the dashboard. Click on the grey x to remove the filter. 

``` eval_rst
.. image:: ../images/bisac_filters3.png
    :width: 400
```    

## Summary
### List of BISAC subjects
The summary section contains a table ‘List of BISAC subjects with usage metrics’ showing the number of Products (eBooks) with usage data per BISAC subject. 

The data contained in the table can be exported to CSV. Hover to the right of the table name ‘List of BISAC subjects with usage metrics’. Three dots will appear. 

``` eval_rst
.. image:: ../images/bisac_summary1.png
    :width: 400
```    

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder. Note that this CSV export also includes the number of ISBNs per BISAC subject. 

### Number of products with usage metrics
The graph ‘Monthly # Products with usage metrics by BISAC subject’ shows the number of products (eBooks) per BISAC subject for each month. By default the graph will show data from 2018. Each BISAC subject has a different coloured line in the graph. 

You can hover over the graph to see the BISAC subject breakdown per month. 

``` eval_rst
.. image:: ../images/bisac_summary2.png
    :width: 400
```    

Clicking on any month and BISAC subject in the graph will filter on it and apply this month and subject to the whole dashboard. After you click in the graph, you will see a ‘Select filters to apply’ popup. Check these are the filters you require, and click on ‘Apply’. 

``` eval_rst
.. image:: ../images/bisac_summary3.png
    :width: 400
```    

The whole dashboard will update to show data for one month and one BISAC subject. 

To remove the month and subject filters, go to the filter bar at the top left of the dashboard. Click on the grey X to remove the subject filter. 

``` eval_rst
.. image:: ../images/bisac_summary4.png
    :width: 400
```   

Then, to remove the time filter, reload the dashboard by clicking on the ‘Subjects - BISAC’ link in the navigation menu at the top right of this dashboard. 

``` eval_rst
.. image:: ../images/bisac_summary5.png
    :width: 400
```   

The data contained in the graph can be exported to CSV by clicking on the three dots beside the graph title ‘Monthly # Products with usage metrics by BISAC subject’, and selecting ‘Download as CSV’ from the ‘Options’ popup. 

## Downloads by BISAC subject

The ‘Downloads by BISAC subject’ section contains three bar graphs:
* ‘Top BISAC subjects by # downloads from OAPEN (Counter 5)’ - this usage data starts from April 2020. 
* ‘Top BISAC subjects by # downloads from GoogleBooks’ - this usage data starts from September 2020. 
* ‘Top BISAC subjects by # downloads from JSTOR’ - this usage data starts from May 2018. 

In each bar graph, you can hover over a section of a coloured bar to see the number of downloads per subject for each month. 

To apply a month and BISAC subject to the whole dashboard, click on any month and its BISAC subject section to filter on it. After you click in the graph, you will see a ‘Select filters to apply’ popup. Check these are the filters you require, and click on ‘Apply’. The whole dashboard will then update to show data for one month and one BISAC subject. 

To remove the month and subject filters, follow the instructions in the Summary section. 

The data in each bar graph can be exported to CSV by following the instructions in the 
Summary section above.  Note that the three dots to open the ‘Options’ popup are beside the graph title e.g. ‘Top BISAC subjects …’, not beside the source name e.g. OAPEN. 

``` eval_rst
.. image:: ../images/bisac_downloads1.png
    :width: 400
```   

Note: The data shown in this section is from the following variables: OAPEN (total_item_requests), Google Books (google_books_sales.qty), JSTOR chapters (Total_Item_Requests). 

## Views by BISAC subject
The ‘Views by BISAC subject’ section contains one bar graph ‘Top BISAC subjects by # views from GoogleBooks’. This graph shows the number of views on Google Books per BISAC subject. You can hover over a section of a coloured bar to see the BISAC subject breakdown per month. 

To apply a month and BISAC subject to the whole dashboard, click on any month and its BISAC subject section to filter on it. After you click in the graph, you will see a ‘Select filters to apply’ popup. Check these are the filters you require, and click on ‘Apply’. The whole dashboard will then update to show the number of views from Google Books for one month and one BISAC subject.

To remove the month and subject filters, follow the instructions in the Summary section. 

Note: The data shown in this section is from the following variables: Google Books (BV_with_Pages_Viewed). 

## Mentions and Events
The ‘Mentions and Events’ section contains one bar graph, ‘Top BISAC subjects by # Crossref Events’. 
Crossref Event Data captures online discussion about research outputs, such as ‘a citation in a dataset or patent, a mention in a news article, Wikipedia page or on a blog, or discussion and comment on social media’ (<https://www.crossref.org/services/event-data/>)

This graph shows the number of mentions and events captured by Crossref Events per BISAC subject. You can hover over a section of a coloured bar to see the BISAC subject breakdown for Crossref Events per month. 

To apply a month and BISAC subject to the whole dashboard, click on any month and its BISAC subject section to filter on it. After you click in the graph, you will see a ‘Select filters to apply’ popup. Check these are the filters you require, and click on ‘Apply’. The whole dashboard will update to show the number of Crossref Events for one month and one BISAC subject. 

To remove the month and subject filters, follow the instructions in the Summary section. 

Note: The data shown in this section is from the following variables: Crossref Events (crossref_events.count).

