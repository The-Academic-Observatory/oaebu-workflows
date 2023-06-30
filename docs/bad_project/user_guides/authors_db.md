# Authors and Volume Editors Dashboard

The ‘Authors/Volume Editors’ dashboard provides data about titles per author(s) or volume editor(s). 
By default this dashboard shows data for all authors and editors of open access eBooks for this publisher. When specific author(s) and/or volume editor(s) are selected in the ‘Select Author(s)/Volume Editor(s)’ field, the ‘Downloads’, ‘Views’ and ‘Events referencing these books’ sections will be automatically updated. 

The sections of this dashboard are:
* [Filter](#filter)
* [Downloads](#downloads)
* [Views](#views)
* [Events referencing these books](#events-referencing-these-books)

The ‘Authors/Volume Editors’ dashboard does not include a filter for titles. To see a list of titles with usage data for an individual author, use the [‘Book ISBNs’ dashboard](./book_isbns_db.md). Add the author in the ‘Select Author’ filter, then scroll down to view the ‘Downloads’, ‘Views’ and ‘Mentions and Events’ sections including the author’s titles.

## Filter
There is one filter in this dashboard – ‘Select Author(s)/Volume Editor(s)’. By default, data will be shown for all authors and editors where usage data is available.  

### Select one value in the filter
Use the dropdown option in the filter to choose a value, or start typing in a filter field to search all values. There is no search button, the rest of the page will automatically update.

### Select multiple values in the filter
To select multiple values in the filter, keep selecting values one by one. This can be either via the dropdown OR by typing directly in the field’s text box and selecting from the matches. 

### Remove value/values from the filter
Selected values of a filter can be removed one at a time, or all removed at the same time. 
For example, to clear one author/editor, click on the X next to the author/editor:

``` eval_rst
.. image:: ../images/authors_filters1.png
    :width: 400
```    

To remove multiple authors at the same time, click on the grey circle icon with a white X inside it:

``` eval_rst
.. image:: ../images/authors_filters2.png
    :width: 400
```    

## Downloads 
The ‘Downloads’ section contains three bar graphs:
* ‘# monthly downloads OAPEN’
* ‘# monthly downloads GoogleBooks’
* ‘# monthly downloads JSTOR’

In each bar graph, you can hover over a bar to see the number of downloads per month for the authors/editors selected in the filter ‘Select Author(s)/Volume Editor(s)’. If no authors/editors are selected in the filter, data will be shown for all authors/editors where usage data is available.  

To apply a month to the whole dashboard, click on any month in a bar graph. To remove the month filter, reload the dashboard by clicking on the ‘Authors/Volume Editors’ link in the navigation menu at the top right of this dashboard.

The data contained in the graphs can be exported to CSV by using the ‘Options’ popup. Hover to the right of one of the bar graph titles e.g. ‘# monthly downloads OAPEN’. Three dots will appear. 


``` eval_rst
.. image:: ../images/authors_downloads1.png
    :width: 600
```    

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder.

Note: The data shown in this section is from the following variables: OAPEN (title_requests, then total_item_requests from April 2020), Google Books (google_books_sales.qty). 

## Views
The ‘Views’ section contains one bar graph – ‘# monthly views from GoogleBooks’. You can hover over a bar to see the number of views per month from Google Books for the authors/editors selected in the filter ‘Select Author(s)/Volume Editor(s)’. If no authors/editors are selected in the filter, data will be shown for all authors and editors where usage data is available.  

To apply a month to the whole dashboard, click on any month in the bar graph. To remove the month filter, reload the dashboard by clicking on the ‘Authors/Volume Editors’ link in the navigation menu at the top right of this dashboard.

The data contained in the graphs can be exported to CSV by using the ‘Options’ popup. Hover to the far right of the bar graph title ‘# monthly views from GoogleBooks’. Three dots will appear. 

``` eval_rst
.. image:: ../images/authors_views1.png
    :width: 600
```    

Click on the three dots to open the ‘Options’ popup, and select ‘Download as CSV’. Depending on your internet browser, the CSV file may be saved to the Downloads folder.
Note: The data shown in this section is from the following variable: Google Books (Google Analytics unique_views).

## Events referencing these books
The ‘Events referencing these books’ section contains one bar graph – ‘# Crossref events’. You can hover over a bar to see the number of Crossref Events for the authors/editors selected in the filter ‘Select Author(s)/Volume Editor(s)’. If no authors/editors are selected in the filter, data will be shown for all authors and editors where usage data is available.  

To apply a month to the whole dashboard, click on any month in the bar graph. To remove the month filter, reload the dashboard by clicking on the ‘Authors/Volume Editors’ link in the navigation menu at the top right of this dashboard. 

The data contained in the graphs can be exported to CSV by using the ‘Options’ popup. Hover to the far right of the bar graph title ‘# Crossref events’. Three dots will appear. 

``` eval_rst
.. image:: ../images/authors_events1.png
    :width: 600
```    

Note: The data shown in this section is from the following variable: Mentions in social media etc. from Crossref Events (crossref_events.count)