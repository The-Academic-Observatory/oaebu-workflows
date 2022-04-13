# Mentions and Events Dashboard

Crossref Event Data captures online discussion about research outputs including eBooks, such as ‘a citation in a dataset or patent, a mention in a news article, Wikipedia page or on a blog, or discussion and comment on social media’ (<https://www.crossref.org/services/event-data/>).

For the pilot project, the Crossref Event data for the University of Michigan Press is from May 2018 onwards, and the event sources are:
* Crossref - relationships, references, and links to DataCite registered content
* Datacite - links to Crossref registered content
* Twitter - Mentions in tweets
* Wikipedia - References on Wikipedia pages

The ‘Mentions and Events’ dashboard shows Crossref Event data from this publisher for all ISBNs by default. The filters can be used to select specific event sources, Title(s) or ISBN(s). 

The sections of this dashboard are:
* [Filters](#filters)
* [Mentions and Events](#mentions-and-events)

## Filters

A filter is a way to narrow a search and look for more specific information. 

``` eval_rst
.. tip::  Only one filter can be used at a time – ‘Select event source(s) from Crossref Events’ OR ‘Select Title(s)’ OR ‘Select ISBN(s)’.    
```    

If you select a filter then add a second filter, the first filter will be automatically removed. For example, if you use ‘Select Title(s)’, then ‘Select ISBN(s)’, the ‘Select Title(s)’ filter will be automatically removed. 

### Select one value in a filter
Use the dropdown option in any filter field to choose a value e.g. a Title in the ‘Select Title(s)’ field, or start typing in a filter field to search all values by specific text. There is no search button to click, and the rest of the dashboard will automatically update.

### Select multiple values in a filter
To select multiple values in a specific filter field, keep selecting values one by one. This can be either via the dropdown OR by typing directly in the field and selecting from the matches. 

### Remove value/values from a filter
Selected value(s) of a filter can be removed one at a time, or all removed at the same time. 
For example, to clear one Title, click on the X next to the Title:

``` eval_rst
.. image:: ../images/mentions_filters1.png
    :width: 400
```  

To remove multiple titles at the same time, click on the grey circle icon with a white X inside it:

``` eval_rst
.. image:: ../images/mentions_filters2.png
    :width: 400
```  

Another way to remove applied filters in a dashboard, is to use the filter bar at the top left of the dashboard. Click on the X to remove the filter. 

``` eval_rst
.. image:: ../images/mentions_filters3.png
    :width: 600
```  

## Mentions and Events

The ‘Mentions and Events’ section shows data from Crossref in four ways:
* Events as a donut graph - ‘# events from Crossref’
* Events by source as a table - ‘# Crossref events by source’
* Events per month as a bar graph - ‘# events per month from Crossref’
* Top titles table - ‘Top titles by # Crossref events’

### Events as a donut graph

Hover over a Crossref Event source in the ‘# events from Crossref’ donut graph to see the number of mentions and events from that event source. The event sources are Crossref, Datacite, Twitter and Wikipedia. 

Click on any Crossref source e.g. Twitter in a donut graph to update the dashboard to show mentions and events for just that Crossref source. 

To clear a Crossref source that was selected in the donut graph, go to the filter bar at the top left of the dashboard. Click on the X to remove the filter.

``` eval_rst
.. image:: ../images/mentions_events1.png
    :width: 400
```  

The data contained in the donut graph can be exported to CSV by clicking on the three dots beside the graph title ‘# events from Crossref’, and selecting ‘Download as CSV’ from the ‘Options’ popup. 

``` eval_rst
.. image:: ../images/mentions_events2.png
    :width: 600
```  

### Events by source as a table

The ‘# Crossref events by source’ table shows a breakdown of the Crossref events from different sources (Crossref, Datacite, Twitter and Wikipedia). 

The data contained in this table can be exported to CSV by clicking on the three dots beside the table title ‘# Crossref events by source’, and selecting ‘Download as CSV’ from the ‘Options’ popup. 

``` eval_rst
.. image:: ../images/mentions_events3.png
    :width: 500
```  

### Events per month as a bar graph

The ‘# events per month from Crossref’ bar graph shows the Crossref events per month from different sources (Crossref, Datacite, Twitter and Wikipedia). 

Click on a month and bar section in ‘# events per month from Crossref’ to see more detail for a specific month and source of Crossref events (Crossref, Datacite, Twitter and Wikipedia). 

``` eval_rst
.. image:: ../images/mentions_events4.png
    :width: 300
```  

In the popup ‘Select filters to apply’, select ‘Apply’. 

``` eval_rst
.. image:: ../images/mentions_events5.png
    :width: 400
```  

To clear a month filter, reload the dashboard by clicking on the ‘Mentions and Events’ link in the navigation menu at the top right of this dashboard. 

``` eval_rst
.. image:: ../images/mentions_events6.png
    :width: 300
```  

The data contained in this table can be exported to CSV by clicking on the three dots beside the graph title ‘# events per month from Crossref’, and selecting ‘Download as CSV’ from the ‘Options’ popup. 

``` eval_rst
.. image:: ../images/mentions_events7.png
    :width: 600
```  


### Top titles table

The ‘Top titles by # Crossref events’ table shows the titles ranked by the highest number of Crossref events per title (includes Crossref, Datacite, Twitter and Wikipedia). 

The data contained in this table can be exported to CSV by clicking on the three dots beside the table title ‘Top titles by # Crossref events’, and selecting ‘Download as CSV’ from the ‘Options’ popup. 

``` eval_rst
.. image:: ../images/mentions_events8.png
    :width: 600
```  
