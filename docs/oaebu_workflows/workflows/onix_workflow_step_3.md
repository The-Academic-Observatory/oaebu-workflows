# ONIX workflow Step 3 - Exporting to Looker Studio

Step three of the ONIX workflow is to export the book_product table to a sequence of flattened data export tables. The data in these tables is not materially different to the book product table, just organised in a way better suited for dashboards in Looker Studio.

```eval_rst
.. image:: ../static/onix_workflow_3.png
   :width: 650
```

Since these are date-sharded tables, their names will be updated each time the workflow is run. When using Google's Looker (previously Data Studio), it is preferable for us to use a static naming scheme. For this reason, after creating the (sharded) _export_ and _quality analysis_ tables, we also create/update a _view_ for table. These views have a static name. By referencing the view, we can keep the Looker dashboards up-to-date without manual intervention.

## Book Metric

### Book Product List Schema

This table is a list of each Book Product. It is primarily used for drop-down fields, or where a list of all the books independent of metrics is desired.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_list.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_book_product_list_latest.csv
   :width: 100%
   :header-rows: 1
   
```

### Book Product Metrics Schema

This table contains metrics, organised by month, that are linked to each book. The country, city, institution, events and referrals expand on this to provided further useful breakdowns of metrics.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_metrics.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_book_product_metrics_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Book Product Author Metrics Schema

This table contains metrics, organised by month and author, that are linked to each author.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_author_metrics.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_book_product_author_metrics_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Book Product Year Metrics Schema

This table contains metrics, organised by published year and month, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_year_metrics.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_book_product_year_metrics_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Book Product Event Metrics Schema

This table contains metrics, organised by month and crossref event type, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_metrics_event.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_book_product_metrics_events_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Book Product Metrics City Schema

This table contains metrics, organised by month and city of measured usage, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_metrics_city.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_book_product_metrics_city_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Book Product Metrics Country Schema

This table contains metrics, organised by month and country of measured usage, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_metrics_country.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_book_product_metrics_country_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Book Product Metrics Events Schema

This table contains metrics, organised by month and crossref event type, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_metrics_event.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_book_product_metrics_events_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Institution List Schema

This table is a list of each unique Institution where metrics are linked too. It is primarily used for drop-down fields, or where a list of all the institutions independent of metrics is desired.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_metrics_institution.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_institution_list_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Book Product Metrics Institutions Schema

This table contains metrics, organised by month and institution for which there is measured activity linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_institution_list.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_book_product_metrics_institution_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Book Product Metrics Publisher Schema

This index contains a summary of metrics, organised by month that are linked to each publisher.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_publisher_metrics.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_book_product_publisher_metrics_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Book Product Subjects BIC Schema

This table contains metrics, organised by month and BIC subject type, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_subject_bic_metrics.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_book_product_subject_bic_metrics_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Book Product Subjects BISAC Schema

This table contains metrics, organised by month and BISAC subject type, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_subject_bisac_metrics.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_book_product_subject_bisac_metrics_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Book Product Subjects THEMA Schema

This table contains metrics, organised by month and THEMA subject type, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_subject_thema_metrics.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_book_product_subject_thema_metrics_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

### Book Product Subject Year Schema

This table contains metrics, organised by published year and month and currently just the BIC subject type, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_subject_year_metrics.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_book_product_subject_year_metrics_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

## QA Related Datasets

### Unmatched Book Metrics Schema

This dataset is helpful for understanding where metrics and books defined in the onix feed are not matched. Helping target data quality tasks upstream of this workflow.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_unmatched_metrics.sql.jinja2)

```eval_rst
.. tabularcolumns:: |p{4.5cm}|l|l|p{6cm}| 
.. csv-table::
   :file: ../../schemas/oaebu_publisher_unmatched_book_metrics_latest.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

