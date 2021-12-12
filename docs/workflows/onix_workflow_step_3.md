# ONIX workflow Step 3 - Exporting to Elasticsearch

Step three of the ONIX workflow is to export the book_product table to a sequence of flattened tables that can be exported to Elasticsearch. The data in these tables is not materially different to the book product table, just organised in a way, better suited for dashboards in Kibana.

``` eval_rst
.. image:: ../static/onix_workflow_3.png
   :width: 650
```

## Book Metric

### Book Product List Schema

This table is a list of each Book Product. It is primarily used for drop-down fields, or where a list of all the books independent of metrics is desired.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_list.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/oaebu_publisher_book_product_list_latest.csv
   :width: 100%
   :header-rows: 1
```

### Book Product Metrics Schema

This table contains metrics, organised by month, that are linked to each book. The country, city, institution, events and referrals expand on this to provided further useful breakdowns of metrics.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_metrics.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/oaebu_publisher_book_product_metrics_latest.csv
   :width: 100%
   :header-rows: 1
```

### Book Product Year Metrics Schema

This table contains metrics, organised by month, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_year_metrics.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/oaebu_publisher_book_product_year_metrics_latest.csv
   :width: 100%
   :header-rows: 1
```

### Book Product Metrics City Schema

This table contains metrics, organised by month and city of measured usage, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_metrics_city.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/oaebu_publisher_book_product_metrics_city_latest.csv
   :width: 100%
   :header-rows: 1
```

### Book Product Metrics Country Schema

This table contains metrics, organised by month and country of measured usage, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_metrics_country.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/oaebu_publisher_book_product_metrics_country_latest.csv
   :width: 100%
   :header-rows: 1
```

### Book Product Metrics Events Schema

This table contains metrics, organised by month and crossref event type, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_metrics_event.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/oaebu_publisher_book_product_metrics_events_latest.csv
   :width: 100%
   :header-rows: 1
```

### Institution List Schema

This table is a list of each unique Institution where metrics are linked too. It is primarily used for drop-down fields, or where a list of all the books independent of metrics is desired.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_metrics_institution.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/oaebu_publisher_institution_list_latest.csv
   :width: 100%
   :header-rows: 1
```

### Book Product Metrics Institutions Schema

This table contains metrics, organised by month and institution for which there is measured activity linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_institution_list.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/oaebu_publisher_book_product_metrics_institution_latest.csv
   :width: 100%
   :header-rows: 1
```

### Book Product Metrics Referrer Schema

This table contains metrics, organised by month and location of referrals, that are linked to each book. Referrals are gathered using a standard feature of the modern web, where new requests to a website often contain a referral field that specifies the website where a link was clicked that brought the end user to your website.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_metrics_referrer.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/oaebu_publisher_book_product_metrics_referrer_latest.csv
   :width: 100%
   :header-rows: 1
```

## Book Subject Metrics

### Book Product Subjects BIC Schema

This table contains metrics, organised by month and BIC subject type, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_subject_bic_metrics.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/oaebu_publisher_book_product_subject_bic_metrics_latest.csv
   :width: 100%
   :header-rows: 1
```

### Book Product Subjects BISAC Schema

This table contains metrics, organised by month and BISAC subject type, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_subject_bisac_metrics.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/oaebu_publisher_book_product_subject_bisac_metrics_latest.csv
   :width: 100%
   :header-rows: 1
```

### Book Product Subjects THEMA Schema

This table contains metrics, organised by month and THEMA subject type, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_subject_thema_metrics.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/oaebu_publisher_book_product_subject_thema_metrics_latest.csv
   :width: 100%
   :header-rows: 1
```

### Book Product Subject Year Schema

This table contains metrics, organised by year and currently just the BIC subject type, that are linked to each book.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_book_subject_year_metrics.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/oaebu_publisher_book_product_subject_year_metrics_latest.csv
   :width: 100%
   :header-rows: 1
```

## QA Related Datasets

### Unmatched Book Metrics Schema

This dataset is helpful for understanding where metrics and books defined in the onix feed are not matched. Helping target data quality tasks upstream of this workflow.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/export_unmatched_metrics.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/oaebu_publisher_unmatched_book_metrics_latest.csv
   :width: 100%
   :header-rows: 1
```