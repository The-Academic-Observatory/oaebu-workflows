# ONIX workflow Step 2 - Linking Metircs

Step 2 of the ONIX workflow takes the metrics fetched through various telescopes, then aggregates and joins them to the book records in the publisher's ONIX feed.

``` eval_rst
.. image:: ../static/onix_workflow_2.png
   :width: 650
```

## Book Product Schema

The output is the book_product table, containing one row per unique book, with a nested month field, which groups all the metrics relating to that book for each calendar month.

[Link to Query](https://github.com/The-Academic-Observatory/oaebu-workflows/blob/develop/oaebu_workflows/database/sql/create_book_products.sql.jinja2)

``` eval_rst
.. csv-table::
   :file: ../schemas/book_product_latest.csv
   :width: 100%
   :header-rows: 1
```