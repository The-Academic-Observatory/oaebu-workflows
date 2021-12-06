# ONIX workflow Step 2 - Linking Metircs

Step 2 of the ONIX workflow is all about linking and aggregrating the metrics, which have arrived through various telescopes, to the book records in the publishers ONIX feed.

``` eval_rst
.. image:: ../static/onix_workflow_2.png
   :width: 650
```

## Book Product Schema

The output is the book_product table, containing one row per unique book, with a nested month field, which groups all the metrics relating to that book for each calendar month.

``` eval_rst
.. csv-table::
   :file: ../schemas/book_product_latest.csv
   :width: 100%
   :header-rows: 1
```