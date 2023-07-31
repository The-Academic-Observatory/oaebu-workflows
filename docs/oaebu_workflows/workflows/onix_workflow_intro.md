Analytic workflows introduction
================================

Analytic workflows process the data ingested by telescope workflows and are also built on top of Apache Airflow DAGs.

``` eval_rst
.. image:: ../static/onix_workflow_all.png
    :width: 650

The pilot project has one core workflow, which is broken into three parts and described below. The parts are:
 1. Aggregating and Mapping book products into works and work families
 2. Linking data from metric providers to book products
 3. Creating export tables for visualisation in dashboards

