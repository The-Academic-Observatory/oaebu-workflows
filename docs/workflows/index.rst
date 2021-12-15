Analytic Workflows
=======================
Analytic workflows process the data ingested by telescope workflows and are also built on top of Apache Airflow DAGs.

.. image:: ../static/onix_workflow_all.png
   :width: 650

The OAEBU Project has one core workflow, which is broken into three parts and described below. The parts are:
 1. Aggregating and Mapping book products into works and work families
 2. Linking data from metric providers to book products
 3. Exporting these linked metrics to Elasticsearch for viewing in Dashboards

.. toctree::
    :maxdepth: 1

    onix_workflow_step_1
    onix_workflow_step_2
    onix_workflow_step_3
