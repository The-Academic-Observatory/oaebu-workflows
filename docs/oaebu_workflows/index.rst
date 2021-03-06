Book Usage Data Workflows
##############################

Book Usage Data Workflows provides Apache Airflow workflows for fetching, processing and analysing data about Open Access Books.

The workflows include: Directory of Open Access Books (DOAB), Google Analytics, Google Books, JSTOR, Oapen IRUS UK,
Oapen Metadata, Onix, UCL Discovery and an Onix Workflow for combining all of this data.

Telescope Workflows
====================
A telescope is a type of workflow used to ingest data from different data sources, and to run workflows that process and
output data to other places. Workflows are built on top of Apache Airflow's DAGs.

.. toctree::
    :maxdepth: 1

    telescopes/index

Analytic Workflows
===================
Analytic workflows process the data ingested by telescope workflows and are also built on top of Apache Airflow DAGs.

.. toctree::
    :maxdepth: 1

    workflows/index


License & Contributing Guidelines
=================================
Information about licenses, contributing guidelines etc.

.. toctree::
    :maxdepth: 1

    license

Python API Reference
=====================
This page contains auto-generated API reference documentation [#f1]_.

.. toctree::
    :maxdepth: 3

    api/oaebu_workflows/index

.. [#f1] Created with `sphinx-autoapi <https://github.com/readthedocs/sphinx-autoapi>`_