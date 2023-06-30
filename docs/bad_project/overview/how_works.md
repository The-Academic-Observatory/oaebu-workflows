How the BAD Project Works
==========================

The BAD project technology stack uses the book industry metadata interchange standard ONIX, combined with open bibliographic metadata (Crossref and OAPEN) to integrate usage data from multiple sources. Data integration through the Book Usage Data Workflows code base is built on an open-source workflow system written in Python. Data workflows (or telescopes) fetch, process, disambiguate and analyse data about open access eBooks from multiple sources. This data is saved to Google Cloud’s BigQuery data warehouse. The multiple workflows include: 

 1. Ingesting data via telescope workflows from Crossref-Metadata, Crossref-Events, Google Analytics, Google Books, JSTOR, IRUS Fulcrum, IRUS OAPEN, ONIX, Thoth, UCL Discovery, and

 2.	A series of analytic workflows to process and combine the data ingested by the telescope workflows.

The processed data in the Google Cloud BiqQuery data warehouse is then visualised in dashboards provided by Looker Studio, a dashboarding solution offered by Google.
The information from our data sources is refreshed on a regular basis, keeping the Dashboard up-to-date. Data access permissions flow through from the underlying sources into the cloud database and the Dashboard. Stakeholder data is sandboxed into separate areas with user access permissions controlling access for each area, providing strong security and privacy. 