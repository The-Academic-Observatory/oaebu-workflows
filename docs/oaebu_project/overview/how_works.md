How the Pilot Project Works
==========================

The pilot project technology stack uses the book industry metadata interchange standard ONIX, combined with open bibliographic metadata (Crossref and OAPEN) to integrate usage data from multiple sources. Data integration through the Book Usage Data Workflows code base is built on an open-source workflow system written in Python. Data workflows (or telescopes) fetch, process, disambiguate and analyse data about open access eBooks from multiple sources. This data is saved to Google Cloud’s BigQuery data warehouse. The multiple workflows include: 

 1. Ingesting data via telescope workflows from DOAB, Crossref-Metadata, Crossref-Fundref, Crossref-Events, Unpaywall, ORCID, Google Analytics, Google Books, JSTOR, OAPEN IRUS UK, OAPEN Metadata, ONIX, UCL Discovery, and

 2.	A series of analytic workflows to process and combine the data ingested by the telescope workflows.

In the pilot project, data sources were automatically ingested via telescopes, or manually imported via data uploads. The manual data uploads were completed for a select number of data sources for two of the dashboard partners (Project Muse, Fulcrum and EBSCO for the University of Michigan Press, and SpringerLink for Springer Nature). Additionally, several data sources (DOAB, Crossref-Fundref, Unpaywall, ORCID) have been ingested via telescopes, but have not been aggregated into the Book Usage Data Analytic Workflows. While this ingested data is stored as part of the pilot, it is not used in the Kibana visualisations, but may become part of future phases of the book usage data dashboard project.

The processed data in the Google Cloud BiqQuery data warehouse is then pushed into Elasticsearch where it is accessible to Kibana, an open-source data analytics and dashboarding system. Dashboards in Kibana were developed through a visual interface in collaboration with pilot project dashboard partners, and specified in JSON format so they can be maintained and versioned in a code repository. Data access permissions flow through from the underlying sources into the cloud database and Elastic/Kibana. Stakeholder data is sandboxed into separate areas with user access permissions controlling access for each area, providing strong security and privacy. 

