Public Data Sources
==========================

The public data sources are where data is publicly available, rather than data provided by a specific pilot project dashboard partner.

## Crossref Event Data
Crossref Event Data captures online discussion about research outputs, such as [‘a citation in a dataset or patent, a mention in a news article, Wikipedia page or on a blog, or discussion and comment on social media’](https://www.crossref.org/services/event-data/). The event data is retrieved using the [Crossref Events API](https://www.eventdata.crossref.org/guide/service/query-api/). Crossref Event data must be queried using a DOI, which the BAD project obtains from Crossref Metadata. 

## Crossref Metadata
Crossref is a non-for-profit membership organization, and an official Digital Object Identifier (DOI) Registration Agency of the International DOI Foundation. They make metadata available for all DOIs registered with [Crossref](https://www.crossref.org/community/). The BAD project uses Crossref Metadata to match ISBNs obtained from a publishers Onix feed to DOIs to query Crossref Event Data.