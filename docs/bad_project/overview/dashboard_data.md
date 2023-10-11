Dashboard Data Sources
==========================

The data sources currently visualised in the Dashboard are detailed in the table below. The standard data sources and variables used are included, other data sources and variables may be supported as an extra add-on service.


``` eval_rst
.. tabularcolumns:: |l|l|l|l| 
.. csv-table::
   :file: dashboard_sources.csv
   :width: 100%
   :header-rows: 1
   :class: longtable
```

## Public data sources
The public data sources are where data is publicly available, rather than data provided by a specific Dashboard partner. No additional permission is required from Dashboard partners for the Dashboard to access the following public data sources.

### Crossref Event Data
Crossref Event Data captures online discussion about research outputs, such as [‘a citation in a dataset or patent, a mention in a news article, Wikipedia page or on a blog, or discussion and comment on social media’](https://www.crossref.org/services/event-data/). The event data is retrieved using the [Crossref Events API](https://www.eventdata.crossref.org/guide/service/query-api/). Crossref Event data must be queried using a DOI, which the Dashboard obtains from Crossref Metadata. 

### Crossref Metadata
Crossref is a non-for-profit membership organization, and an official Digital Object Identifier (DOI) Registration Agency of the International DOI Foundation. They make metadata available for all DOIs registered with [Crossref](https://www.crossref.org/community/). The Dashboard uses Crossref Metadata to match ISBNs obtained from a publishers Onix feed to DOIs to query Crossref Event Data.

### OAPEN Metadata
OAPEN enables libraries and aggregators to use the metadata of all available titles in the OAPEN Library. The metadata is available in different formats and the Dashboard harvests the data in the XML format to obtain an file in ONIX format for the OAPEN platform.

### Thoth
Thoth is a free, open metadata service that publishers can choose to utilise as a solution for metadata storage. Thoth can provide metadata upon request in a number of formats. The Dashboard uses the [Thoth Export API](https://export.thoth.pub/#get-/formats/-format_id-) to download metadata for publishers in an ONIX format. 

### UCL Discovery
University College London (UCL) is an eBook publisher, and partner in the Dashboard. UCL Discovery is UCL's open access repository, showcasing and providing access to the full texts of UCL research publications.

## Private data sources - permission required
The following private data sources require specific permission from the Dashboard partner for the Dashboard to access data. 

### Google Analytics Universal
Google Analytics Universal monitors and records web traffic for specific websites. If a Dashboard partner had configured Google Analytics on their publisher website, the Google Analytics data can be used to find out which countries and territories website visitors are from.

### Google Books
The Google Books Partner program hosts eBooks, including some free open access eBooks. eBook publishers can then download usage reports from [Google Books](https://play.google.com/books/publish/). The Dashboard uses data from the Google Play sales transaction report and the Google Books Traffic Report.

### JSTOR
[JSTOR](https://about.jstor.org/librarians/books/open-access-books-jstor/) is a digital library, offering over 7000 open access eBooks. Publisher usage reports offer details about the use (views and downloads) of eBooks by institution, and country. 

### ONIX-FTP feed from publishers
[ONIX](https://www.editeur.org/83/Overview/) is a standard that book publishers use to share information about the books that they have published. BAD project dashboard partners that have ONIX feeds are given credentials and access to their own upload folder on the Mellon SFTP server. The BAD project dashboard partner uploads their ONIX feed to their upload folder on a weekly, fortnightly or monthly basis. The Book Usage Data Workflows ONIX telescope downloads, transforms (with the ONIX parser Java command line tool) and then loads the ONIX data into BigQuery for further processing.


## Private data sources - no additional permission required
The following private data sources do not require additional permission from the Dashboard partner for the Dashboard to access data. 

### IRUS Fulcrum
IRUS provides COUNTER standard access reports for eBooks hosted on the Fulcrum platform. [Fulcrum](https://www.press.umich.edu/librarians) is a “community-developed, open source platform for digital scholarship” which provides “users the ability to read books with associated digital enhancements, such as: 3-D models, embedded audio, video, and databases; zoomable online images, and interactive media”.

### IRUS OAPEN
IRUS provides COUNTER standard access reports for eBooks hosted on the [OAPEN library and platform](https://oapen.org/). OAPEN "promotes and supports the transition to open access for academic books by providing open infrastructure services to stakeholders in scholarly communication". Almost all eBooks on OAPEN are provided as a PDF file for the whole book. The reports show access figures for each month, and the location (IP address) of the access. Within the OAPEN Google Cloud project (located in Europe), IP addresses are replaced with geographical information (city and country). This means that IP addresses are not stored within the Dashboard data, and only de-identified geographical information transferred to the Dashboard.
