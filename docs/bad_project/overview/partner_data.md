BAD Project Dashboard Partner Data Sources
==========================

The BAD project dashboard partner data sources are where permission has been granted for the BAD project to access usage data about publishers’ open access eBook collections, or is publicly available but specific to a particular publisher. 

## Google Analytics
Google Analytics monitors and records web traffic for specific websites. If a BAD project dashboard partner has configured Google Analytics on their publisher website, the Google Analytics data can be used to find out which countries and territories website visitors are from. 

## Google Books
The Google Books Partner program hosts eBooks, including some free open access eBooks. eBook publishers can then download usage reports from [Google Books](https://play.google.com/books/publish/). The BAD project uses data from the Google Play sales transaction report and the Google Books Traffic Report.

## IRUS Fulcrum
IRUS provides COUNTER standard access reports for eBooks hosted on the Fulcrum platform. [Fulcrum](https://www.press.umich.edu/librarians) is a “community-developed, open source platform for digital scholarship” which provides “users the ability to read books with associated digital enhancements, such as: 3-D models, embedded audio, video, and databases; zoomable online images, and interactive media”.

## IRUS OAPEN
IRUS provides COUNTER standard access reports for eBooks hosted on the [OAPEN library and platform](https://oapen.org/). OAPEN "promotes and supports the transition to open access for academic books by providing open infrastructure services to stakeholders in scholarly communication". Almost all eBooks on OAPEN are provided as a PDF file for the whole book. The reports show access figures for each month, and the location (IP address) of the access. Within the OAPEN Google Cloud project (located in Europe), IP addresses are replaced with geographical information (city and country). This means that IP addresses are not stored within the BAD project data, and only de-identified geographical information transferred to the BAD project.

## JSTOR
[JSTOR](https://about.jstor.org/librarians/books/open-access-books-jstor/) is a digital library, offering over 7000 open access eBooks. Publisher usage reports offer details about the use (views and downloads) of eBooks by institution, and country. 

## ONIX-FTP feed from publishers
[ONIX](https://www.editeur.org/83/Overview/) is a standard that book publishers use to share information about the books that they have published. BAD project dashboard partners that have ONIX feeds are given credentials and access to their own upload folder on the Mellon SFTP server. The BAD project dashboard partner uploads their ONIX feed to their upload folder on a weekly, fortnightly or monthly basis. The Book Usage Data Workflows ONIX telescope downloads, transforms (with the ONIX parser Java command line tool) and then loads the ONIX data into BigQuery for further processing.

## OAPEN Metadata
OAPEN enables libraries and aggregators to use the metadata of all available titles in the OAPEN Library. The metadata is available in different formats and the BAD project harvests the data in the XML format to obtain an file in ONIX format for the OAPEN platform.

## Thoth
Thoth is a free, open metadata service that publishers can choose to utilise as a solution for metadata storage. Thoth can provide metadata upon request in a number of formats. The BAD project uses the [Thoth Export API](https://export.thoth.pub/#get-/formats/-format_id-) to download metadata for publishers in an ONIX format. 

## UCL Discovery
University College London (UCL) is an eBook publisher, and partner in the BAD project. UCL Discovery is UCL's open access repository, showcasing and providing access to the full texts of UCL research publications.