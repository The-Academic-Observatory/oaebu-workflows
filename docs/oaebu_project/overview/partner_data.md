Partner Data Sources
==========================

The partner data sources are where permission has been granted for the OAeBU project to access usage data about publishers’ open access eBook collections.  

## ONIX-FTP feed from publishers
ONIX is a standard that book publishers use to share information about the books that they have published (<https://www.editeur.org/83/Overview/>). Publishers that have ONIX feeds are given credentials and access to their own upload folder on the Mellon SFTP server. The publisher uploads their ONIX feed to their upload folder on a weekly, fortnightly or monthly basis. The OAeBU ONIX telescope downloads, transforms (with the ONIX parser Java command line tool) and then loads the ONIX data into BigQuery for further processing.

## OAPEN IRUS-UK
IRUS-UK provides OAPEN COUNTER standard access reports. Almost all eBooks on OAPEN are provided as a PDF file for the whole book. The reports show access figures for each month, and the location (IP address) of the access. Within the OAPEN Google Cloud project (located in Europe), IP addresses are replaced with geographical information (city and country). This means that IP addresses are not stored within the OAeBU project data, and only de-identified geographical information transferred to the OAeBU project.

## JSTOR
JSTOR is a digital library, offering over 7000 open access eBooks (<https://about.jstor.org/librarians/books/open-access-books-jstor/>). This includes titles from the University of Michigan Press, UCL Press and ANU Press (<https://about.jstor.org/librarians/books/participating-book-publishers/>). Publisher usage reports offer details about the use (views and downloads) of eBooks by institution, and country. 

## Google Books
The Google Books Partner program hosts eBooks, including some free open access eBooks. eBook publishers can then download usage reports from Google Books (<https://play.google.com/books/publish/>). OAeBU uses data from the Google Play sales transaction report and the Google Books Traffic Report.

## Google Analytics
Google Analytics monitors and records web traffic for specific websites. If an OAeBU partner has configured Google Analytics on their publisher website, the Google Analytics data can be used to find out which countries and territories website visitors are from. 

## UCL Discovery
University College London (UCL) is an eBook publisher, and partner in the OAeBU project. UCL Discovery is UCL's open access repository, showcasing and providing access to the full texts of UCL research publications. This data source is ingested and aggregated into the final tables for the OAeBU pilot. 

## Fulcrum
Fulcrum is a “community-developed, open source platform for digital scholarship” which provides “users the ability to read books with associated digital enhancements, such as: 3-D models, embedded audio, video, and databases; zoomable online images, and interactive media” (<https://www.press.umich.edu/librarians>). The Fulcrum data source is specific to the University of Michigan Press in the OAeBU pilot, and is a manual data upload (.csv file) so does not include ID-matching and linking to other data sources in the project.
The University of Michigan Press Ebook Collection can be accessed at: <https://www.fulcrum.org/michigan>.

## MUSE
Project MUSE provides a platform which hosts journals and books from multiple publishers including the University of Michigan Press, University College London and Wits University Press. Some of the MUSE offerings are open access eBooks (<https://about.muse.jhu.edu/muse/open-access-overview/>). The MUSE data source is specific to the University of Michigan Press in the OAeBU pilot, and is a manual data upload (.csv file) so does not include ID-matching and linking to other data sources in the project.
To look for open access book titles on Project MUSE, go to <https://muse.jhu.edu/search?action=oa_browse>, and select ‘Content Type’ = Books.

## EBSCO
EBSCO hosts collections of different publications including eBooks, with some open access. They provide data about the usage of these ebooks to publishers, such as University of Michigan Press (<https://more.ebsco.com/ebooks-open-access-2021.html>). This is a manual data upload (.csv file) so does not include ID-matching and linking to other data sources in the project, and is specific to the University of Michigan Press in the OAeBU pilot.
  
## SpringerLink
SpringerLink is an online collection of Springer Nature’s electronic and printed journals and books (<https://www.springer.com/gp/help/about-springerlink/18548>).  This is a manual data upload (via .csv on a Google Cloud Platform bucket) so does not include ID-matching and linking to other data sources in the project, and is specific to Springer Nature. 
