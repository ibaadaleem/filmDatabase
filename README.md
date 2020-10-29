## filmDatabase
Project to extract imdb data about films and store it into a postgresql table

# Set up
What you need set up before you start:
Install the following
*Python 3.6 
*Postgresql 13.0 database access
	
The following Python libraries are required for the installation
*pip install -Iv pscopg2==2.8.6
*pip install -Iv sqlalchemy==1.3.20
*pip install -Iv pandas==1.1.3
*pip install -Iv bs4==4.6.0	
	
Download the following files to the encrypted folder (unzipped)
*https://www.kaggle.com/rounakbanik/the-movies-dataset/version/7#movies_metadata.csv
*https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz

# Running
Open the main.py and configure the details for the postgresql access if required (including username). 
By default this will store the data into a database called 'film_database' but feel free to rename this. 
After that you can simply run the main.py script from command prompt (or equivalent), where you will be prompted for the postgresql password, and after that it will go on its way!

The whole thing can be run multiple times as long as 
1.The files exist in the encrypted folder and
2.A password for the postgresql is provided each run

This process uses a mix of python, mainly pandas, to extract the files and do some pre processing before loading it into a postgresql database. Then the relations between the tables are created via SQL and the final output made in the 'live' schema. The preprocessing is largely handled in pandas mainly just because it was easier to deal with nested tables that way, otherwise I'd rather do as much as I can in SQL as I'm working on a very old laptop with not a huge amount of RAM!
The uniqueness and relationships between the tables should be confirmed via the use of primary and foreign keys. Whilst creating this program I was doing manual checks at stages to confirm the data looked sensible as well (e.g. genres made sense, year for movies made sense)

# Overview
The CSV files are processed via pandas and loaded straight into postgresql tables with little preprocessing other than splitting the tables into the correct structures. These are then all loaded into the 'loading' schema where the rest is done via SQL to create the links and do any required further cleaning.
Initially the plan was to do something similar for the xml file containing all the wikipedia data, either by loading it straight into postgresql or via pandas, but both approaches ended up falling flat as I kept running into memory errors. Instead I've used the titles obtained from the movies_metadata.csv file to try and web scrape the information from the wikipedia site itself, although that's not quite as accurate due to the fact that not all the URLs resolved correctly. Anything with punctuation for example wasn't going to work well in a URL.








 

 

