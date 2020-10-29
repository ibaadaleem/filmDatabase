import os
from pathlib import Path
import sys
from getpass import getpass

import pandas as pd
import psycopg2 
import sqlalchemy

import decryptFiles
import fileManagement
import preProcessFiles
import pythonPostgresql
import scrapeWikipedia

class Controller:

	def __init__(self, user, port, host, database, baseFolder):
		self.user = user
		self.password = getpass('Input postgresql user password: ')
		self.port = port
		self.host = host
		self.database = database
		
		self.encryptedFolderPath = os.path.join(baseFolder,'inputFiles','encrypted')
		self.decryptedFolderPath = os.path.join(baseFolder,'inputFiles','decrypted')	

	def extractFiles(self):
		       
		fileManagement.checkPathExists(self.encryptedFolderPath)
		fileManagement.recreateFolderPath(self.decryptedFolderPath)
	
		for file in os.listdir(self.encryptedFolderPath):
			zipType = Path(file).suffix.replace('.','')
			decryptFiles.decryptFile(os.path.join(self.encryptedFolderPath,file),self.decryptedFolderPath,zipType)
	
		fileManagement.checkPathExists(self.decryptedFolderPath)
		print('The following files have been extracted')
		for file in os.listdir(self.decryptedFolderPath):
			print(file)
			
		fileManagement.sortFilesByExtension(self.decryptedFolderPath)
	
	
	def processAndLoadFiles(self):
		filesToLoad = ['movies_metadata.csv']
		outputDictList = []
	
		#load files listed above
		print(f'Checking {self.decryptedFolderPath}')
		for folder in os.listdir(self.decryptedFolderPath):
			print(f'{folder} found')
			
			if os.path.isdir(os.path.join(self.decryptedFolderPath, folder)):
				print(f'Confirmed {folder} is a folder')
				
				for file in os.listdir(os.path.join(self.decryptedFolderPath, folder)):
					print(f'File {file} found')
					
					if file in filesToLoad:
						if file == 'movies_metadata.csv':
							print(f'Processing {file}')
							dictList = preProcessFiles.movies_metadata(os.path.join(self.decryptedFolderPath,folder,file))
							outputDictList += dictList
							print(f'Processed {file}')
					else:
						print(f'File {file} ignored')
	
	
		#create schemas in postgres if not already there
		conn = None
		try:
			pythonPostgresql.createDatabaseIfNotExists(self.user,self.password,self.port,self.host,self.database)
	
			conn = psycopg2.connect(
			host=self.host,
			user=self.user,
			password=self.password,
			database=self.database)
				
			cur = conn.cursor()
			
			loadingSchema = 'loading'
			liveSchema = 'live'
			
			cur.execute(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{loadingSchema}';")
			exists = cur.fetchone()
			if not exists:		
				print(f'Schema {loadingSchema} does not exist, creating now')
				cur.execute(f'CREATE SCHEMA {loadingSchema}')
			else:			
				print(f'Schema {loadingSchema} already exists')	
				
			cur.execute(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{liveSchema}';")
			exists = cur.fetchone()
			if not exists:		
				print(f'Schema {liveSchema} does not exist, creating now')
				cur.execute(f'CREATE SCHEMA {liveSchema}')
			else:			
				print(f'Schema {liveSchema} already exists')
	
			conn.commit()
				
			cur.close()
			
		except (Exception, psycopg2.DatabaseError) as error:
			print(error)
			raise
	
		finally:
			if conn is not None:
				conn.close()
				print('Database connection closed.')
						
	
		#load data from dataframes into loading tables
		engine = pythonPostgresql.createEngine(self.user,self.password,self.port,self.host,self.database) 
		pythonPostgresql.createLoadingTables(outputDictList,engine)
	
	
	def cleanDataInSQL(self):
		#Do final processing in SQL
		print('Cleaning data in SQL')
		conn = None
		try:
			conn = psycopg2.connect(
			host=self.host,
			user=self.user,
			password=self.password,
			database=self.database)
				
			cur = conn.cursor()
						
			#create the combinations tables; assign ids for the table
			print('creating live.genres_combinations')
			cur.execute('drop table if exists live.genres_combinations;')
			cur.execute('''create table live.genres_combinations as
				select dense_rank() over (order by genres) as genres_combinations_id,
				a.genres_id,
				a.genres
				from loading.genres_combinations a 
				;''')	
	
			print('creating live.production_companies_combinations')
			cur.execute('drop table if exists live.production_companies_combinations;')
			cur.execute('''create table live.production_companies_combinations as
				select dense_rank() over (order by production_companies) as production_companies_combinations_id,
				a.production_companies_id,
				a.production_companies
				from loading.production_companies_combinations a 
				;''')		
	
			print('creating live.genres')
			cur.execute('drop table if exists live.genres;')
			cur.execute('''create table live.genres as
				select *
				from loading.genres a
				;''')
						
			print('creating live.production_companies')
			cur.execute('drop table if exists live.production_companies;')
			cur.execute('''create table live.production_companies as
				select *
				from loading.production_companies a
				;''')
				
			print('creating live.movies')
			cur.execute('drop table if exists live.movies;')
			cur.execute('''create table live.movies as
				select distinct id as movie_id,
				a.title,
				a.budget,
				a.revenue,
				cast(case when revenue != 0 then budget/revenue else null end
					as decimal(18,4)) as budget_revenue_ratio,
				a.year,
				a.vote_average as rating,
				b.genres_combinations_id,
				c.production_companies_combinations_id
				from loading.movies a
				left join (select distinct genres, genres_combinations_id from live.genres_combinations) b
				on a.genres = b.genres
				left join (select distinct production_companies, production_companies_combinations_id from live.production_companies_combinations) c
				on a.production_companies = c.production_companies
				where a.id is not null
				;''')
	
			print('Dropping unnecessary columns')
			cur.execute('alter table live.genres_combinations drop column genres;')
			cur.execute('alter table live.production_companies_combinations drop column production_companies;')
		
			print('create primary and foreign keys')
			cur.execute('''
			alter table live.movies 
			add constraint pk__movie_id 
			primary key (movie_id);
			
			alter table live.genres 
			add constraint pk__genres_id 
			primary key (genres_id);
			
			alter table live.production_companies 
			add constraint pk__production_companies_id 
			primary key (production_companies_id);
			
			alter table live.genres_combinations 
			add constraint pk__genres_combinations_id__genres_id 
			primary key (genres_combinations_id, genres_id);
			
			alter table live.production_companies_combinations 
			add constraint pk__production_companies_combinations_id__production_companies_id 
			primary key (production_companies_combinations_id, production_companies_id);
					
			alter table live.genres_combinations 
			add constraint fk__genres_id 
			foreign key (genres_id) 
			references live.genres (genres_id);
			
			alter table live.production_companies_combinations 
			add constraint fk__production_companies_id 
			foreign key (production_companies_id) 
			references live.production_companies (production_companies_id);
			
			''')
	
	
			#create top 1000 movies by ratio 
			print('creating live.movies_top_1000')
			cur.execute('drop table if exists live.movies_top_1000;')
			cur.execute('''create table live.movies_top_1000 as 
				select *,
				cast(null as text) as url,
				cast(null as text) as abstract
				from 
				live.movies 
				order by budget_revenue_ratio desc NULLS LAST
				limit 1000
				;''')
			
			conn.commit()
				
			cur.close()
			
		except (Exception, psycopg2.DatabaseError) as error:
			print(error)
			raise
	
		finally:
			if conn is not None:
				conn.close()
				
	def getWikiData(self):
		#get movie titles in top 1000
		conn = None
		try:
			conn = psycopg2.connect(
			host=self.host,
			user=self.user,
			password=self.password,
			database=self.database)
				
			cur = conn.cursor()
			
			cur.execute(f"SELECT movie_id,title FROM live.movies_top_1000")
			movieTitles = dict(cur.fetchall())
										
			cur.close()
			
		except (Exception, psycopg2.DatabaseError) as error:
			print(error)
			raise
	
		finally:
			if conn is not None:
				conn.close()
		
		wikiUrlsDict = scrapeWikipedia.generateWikipediaUrls(movieTitles)
		
		#now go through the URLs and a)verify if URL is correct 
		#and b) get the abstract
		for key,value in wikiUrlsDict.items():
			urlsDict = scrapeWikipedia.getWikiInformation(value)
			wikiUrlsDict[key] = urlsDict
		
		df = pd.DataFrame(wikiUrlsDict).T#transpose
		df = df.reset_index()
		
		dfDict={'tableName':'wiki', 
		'dataframe':df,
		'schema':'loading',
		'columnList':['index','url','url_valid','abstract'],
		'dtype':{'index':sqlalchemy.types.Integer,
			'url':sqlalchemy.types.Text,
			'url_valid':sqlalchemy.types.Boolean,
			'abstract':sqlalchemy.types.Text,
			}
		}
		
		#load into temp table
		engine = pythonPostgresql.createEngine(self.user,self.password,self.port,self.host,self.database) 
		pythonPostgresql.createLoadingTables([dfDict],engine)
			
		#process in SQL
		conn = None
		try:
			conn = psycopg2.connect(
			host=self.host,
			user=self.user,
			password=self.password,
			database=self.database)
				
			cur = conn.cursor()
			
			cur.execute('''
			update loading.wiki
			set url_valid = false
			where abstract like '%may refer to:%';
			
			drop table if exists live.wiki;
			create table live.wiki as 
			select index as movie_id,
			url,
			abstract
			from loading.wiki
			where url_valid is true;
			
			update live.movies_top_1000 a
			set url = b.url,
			abstract = b.abstract
			from live.wiki b
			where a.movie_id = b.movie_id;
			
			''')


			conn.commit()
										
			cur.close()
			
		except (Exception, psycopg2.DatabaseError) as error:
			print(error)
			raise
	
		finally:
			if conn is not None:
				conn.close()		
	
	
	
