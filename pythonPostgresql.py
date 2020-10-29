
import pandas as pd
import sqlalchemy
import psycopg2 
		
		
def createDatabaseIfNotExists(user,password,port,host,database):
	conn = None
	try:
		print(f'Checking if {database} exists')
		
		conn = psycopg2.connect(
		host=host,
		user=user,
		password=password)
		conn.autocommit = True #needed to create DB via python
		
		cur = conn.cursor()		
		cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{database}'")
		exists = cur.fetchone()
		if not exists:		
			print(f'Database {database} does not exist, creating now')
			cur.execute(f'CREATE DATABASE {database}')
			print(f'Database {database} created')
		else:			
			print(f'Database {database} already exists')
			
		cur.close()
		
	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	
	finally:
		if conn is not None:
			conn.close()
			print('Database connection closed.')


def createEngine(user,password,port,host,database):
	connDetails = (f'postgresql://{user}:{password}@localhost:{port}/{database}')
	engine = sqlalchemy.create_engine(connDetails)
	
	return engine
	

def createLoadingTables(dictList, engine):
	#Takes in a dictionary of dataframes along with info about how to insert
	#these into postgresql tables#	
	print('Creating loading tables in postgresql')
	for dt in dictList:
		print(f"Processing {dt['tableName']}")
		
		dt['dataframe'][dt['columnList']].to_sql(dt['tableName'],
			con=engine,
			schema=dt['schema'],
			dtype=dt['dtype'],
			if_exists='replace',
			index=False)

