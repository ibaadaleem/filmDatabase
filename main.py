import sys
import os

import filmDatabaseController

if __name__ == '__main__':
	#change below as necessary
	user = 'postgres'
	password = getpass('Input postgresql user password: ')
	port = '5432'
	database = 'film_database'
	host = 'localhost'
	database = database.lower() #postgresql defaults everything to lowercase

	print('Process starting...')
	
	baseFolder = os.path.dirname(sys.argv[0]) 
	controller = filmDatabaseController.Controller(user,port,host,database,baseFolder)
		
	controller.extractFiles()
	controller.processAndLoadFiles()
	controller.cleanDataInSQL()
	controller.getWikiData()

	print('Finished!')
