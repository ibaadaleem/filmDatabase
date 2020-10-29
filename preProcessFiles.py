import os
from pathlib import Path
from datetime import datetime

import pandas as pd
import sqlalchemy


def createChildDataframes(dataframe, combinationColumnName):
	#pull out all values into a separate dataframe
	combinationDf1 = dataframe[[combinationColumnName]].copy()
	#remove dupes
	combinationDf1 = combinationDf1.drop_duplicates()
	combinationDf1 = combinationDf1.dropna(subset=[combinationColumnName])
	combinationDf1 = combinationDf1.reset_index()
	
	#Add a column that converts the string representation of a list into actual list
	combinationDf1['combinationList'] = combinationDf1[combinationColumnName].apply(lambda x: eval(x))
	
	#drop rows that don't appear to be correct (due to columns being transposed or such)
	combinationDf1 = combinationDf1[combinationDf1['combinationList'].apply(lambda x: type(x) is list)]
	
	#Pivot table so that the list is broken up distinct rows per combinationId
	combinationDf2 = pd.DataFrame(combinationDf1['combinationList'].to_list(),index=combinationDf1[combinationColumnName])
	combinationDf3 = pd.melt(combinationDf2.reset_index(), id_vars=[combinationColumnName])
			
	#delete all the null values
	combinationDf3 = combinationDf3.dropna(subset=['value'])
	combinationDf3 = combinationDf3.reset_index()
	
	#expand dict values
	combinationDf4 = pd.json_normalize(combinationDf3['value'])
	
	#join back together to get combinationId along with bottom level ids
	combinationDf5 = pd.concat([combinationDf3,combinationDf4],axis=1,join='inner')
	distinctColIdName = combinationColumnName+'_id'
	distinctColName = combinationColumnName+'_name'
	
	combinationDf5 = combinationDf5.rename(columns={'id':distinctColIdName
	,'name':distinctColName,'value':'combinationValue'})
	#combinationDf5 = combinationDf5.set_index('combinationId')
	combinationDf5 = combinationDf5.drop(columns=['variable','combinationValue'])
	
	#get distinct values into a new dataframe
	distinctDf1 = combinationDf5[[distinctColIdName,distinctColName]]
	distinctDf2 = distinctDf1.drop_duplicates()
	distinctDf2 = distinctDf2.set_index(distinctColIdName)
	
	
	combinationDfFinal = combinationDf5
	distinctDfFinal = distinctDf2.reset_index()
	
	return combinationDfFinal, distinctDfFinal
	

def movies_metadata(filepath):
	#define relevant columns
	usecols = ['id','title','release_date','revenue','budget','production_companies','genres','vote_average']
	
	moviesDf = pd.read_csv(filepath,usecols=usecols)
	
	#manual cleaning of number/date fields because some of the values in the csv
	#havent been delimited correctly which was giving me a nightmare. anything not
	#convertible to an int gets turned into NaN, which then gets turned into None
	moviesDf['id'] = moviesDf['id'].apply(lambda x: pd.to_numeric(x,errors='coerce'))
	moviesDf['revenue'] = moviesDf['revenue'].apply(lambda x: pd.to_numeric(x,errors='coerce'))
	moviesDf['budget'] = moviesDf['budget'].apply(lambda x: pd.to_numeric(x,errors='coerce'))
	moviesDf['vote_average'] = moviesDf['vote_average'].apply(lambda x: pd.to_numeric(x,errors='coerce'))
	
	#extract year manually from the release_date string. theoretically i should be able
	#to convert the release date into a datetime and then extract the year but kept
	#giving me NaT for every single value...hacky fix in the name of time
	moviesDf['year'] = moviesDf['release_date'].apply(lambda x: str(x).split('-')[0])
	
	moviesDf = moviesDf.replace('nan',None)
	
	#extract all the genres and ready the child tables
	print('Extracting genres')
	genresCombinationsDf, genresDf = createChildDataframes(moviesDf, 'genres')
	
	#extract all the production companies and ready the child tables
	print('Extracting production companies')
	productionCompaniesCombinationsDf, productionCompaniesDf = createChildDataframes(moviesDf, 'production_companies')
	
	#create Dicts with details ready for inserting to postgresql
	movieDict={'tableName':'movies', 
		'dataframe':moviesDf,
		'schema':'loading',
		'columnList':['id','title','budget','revenue','year','vote_average','genres','production_companies'],
		'dtype':{'id':sqlalchemy.types.BigInteger,
			'title':sqlalchemy.types.Text,
			'budget':sqlalchemy.types.Numeric,
			'revenue':sqlalchemy.types.Numeric,
			'year':sqlalchemy.types.Integer,
			'vote_average':sqlalchemy.types.Numeric,
			'genres':sqlalchemy.types.Text,
			'production_companies':sqlalchemy.types.Text
			}
		}	

	genresDict={'tableName':'genres', 
		'dataframe':genresDf,
		'schema':'loading',
		'columnList':['genres_id','genres_name'],
		'dtype':{'genres_id':sqlalchemy.types.Integer,
			'genres_name':sqlalchemy.types.Text
			}
		}
		
	genresCombinationsDict={'tableName':'genres_combinations', 
		'dataframe':genresCombinationsDf,
		'schema':'loading',
		'columnList':['genres','genres_id'],
		'dtype':{'genres':sqlalchemy.types.Text,
			'genres_id':sqlalchemy.types.Integer
			}
		}
		
	productionCompaniesDict={'tableName':'production_companies', 
		'dataframe':productionCompaniesDf,
		'schema':'loading',
		'columnList':['production_companies_id','production_companies_name'],
		'dtype':{'production_companies_id':sqlalchemy.types.Integer,
			'production_companies_name':sqlalchemy.types.Text
			}
		}

	productionCompaniesCombinationsDict={'tableName':'production_companies_combinations', 
		'dataframe':productionCompaniesCombinationsDf,
		'schema':'loading',
		'columnList':['production_companies','production_companies_id'],
		'dtype':{'production_companies':sqlalchemy.types.Text,
			'production_companies_id':sqlalchemy.types.Integer
			}
		}
		
	dictList = [movieDict,genresDict,genresCombinationsDict,productionCompaniesDict,productionCompaniesCombinationsDict]
	
	return dictList
	
	