import requests

from bs4 import BeautifulSoup


def generateWikipediaUrls(suffixDict):
	#wikipedia urls are pretty simple but they're cse senstive for some reason
	#basically just keep the case but replace spaces with underscores
	wikiPrefix = 'https://en.wikipedia.org/wiki/' 
	
	wikiUrlsDict = {}
	for key,value in suffixDict.items():
		wikiUrlsDict[key] = {
			'title':value,
			'url':wikiPrefix+value.replace(' ','_'),
			'url_valid': True,
			'abstract': None
			}
	
	return wikiUrlsDict
	

def getWikiInformation(urlDict):	
	try:
		res=requests.get(urlDict['url'])
		res.raise_for_status()
		
		soup = BeautifulSoup(res.text, 'html5lib')
		#loop through and get the first paragraph containing
		#the title in it
		paragraphs = soup.find_all('p')
		for paragraph in paragraphs:
			if urlDict['title'] in paragraph.text:
				urlDict['abstract'] = paragraph.text
				break
		
	except requests.exceptions.HTTPError:
		print (f"{urlDict['url']} invalid")
		urlDict['url_valid'] = False 

	return urlDict

