import os
import shutil
import zipfile
import gzip

import fileManagement


def decryptFile(inputFilePath, outputFolderPath, zipType):
	#check input and output folder paths exist
	fileManagement.checkPathExists(inputFilePath)
	fileManagement.checkPathExists(outputFolderPath)	

	#extract filename from final part of the folder path
	inputFileName = os.path.basename(inputFilePath)
	
	print(f'Decrypting {inputFileName}')
	
	#extract all files to output folder depending on zip type
	if zipType == 'zip':
		decryptFileZip(inputFilePath, outputFolderPath)
	elif zipType == 'gz':
		decryptFileGz(inputFilePath, outputFolderPath, inputFileName)
	elif zipType == 'txt':
		pass
	else:
		raise Exception(f'zip type {zipType} not valid')
		
	print(f'Decrypted {inputFileName}')
	

def decryptFileZip(inputFilePath, outputFolderPath):	
	with zipfile.ZipFile(inputFilePath, 'r') as zipRef:
		zipRef.extractall(outputFolderPath)
	
	
def decryptFileGz(inputFilePath, outputFolderPath, inputFileName):
	#get filename without final part of extension i.e. the .gz
	outputFileName = os.path.splitext(inputFileName)[0]
	
	with gzip.open(inputFilePath, 'rb') as f_in:
		with open(os.path.join(outputFolderPath,outputFileName), 'wb') as f_out:
			shutil.copyfileobj(f_in, f_out)

