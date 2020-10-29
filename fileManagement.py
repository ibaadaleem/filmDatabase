import os
import shutil
from pathlib import Path


def checkPathExists(filePath):
	if not (os.path.exists(filePath)):
		errorMessage = f'File Path not found: {filePath}'
		print(errorMessage)
		raise FileNotFoundError(errorMessage)
		
		
def recreateFolderPath(filepath):
	if not (os.path.exists(filepath)):
		print(f'Creating filepath: {filepath}')
		os.mkdir(filepath)		
	else:
		print(f'Recreating filepath: {filepath}')
		shutil.rmtree(filepath)
		os.mkdir(filepath)	


def sortFilesByExtension(filePath):
	#moves files to subfolders based on their extension type
	#to make loading a bit easier
	print('Sorting files by extension type')
	fileExtensions = set()
	files = []
	for file in os.listdir(filePath):
		if os.path.isfile(os.path.join(filePath, file)):
			files.append(file)
			fileExtensions.add(Path(file).suffix)
		
	#create/recreate empty subfolders
	for fileExtension in fileExtensions:
		fileExtensionFolderName = fileExtension.replace('.','')
		print(fileExtensionFolderName)
		recreateFolderPath(os.path.join(filePath, fileExtensionFolderName))
	
	for file in files:
		fileExtensionFolderName = Path(file).suffix.replace('.','')
		oldFilePath = os.path.join(filePath, file)
		newFilePath = os.path.join(filePath, fileExtensionFolderName, file)
		print(f'Moving {oldFilePath} to {newFilePath}')
		shutil.move(oldFilePath, newFilePath)
		