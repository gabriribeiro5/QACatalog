import yaml

# Recebe endereço e nome de qualquer arquivo Yaml e retorna os dados do  mesmo
def loadDataFrom(filePathName):
        """
        filePathName: full path and name of the file to be loaded
        """
        with open(file=filePathName, mode='r', encoding='utf-8') as anyFile:
            print(f"Loading data from {filePathName}")
            fileData = yaml.safe_load(anyFile)
            print("Closing refered file")
            anyFile.close()
            return fileData

# Recebe conteúdo de input + endereço e nome de qualquer arquivo Yaml. Carrega dados de input no arquivo especificado.
def updateFile(newData, filePathName):
    """
    newData: data structure to be loaded at filePathName
    filePathName: target file for data to be loaded
    """
    with open(file=filePathName, mode='w', encoding='utf-8') as anyFile:
        print(f"Dumping data into target file")
        yaml.dump(newData, anyFile)