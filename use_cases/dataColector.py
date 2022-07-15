# Dealing with ImportError: attempted relative import with no known parent package
import sys

sys.path.append('.')

import fitz #PyMuPDF
from utils import yamlManager
import logging

class PDF_Master(object):
    def __init__(self, fileName:str=None, dirName:str="data"):
        """
        fileName: give me a PDF name and I will handle it
        dirName: primary source directory is /data, any other dir must be specified
        """
        logging.info("Starting class PDF Master")

        if fileName is None:
            response = "no file specified"
            return response

        if ".pdf" in fileName:
            fileName = fileName.split(".")[0]

        self.filePathName = f"{dirName}/{fileName}.pdf"
        logging.info(f"Openning PDF (__init__): {self.filePathName}")
        self.doc = fitz.open(self.filePathName)
        

    def getQuestions(self):
        logging.info("Searching PDF for questions")
        allQuestions = []
        pageCount = 0        
        
        for page in self.doc.pages():
            pageCount += 1
            blocks = page.get_text("blocks") # all paragraphs
            copy = False
            question = []
            refStart = '. Question'
            refEnd = '1: '

            for paragraph in blocks:
                if copy: 
                    if refEnd not in paragraph[4]:
                        question = f'{question}{paragraph[4]}'
                    else:
                        lastparagraph = paragraph[4].split('1:')[0]
                        question = f'{question} {lastparagraph}'
                    allQuestions.append(question)
                
                copy = False
                if refStart in str(paragraph[4]):
                    question = f'[page {pageCount}] {paragraph[4]}' # "-13" refers to the character where the question number is located
                    copy = True
                else:
                    copy = False

        logging.info(f"Reading process completed. Total read pages: {pageCount}. Closing PDF {self.filePathName}")
        self.doc.close()
        return allQuestions

    def totalPages(self):
        value = len(self.doc)
        return value

    def newFile(self, newFilePathName:str=None, copyFromPage=0, copyToPage=0):
        """
        newFilePathName = "path/to/fileName.pdf",
        copyFromPage = initial page to copy from source,
        copyToPage = last page to copy from source
        """
        if ".pdf" in self.filePathName:
            self.filePathName = self.filePathName.split(".")[0]

        if newFilePathName is None:
            newFilePathName = f"{self.filePathName}_newFile.pdf"

        newDoc = fitz.open()                 # new empty PDF
        newDoc.insert_pdf(self.doc,from_page = copyFromPage, to_page = copyToPage)  # first 10 pages
        logging.info(f"saving new PDF: {newFilePathName}")
        newDoc.save(f"{newFilePathName}")
        
class YAML_Master(object):
    def __init__(self, fileName:str=None, dirName:str="data"):
        """
        fileName: give me a YAML name and I will handle it
        dirName: primary source directory is /data, any other dir must be specified
        """
        logging.info("Starting class YAML Master")

        if fileName is None:
            response = "no file specified"
            return response

        extention = ".yaml" or ".yml"
        if extention in fileName:
            fileName = fileName.split(".")[0]

        self.filePathName = f"{dirName}/{fileName}.yaml"
        logging.info(f"Openning YAML (__init__): {self.filePathName}")
        self.doc = yamlManager.loadDataFrom(self.filePathName)

    def findOutBadge(self, sourceFileName):
        self.badgesRef = self.doc["cloudBadges"]

        for p in self.badgesRef:
            provider = p.key if p.key.lower in sourceFileName.lower else None
        
        for bdgNames in self.badgesRef[provider]:
            possibleBadges = []
            possibleBadges = [bdgNames[0] for name in bdgNames if name in sourceFileName]

        return provider, possibleBadges