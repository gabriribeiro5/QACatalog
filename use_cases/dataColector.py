# Dealing with ImportError: attempted relative import with no known parent package
import sys

sys.path.append('.')
from pathlib import Path, PurePath

import fitz #PyMuPDF
import logging

class PDF_Master(object):
    def __init__(self, filePathName:str=None):
        """
        filePathName: give me a PDF path/name and I will handle it
        """
        logging.info("Starting class PDF Master")

        if filePathName is None:
            response = "no file specified"
            return response

        self.filePathName = Path(filePathName)
        logging.info(f"Openning PDF (__init__): {self.filePathName}")
        self.doc = fitz.open(self.filePathName)
        
    def getQuestions(self):
        logging.info("Searching PDF for questions")
        allQuestions = {}
        keysList = [] #will be used to find the right allQuestions key by index (dicts don't have indexes, only keys)
        pageCount = 0
        
        for page in self.doc.pages():
            pageCount += 1
            blocks = page.get_text("blocks") # all paragraphs
            copy = False
            qNum = str
            pageAndQuestionNum = []
            refStart = '. Question'
            refEnd = '1: '

            for paragraph in blocks:
                if copy: 
                    if refEnd not in paragraph[4]:
                        allQuestions[pageAndQuestionNum]=paragraph[4]
                        pass
                        # question = f'{question}{paragraph[4]}'
                    else:
                        lastparagraph = paragraph[4].split('1:')[0]
                        allQuestions[pageAndQuestionNum]=lastparagraph
                        # question = f'{question} {lastparagraph}'
                    # allQuestions.append(question)
                copy = False

                if refStart in str(paragraph[4]): # paragraph[4] = 1. Question
                    qNum = ""
                    for n in paragraph[4]:
                        if str.isdecimal(n) and n != ".":
                            qNum = str(qNum)+str(n)
                    pageAndQuestionNum = f'[page {pageCount}] {qNum}'
                    keysList.append(pageAndQuestionNum)
                    copy = True

        logging.info(f"Reading process completed. Total read pages: {pageCount}. Closing PDF {self.filePathName}")
        self.doc.close()
        return allQuestions, keysList

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