# Dealing with ImportError: attempted relative import with no known parent package
import pdb
import sys
sys.path.append('.')

import fitz #PyMuPDF
import logging

class PDF_Master(object):
    def __init__(self):
        pass
    
    def openPDF(self, fileName:str=None, dirName:str="data"):
        if fileName is None:
            response = "no file specified"
            return response
        
        if ".pdf" in fileName:
            fileName = fileName.split(".")[0]

        pdf_file = f"{dirName}/{fileName}.pdf"

        doc = fitz.open(pdf_file)
        print(type(doc.pages()))
        return doc

    def getQuestions(self, fileName:str=None, dirName:str="data"):
        doc = self.openPDF(fileName)

        allQuestions = []
        count = 0
        print(type(doc.pages()))
        for page in doc.pages():
            if count == 0:
                print(type(page))
            blocks = page.get_text("blocks") # all paragraphs
            copy = False
            question = []
            refStart = '. Question'
            refEnd = '1: '

            for paragraph in blocks:
                if copy:
                    if refEnd not in paragraph[4]:
                        question = f'{question} {paragraph[4]}'
                    else:
                        lastparagraph = paragraph[4].split('1:')[0]
                        question = f'{question} {lastparagraph}'
                    allQuestions.append(question)
                
                copy = False
                if refStart in str(paragraph[4]):
                    count += 1
                    question = f'{count}. Question'
                    copy = True
                else:
                    copy = False
        doc.close()    
        return allQuestions
                
                

a = PDF_Master()
b = [a.getQuestions("AWS_CDA_Practice+Questions_DCT_2021")]
# print(b)