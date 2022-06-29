# Dealing with ImportError: attempted relative import with no known parent package
import sys
sys.path.append('.')

import PyPDF2
import logging

class PDF_Master(object):
    def __init__(self):
        pass

    def readFile(self, fileName:str=None, dirName:str="data"):
        if fileName is None:
            response = "no file specified"
            return response
        
        if ".pdf" in fileName:
            fileName = fileName.split(".")[0]

        pdf_file = f"{dirName}/{fileName}.pdf"
        with open(pdf_file, "rb") as pdf_file:
            logging.info("opening pdf file")
            read_pdf = PyPDF2.PdfFileReader(pdf_file)
            number_of_pages = read_pdf.getNumPages()
            page = read_pdf.pages[5]
            page_content = page.extractText()
        print(page_content)

a = PDF_Master()
a.readFile("AWS_CDA_Practice+Questions_DCT_2021")