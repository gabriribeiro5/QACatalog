# Dealing with ImportError: attempted relative import with no known parent package
import sys
sys.path.append('.')

from user_interface import talkToUser
from use_cases import tagManager, dataColector

class Manager(object):
    def __init__(self, appName):
        pass
    
    def hasNewData(self, directorie:str = f"controller/"):
        pass

    def extractPDFContent(self):
        dataColector.PDF_Master.readFile("controller", "AWS_CDA_Practice+Questions_DCT_2021.pdf")
        pass

    def frameQuestions(self):
        pass

    def catalogTags(self):
        # tagManager
        pass

    def searchQuestion(self):
        pass

