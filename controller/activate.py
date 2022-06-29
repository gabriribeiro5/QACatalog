# Dealing with ImportError: attempted relative import with no known parent package
import sys
sys.path.append('.')

from user_interface import talkToUser

class Manager(object):
    def __init__(self, appName):
        pass

    def extractPDFContent(self):
        pass

    def frameQuestions(self):
        pass

    def catalogTags(self):
        pass

    def searchQuestion(self):
        pass

    def returnQuestion(self):
        response = self.searchQuestion()
        # define delivery format
        pass

    
    ui = talkToUser.start_talk()
    ui.inputHandler()