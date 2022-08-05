# Dealing with ImportError: attempted relative import with no known parent package
import sys
sys.path.append('.')

import logging

from user_interface import talkToUser
from use_cases import dataColector, dataOrganizer, searchEngine
from utils import yamlManager
from glob import glob


class Manager_Toolset(object):
    def __init__(self, configData):
        """
        appName: give me a PDF name and I will handle it
        dataDir: primary source directory is /data, any other dir must be specified
        """
        logging.info("Opening Manager Toolset")
        self.configData = configData
        self.appName = self.configData["appName"]
        self.dataDir = self.configData["sourceDir"]
        
        logging.info(f"Source dir: {self.dataDir}")

    def listSourceFiles(self):
        logging.info("Looking for PDF files at sourceDir")
        filePattern = f"{self.dataDir}*.pdf"
        allFilesPathName = glob(filePattern)
        
        return allFilesPathName
    
    def hasNewData(self):
        logging.info("Searching for new data files")
        
        hasNewData = False
        knownFiles = self.configData["knownFiles"]
        searchSource = self.configData["searchSource"]
        allFilesPathName = self.listSourceFiles() #load .pdf from dataDir
        self.newFilesPathName = []

        for file in allFilesPathName:
            if file not in knownFiles or searchSource=="primaryLayer":
                hasNewData = True
                self.newFilesPathName.append(file)
                
        
        return hasNewData, self.newFilesPathName

    def extractPDFContent(self):
        hasNewData = self.hasNewData()
        allFilesPathName = hasNewData[1]
        newContent = {}
        badgesRef = self.configData["cloudBadges"]
        if hasNewData[0]:
            logging.info("Extracting new PDF content")
            for filePathName in allFilesPathName:
                pdf = dataColector.PDF_Master(filePathName)
                allQuestions, questionsKeysList = pdf.getQuestions()
                newContent[filePathName]=allQuestions # add new item to the dict
        else:
            logging.info("Nothing new to read")
            newContent = 0
        return newContent, questionsKeysList # questionsKeysList is to help finding questions by index

    def catalogTags(self):
        # tagManager
        pass
    
    def fitDataToYaml(self):
        """
        ==== searchSource.yaml ===
        CERTIFICATION BADGE:
        - BadgeName: Developer, Solutions Architect, SysOps Admin, etc...
        - Files:
            - fileName: AWS_NUNSEIQUELA.pdf
            - Questions:
                - [page 1] 1. Question \n A Developer wants to...?
                - [page 1] 2. Question \n A Developer wants to...?
                - ...
        """

        pass

    def registerKnownFiles(self):
        self.configData["knownFiles"].append(self.newFiles)

        yamlManager.updateFile(self.configData, "config.yaml")

    def primaryUserCommunication(self):
        logging.info("calling primary user interaction")
        # do sth
        
        logging.info("end of primary user interaction")

    def allowUserRequests(self):
        pass

    def msgToUser(self, msg):
        pass

    def searchQuestions(self, keywords):
        logging.info("A search has been requested. Getting search engine's source (at config.yaml)")
        searchSource = self.configData["searchSource"]
        logging.info(f"searchSource is: {searchSource}")
        
        if searchSource == "primaryLayer": #PDF: data extraction source
            logging.info("Requesting allQuestions from all PDF files in sourceDir")
            filesAndQuestions, questionsKeysList = self.extractPDFContent() #files, allQuestions and KeyList to support finding questions by index
            files = filesAndQuestions.keys()
            for f in files:
                allQuestions = filesAndQuestions.get(f)
                logging.info(f"Running thru: {f}")
                search = searchEngine.Search_Class(keywords, allQuestions, questionsKeysList)
                rankMatches = search.rankMatches()
                return rankMatches
            
        elif searchSource == "secondLayer": #YAML: structured data endpoint
            msg = f"cant work with this layer yet: {searchSource}"
            raise msg
        elif searchSource == "thirdLayer": #DATABASE: high availability source
            msg = f"cant work with this layer yet: {searchSource}"
            raise msg
        else:
            msg = f"unrecognized layer: {searchSource}"
            raise msg

        
        logging.info("search engine is done")
        

class Manager_Work_Flow(object):
    def __init__(self, configData):
        logging.info("Manager wake up")
        self.configData = configData
        self.tools = Manager_Toolset(self.configData)
        logging.info("Manager ready to work")

    def runTasks(self):
        logging.info("Starting tasks work flow")
        # Is there a user call? Initiate Talk. Do NOT allow search requests.
        self.tools.primaryUserCommunication()
        # Are there new files to read?
        hasNewData = self.tools.hasNewData()
        if hasNewData[0]:
            # >> Y: notify user, start reading, fitDataToYaml, registerKnownFiles, updateConfigData
            msg = f"New file(s) to read: {hasNewData[1]}"
            self.tools.msgToUser(msg)
            if self.configData["async"]:
                msg = f"No problem. I can read while we talk."
                self.tools.msgToUser(msg)
                # allowUserRequests
            else:
                msg = f"Please wait while I finish reading"
                self.tools.msgToUser(msg)

        # >> N: allowUserRequests
        pass

        # >> runUserRequests
        words = ["lambda", "s3"]
        questions = self.tools.searchQuestions(words)
        print(f"questions: {questions}")
        

