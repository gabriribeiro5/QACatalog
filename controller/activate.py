# Dealing with ImportError: attempted relative import with no known parent package
from importlib.metadata import files
import sys

from tomlkit import key_value
sys.path.append('.')
import logging

from user_interface import talkToUser
from use_cases import tagManager, dataColector
from utils import yamlManager


class Manager_Toolset(object):
    def __init__(self, configData):
        """
        appName: give me a PDF name and I will handle it
        dataDir: primary source directory is /data, any other dir must be specified
        """
        logging.info("Manager initialized")
        self.configData = configData
        self.appName = self.configData["appName"]
        self.dataDir = self.configData["sourceDir"]

        logging.info(f"Aplication name: {self.appName}")
        logging.info(f"Source dir: {self.dataDir}")
    
    def hasNewData(self):
        logging.info("Searching for new data files")
        
        hasNewData = False
        knownFiles = [] #load from config.yaml
        allFiles = [] #load .pdf from dataDir
        self.newFiles = []

        knownFiles = self.configData["knownFiles"]

        for file in allFiles:
            if file in knownFiles:
                pass
            else:
                hasNewData = True
                self.newFiles.append(file)
        
        return hasNewData, self.newFiles

    def extractPDFContent(self):
        hasNewData = self.hasNewData()
        files = hasNewData[1]
        newContent = {}
        badgesRef = self.configData["cloudBadges"]

        if hasNewData[0]:
            logging.info("Extracting new PDF content")
            for file in files:
                badge = dataColector.YAML_Master.findOutBadge(file)
                allQuestions = dataColector.PDF_Master.getQuestions(file, self.dataDir)
                key_value = file, allQuestions
                newContent.append(key_value)
        else:
            logging.info("Nothing new to read")
            newContent = 0
        return newContent

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

    def searchQuestion(self):
        logging.info("search engine activated")
        searchSource = self.configData["searchSource"]

        if searchSource == "primarySource":
            pass
        elif searchSource == "yaml":
            pass
        elif searchSource == "database":

        
        logging.info("search engine is done")
        

class Manager_Work_Flow(object):
    def __init__(self, configData):
        self.configData = configData
        self.tools = Manager_Toolset(self.configData)

    def checkForTasks(self):
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
            else:
                msg = f"Please wait while I finish reading"
                self.tools.msgToUser(msg)

        # >> N: allowUserRequests
        pass

