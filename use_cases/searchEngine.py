# Dealing with ImportError: attempted relative import with no known parent package
import sys

sys.path.append('.')

import logging

class Search_Class(object):
    def __init__(self, keywords:list=None, allQuestions:dict=None, questionsKeysList:list=None):
        """
        keywords: all reference words provided by the user when requesting a Search
        allQuestions: Set of questions captured from the searchSource described at config.yaml 
        questionsKeysList: List of keys from allQuestionns with the same index as the dict allQuestions. Will be used to find the questions by index.
        """
        logging.info("Search Class initialized")

        if keywords is None:
            response = "No reference words have been provided by the user. Search Engine wont move on."
            raise response

        if allQuestions is None:
            response = "No questions have been provided by the system. Search Engine wont move on."
            raise response

        self.keywords = keywords
        self.allQuestions = allQuestions
        self.questionsKeysList = questionsKeysList

        
    def findMatches(self):
        logging.info("matching keywords with questions")
        allMatches = {}
        questionIndex = -1

        for k in self.allQuestions:
            question = self.allQuestions[k].lower()
            questionIndex += 1 # starts at zero
            for keyword in self.keywords:
                keyword = keyword.lower()
                if keyword in question: # it's a match
                    if questionIndex not in allMatches:
                        allMatches[questionIndex]=[keyword] # add new question to the list
                    else:
                        allMatches[questionIndex].append(keyword) # add keyword to existing questions on the list
                else:
                    pass
        
        logging.info(f"Matching process completed. Returning {len(allMatches)} matches")
        
        return allMatches # dict {questionIndex: ['anyWord']}
        # REMEMBER: questionIndex starts at zero
    
    def rankMatches(self):
        matches = self.findMatches()
        refRank = {}
        rankedMatches = {}

        # count matches | primary punctuation
        for index in matches:
            matchCount = len(matches[index])
            refRank[index] = matchCount

        # rearenge matches by punctuation (lower -> higher)
        refRank = dict(sorted(refRank.items(), key=lambda item: item[1], reverse=True))
        
        for index in refRank:
            qNumber = f"{self.questionsKeysList[index]}:"
            rankedMatches[qNumber]=matches[index]

        return rankedMatches