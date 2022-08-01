# Dealing with ImportError: attempted relative import with no known parent package
import sys

sys.path.append('.')

import logging

class Search_Class(object):
    def __init__(self, keywords:list=None, allQuestions:list=None):
        """
        keywords: all reference words provided by the user when requesting a Search
        allQuestions: Set of questions captured from the searchSource described at config.yaml 
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

        
    def findMatches(self):
        logging.info("matching keywords with questions")
        allMatches = {}
        questionIndex = -1

        for q in self.allQuestions:
            q = q.lower()
            questionIndex += 1 # starts at zero
            for keyword in self.keywords:
                keyword = keyword.lower()
                if keyword in q: # it's a match
                    if questionIndex not in allMatches:
                        allMatches[questionIndex]=[keyword] # add question to the list
                    else:
                        allMatches[questionIndex].append(keyword) # add keyword to existing questions on the list
                else:
                    pass
        
        logging.info(f"Matching process completed. Returning {len(allMatches)} matches")
        
        print(allMatches)
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

        print(f"unsorted rank: {refRank}")

        # rearenge matches by punctuation (lower -> higher)
        refRank = dict(sorted(refRank.items(), key=lambda item: item[1], reverse=True))
        
        for i in refRank:
            rankedMatches[i]=matches[i]

        print(f"final rank: {rankedMatches}")
        return rankedMatches