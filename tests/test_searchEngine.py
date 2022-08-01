# Dealing with ImportError: attempted relative import with no known parent package
import sys

sys.path.append('.')

from use_cases.dataColector import PDF_Master
from use_cases.searchEngine import Search_Class

import unittest.main
from unittest import TestCase
from unittest.mock import patch, MagicMock


class test_searchEngine(TestCase):
    def test_findMatches_returns_one_match(self):    
        # define static values
        expectedValue = {1: ['sns']}
            # syntax = {questionIndex: ['anyWord', 'anotherWord']}
            # REMEMBER: questionIndex starts at zero. It's not the question's real number
        errorMessage = "Returned value does not match the expected value"

        # get required parameters
        test_source_doc = PDF_Master("test_source", "tests")
        allQuestions = test_source_doc.getQuestions()
        keywords = ["SNS"]

        # get values from tested method
        search = Search_Class(keywords, allQuestions)
        returnedValue = search.findMatches()

        self.assertEqual(expectedValue, returnedValue, errorMessage)

    # @patch('use_cases.searchEngine.Search_Class.findMatches')
    def test_rankMatches_returns_rankedMatches(self):
        # define static values
        allMatches = {0: ['developer', 'lambda'], 1: ['developer', 'lambda', 'sns'], 2: ['s3'], 3: ['s3'], 4: ['lambda'], 5: ['developer'], 6: ['developer'], 7: ['developer', 'lambda'], 8: ['developer'], 9: ['developer'], 10: ['developer', 's3'], 11: ['developer', 's3'], 12: ['lambda']}
        expectedValue = {1: ['developer', 'lambda', 'sns'], 0: ['developer', 'lambda'], 7: ['developer', 'lambda'], 10: ['developer', 's3'], 11: ['developer', 's3'], 2: ['s3'], 3: ['s3'], 4: ['lambda'], 5: ['developer'], 6: ['developer'], 8: ['developer'], 9: ['developer'], 12: ['lambda']}
            # syntax = {questionIndex: ['anyWord', 'anotherWord']}
            # REMEMBER: questionIndex starts at zero. It's not the question's real number
        errorMessage = "Returned value does not match the expected value"

        # get required parameters
        test_source_doc = PDF_Master("test_source", "tests")
        allQuestions = test_source_doc.getQuestions()
        keywords = ["developer", "lambda", "S3", "SNS"]

        # get values from tested method
        search = Search_Class(keywords, allQuestions)
        Search_Class.findMatches = MagicMock(return_value=allMatches)
        returnedValue = search.rankMatches()

        self.assertEqual(expectedValue, returnedValue, errorMessage)

  
if __name__ == '__main__':
    unittest.main()