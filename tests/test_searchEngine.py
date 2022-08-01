# Dealing with ImportError: attempted relative import with no known parent package
import sys

sys.path.append('.')

from use_cases.dataColector import PDF_Master
from use_cases.searchEngine import Search_Class

import unittest.main
from unittest import TestCase

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

    # def test_class_PDF_Master_raise_type_error_for_parameter_None(self):
    #     with self.assertRaises(TypeError):
    #         returnedValue = PDF_Master() # None type parameter

    # def test_class_PDF_Master_gets_unexistent_file(self):
    #     no_file = "unexistent_file.pdf"
    #     msg = f"no such file: 'data/{no_file}'"
    #     self.assertRaises(FileNotFoundError, PDF_Master(no_file), msg)
  
if __name__ == '__main__':
    unittest.main()