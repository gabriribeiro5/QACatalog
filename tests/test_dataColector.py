# Dealing with ImportError: attempted relative import with no known parent package
import sys

sys.path.append('.')

from use_cases.dataColector import PDF_Master

import unittest.main
from unittest import TestCase
from unittest.mock import MagicMock
from unittest import mock

class test_dataColector(TestCase):

    def test_dataColector_returns_first_question(self):    
        # define static values
        expectedValue = "1. Question: A developer is planning to use a Lambda function to process incoming requests from an Application Load \nBalancer (ALB). How can this be achieved? \n"
        errorMessage = "Expected value and returned value are not equal !"

        # get values from tested method
        source_test_doc = PDF_Master("source_test_dataColector", "tests")
        questions = source_test_doc.getQuestions()
        returnedValue = questions[0]

        self.assertEqual(expectedValue, returnedValue, errorMessage)
    

    def test_dataColector_gets_none(self):
        pass
    

  
if __name__ == '__main__':
    unittest.main()