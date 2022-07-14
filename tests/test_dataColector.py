# Dealing with ImportError: attempted relative import with no known parent package
import sys

sys.path.append('.')

from use_cases.dataColector import PDF_Master

import unittest.main
from unittest import TestCase
from unittest.mock import MagicMock
from unittest import mock

class test_dataColector(TestCase):

    def test_getQuestions_returns_first_question(self):    
        # define static values
        expectedValue = "1. Question: A developer is planning to use a Lambda function to process incoming requests from an Application Load \nBalancer (ALB). How can this be achieved? \n"
        errorMessage = "Returned value does not match the expected value"

        # get values from tested method
        source_test_doc = PDF_Master("source_test_dataColector", "tests")
        questions = source_test_doc.getQuestions()
        returnedValue = questions[0]

        self.assertEqual(expectedValue, returnedValue, errorMessage)

    def test_class_PDF_Master_raise_type_error_for_parameter_None(self):
        with self.assertRaises(TypeError):
            returnedValue = PDF_Master() # None type parameter

    # def test_class_PDF_Master_gets_unexistent_file(self):
    #     no_file = "unexistent_file.pdf"
    #     msg = f"no such file: 'data/{no_file}'"
    #     self.assertRaises(FileNotFoundError, PDF_Master(no_file), msg)
  
if __name__ == '__main__':
    unittest.main()