# Dealing with ImportError: attempted relative import with no known parent package
import sys

from requests import patch
sys.path.append('.')

from use_cases.dataColector import PDF_Master

import unittest.main
from unittest import TestCase
from unittest.mock import MagicMock

class test_dataColector(TestCase, MagicMock):

    # @patch(PDF_Master.openPDF)
    def test_dataColector_gets_none(self):
        doc = """
        SET 1: PRACTICE QUESTIONS ONLY
        Click here to go directly to Set 1: Practice Questions, Answers & Explanations

        1. Question
        
        A developer is planning to use a Lambda function to process incoming requests from an Application Load Balancer (ALB). How can this be achieved?
        1: Create a target group and register the Lambda function using the AWS CLI
        2: Create an Auto Scaling Group (ASG) and register the Lambda function in the launch configuration
        3: Setup an API in front of the ALB using API Gateway and use an integration request to map the request to the Lambda function
        4: Configure an event-source mapping between the ALB and the Lambda function

        2. Question
        
        A developer is troubleshooting problems with a Lambda function that is invoked by Amazon SNS and repeatedly fails. How can the developer save discarded events for further processing?
        1: Enable CloudWatch Logs for the Lambda function
        2: Configure a Dead Letter Queue (DLQ)
        3: Enable Lambda streams
        4: Enable SNS notifications for failed events
        
        """

        pdfClass = PDF_Master()
        pdfClass.openPDF.pages = MagicMock(return_value=doc)

        firstValue = ["""1. Question
        A developer is planning to use a Lambda function to process incoming requests from an Application Load Balancer (ALB). How can this be achieved?""",
        """2. Question
        A developer is troubleshooting problems with a Lambda function that is invoked by Amazon SNS and repeatedly fails. How can the developer save discarded events for further processing?"""]
        secondValue = PDF_Master.getQuestions()
        # error message in case if test case got failed
        message = "First value and second value are not equal !"

        self.assertEqual(firstValue, secondValue, message)

  
if __name__ == '__main__':
    unittest.main()