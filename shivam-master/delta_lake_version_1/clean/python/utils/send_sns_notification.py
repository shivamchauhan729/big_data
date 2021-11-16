#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'Godaddy'

#######################################Module Information########################################################
#  Module Name         : SendSnsNotification                                               					    #
#  Purpose             : For sending SNS notification to subscribed email					                    #
#  Input Parameters    : Subject, Message, snsTopicArn                                                          #
#  Output Value        : Send SNS Notification                                                                  #                      
#################################################################################################################

# Library and external modules declaration
import boto3

# Boto3 Attributes
sns_client = boto3.client('sns')

class SendSnsNotification(object):
    """
    Sends SNS Notification to subscribed ARN with the given subject and message
    """
    def send_notification(self, subject, message, sns_topic_arn):
        try:
            response = sns_client.publish(
                TopicArn=sns_topic_arn,
                Message=message,
                Subject=subject
            )
            print("####### - SNS Sent Successfully")
            return response
        except Exception as e:
            print("####### - Error Occured while sending SNS Notification")
            print("####### - Error Message is -> " +str(e))