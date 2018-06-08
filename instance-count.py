import json
from pprint import pprint
import csv
import boto3
import os
import datetime
import dateutil.parser as parser
import pickle
#print 'instance count script launched...'

cw_client = boto3.client('cloudwatch')
ec2_client = boto3.client('ec2')
sns_client = boto3.client('sns')
s3 = boto3.resource('s3')
cloudwatch = boto3.resource('cloudwatch')
response = ec2_client.describe_instances()
email = 'You are receiving this email because the following changes took place in the EC2 Environment:\n\n'

def returnNotMatches(a, b):
    return [x for x in b if x not in a]

here = os.path.dirname(os.path.abspath(__file__))

filename = os.path.join(here, 'instances.txt')

f = open(filename,'rb')
old_env = pickle.load(f)
f.close()

instances = []
reservations = response['Reservations']

for reservation in reservations:
        instance_dict = {}
        instance_dict['id'] = reservation['Instances'][0]['InstanceId']
        try:
                instance_dict['name'] = reservation['Instances'][0]['Tags'][0]['Value']
        except KeyError:
                print 'unnamed instance'
                instance_dict['name'] = 'Unnamed Instance'
        instance_dict['type'] = reservation['Instances'][0]['InstanceType']
        instance_dict['state'] = reservation['Instances'][0]['State']['Name']
        instances.append(dict(instance_dict))

differences = returnNotMatches(old_env,instances)

for instance in differences:
        if (instance['state'].lower()[:4] == 'stop' ) or (instance['state'].lower() == 'Terminated') or (instance['state'].lower() == 'pending') or (instance['state'].lower() == 'shutting-down'):
                email += '{0} was {1}\n'.format(instance['name'],instance['state'])
                email += 'Full Details: {0}\n\n'.format(instance)
        elif (instance['state'].lower() == 'running' ):
                email += '{0} was started\n'.format(instance['name'])
                email += 'Full Details: {0}\n\n'.format(instance)

email += '\n--- Current Environment View ---\n\n'
for instance in instances:
        email += str(instance)
        email += '\n'

email += '\n\n--- Previous Environment View ---\n\n'
for instance in old_env:
        email += str(instance)
        email += '\n'

f = open(filename,'wb')
pickle.dump(instances,f)
f.close()

if (len(differences)>0):
        sns_response = sns_client.publish(
            TopicArn='arn:aws-us-gov:sns:us-gov-west-1:xxxxxxxx:instance-count',
            Message=email,
            Subject='EC2 Environment Change Alert'
        )
