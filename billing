import pandas as pd
import csv
import boto3
import os
import zipfile
import zlib
import botocore
import pandas as pd
import datetime
import json
import math
import short_url
from datetime import timedelta



sns_client = boto3.client('sns')
ec2_client = boto3.client('ec2')
s3 = boto3.resource('s3')
accounts = {"Managed Services": 2,"PPB":1}
email_text = ''
summary_values = {'month':0,'week':0,'day':0}
api_data = {}
abbrev_prods = ['Cloudtrail','Cloudwatch','EC2','RDS','S3','KMS','SNS','SQS','Config','Kinesis', 'VPC','Cloudfront','Sagemaker','Rekognition','Polly','Route 53','Registrar','Lambda']
long_prods = ['AWS CloudTrail','AmazonCloudWatch','Amazon Elastic Compute Cloud','Amazon Relational Database Service','Amazon Simple Storage Service','AWS Key Management Service','Amazon Simple Notification Service','Amazon Simple Queue Service','AWS Config','Amazon Kinesis', 'Amazon Virtual Private Cloud','Amazon CloudFront','Amazon SageMaker','Amazon Rekognition','Amazon Polly','Amazon Route 53','Amazon Registrar','AWS Lambda']

for dlt_acc, value in accounts.iteritems():
        session = boto3.Session(profile_name='dlt_billing_{0}'.format(value))
        s3_client = session.client('s3')
        buckets = s3_client.list_buckets()
        acc_name = dlt_acc
        api_data["account"] = acc_name.replace(' ','_')
        name = buckets['Buckets'][0]['Name']
        s3_contents = s3_client.list_objects(Bucket=name)

        s3_objects = s3_contents['Contents']

        # get the last object in the list so you can pull the most recent file

        key = s3_objects[-1]['Key']
        print key
        s3_client.download_file(name,key,'test.zip')

        zip_ref = zipfile.ZipFile('test.zip','r')
        zip_ref.extractall('./')
        zip_ref.close()

        #----Extract Billing Data----

        df = pd.read_table(key[:-4], sep=',')  # create a dataframe

        #turn the date columns into datetime
        df['UsageStartDate'] = pd.to_datetime(df['UsageStartDate'])
        df['UsageEndDate'] = pd.to_datetime(df['UsageEndDate'])

        productlist = df['ProductName'].unique()
        #print productlist
        products = [x for x in productlist if str(x) != 'nan']

        #get today's date
        today = datetime.date.today() - timedelta(days=1)
        day = today.day
        month = today.month
        year = today.year

        api_data['timestamp'] = str(today)
        enddate = datetime.datetime(year=year,month=month,day=day,hour=23,minute=59,second=59)
        print enddate
        ppblen = 43
        mslen = 63
        dash = '-'
        header = '{0} Spend Details ({1})'.format(dlt_acc,today)
        if (value == 1):
                email_text += '{0}\n{1}\n{0}\n\n'.format(dash*(ppblen),header)
        else:
                email_text += '{0}\n{1}\n{0}\n\n'.format(dash*(mslen),header)


        def dateprinter(edate,sdate,timeframe,summ_key):
            print 'edate: {0}\nsdate: {1}\n\n'.format(edate,sdate)
            global email_text
            print '{0} |||| {1}'.format(edate,sdate)
            newdf = df[(df['UsageStartDate'] >= sdate) & (df['UsageStartDate'] <= edate)]
            email_text += 'Time Period: {0} ({1} - {2})\n\n'.format(timeframe,sdate.date(),edate.date())
            #price by product
            for product in products:
                spend = round(newdf.loc[newdf['ProductName'] == product, 'BlendedCost'].sum(),2)
                if (math.isnan(spend)):
                        spend = 0
                email_text += '\t{0}: ${1}\n'.format(abbrev_prods[long_prods.index(product)], spend)
                if (summ_key == 'day'):
                        serv_idx = long_prods.index(product)
                        api_data[str(abbrev_prods[serv_idx])] = spend
            if (summ_key == 'day'):
                print json.dumps(api_data)

            #total price
            total = round(newdf['BlendedCost'].sum(),2)
            email_text += '\t---------------------------\n\tTotal Spend: ${0}\n\n'.format(total)
            if (summ_key == 'month'):
                summary_values['month'] += total
            elif (summ_key == 'week'):
                summary_values['week'] += total
            else:
                summary_values['day'] += total
        #month
        dateprinter(enddate,datetime.datetime(year=year,month=month,day=1,hour=0,minute=0,second=0),'Month to Date','month')

        #week
        if (day == 1):
                dateprinter(datetime.datetime(year=year,month=month,day=1,hour=23,minute=59,second=59),datetime.datetime(year=year,month=month,day=1,hour=0,minute=0,second=0),'Last 7 Days','week')
        elif (day <= 7):
                dateprinter(enddate,datetime.datetime(year=year,month=month,day=1,hour=0,minute=0,second=0),'Last 7 Days','week')
        else:
                dateprinter(enddate,datetime.datetime(year=year,month=month,day=day-6,hour=0,minute=0,second=0),'Last 7 Days','week')

        #yesterday
        #print inspect.currentframe().f_back.f_lineno
        if (day == 1):
                #print 'x'
                dateprinter(datetime.datetime(year=year,month=month,day=1,hour=23,minute=59,second=59),datetime.datetime(year=year,month=month,day=1,hour=0,minute=0,second=0), 'Yesterday','day')
        else:
                dateprinter(enddate,datetime.datetime(year=year,month=month,day=day,hour=0,minute=0,second=0), 'Yesterday','day')


summary_text = '-'*41 + '\n'
summary_text += 'DLT Accounts Billing Overview\n'
summary_text += '-'*41 + '\n\n'
summary_text += 'Month to Date ({1} - {2}): ${0}\n'.format(round(summary_values['month'],2),datetime.date(year=year,month=month,day=1),enddate.date())
if (day <= 7):
        summary_text += 'Last 7 Days ({1} - {2}): ${0}\n'.format(round(summary_values['week'],2),datetime.date(year=year,month=month,day=1),enddate.date())
else:
        summary_text += 'Last 7 Days ({1} - {2}): ${0}\n'.format(round(summary_values['week'],2),datetime.date(year=year,month=month,day=day-7),enddate.date())
if (day == 1):
        summary_text += 'Yesterday ({1}): ${0}\n\n'.format(round(summary_values['day'],2),datetime.date(year=year,month=month,day=1))
        print round(summary_values['day'],2),datetime.date(year=year,month=month,day=1)
else:
        summary_text += 'Yesterday ({1}): ${0}\n\n'.format(round(summary_values['day'],2),datetime.date(year=year,month=month,day=day))
        print round(summary_values['day'],2),datetime.date(year=year,month=month,day=day)

sns_response = sns_client.publish(
    TopicArn='arn:aws-us-gov:sns:xxxxxxxxxx:billing',
#    TopicArn='arn:aws-us-gov:sns:xxxxxxxxxxxx:billing-test',
    Message=summary_text + email_text,
    Subject='PP&B AWS Daily Account Billing Update ({0})'.format(today)
)
