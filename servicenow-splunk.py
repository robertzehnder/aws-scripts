import requests
import json
import boto3
import datetime
import os
from requests.auth import HTTPBasicAuth

s3 = boto3.resource('s3')
bucket = 'xxxxxxx'

#Gets Servicenow login credentials

auth_items = ['username','password']
creds = {}

for item in auth_items:
    key = 'script_parameters/credentials/{0}.txt'.format(item)
    obj = s3.Object(bucket, key)
    creds[item] = obj.get()['Body'].read().decode('utf-8')

#print creds
user = creds['username']
pwd = creds['password']

#Get Load Balancer URLs

load_balancers = ['STG','PRD']
urls = {}

for lb in load_balancers:
    key = 'script_parameters/load_balancers/{0}_URL.txt'.format(lb)
    obj = s3.Object(bucket, key)
    urls[lb] = obj.get()['Body'].read().decode('utf-8')

print urls

# --- Pulls data from servicenow based on parameters for each table ---
def servicenow_get(url,table,query):
    url = "{0}/api/now/table/{1}{2}".format(url,table,query)
    print url
    headers = {"Content-Type":"application/json","Accept":"application/json"}
    response = requests.get(url, auth=HTTPBasicAuth(user, pwd), headers=headers )
    data = response.json()
    return data

# --- Converts API Data into apprropriate file format and size before sending to S3 ---
def s3_post(env,table,data):
    filename = '{0}.json'.format(table)
    time_str = str(datetime.datetime.utcnow())
    time = time_str[:-7].replace(' ','_')
    last = len(data)-1

    def file_separator(lines, n):
        for i in xrange(0, len(lines), n):
            yield lines[i: i + n]

    filenum = 1
    for index, lines in enumerate(file_separator(data, 10)):
        with open(filename, 'w+') as f:
            f.write("""{"results": [""")

            lst = map(str,lines)
            line = ",".join(lst)
            f.write(line)

            f.write("]}")
            f.seek(0)
            KEY = '{0}/{1}/{0}-{1}-{2}__{3}.json'.format(table,env,filenum,time)
            filenum += 1
            boto3.resource('s3').Bucket('xxxxxxx').put_object(Key=KEY,Body=f)
            print KEY + '\n'

'''_
The next section contains all of the functions that get data
from servicenow and perform any necessary transformations
before sending it to Splunk
'''

def sys_audit(env,url):
    table = 'sys_audit'
    query = "?sysparm_query=tablename!%3Dsys_embedded_help_queue%5Esys_created_onONLast%2015%20minutes%40javascript%3Ags.minutesAgoStart(15)%40javascript%3Ags.minutesAgoEnd(0)"
#   query = "?sysparm_query=sys_created_on%3Cjavascript%3Ags.dateGenerate('2018-03-28'%2C'00%3A00%3A00')%5Esys_created_on%3Ejavascript%3Ags.dateGenerate('2017-05-01'%2C'23%3A59%3A59')"
    sn_api_data = servicenow_get(url,table,query)
    data_to_post = []
    for item in sn_api_data['result']:
        data_to_post.append(json.dumps({
            'environment':env,
            'field_name':item['fieldname'],
            'new_value':item['newvalue'],
            'created':item['sys_created_on'],
            'table_name':item['tablename'],
            'user':item['user'],
            'old_value':item['oldvalue']
        }))

    data_to_post = []


    s3_post(env,table,data_to_post)

def sysevent(env,url):
    table = 'sysevent'
    query = "?sysparm_query=name!%3Dglide.heartbeat%5Esys_created_onONLast%2015%20minutes%40javascript%3Ags.minutesAgoStart(15)%40javascript%3Ags.minutesAgoEnd(0)"
#    query = "?sysparm_query=sys_created_on%3Cjavascript%3Ags.dateGenerate('2018-03-28'%2C'00%3A00%3A00')%5Esys_created_on%3Ejavascript%3Ags.dateGenerate('2017-05-01'%2C'23%3A59%3A59')"
    sn_api_data = servicenow_get(url,table,query)
    data_to_post = []
    for item in sn_api_data['result']:
        data_to_post.append(json.dumps({
            'environment':env,
            'user_name':item['user_name'],
            'name':item['name'],
            'created':item['sys_created_on'],
            'parm1':item['parm1'],
            'parm2':item['parm2'],
            'claimed_by':item['claimed_by']
        }))

    s3_post(env,table,data_to_post)

def syslog_transaction(env,url):
    table = 'syslog_transaction'
    query = "?sysparm_query=sys_created_by%3Dsystem%5Esys_created_onONLast%2015%20minutes%40javascript%3Ags.minutesAgoStart(15)%40javascript%3Ags.minutesAgoEnd(0)"
#    query = "?sysparm_query=sys_created_on%3Cjavascript%3Ags.dateGenerate('2018-03-28'%2C'00%3A00%3A00')%5Esys_created_on%3Ejavascript%3Ags.dateGenerate('2017-05-01'%2C'23%3A59%3A59')"
    sn_api_data = servicenow_get(url,table,query)
    data_to_post = []
    count = 0
    for item in sn_api_data['result']:
        ip_list = item['remote_ip'].split(',')
        data_to_post.append(json.dumps({
            'environment':env,
            'type':item['type'],
            'request_ip':ip_list[0],
            'created':item['sys_created_on'],
            'sql_time':item['sql_time'],
            'system_id':item['system_id'],
            'sys_created_by':item['sys_created_by'],
            'table':item['table'],
            'url':item['url'],
            'transaction_processing_time':item['transaction_processing_time']
        }))

    s3_post(env,table,data_to_post)

def sys_attachment(env,url):
    table = 'sys_attachment'
    query = "?sysparm_query=sys_created_onONLast%2015%20minutes%40javascript%3Ags.minutesAgoStart(15)%40javascript%3Ags.minutesAgoEnd(0)"
#    query = "?sysparm_query=sys_created_on%3Cjavascript%3Ags.dateGenerate('2018-03-28'%2C'00%3A00%3A00')%5Esys_created_on%3Ejavascript%3Ags.dateGenerate('2017-05-01'%2C'23%3A59%3A59')"
    sn_api_data = servicenow_get(url,table,query)
    data_to_post = []
    for item in sn_api_data['result']:
        data_to_post.append(json.dumps({
            'environment':env,
            'file_name':item['file_name'],
            'table_name':item['table_name'],
            'updated':item['sys_updated_on'],
            'user_name':item['u_user_name']
        }))

    s3_post(env,table,data_to_post)

#This section takes the URLs for each environment and repeats the process of getting data from each type of table

for env,url in urls.iteritems():
    try:
        print env
        sys_audit(env,url)
        sysevent(env,url)
        syslog_transaction(env,url)
        sys_attachment(env,url)
    except Exception:
        print Exception
        continue
