#!/usr/bin/python

from urlparse import urlparse
import multiprocessing
import boto3
import json
import sys
import subprocess
import os

def exists(args, filename, table='Compression'):
    dynamodb = boto3.client("dynamodb", region_name=args.region, endpoint_url=args.endpoint)

    response = dynamodb.get_item(TableName=table, Key={'Filename':{'S':filename}})
    try:
        response['Item']
        return True;
    except KeyError:
        return False;
#

def store(data, args, table='Compression'):
    dynamodb = boto3.resource("dynamodb", region_name=args.region, endpoint_url=args.endpoint)
#
    table = dynamodb.Table(table)
    data = json.loads(data)
    response = table.put_item(Item=data)
    try:
        response = table.put_item(Item=data)
    except:
        print data
        raise

def compress(filename, args):

    env = dict(os.environ)
    env['DYLD_LIBRARY_PATH'] = '/Users/hobu/pdal-build/lib/'
#    command = """DYLD_LIBRARY_PATH=/Users/hobu/pdal-build/lib/ /Users/hobu/dev/git/pdal-compression-test/pdal-compression-test %s""" % filename

    command = """pdal-compression-test %s""" % filename
#     print command
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, env=env, shell=True)
    proc.wait()
    data = proc.stdout.read()

    return data

def mp_worker((filename, args)):
    print " Processsing %s" % (filename)
    if not exists(args, filename):
        data = compress(filename, args)
        store(data, args)
    print " Process %s\tDONE" % filename

def get_files(args):

    o = urlparse(args.bucket)


    path = o.path.strip('/')
    path = path +'/'

    output = []
#     print path, o.hostname
    client = boto3.client('s3')
    resource = boto3.resource('s3')
    paginator = client.get_paginator('list_objects')
    for result in paginator.paginate(Bucket=o.hostname, Delimiter='/', Prefix=path):
#         import pdb;pdb.set_trace()
#         if result.get('CommonPrefixes') is not None:

        if result.get('Contents') is not None:
            for f in result.get('Contents'):
                filename = 's3://'+o.hostname+'/'+f['Key']
                output.append((filename, args))

    return output

def handler(args):
    p = multiprocessing.Pool(args.pool_size)

    filenames = get_files(args)
#     filenames = filenames[0:2]
#    filenames= [('s3://iowa-lidar/IA_LAZlib/02204732.laz', args),]
    p.map(mp_worker, filenames)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Queue up some operations')
    parser.add_argument('--pool_size', dest='pool_size', default=2, type=int)
    parser.add_argument('--endpoint', dest='endpoint', default='https://dynamodb.us-east-1.amazonaws.com')
    parser.add_argument('--region', dest='region', default='us-east-1')
    parser.add_argument('--bucket', dest='bucket')

    args = parser.parse_args()
    handler(args)
