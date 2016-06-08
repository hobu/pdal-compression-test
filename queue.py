#!/usr/bin/python

from urlparse import urlparse
import multiprocessing
import boto3
import json
import sys
import subprocess
import os


def store(data, args, table='Compression'):
    dynamodb = boto3.resource("dynamodb", region_name=args.region, endpoint_url=args.endpoint)

    table = dynamodb.Table(table)
    data = json.loads(data)
    response = table.put_item(Item=data)


def compress(filename, args):

    env = dict(os.environ)
    env['DYLD_LIBRARY_PATH'] = '/Users/hobu/pdal-build/lib/'
#     command = """DYLD_LIBRARY_PATH=/Users/hobu/pdal-build/lib/ /Users/hobu/dev/git/pdal-compression-test/pdal-compression-test %s""" % filename

    command = """pdal-compression-test %s""" % filename
#     print command
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, env=env, shell=True)
    proc.wait()
    data = proc.stdout.read()

    return data

def mp_worker((filename, args)):
    print " Processsing %s" % (filename)
    try:
        data = compress(filename, args)
        store(data, args)
    except:
        pass
    print " Process %s\tDONE" % filename

def get_files(args):

    o = urlparse(args.bucket)


    path = o.path.strip('/')

#     print path, o.hostname
    client = boto3.client('s3')
    output = []
    objects = client.list_objects(Bucket=o.hostname)['Contents']
    for key in objects:
        filename = 's3://'+o.hostname+'/'+key['Key']
        output.append((filename, args))
    return output

def handler(args):
    p = multiprocessing.Pool(args.pool_size)

    filenames = get_files(args)
    filenames = filenames[0:2]
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
