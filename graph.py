#!/usr/bin/python

# s3://iowa-lidar/IA_LAZlib/02244794.laz

import boto3
import numpy as np

from urlparse import urlparse


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
def exists(args, filename, table='Compression'):
    dynamodb = boto3.client("dynamodb", region_name=args.region, endpoint_url=args.endpoint)

    response = dynamodb.get_item(TableName=table, Key={'Filename':{'S':filename}})
    try:
        response['Item']
        return True;
    except KeyError:
        return False;


def get_data(args, table='Compression'):
    dynamodb = boto3.resource("dynamodb", region_name=args.region, endpoint_url=args.endpoint)

    table = dynamodb.Table(table)
    response = table.scan()
    output = []
    for i in response['Items']:
        output.append(i)

    dt = np.dtype([('laz_time', np.float),
                   ('lzma_time', np.float),
                   ('laz_size', np.float),
                   ('laszip_size', np.float),
                   ('lzma_size', np.float),
                   ('size', np.float),
                   ('count', np.int64)])
    data = np.full(len(output), 0, dtype=dt)

    count = 0
    for record in output:
        data[count]['laz_time'] = float(record['lazperf']['compression_time']) * 1e-6
        data[count]['lzma_time'] = float(record['lzma']['compression_time']) * 1e-6
        data[count]['laz_size'] = float(record['lazperf']['compressed_size']) / (1024 * 1024)
        data[count]['laszip_size'] = float(record['stats']['laz_size']) / (1024 * 1024)
        data[count]['lzma_size'] = float(record['lzma']['compression_size']) / (1024 * 1024)
        data[count]['size'] = float(record['stats']['buffer_size'] )  / (1024 * 1024)

        data[count]['count'] = record['stats']['count']
        count = count + 1


    return data


def plot_time(data):
    import matplotlib.pyplot as plt

    fig= plt.figure(figsize=(12, 10))
    ax = fig.add_subplot(1,1,1)
    scatter = ax.scatter(data['laz_time'], data['lzma_time'], c= data['size'])
    plt.xlim([0, 20])
    plt.ylim([0, 100])
    plt.title("Lazperf vs. LZMA compression time")
    ax.grid(color='b', linestyle='-', linewidth=0.5)
    ax.set_xlabel('lazperf Compression Time (sec)')
    ax.set_ylabel('LZMA Compression Time (sec)')
    cb = plt.colorbar( scatter)
    cb.set_label('LAS size (mb)')

    fig.savefig("laz-vs-lzma-time.png")

def plot_laz_v_lzma(data):
    import matplotlib.pyplot as plt
    import matplotlib

    fig= plt.figure(figsize=(12, 10))
    ax = fig.add_subplot(1,1,1)
    scatter = ax.scatter(data['laszip_size'], data['lzma_size'], c = data['size'])
    plt.xlim([0, max(data['laszip_size'])])
    plt.ylim([0, max(data['laz_size'])])
    plt.title("LAZ vs. LZMA")
    ax.grid(color='b', linestyle='-', linewidth=0.5)
    ax.set_xlabel('LASzip size (mb)')
    ax.set_ylabel('LZMA size (mb)')
    ax.get_xaxis().set_major_formatter(
    matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    ax.get_yaxis().set_major_formatter(
    matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
#     ax.legend()
    cb = plt.colorbar( scatter)
    cb.set_label('LAS size (mb)')

    fig.savefig("laszip-vs-lzma-size.png")

def plot_laz_v_lazperf(data):
    import matplotlib.pyplot as plt
    import matplotlib

    fig= plt.figure(figsize=(12, 10))
    ax = fig.add_subplot(1,1,1)
    scatter = ax.scatter(data['laszip_size'], data['laz_size'], c = data['size'])
    plt.xlim([0, max(data['laszip_size'])])
    plt.ylim([0, max(data['laz_size'])])
    plt.title("LAZ vs. Lazperf")

    ax.grid(color='b', linestyle='-', linewidth=0.5)
    ax.set_xlabel('LASzip size (mb)')
    ax.set_ylabel('lazperf size (mb)')
    ax.get_xaxis().set_major_formatter(
    matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    ax.get_yaxis().set_major_formatter(
    matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
#     ax.legend()
    cb = plt.colorbar( scatter)
    cb.set_label('LAS size (mb)')

    fig.savefig("laszip-vs-lazperf-size.png")

def plot_size(data):
    import matplotlib.pyplot as plt
    import matplotlib

    fig= plt.figure(figsize=(12, 10))
    ax = fig.add_subplot(1,1,1)
    scatter = ax.scatter(data['laz_size'], data['lzma_size'], c = data['size'])
    plt.xlim([0, max(data['laz_size'])])
    plt.ylim([0, max(data['laz_size'])])
    plt.title("Lazperf vs. LZMA")

    ax.grid(color='b', linestyle='-', linewidth=0.5)
    ax.set_xlabel('lazperf size (mb)')
    ax.set_ylabel('LZMA size (mb)')
    ax.get_xaxis().set_major_formatter(
    matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    ax.get_yaxis().set_major_formatter(
    matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
#     ax.legend()
    cb = plt.colorbar( scatter)
    cb.set_label('LAS size (mb)')

    fig.savefig("laz-vs-lzma-size.png")

def plot_ratio(data):
    import matplotlib.pyplot as plt

    fig= plt.figure(figsize=(10, 10))
    ax = fig.add_subplot(1,1,1)

    ratio = data['size']/data['laz_size']
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(12, 5))

    # rectangular box plot
    bplot1 = axes[0].boxplot(ratio,
                             vert=True,   # vertical box aligmnent
                             patch_artist=True)   # fill with color

    # notch shape box plot
    bplot2 = axes[1].boxplot(ratio,
                             notch=True,  # notch shape
                             vert=True,   # vertical box aligmnent
                             patch_artist=True)   # fill with color

    # fill with colors
    colors = ['pink', 'lightblue', 'lightgreen']
    for bplot in (bplot1, bplot2):
        for patch, color in zip(bplot['boxes'], colors):
            patch.set_facecolor(color)

#     # adding horizontal grid lines
#     for ax in axes:
#         ax.yaxis.grid(True)
#         ax.set_xticks([y+1 for y in range(len(data))], )
#         ax.set_xlabel('xlabel')
#         ax.set_ylabel('ylabel')
#
#     # add x-tick labels
#     plt.setp(axes, xticks=[y+1 for y in range(len(data))],
#              xticklabels=['x1', 'x2', 'x3', 'x4'])

#     ax.boxplot(ratio)
#     plt.xlim([0, 100])
#     plt.ylim([0, 100])
# #     ax.grid(color='b', linestyle='-', linewidth=0.5)
#     ax.set_xlabel('lazperf Compression Time (sec)')
#     ax.set_ylabel('LZMA Compression Time (sec)')

    fig.savefig("laz-ratio.png")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Queue up some operations')
    parser.add_argument('--pool_size', dest='pool_size', default=2, type=int)
    parser.add_argument('--endpoint', dest='endpoint', default='https://dynamodb.us-east-1.amazonaws.com')
    parser.add_argument('--region', dest='region', default='us-east-1')
    parser.add_argument('--bucket', dest='bucket')

    args = parser.parse_args()
    args.bucket='s3://iowa-lidar/IA_LAZlib/'
    data = get_data(args)
    print len(data)
    print data[0]
    plot_time(data)
    plot_size(data)
    plot_ratio(data)
    plot_laz_v_lzma(data)
    plot_laz_v_lazperf(data)
#     import pdb;pdb.set_trace()
#     files = get_files(args)
#     print 'file length: ', len(files)
#     print files[0]
#     filename = 's3://iowa-lidar/IA_LAZlib/02144750.laz2'
#     response = exists(args, filename)
#     filename = 's3://iowa-lidar/IA_LAZlib/02144750.laz'
#     response = exists(args, filename)
