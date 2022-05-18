import json
import random
import argparse
from datetime import datetime

#
# @boto3, ensure installed within miniconda3 'site-packages' path
#
import pip
try:
    import boto3
except ModuleNotFoundError:
    pip.main(['install', 'boto3'])
    import boto3


def get_data():
    '''

    generate sample stock data prices

    '''

    return {
        'utc': str(datetime.now().isoformat()),
        'type': 'get_live_price',
        'source': 'xxx',
        'ticker': random.choice(['AAPL', 'AMZN', 'MSFT', 'TSLA']),
        'name': 'xxx common stock',
        'sector': 'technology',
        'industry': 'consumer electronics',
        'price': round(random.random() * 100, 2)
    }


def generate(stream_name, kinesis_client, partition_key):
    '''

    ingest sample data into stream_name

    '''

    while True:
        data = get_data()
        print(data)
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=partition_key
        )


if __name__ == '__main__':
    '''

    generate stock market data as follows:

    (base) jeff1evesque@computer kinesis-analytics-demo % python3 stock.py \
        --stream-name YourStreamName

    Note: https://stackoverflow.com/a/67455283

    '''

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s',
        '--stream-name',
        help='kinesis stream name',
        required=True
    )
    parser.add_argument(
        '-p',
        '--partition-key',
        help='kinesis stream partition key',
        default='partitionkey',
        required=False
    )
    args = parser.parse_args()

    generate(args.stream_name, boto3.client('kinesis'), args.partition_key)
