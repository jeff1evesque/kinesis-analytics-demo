"""

tumbling_windows.py
~~~~~~~~~~~~~~~~~~~
This module:
    1. Creates a table environment
    2. Creates a source table from a Kinesis Data Stream
    3. Creates a sink table writing to a Kinesis Data Stream
    4. Queries from the Source Table and creates a tumbling window over 1
       minute to calculate a candlestick (min, max, first_value, and last_value) over the window

Note: https://github.com/aws-samples/pyflink-getting-started/blob/main/pyflink-examples/SlidingWindows/sliding-windows.py

"""

import os
import json
from udf.candlestick import created_period
from datetime import datetime, timezone

#
# pyflink: conditionally install apache-flink (with java 8 or 11 already installed)
#
# Note: AWS supports flink 1.13.2:
#
#     https://docs.aws.amazon.com/kinesisanalytics/latest/java/how-creating-apps.html
#
try:
    from pyflink.table import EnvironmentSettings, StreamTableEnvironment
    from pyflink.table.window import Slide

except ModuleNotFoundError:
    import sys, subprocess

    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'apache-flink==1.13.2'])

finally:
    from pyflink.table import EnvironmentSettings, StreamTableEnvironment
    from pyflink.table.window import Slide

#
# 1. Creates a Table Environment
#
env_settings = (
    EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
)
table_env = StreamTableEnvironment.create(environment_settings=env_settings)
APPLICATION_PROPERTIES_FILE_PATH = '/etc/flink/application_properties.json'

#
# register udf
#
table_env.create_temporary_system_function('created_period', created_period)

#
# When running PyFlink with more parallelism than available kinesis data stream
# shards, some consumer instances could idle, preventing watermarks in the event
# time processing application to advance. Workarounds include setting the same
# number of parallelism as shards, or defining 'Shard Idle Interval Milliseconds'
#
# https://github.com/aws-samples/pyflink-getting-started/issues/1#issuecomment-1148647011
#
table_env.get_config().get_configuration().set_string(
    'parallelism.default',
    '1'
)

#
# set env var for local environment
#
is_local = (True if os.environ.get('IS_LOCAL') else False)
print('is_local: {}'.format(is_local))

if is_local:
    #
    # overwrite properties and pass jars delimited by a semicolon (;)
    #
    APPLICATION_PROPERTIES_FILE_PATH = 'application_properties.json'
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    table_env.get_config().get_configuration().set_string(
        'pipeline.jars',
        'file://{}/flink-sql-connector-kinesis_2.12-1.13.2.jar'.format(CURRENT_DIR),
    )


def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, 'r') as file:
            contents = file.read()
            properties = json.loads(contents)
            return properties
    else:
        print('A file at "{}" was not found'.format(APPLICATION_PROPERTIES_FILE_PATH))


def property_map(props, property_group_id):
    for prop in props:
        if prop['PropertyGroupId'] == property_group_id:
            return prop['PropertyMap']


def create_table(
    table_name,
    stream_name,
    region,
    stream_initpos,
    timestamp_format_standard,
    connector='kinesis'
):
    """

    @timestamp_format_standard
        SQL, parse input TIMESTAMP prices in yyyy-MM-dd HH:mm:ss.s{precision}; and
            parse input TIMESTAMP_LTZ prices in yyyy-MM-dd HH:mm:ss.s{precision}Z

        ISO-8601, parse input TIMESTAMP in yyyy-MM-ddTHH:mm:ss.s{precision}; and
            parse input TIMESTAMP_LTZ in yyyy-MM-ddTHH:mm:ss.s{precision}Z

    """

    return """CREATE TABLE {0} (
        ticker VARCHAR(6),
        price DOUBLE,
        utc TIMESTAMP(3),
        WATERMARK FOR utc AS utc - INTERVAL '20' SECOND
    )
    PARTITIONED BY (ticker)
    WITH (
        'connector' = '{5}',
        'stream' = '{1}',
        'aws.region' = '{2}',
        'scan.stream.initpos' = '{3}',
        'sink.partitioner-field-delimiter' = ';',
        'sink.producer.collection-max-count' = '100',
        'format' = 'json',
        'json.timestamp-format.standard' = '{4}'
    )""".format(
        table_name,
        stream_name,
        region,
        stream_initpos,
        timestamp_format_standard,
        connector
    )


def create_print_table(table_name, watermark_interval='20', connector='print'):
    """

    print connector for sink

    Note: when print is used as sink, the job results are printed to standard output.
          if you don’t need to view the output, you can use 'blackhole' as sink

    """

    return """CREATE TABLE {0} (
        ticker VARCHAR(6),
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        first_price DOUBLE,
        last_price DOUBLE,
        min_price DOUBLE,
        max_price DOUBLE
    ) WITH (
        'connector' = '{2}'
    )""".format(table_name, watermark_interval, connector)


def candlestick_aggregation(
    tbl_env,
    input_table_name,
    sliding_window_every='1',
    sliding_window_on='utc',
    field_tumble='price',
    field_ticker='ticker'
):
    """

    generate lowest and highest value (whisker), as well as first and last
    value (body) for each sliding window to help generate candlestick data.

    """

    sliding_window_table = tbl_env.sql_query('''
        SELECT
            {4},
            TUMBLE_START({1}, INTERVAL {2}) AS window_start,
            TUMBLE_END({1}, INTERVAL {2}) AS window_end,
            FIRST_VALUE({3}) AS first_price,
            LAST_VALUE({3}) AS last_price,
            MIN({3}) AS min_price,
            MAX({3}) AS max_price
        FROM {0}
        GROUP BY
            TUMBLE({1}, INTERVAL {2}),
            {4}
    '''.format(
        input_table_name,
        sliding_window_on,
        sliding_window_every,
        field_tumble,
        field_ticker
    ))

    return sliding_window_table


def main():
    # Application Property Keys
    consumer_property_group_key = 'consumer.config.0'
    sliding_property_group_key = 'sliding_window.config'

    # tables
    input_table_name = 'input_table'
    output_table_name = 'output_table'

    # get application properties
    props = get_application_properties()

    consumer_property_map = property_map(props, consumer_property_group_key)
    sliding_window_map = property_map(props, sliding_property_group_key)

    input_stream = consumer_property_map['input.stream.name']
    input_region = consumer_property_map['aws.region']
    stream_initpos = consumer_property_map['flink.stream.initpos']

    sliding_window_over = consumer_property_map['flink.sliding_window.over']
    sliding_window_every = consumer_property_map['flink.sliding_window.every']
    sliding_window_on = consumer_property_map['flink.sliding_window.on']
    timestamp_format_standard = consumer_property_map['json.timestamp_format_standard']

    #
    # 2. Creates a source table from a Kinesis Data Stream
    #
    table_env.execute_sql(
        create_table(
            input_table_name,
            input_stream,
            input_region,
            stream_initpos,
            timestamp_format_standard
        )
    )

    tbl_input = table_env.from_path(input_table_name)
    print('\nSource Schema')
    tbl_input.print_schema()

    #
    # 3. Creates a sink table writing to a Kinesis Data Stream
    #
    # Note: 'create_table' invocation must replace below 'create_print_table'
    #       if an actual sink (i.e. kinesis stream) is desired
    #
    if is_local:
        table_env.execute_sql(
            create_print_table(output_table_name)
        )

    else:
        table_env.execute_sql(
            create_print_table(output_table_name, connector='blackhole')
        )

    tbl_sink = table_env.from_path(output_table_name)
    print('\nSink Schema')
    tbl_sink.print_schema()

    #
    # 4. Queries from the Source Table and creates a sliding window over 10
    #    seconds to calculate the minimum price over the window.
    #
    print('sliding_window_over: {}'.format(sliding_window_over))
    print('sliding_window_every: {}'.format(sliding_window_every))
    print('sliding_window_on: {}'.format(sliding_window_on))

    sliding_window_table = candlestick_aggregation(
        table_env,
        input_table_name,
        sliding_window_every,
        sliding_window_on
    )

    print('\nsliding_window_table')
    sliding_window_table.print_schema()

    print('\ncreating temporary view for sliding window table to access within SQL')
    table_env.create_temporary_view('sliding_window_table', sliding_window_table)

    #
    # 5. These sliding windows are inserted into the sink table
    #
    table_result1 = table_env.execute_sql(
        'INSERT INTO {0} SELECT * FROM {1}'.format(
            output_table_name,
            'sliding_window_table'
        )
    )

    if is_local:
        table_result1.wait()
    else:
        print(table_result1.get_job_client().get_job_status())

    table_env.execute_sql('tbl-sliding-window')


if __name__ == '__main__':
    main()
