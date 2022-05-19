'''

sliding_windows.py
~~~~~~~~~~~~~~~~~~~
This module:
    1. Creates a table environment
    2. Creates a source table from a Kinesis Data Stream
    3. Creates a sink table writing to a Kinesis Data Stream
    4. Queries from the Source Table and creates a sliding window over 10
       seconds to calculate the minimum value over the window.
    5. (REMOVED) These sliding window results are inserted into the Sink table.

Note: https://github.com/aws-samples/pyflink-getting-started/blob/main/pyflink-examples/SlidingWindows/sliding-windows.py

'''

import os
import json

#
# pyflink: conditionally install apache-flink with java 11
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

APPLICATION_PROPERTIES_FILE_PATH = '/etc/flink/application_properties.json' # on kda

#
# set env var for local environment
#
is_local = (True if os.environ.get('IS_LOCAL') else False)

if is_local:
    #
    # only for local, overwrite variable to properties and pass in your jars
    # delimited by a semicolon (;)
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
    timestamp_format_standard
):
    '''

    @timestamp_format_standard
        SQL, parse input TIMESTAMP values in yyyy-MM-dd HH:mm:ss.s{precision}; and
            parse input TIMESTAMP_LTZ values in yyyy-MM-dd HH:mm:ss.s{precision}'Z'

        ISO-8601, parse input TIMESTAMP in yyyy-MM-ddTHH:mm:ss.s{precision}; and
            parse input TIMESTAMP_LTZ in yyyy-MM-ddTHH:mm:ss.s{precision}'Z'

    '''

    return """CREATE TABLE {0} (
        ticker VARCHAR(6),
        price DOUBLE,
        utc TIMESTAMP(3),
        WATERMARK FOR utc AS utc - INTERVAL '5' SECOND
    )
    PARTITIONED BY (ticker)
    WITH (
        'connector' = 'kinesis',
        'stream' = '{1}',
        'aws.region' = '{2}',
        'scan.stream.initpos' = '{3}',
        'sink.partitioner-field-delimiter' = ';',
        'sink.producer.collection-max-count' = '100',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601'
    )""".format(table_name, stream_name, region, stream_initpos, timestamp_format_standard)


def create_print_table(table_name):
    '''

    print connector for sink

    Note: when print is used as sink, the job results are printed to standard output.
          if you don’t need to view the output, you can use 'blackhole' as sink

    '''

    return """CREATE TABLE {0} (
        ticker VARCHAR(6),
        price DOUBLE,
        utc TIMESTAMP(3),
        WATERMARK FOR utc AS utc - INTERVAL '20' SECOND
    ) WITH (
        'connector' = 'print'
    )""".format(table_name)


def perform_sliding_window_aggregation(
    input_table_name,
    sliding_window_over='8.hours',
    sliding_window_every='1.minutes',
    sliding_window_on='utc',
    sliding_window_alias='eight_hour_window'
):
    '''

    perform sliding window

    '''

    input_table = table_env.from_path(input_table_name)
    sliding_window_table = (
        input_table.window(
            Slide.over(sliding_window_over)
            .every(sliding_window_every)
            .on(sliding_window_on)
            .alias(sliding_window_alias)
        )
        .group_by('ticker, {}'.format(sliding_window_alias))
        .select('ticker, price.min as price, {}.end as {}'.format(
            sliding_window_alias,
            sliding_window_on
        ))
    )
    #sliding_window_table = input_table.select('ticker, price, utc')

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
    sliding_window_alias = consumer_property_map['flink.sliding_window.alias']
    timestamp_format_standard = consumer_property_map['json.timestamp_format_standard']

    #
    # 2. Creates a source table from a Kinesis Data Stream
    #
    response = table_env.execute_sql(
        create_table(
            input_table_name,
            input_stream,
            input_region,
            stream_initpos,
            timestamp_format_standard
        )
    )

    #
    # 3. Creates a sink table writing to a Kinesis Data Stream
    #
    # Note: and appropriate 'create_table' invocation needs to be supplied
    #       below if a kinesis stream sink is desired
    #
    if is_local:
        table_env.execute_sql(
            create_print_table(output_table_name)
        )

    #
    # 4. Queries from the Source Table and creates a sliding window over 10
    #    seconds to calculate the minimum value over the window.
    #
    sliding_window_table = perform_sliding_window_aggregation(
        input_table_name,
        sliding_window_over,
        sliding_window_every,
        sliding_window_on,
        sliding_window_alias
    )
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


if __name__ == '__main__':
    main()
