from helpers.get_data import get_url

from pyspark import SparkContext, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row,SparkSession
from pyspark.sql.functions import to_json,struct, col, lit
from pyspark.sql.utils import IllegalArgumentException
from helpers.get_data import get_url
from pyspark.sql.types import DateType
from helpers.kafka import KafkaWriter, get_topic, getjsonproducer,getstrproducer
from config import config
import json, math, datetime
import copy

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def stream_validation(bootstrap_servers,datasource,table,validation_config):
    sc = SparkContext(appName="PythonSparkStreamingKafka")
    sqlc = SQLContext(sc)
    ssc = StreamingContext(sc,10)
    jdbc_url , jdbc_properties = get_url(datasource)

    #create kafka producer in master
    producer = KafkaWriter(bootstrap_servers,datasource,table)
    producer.produce_debug("validation conig {}".format(validation_config))

    #get topic for ingestion
    topic = get_topic(datasource,table)
    def get_table_df(table):
        df = sqlc.read.jdbc(
                url = jdbc_url,
                table = table,
                properties = jdbc_properties)
        df.describe()
        df.show()
        return df

    brokerlist = ",".join(bootstrap_servers)
    kafka_properties = {}
    kafka_properties["metadata.broker.list"] = brokerlist
    producer.produce_debug("intiializing stream on topic {}".format(topic))
    kafkaStream = KafkaUtils.createDirectStream(ssc,\
                                                [topic],\
                                                kafka_properties)
#    global dependencies
    dependencies = {}
    for rule in validation_config:
        dependencies[rule.name] = []
        for d in rule.dependencies:
            producer.produce_debug("getting dependency for {}".format(d))
            name = rule.name
            dependencies[rule.name].append(get_table_df(d))

    #decode json to ds
    data_ds = kafkaStream.map(lambda v: json.loads(v[1]))

    # to print a count of the batch to spark console...
    data_ds.count().map(lambda x:'Records in this batch: %s' % x).union(data_ds).pprint()

    def wrap_rule(rulefunc):
        def wrapped_rule(time,rdd):
            global ruleindex

            spark = getSparkSessionInstance(rdd.context.getConf())
            stream_df = sqlc.createDataFrame(rdd.map(lambda v:json.loads(v)["record"]))

            # extract configuration from rule and get dependencies for current rule
            ruleconfig = rule.config

            ruledependencies = dependencies.get(rule.name)

            #message for debugger
            msg = ["{} : {}".format(k,v) for k,v in ruleconfig.items()]
            msg = ["in rule.. {} config is".format(rule.name)] + msg
            msg.append("{} dependencies for this rule".format(ruledependencies))
            msg.append("ruleindex is {}".format(ruleindex))
            producer.produce_debug("\n".join(msg))

            try:
                stream_df = stream_df.subtract(invalidated)
            except NameError as e:
                pass

            try:
                #get validated and new invalidated Df from the rule application
                #valid df exists in main func namespace, this function overwrites it with the new version after this rule
                new_invalid = rulefunc(stream_df,ruleconfig,ruledependencies)
                try:
                    invalidated = invalidated.union(new_invalid)
                except UnboundLocalError as e:
                    invalidated = new_invalid

                #if this is the last rule to be run, send data to kafka from the valid and invalid ones
                if ruleindex[0] == ruleindex[1]:
                    stream_df = stream_df.subtract(invalidated)
                    producer.produce_debug("sending to kafka on ruleindex {}".format(ruleindex))
                    send_valid = stream_df.toJSON().collect()

                    for data in send_valid:
                        producer.send_next(record = data, validity = True, rejectionrule = rule.rejectionrule)
                    del send_valid

                    send_invalid = invalidated.toJSON().collect()
                    for data in send_invalid:
                        producer.send_next(record = data, validity = False, rejectionrule = rule.rejectionrule)
                    del send_invalid
                    producer.stat_remnants()
                else:
                    producer.produce_debug("NOT sending to kafka on ruleindex {}".format(ruleindex))
            except ValueError as e:
                #processing gives an emptyRDD error if the stream producer isn't running
                producer.produce_debug("restart the stream producer! {}".format(e))
        return wrapped_rule

    def check_exists(stream_df,ruleconfig,dependencies):
        joinon = ruleconfig.get("join_cols")
        identcol = ruleconfig.get("identcol")

        #rule assumes only one dependency is loaded
        dependency = dependencies[0]

#        dependency.show()
        #validated items are indicated by a successful inner join on the configured columns
#        stream_df.show()
#        validated = stream_df.join(dependency,joinon,"inner")
        #invalid items are indicated by a left outer join on the dependenc table on the configured column
#        new_invalid = stream_df.join(dependency,joinon,"left_anti").where(col(identcol).isNull())

        new_invalid = stream_df.join(dependency,joinon,"left_anti")
        new_invalid = new_invalid.drop(identcol)

        return new_invalid


    def check_lead_time(time,rdd):
        producer.produce_debug("in process.. {}".format(time))
        # get Context for this rdd https://stackoverflow.com/questions/36051299/how-to-subtract-a-column-of-days-from-a-column-of-dates-in-pyspark
        spark = getSparkSessionInstance(rdd.context.getConf())
        # get coniguration out of rule
        config = rule.config
        # only expecting one dependency for this rule, so take first item from list by default
        dependency = dependencies.get(rule.name)[0]

        try:
            stream_df = spark.createDataFrame(rdd.map(lambda v:json.loads(v)["record"]))
            joined = stream_df.join(dependency, on = config.get("join_columns"))
            def mapfunc(record):
                start_column = record[config["start_column"]]
                stop_column = record[config["stop_column"]]
                leadtime_column = record[config["leadtime_column"]]
                min_stop = start_column + timedelta(days = leadtime_column)
                if stop_column < min_stop:
                    return False
                else:
                    return True
            valildated = mapped.filter(lambda x: x[0] == False)
            validated = stream_df.join(dependency,on = list(config.keys()))
            invalid = stream_df.join(dependency,on = list(config.keys()),how = "left_outer")
        except ValueError as e:
            producer.produce_debug("restart the stream producer! {}".format(e))

    #hardcoded which validation functions are active
    validation_functions = {"check_exists":check_exists }

    global ruleindex
    countrules = 0
    producer.produce_debug('validation config {}'.format(validation_config))
    rulecount = len(validation_config)
    for rule in validation_config:
        countrules += 1
        ruleindex = (countrules,rulecount)
        validator = validation_functions.get(rule.method)
        validator = wrap_rule(validator)
        data_ds.foreachRDD(validator)

        message = ['processing rule {}'.format(rule),\
                   "wrapped validator is {}".format(validator)]
        producer.produce_debug("\n".join(message))

    ssc.start()
    ssc.awaitTermination()
