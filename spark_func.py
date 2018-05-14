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

#rules are defined in the project-root/config/methods.py file
from config.methods import *

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
    kafkaStream = KafkaUtils.createDirectStream(ssc,\
                                                [topic],\
                                                kafka_properties)
#    global dependencies
    dependencies = {}
    for rule in validation_config:
        dependencies[rule.name] = []
        for d in rule.dependencies:
            name = rule.name
            dependencies[rule.name].append(get_table_df(d))

    #decode json to ds
    data_ds = kafkaStream.map(lambda v: json.loads(v[1]))

    # to print a count of the batch to spark console...
    data_ds.count().map(lambda x:'Records in this batch: %s' % x).union(data_ds).pprint()
    def wrap_validator(rulefuncs):
        def wrapped_rules(time,rdd):
            spark = getSparkSessionInstance(rdd.context.getConf())
            try:
                stream_df = sqlc.createDataFrame(rdd.map(lambda v:json.loads(v)))
            except ValueError as e:
                producer.produce_debug("producer is empty, closing spark... ")
                exit()

            # extract configuration from rule and get dependencies for current rule
            ruleconfig = rule.config
            try:
                for arule in rulefuncs:
                    # iterate over the rule functions and update the stream and invalidated dataframes accordingly
                    rulename = arule[0]
                    ruleconfig = arule[1]
                    func = arule[2]
                    ruledependencies = dependencies.get(rulename)
                    new_invalid = func(stream_df,ruleconfig,ruledependencies)
                    msg = ["\n\nexecuting rule name {}".format(rulename)]
                    msg += ["func {}".format(func.__name__)]
                    msg += ["config {}".format(ruleconfig)]
                    producer.produce_debug("\n".join(msg))
                    try:
                        invalidated = invalidated.union(new_invalid)
                    except UnboundLocalError as e:
                        invalidated = new_invalid

                    stream_df = stream_df.subtract(invalidated)

                send_valid = stream_df.toJSON().collect()
                for data in send_valid:
                    producer.send_next(record = data, validity = True, rejectionrule = rule.rejectionrule)
                del send_valid
                send_invalid = invalidated.toJSON().collect()
                for data in send_invalid:
                    producer.send_next(record = data, validity = False, rejectionrule = rule.rejectionrule)
                del send_invalid
                producer.stat_remnants()
            except ValueError as e:
                #processing gives an emptyRDD error if the stream producer isn't running
                producer.produce_debug("killing spark proc... got empty RDD {}".format(e))
                exit()
        return wrapped_rules

    #hardcoded which validation functions are active

    validation_functions = {"check_exists":check_exists ,\
                            "check_lead_time":check_lead_time}

    list_of_rules = [(r.name,r.config,validation_functions.get(r.method)) for r in validation_config]
    validator = wrap_validator(list_of_rules)
    data_ds.foreachRDD(validator)

    msg = ["\n\n\nabout to start spark StreamingContext.."]
    msg += ["topic {}".format(topic)]
    msg += ["list of rules {}".format([x[0] for x in list_of_rules])]
    msg += ["list of dependencies {}".format(", ".join(list(dependencies.keys())))]
    producer.produce_debug("\n".join(msg))


    ssc.start()
    ssc.awaitTermination()
