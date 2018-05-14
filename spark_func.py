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
    producer.produce_debug("validation config {}".format(validation_config))

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

    def wrap_validator(rulefuncs):
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
                for rule in rulefuncs:
                    # iterate over the rule functions and update the stream and invalidated dataframes accordingly
                    new_invalid = rulefunc(stream_df,ruleconfig,ruledependencies)
                    try:
                        invalidated = invalidated.union(new_invalid)
                    except UnboundLocalError as e:
                        invalidated = new_invalid
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
            except ValueError as e:
                #processing gives an emptyRDD error if the stream producer isn't running
                producer.produce_debug("restart the stream producer! {}".format(e))
        return wrapped_rule

    #hardcoded which validation functions are active
    validation_functions = {"check_exists":check_exists ,\
                            "check_lead_time":check_lead_time}
    global ruleindex
    countrules = 0
    producer.produce_debug('validation config {}'.format(validation_config))
    rulecount = len(validation_config)
    validators = [validation_functions.get(r.method) for r in validation_config]
    validator = wrap_validators(validators)
    data_ds.foreachRDD(validator)

    ssc.start()
    ssc.awaitTermination()

