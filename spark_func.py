import os
import sys
from helpers.get_data import get_url
from pyspark import SparkContext, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row,SparkSession
from pyspark.sql.functions import to_json,struct
from pyspark.sql.utils import IllegalArgumentException
from helpers.get_data import get_url
from helpers.kafka import KafkaWriter, get_topic, getjsonproducer,getstrproducer
from config import config
import json, math, datetime

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def sendKafka(itr):
    global producer
    producer = KafkaWriter(config.TESTING_SERVER)
    for record in itr:
        producer.produce_debug(json.dumps(record))

def send_validation(itr):
    topic = "vaidated"
    producer = getjsonproducer(config.TESTING_SERVER)
    for record in itr:
        producer.send(topic,json.dumps(record))

def send_rejection(itr):
    topic = "rejected"
    producer = getjsonproducer(config.TESTING_SERVER)
    for record in itr:
        producer.send(topic,json.dumps(record))

def stream_validation(bootstrap_servers,datasource,table,validation_config):
    sc = SparkContext(appName="PythonSparkStreamingKafka")
    sqlc = SQLContext(sc)
    ssc = StreamingContext(sc,5)
    jdbc_url , jdbc_properties = get_url(datasource)

    #create kafka producer in master
    global producer
    producer = KafkaWriter(bootstrap_servers,datasource,table)
    producer.produce_debug("validation conig {}".format(validation_config))

    #get topic for ingestion
    topic = get_topic(datasource,table)

    def get_table_df(table):
        df = sqlc.read.jdbc(
                url = jdbc_url,
                table = table,
                properties = jdbc_properties)
        return df

    #create stream for injestion

    brokerlist = ",".join(bootstrap_servers)
    kafka_properties = {}
    kafka_properties["metadata.broker.list"] = brokerlist

    producer.produce_debug("intiializing stream on topic {}".format(topic))
    kafkaStream = KafkaUtils.createDirectStream(ssc,\
                                                [topic],\
                                                kafka_properties)

    #decode json to ds
    data_ds = kafkaStream.map(lambda v: json.loads(v[1]))
    data_ds.count().map(lambda x:'Records in this batch: %s' % x).union(data_ds).pprint()


    rule = validation_config[0]
    def check_exists(time,rdd):
        rdd  = rdd.map(lambda v:json.loads(v)["record"])
        producer.produce_debug("in process.. {}".format(time))
        spark = getSparkSessionInstance(rdd.context.getConf())
        dependencies = {}
        for d in rule.dependencies:
            producer.produce_debug("getting dependency for {}".format(d))
            name = rule.name
            dependencies[name] = get_table_df(d)
        config = rule.config
        try:
            stream_df = spark.createDataFrame(rdd)
            validated = stream_df.join(dependencies[rule.name],on = list(config.keys()))
            invalid = stream_df.join(dependencies[rule.name],on = list(config.keys()),how = "left_outer")
            validated.toJSON().foreachPartition(sendKafka)
#            validated.toJSON().foreachPartition(send_validation)
#            if validated.count() > 0:
#                validated.toJSON().foreachPartition(send_validation)
#            if invalid.cont() > 0 :
#                invalid.toJSON().foreachPartition(send_rejection)
        except ValueError as e:
            producer.produce_debug("restart the stream producer! ")

    validation_functions = {"check_exists":check_exists}
#    module = sys.modules[__name__]
#    validator = getattr(module,rule.method)
    validator = validation_functions.get(rule.method)

    data_ds.foreachRDD(validator)
    ssc.start()
    ssc.awaitTermination()


# Write key-value data from a DataFrame to Kafka using a topic specified in the data
