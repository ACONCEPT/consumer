import os
import sys
from pyspark import SparkContext, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import DataFrameReader
from pyspark.sql.utils import IllegalArgumentException
from helpers.get_data import get_url
from helpers.kafka import KafkaWriter, get_topic, getproducer
import json, math, datetime

def methodlist(obj):
    return [x for x in dir(obj) if x[0] != "_"]

def row_handler(rdd_row):
    print("row handler got rdd row {}".format(rdd_row))

def sendkafka(messages):
    producer = getproducer("localhost:9092")
    print("in sendkafka")
    for message in messages:
        print("in sendkafka")
        producer.send_messages('debug', message)

def rdd_handler(rdd):
    rdd.foreach(row_handler)

class StreamValidation(object):
    """
    Stream Validation object takes a set of Validation Rules related to a single input stream
    as input.

    TODO: The first task is to validate the rule's configuration based on the meta of
    the base rule evaluation method

    after the rules are validated, the data dependencies for each rule are validated against
    the data source

    after all dependencies are loaded, a kafka stream is opened up for the table, and the rules
    are evaluated for each row in series while the stream is active
    all messages in the queue are processed


    """
    def __init__(self,\
                 validation_rules,\
                 bootstrap_servers,\
                 table,
                 datasource,
                 debug = False,\
                 streaming_context_size = 10 ):
        self.debug = debug
        self.validation_rules = validation_rules
        self.sc = SparkContext(appName="PythonSparkStreamingKafka")
        self.sc.setLogLevel("WARN")
        self.bootstrap_servers = bootstrap_servers
        self.ssc = StreamingContext(self.sc,streaming_context_size)
        self.sqlc = SQLContext(self.sc)
        self.jdbc_url , self.jdbc_properties = get_url(datasource)
        self.producer = KafkaWriter(bootstrap_servers,datasource,table)
        debug_msg = "create StreamWorker with jdbc connection {}".format(self.jdbc_url)
        self.datasource  = datasource
        self.table = table
        self.produce_debug(debug_msg)

    def test_SQL(self):
        self.get_table_df("part_customers")
        self.produce_debug(dir(self.part_customers))
        self.produce_debug(str(type(self.part_customers)))
        self.part_customers.show()
        self.start_stream()

    def test_stream(self):
        kafkaStream = KafkaUtils.createDirectStream(self.ssc, ['test'], {"metadata.broker.list":",".join(self.bootstrap_servers)})
        data_ds = kafkaStream.map(lambda v: json.loads(v[1]))
        data_ds.foreachRDD(self.producer.test_handler)
        self.start_stream()

    def produce_debug(self,msg):
        if self.debug:
            self.producer.produce_debug(msg)

    def get_table_df(self,table):
        df = self.sqlc.read.jdbc(
                url = self.jdbc_url,
                table = table,
                properties = self.jdbc_properties)
        setattr(self,table,df)

    def start_stream(self):
        try:
           self.ssc.start()
        except IllegalArgumentException as e:
            self.producer.produce_debug(" stream ran without actionable output ")
        self.ssc.awaitTermination()

    def create_validation_stream(self):
        topic = get_topic(self.datasource,self.table)
        brokerlist = ",".join(self.bootstrap_servers)
        self.produce_debug("creating directstream on topic {}\nbrokerlist {}".format(topic,brokerlist))
        kafka_properties = {}
        kafka_properties["metadata.broker.list"] = brokerlist
        kafkaStream = KafkaUtils.createDirectStream(self.ssc,\
                                                    [topic],\
                                                    kafka_properties)

        data_ds = kafkaStream.map(lambda v: json.loads(v[1]))
        data_ds.foreachRDD(rdd_handler)

#        sent_data = data_ds.mapPartitions(sendkafka)
#        messageDF = self.sc.createDataFrame(sent_data, "string")
#        messageDF.write\
#                .format("kafka")\
#                .option("topic", topic_name)\
#                .option("kafka.bootstrap.servers", bootstrap_servers)\
#                .save()


        self.start_stream()
#        self.produce_debug("data_ds is {}".format(type(data_ds)))
#        for rule in self.stream_rules:
#            self.produce_debug("processing rule {} on {}".format(rule,self.table))
#            self.validation_method = getattr(self,rule.method)
#            self.validation_rule = rule
#            data_ds.foreachRDD(self.validation_handler)
#        self.start_stream()

    def evaluate_rules(self,force_table = False):
        self.stream_rules = []
        if force_table:
            self.table = force_table
            first = False
        else:
            first = True
        self.dependencies = []
        for rule in self.validation_rules:
            self.produce_debug("processing rule {} for table {}. {} {}".\
                                        format(rule.name,rule.table,rule.dependencies,type(rule.dependencies)))
            if first:
                self.table = rule.table
            else:
                if rule.table != self.table:
                    self.produce_debug("discarding rule {}, table \
                                                mismatch current table = {}".\
                                                format(rule.name,self.table))
                    return
            self.stream_rules.append(rule)
            self.add_dependency(rule.dependencies)

    def add_dependency(self,new_dep):
        if not hasattr(self,"dependencies"):
            self.dependencies = []
        if isinstance(new_dep,list):
            new_deps = [d for d in new_dep if d not in self.dependencies]
            self.dependencies += new_deps
        elif new_dep not in self.dependencies:
            self.dependencies.append(new_dep)

    def load_dependencies(self):
        self.producer.produce_debug("loading dependencies {}".format(self.dependencies))
        if self.dependencies:
            for dep in self.dependencies:
                self.get_table_df(dep)
                self.produce_debug("added dependency {} rows {}".format(dep,getattr(self,dep).count()))
        else:
            self.produce_debug("no dependencies to load")

    def test_evaluate_rules(self):
        self.evaluate_rules()
        self.load_dependencies()

    def check_exists(self,record,config = False,dependency = False):
        self.produce_debug("running check_exists {}, {}".format(config, dependency))
        df = getattr(self,dependency)
        query =[]
        for record_col,dep_col in config.items():
            try:
                query.append("{} = {}".format(dep_col,record[record_col]))
            except Exception as e:
                self.produce_debug("error {} on record of type {}".format(e, type(record)))
                self.produce_debug("record keys {}".format(",".join(record.keys())))
                raise e
        query = " AND ".join(query)
        result = df.filter(query)
        if result.count() >= 1:
            valid = True
        else:
            valid = False
        return valid


if __name__ == '__main__':
    pass
