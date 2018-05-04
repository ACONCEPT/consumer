import os
import sys
sys.path.append(os.environ["PROJECT_HOME"])
from pyspark import SparkContext, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import DataFrameReader
from pyspark.sql.utils import IllegalArgumentException
from helpers.get_data import get_url
from helpers.validation import ValidationRule
from helpers.kafka import KafkaWriter, get_topic
from config.config import BOOTSTRAP_SERVERS,ZOOKEEPER_SERVERS,TESTING_SERVER
import json, math, datetime

def methodlist(obj):
    return [x for x in dir(obj) if x[0] != "_"]

class StreamIngestion(object):
    """
    Stream Ingestion object takes a set of Validation Rules related to a single input stream
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
        self.ssc = StreamingContext(self.sc,streaming_context_size)
        self.sqlc = SQLContext(self.sc)
        self.jdbc_url , self.jdbc_properties = get_url()
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
        kafkaStream = KafkaUtils.createDirectStream(self.ssc, ['test'], {"metadata.broker.list":",".join(BOOTSTRAP_SERVERS)})
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

    def validation_handler(self,datetime,message):
        self.produce_debug("made it to validation handler with values {} and {}".format(datetime,message))
        records = message.collect()
        self.produce_debug(records)
        for record in records:
            if isinstance(record,str):
                record = json.loads(record)
            if "record" in list(record.keys()):
                record = record["record"]
            validity = self.validation_method(record,self.validation_rule.config,self.validation_rule.dependencies[0])
            self.producer.send_next(record, validity, self.validation_rule.rejectionrule)

    def create_validation_stream(self):
        topic = get_topic(self.datasource,self.table)
        self.produce_debug("creating directstream on topic {}".format(topic))
        kafkaStream = KafkaUtils.createDirectStream(self.ssc,\
                                                    [topic],\
                                                    {"metadata.broker.list":",".\
                                                     join(BOOTSTRAP_SERVERS)})
        data_ds = kafkaStream.map(lambda v: json.loads(v[1]))
        for rule in self.stream_rules:
            self.produce_debug("processing rule {} on {}".format(rule,self.table))
            self.validation_method = getattr(self,rule.method)
            self.validation_rule = rule
            data_ds.foreachRDD(self.validation_handler)
        self.start_stream()

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
            self.dependencies += new_dep
        else:
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


def main(bootstrap_servers):
    validation_rules = [ValidationRule(name = "check_customer_ids",\
                        table = "sales_orders",\
                        column = ["customer_id",\
                                "part_id"],\
                        dependencies = ["part_customers"],\
                        method = "check_exists",\
                        config = {"customer_id":"customer_id",\
                                "part_id":"part_id"},
                        rejectionrule = "notexists")]
    worker = StreamIngestion(validation_rules,bootstrap_servers,datasource = "test_database",table = "sales_orders",debug = True)
    worker.produce_debug("running testsql")
    worker.evaluate_rules()
    worker.load_dependencies()
    worker.create_validation_stream()

if __name__ == '__main__':
    global BOOTSTRAP_SERVERS
    global producer
    if "joe" in os.environ["HOME"]:
        print("setting boosttrap servers to localhost in spark consumer")
        bootstrap_servers = TESTING_SERVER
    else:
        bootstrap_servers = BOOTSTRAP_SERVERS
    main(bootstrap_servers)

