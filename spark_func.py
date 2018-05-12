from helpers.get_data import get_url


from pyspark import SparkContext, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row,SparkSession
from pyspark.sql.functions import to_json,struct, col
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

    brokerlist = ",".join(bootstrap_servers)
    kafka_properties = {}
    kafka_properties["metadata.broker.list"] = brokerlist
    producer.produce_debug("intiializing stream on topic {}".format(topic))
    kafkaStream = KafkaUtils.createDirectStream(ssc,\
                                                [topic],\
                                                kafka_properties)
    dependencies = {}
    for rule in validation_config:
        dependencies[rule.name] = []
        for d in rule.dependencies:
            producer.produce_debug("getting dependency for {}".format(d))
            name = rule.name
            dependencies[rule.name].append(get_table_df(d))

    #decode json to ds
    data_ds = kafkaStream.map(lambda v: json.loads(v[1]))
    #data_ds.count().map(lambda x:'Records in this batch: %s' % x).union(data_ds).pprint()

    def check_exists(time,rdd):
#        producer.produce_debug("in process.. {}".format(time))

        # get Context for this rdd
        spark = getSparkSessionInstance(rdd.context.getConf())

        ruleconfig = rule.config
        joinon = ruleconfig.get("join_cols")
        identcol = ruleconfig.get("identcol")

        # only expecting one dependency for this rule, so take first item from list by default
        dependency = dependencies.get(rule.name)[0]
        dependency.show()
        try:
            #turn current streamdata into dataframe
            stream_df = spark.createDataFrame(rdd.map(lambda v:json.loads(v)["record"]))
#            producer.produce_debug("joining on {}".format(joinon))

            #validated items are indicated by a successful inner join on the configured columns
            validated = stream_df.join(dependency,joinon,"inner")
            #invalid items are indicated by a left outer join on the dependenc table on the configured column
            invalided = stream_df.join(dependency,joinon,"left_outer").where(col(identcol).isNull())

            # collect the valid and invalid data and send them to their respective topics
            valid_json = validated.toJSON().collect()
#            producer.produce_debug("valid orders {} ".format(len(valid_json)))
            for data in valid_json:
                producer.send_next(record = data, validity = True, rejectionrule = rule.rejectionrule)
                #producer.produce_valid(data)
            del valid_json
            del validated

            invalid_json = invalided.toJSON().collect()
            producer.produce_debug("invalid orders {} ".format(len(invalid_json)))
            for data in invalid_json:
                producer.send_next(record = data, validity = False, rejectionrule = rule.rejectionrule)
                #producer.produce_reject(data)
            del invalid_json
            del invalided

        except ValueError as e:
            producer.produce_debug("restart the stream producer! {}".format(e))

    def check_lead_time(time,rdd):
        producer.produce_debug("in process.. {}".format(time))
        # get Context for this rdd https://stackoverflow.com/questions/36051299/how-to-subtract-a-column-of-days-from-a-column-of-dates-in-pyspark
        spark = getSparkSessionInstance(rdd.context.getConf())
        # get coniguration out of rule
        config = rule.config
        # only expecting one dependency for this rule, so take first item from list by default
        dependency = dependencies.get(rule.name)[0]
        try:
            #turn current streamdata into dataframe
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
#            mapped = joined.map(lambda x:mapfunc(x[],x[],x[])).cache()
            #validated items are indicated by a successful inner join on the configured columns
            valildated = mapped.filter(lambda x: x[0] == False)
            validated = stream_df.join(dependency,on = list(config.keys()))
            #invalid items are indicated by a left outer join on the dependenc table on the configured column
            invalid = stream_df.join(dependency,on = list(config.keys()),how = "left_outer")
            # collect the valid and invalid data and send them to their respective topics
            valid_json = validated.toJSON().collect()
            for data in valid_json:
                producer.send_next(record = data, validity = True, rejectionrule = rule.rejectionrule)
                #producer.produce_valid(data)
            del valid_json
            invalid_json = invalid.toJSON().collect()
            for data in invalid_json:
                producer.send_next(record = data, validity = False, rejectionrule = rule.rejectionrule)
                #producer.produce_reject(data)
        except ValueError as e:
            producer.produce_debug("restart the stream producer! {}".format(e))

    #hardcoded which validation functions are active
    validation_functions = {"check_exists":check_exists }

    for rule in validation_config:
        validator = validation_functions.get(rule.method)
        data_ds.foreachRDD(validator)


    ssc.start()
    ssc.awaitTermination()
    producer.stat_remnants()


#        if rule.method == "check_leadtime":
#            rule.config = {}
#            rule.config["start_column"] = "order_creation_date"
#            rule.config["stop_column"] = "order_expected_delivery"
#            rule.config["leadtime_column"] = "delivery_lead_time"
#            rule.config["join_columns"] = ["part_id","customer_id"]
