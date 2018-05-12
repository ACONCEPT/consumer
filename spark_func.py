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
        df.describe()
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

    # to print a count of the batch to spark console...
    #data_ds.count().map(lambda x:'Records in this batch: %s' % x).union(data_ds).pprint()

    def wrap_rule(rulefunc):
        def wrapped_rule(time,rdd):
            # extract configuration from rule and get dependencies for current rule
            ruleconfig = rule.config
            dependencies = dependencies.get(rule.name)

            # filter out ay already invalidated records from the stream dataframe
            stream_df = stream_df.join(invalidated.withcol("ident",lit(1)),["id"],"left_outer").where(col("ident").isNull())

            # attempt processing of actual rule on stream df
            try:
                #get validated and new invalidated Df from the rule application
                #valid df exists in main func namespace, this function overwrites it with the new version after this rule
                validated, new_invalid = rulefunc(rdd)

                #unions new invalid records with the main invalidated dataframe
                invalidated = invalidated.union(new_invalid)

            except ValueError as e:

                #processing gives an emptyRDD error if the stream producer isn't running
                producer.produce_debug("restart the stream producer! {}".format(e))

        return wrapped_rule(rulefunc)

    def check_exists(rdd):
        joinon = ruleconfig.get("join_on")

        #rule assumes only one dependency is loaded
        dependency = dependencies[0]

        #validated items are indicated by a successful inner join on the configured columns
        validated = stream_df.join(dependency,joinon,"inner")

        #invalid items are indicated by a left outer join on the dependenc table on the configured column
        new_invalid = stream_df.join(dependency,joinon,"left_outer").where(col(identcol).isNull())

        return validated , new_invalid


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

        except ValueError as e:
            producer.produce_debug("restart the stream producer! {}".format(e))

    #hardcoded which validation functions are active
    validation_functions = {"check_exists":check_exists }
    spark = getSparkSessionInstance(rdd.context.getConf())
    stream_df = spark.createDataFrame(rdd.map(lambda v:json.loads(v)["record"]))

    global validated
    global invalidated
    validated = stream_df.filter('1 = 2')
    invalidated = stream_df.filter('1 = 2')

    for rule in validation_config:
        validator = validation_functions.get(rule.method)
        validator = wrap_rule(validator)
        data_ds.foreachRDD(validator)

    # collect the valid and invalid data and send them to their respective topics
    send_valid = validated.toJSON().collect()
    for data in send_valid:
        producer.send_next(record = data, validity = True, rejectionrule = rule.rejectionrule)
    del send_valid

    send_invalid = invalid.toJSON().collect()
    for data in send_invalid:
        producer.send_next(record = data, validity = False, rejectionrule = rule.rejectionrule)
    del send_invalid

    ssc.start()
    ssc.awaitTermination()
    producer.stat_remnants()


#        if rule.method == "check_leadtime":
#            rule.config = {}
#            rule.config["start_column"] = "order_creation_date"
#            rule.config["stop_column"] = "order_expected_delivery"
#            rule.config["leadtime_column"] = "delivery_lead_time"
#            rule.config["join_columns"] = ["part_id","customer_id"]
