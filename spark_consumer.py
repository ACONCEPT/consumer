import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'
import numpy as np
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
from insight_project.config import BOOTSTRAP_SERVERS,ZOOKEEPER_SERVERS
import json, math, datetime

def main():
    sc = SparkContext(appName="PythonSparkStreamingKafka")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc)
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['test'], {"metadata.broker.list":BOOTSTRAP_SERVERS})
    data_ds = kafkaStream.map(lambda v: json.loads(v[1]))
    data_ds.show()
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':

    main()

