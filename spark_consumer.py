import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from config import BOOTSTRAP_SERVERS,ZOOKEEPER_SERVERS
import json, math, datetime
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,\
            value_serializer=lambda v: json.dumps(v).encode("utf-8"))

def handler(message):
    records = message.collect()
    for record in records:
        producer.send("sparkout",str(record))
        producer.flush()

class KSWorker(object):
    def __init__(self):
        self.sc = SparkContext(appName="PythonSparkStreamingKafka")
        self.sc.setLogLevel("WARN")
        self.ssc = StreamingContext(self.sc,10)

    def startStream(self):
        self.ssc.start()
        self.ssc.awaitTermination()

    def testStream(self):
        kafkaStream = KafkaUtils.createDirectStream(self.ssc, ['test'], {"metadata.broker.list":",".join(BOOTSTRAP_SERVERS)})
        data_ds = kafkaStream.map(lambda v: json.loads(v[1]))
        data_ds.foreachRDD(handler)
        self.startStream()

def main():
    worker = KSWorker()
    worker.testStream()

if __name__ == '__main__':
    main()
