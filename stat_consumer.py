#! usr/bin/env python3
import sys
from kafka import KafkaConsumer
from postgreslib.database_connection import DBConnection
import os
import json

def insert_db(dbc,**kwargs):
    base_stmt = "insert into {} ({}) values ({});"
    table = kwargs.pop("table")
    column_definitions = dbc.descriptions.get(table)
    cols = []
    vals = []
    for column,data in kwargs.items():
        column_type = column_definitions.get(column,False)
        cols.append(column)
        if "CHAR" in column_type.upper():
            val = "'{}'".format(data)
        elif "INT" in column_type.upper():
            val = "{}".format(str(int(data)))
        elif "TIME" in column_type.upper():
            val = "TIMESTAMP '{}'".format(data.isoformat())
        else:
            val  = data
        vals.append(val)
    columns = ", ".join(cols)
    values = ", " .join(vals)
    stmt = base_stmt.format(table,columns,values)
    dbc.execute_cursor(stmt)

def consume_stats(bootstrap_servers, datasource):
    dbc = DBConnection(datasource)
    dbc.get_base_table_descriptions()
    consumer = KafkaConsumer(topic,\
            group_id  = "test",\
            bootstrap_servers=bootstrap_servers,\
            auto_offset_reset ="smallest",\
            value_deserializer =lambda m: json.loads(m.decode('utf-8')))
    consumer.subscribe("stats")
    insert_kwargs = {}
    issert_kwargs.update({"table":"tracked_topics"})
    for message in consumer:
        stat_topic = message.value["topic"]
        stat_count = message.value["count"]
        insert_kwargs.update({"topic_name":str(stat_topic),\
                              "quantity":int(stat_count)})
        insert_db(dbc,**insert_kwargs)

if __name__ == '__main__':
    global DEFINITIONS
    fn = os.environ.get("HOME") +"/clusterinfo"
    topic = sys.argv[1].strip()
    with open(fn ,"r") as f:
        bootstrap_servers = ["{}:9092".format(x) for x in f.readlines()]
    if "joe" in os.environ.get("HOME"):
        print("setting test bootstrap server")
        bootstrap_servers = ["{}:9092".format("localhost")]
    consume_test_topic(bootstrap_servers,topic)
