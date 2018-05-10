#! usr/bin/env python3
import sys
from kafka import KafkaConsumer
from postgreslib.database_connection import DBConnection
import os
import json

def insert_db(dbc,**kwargs):
    base_stmt = "insert into {} ({}) values ({});"
    table = kwargs.pop("table")
    commit = kwargs.pop("commit",False)
    column_definitions = dbc.descriptions.get(table)
    cols = []
    commit_interval = 100
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
    consumer = KafkaConsumer("stats",\
            group_id  = "test",\
            bootstrap_servers=bootstrap_servers,\
            auto_offset_reset ="smallest",\
            value_deserializer =lambda m: json.loads(m.decode('utf-8')))
    insert_kwargs = {}
    insert_kwargs.update({"table":"tracked_topics"})
    for i,  message in enumerate(consumer):
        jsonmsg = json.loads(message.value)
        print(jsonmsg)
        try:
            stat_topic = jsonmsg["topic"]
            stat_count = jsonmsg["count"]
            stat_time = jsonmsg["timestamp"]
            insert_kwargs.update({"topic_name":str(stat_topic),\
                              "quantity":int(stat_count),\
                              "timestamp":stat_time})
            insert_db(dbc,**insert_kwargs)
            if i % 25  == 0:
                dbc.commit_connection()
        except:
            pass
    dbc.commit_connection()


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
