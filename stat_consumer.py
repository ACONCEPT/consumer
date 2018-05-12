#! usr/bin/env python3

from kafka import KafkaConsumer
from postgreslib.database_connection import DBConnection
import os
import json
def get_records(query,dbc):
           response , header = dbc.execute_query(query)
           records = [{h.name:v for h,v in zip(header,r)} for r in response]
           return records

def transform_date(dtcol):
    return dtcol.strftime("%d/%m/%y %H:%M:%S")

def run_statistics(datasource):
    print("running stats... ")
    dbc = DBConnection(datasource)
    dbc.get_base_table_descriptions()
    query = "select topic_name, min(timestamp) start_time,max(timestamp) end_time, max(quantity) amount from tracked_topics group by topic_name;"
    records = get_records(query = query,dbc = dbc)
    runid = get_records("select max(run_id) + 1 newid  from run_stats;",dbc)[0]["newid"]
    insert_kwargs = {"table":"run_stats"}
    for record in records:
        record["elapsed_time"] = (record["end_time"] - record["start_time"]).total_seconds()
        insert_kwargs.update({"topic_name":record["topic_name"],\
                              "run_id":runid,\
                              "start_time":record["start_time"],\
                              "end_time":record["end_time"],\
                              "elapsed_time":record["elapsed_time"],\
                              "amount":record["amount"]})
        insert_db(dbc,**insert_kwargs)
    dbc.execute_cursor("delete from tracked_topics where 1 = 1;",commit = True)


def insert_db(dbc,**kwargs):
    base_stmt = "insert into {} ({}) values ({});"
    table = kwargs.pop("table")
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
            val = "TIMESTAMP '{}'".format(data)

        else:
            val  = data
        vals.append(val)
    columns = ", ".join(cols)
    values = ", " .join(vals)
    stmt = base_stmt.format(table,columns,values)
    dbc.execute_cursor(stmt,commit = True)

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
        stat_topic = jsonmsg["topic"]
        stat_count = jsonmsg["count"]
        stat_time = jsonmsg["timestamp"]
        insert_kwargs.update({"topic_name":str(stat_topic),\
                          "quantity":int(stat_count),\
                          "timestamp":stat_time})
        insert_db(dbc,**insert_kwargs)
        if i % 100 == 0:
            print(" inserted i {} tracked records".format(i))
        if i == 1000000:
            break
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
