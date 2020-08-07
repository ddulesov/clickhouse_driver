from clickhouse_driver import Client
import datetime
import pytz
import os
import datetime
import time
import sys

from urllib.parse import urlparse

url = os.environ.get('DATABASE_URL',"tcp://default@localhost/default?compression=lz4")

url = urlparse(url)._replace(scheme='clickhouse').geturl()

client = Client.from_url(url)


ddl = ("CREATE TABLE IF NOT EXISTS perf_py ("
      "id  UInt32,"
      "name  String,"
      "dt   DateTime "
      ") Engine=MergeTree PARTITION BY name ORDER BY dt")
          
client.execute("DROP TABLE IF EXISTS perf_py")
client.execute( ddl )

NAMES = ["one","two","three","four","five"];

BSIZE = 10000
CIRCLE = 1000

def next_block(i):
    now = datetime.datetime.now()
    sec = datetime.timedelta(seconds=1)
        
    block = []
    for i in range(BSIZE):
        block.append([i,  NAMES[ i % len(NAMES) ],  now + i * sec ] )

    return block


def select_perf():
    start_time = time.time_ns()
    settings = {'max_block_size': 100000}
    rows = client.execute_iter("SELECT * FROM perf",settings=settings)

    for row in rows:
        pass

    print("elapsed %s msec" % ( (time.time_ns() - start_time)/1000000 ))


def insert_perf():

    start_time = time.time_ns()

    for i in range(0,CIRCLE):
        client.execute('INSERT INTO perf_py (id, name, dt) VALUES', next_block(i) )

    print("elapsed %s msec" % ( (time.time_ns() - start_time)/1000000 ))
               
if __name__ == "__main__":
    if len( sys.argv )<2:
        print("specify perf test. 'insert' or 'select'. bench.py <name>.")
        sys.exit(1)
    perf_name = sys.argv[1]

    f = globals()[ perf_name + "_perf" ]
    f()
