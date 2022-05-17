from itertools import combinations
import itertools
import re
from typing import List, Set
from matplotlib import collections
import psycopg2
import pandas as pd
from typing import List
import time
from tqdm import tqdm, trange


def getInput() -> List[str]:
    input = []
    with open('./tpcc/query.txt', "r", encoding='utf-8') as f:
        for line in f.readlines():
            input.append('EXPLAIN ANALYZE '+ line)
    return input


def execute():
    user = "Sevent"
    database = "test"
    port = "5432"
    password = ""
    connect = psycopg2.connect(database=database,
                               user=user, password=password, port=port)
    cur = connect.cursor()
    cur.execute("set enable_bitmapscan = off;")
    cur.execute("set enable_hashagg = on;")
    cur.execute("set enable_hashjoin = off;")
    cur.execute("set enable_indexscan = off;")
    cur.execute("set enable_indexonlyscan = off;")
    cur.execute("set enable_material = on;")
    cur.execute("set enable_mergejoin = on;")
    cur.execute("set enable_nestloop = on;")
    cur.execute("set enable_parallel_append = on;")
    cur.execute("set enable_seqscan = on;")
    cur.execute("set enable_sort = on;")
    cur.execute("set enable_tidscan = on;")
    cur.execute("set enable_partitionwise_join = on;")
    cur.execute("set enable_partitionwise_aggregate = on;")
    cur.execute("set enable_parallel_hash = on;")
    cur.execute("set enable_partition_pruning = on;")
    sqls = getInput()
    pbar = tqdm(sqls)

    for sql in pbar:
        query_plan = pd.read_sql(sql, con=connect)
        line1 = query_plan.values[0][0]  # 查询计划的第一行
        with open("./tpcc/cost.txt", "a", encoding='utf-8') as f:
            f.write(line1 + '\n')

execute()