from cmath import cos
import re
import pandas as pd
import psycopg2


# with open("D:\\update.txt","r",encoding='utf-8') as f:
#         for line in f.readlines():
#             m = re.match(r'^(UPDATE)(\s+CUSTOMER)(.+?;)$',line)# from customer的sql
#             if m:# 匹配成功
#                 with open("D:\\tpcc_tables\\workload\\wl2.sql","a",encoding='utf-8') as fi:
#                     fi.write(line)


"""计算平均代价"""
count = int(input("请输入sql数量："))
cost = 0
with open("D:\\tpcc_tables\\log\\cost.txt","r",encoding='utf-8') as f:
    for line in f.readlines():
        m = re.match(r'(.+?)(cost=)(.+?)(\.\.)(.+?)(\srows=)(.+)',line)
        # ..之前是预估的启动代价，即找到符合该节点条件的第一个结果预估所需要的代价
        # ..之后是预估的总代价
        # 父节点的启动代价包含子节点的总代价
        cost += float(m.group(5))
cost /= count
print(cost)



"""执行原始sql并存储代价"""
# user = "postgres"
# database = "tpcc"
# port = "5432"
# password = "102041"
# connect = psycopg2.connect(database=database,
#     user=user,password=password,port=port)
# cur = connect.cursor()
# cur.execute("set enable_bitmapscan = on;")
# cur.execute("set enable_hashagg = on;")
# cur.execute("set enable_hashjoin = on;")
# cur.execute("set enable_indexscan = off;")
# cur.execute("set enable_indexonlyscan = off;")
# cur.execute("set enable_material = on;")
# cur.execute("set enable_mergejoin = on;")
# cur.execute("set enable_nestloop = on;")
# cur.execute("set enable_parallel_append = on;")
# cur.execute("set enable_seqscan = on;")
# cur.execute("set enable_sort = on;")
# cur.execute("set enable_tidscan = on;")
# cur.execute("set enable_partitionwise_join = on;")
# cur.execute("set enable_partitionwise_aggregate = on;")
# cur.execute("set enable_parallel_hash = on;")
# cur.execute("set enable_partition_pruning = on;")

# with open("D:\\tpcc_tables\\workload\\cus_dis_ware\\5000select.txt","r",encoding='utf-8') as f:
#     i = 0
#     for line in f.readlines():
#         try:
#             sql = "EXPLAIN " + line
#             query_plan = pd.read_sql(sql,con=connect)
#             line1 = query_plan.values[0][0] # 查询计划的第一行
#             with open("D:\\tpcc_tables\\log\\cost.txt","a",encoding='utf-8') as f:
#                 f.write(line1+'\n')
#             i += 1
#         except Exception as e:
#             print("第{}条sql出现问题".format(i))




   


            


