from itertools import combinations
import itertools
import re
from typing import List, Set
from matplotlib import collections
import psycopg2
import pandas as pd


class Table():
    def __init__(self, id, name="", attr_list=[], pk=[], fk=[]) -> None:
        self.id = id
        self.name = name
        self.attr_list = attr_list
        self.pk = pk
        self.fk = fk

    def __str__(self) -> str:
        return "id:{}, name:{}, attr_list:{}, pk:{}, fk:{}".format(self.id, self.name, self.attr_list, self.pk, self.fk)


class Join():
    def __init__(self, left, right, condition: str = None) -> None:
        self.left = left
        self.right = right
        self.condition = condition  # 连接条件

    # 返回后序遍历连接树得到的关系表列表
    def getJoinOrder(self) -> List[Table]:
        def getJoinOrder(root, joinOrder):
            if isinstance(root, Table):
                joinOrder.append(root)
                return
            getJoinOrder(root.left, joinOrder)
            getJoinOrder(root.right, joinOrder)

        joinOrder = []
        getJoinOrder(self, joinOrder)
        return joinOrder

    def getAttrs(self) -> Set[str]:
        attrs = set()
        joinOrder = self.getJoinOrder()
        for t in joinOrder:
            attrs = attrs.union(set(t.attr_list))
        return attrs


# 预处理sql
def parseStmt(sql: str, columns: List[str]):
    re_sql = re.compile(r'^(SELECT\s+)(.+?)(\s+)(FROM\s+)(.+?)(\s+)(WHERE\s+)(.+?)(ORDER BY\s+.+?;|;)$')
    groups = re_sql.match(sql).groups()
    # print(groups)
    select_ = groups[1]
    from_ = groups[4]
    where_ = groups[7]
    order_by = groups[8]
    if order_by != ';':
        order_attr = re.compile(r'(ORDER BY\s+)(.+?)(;)').match(order_by).groups()[1]
        for i, c in enumerate(columns):
            if c == order_attr:
                order_attr = i
                break
    else:
        order_attr = None
    query_attrs_strs = re.split(r'[\s\,]+', select_)
    query_attrs = []
    for q in query_attrs_strs:
        for i, c in enumerate(columns):
            if c.upper() == q.upper():
                query_attrs.append(i)
                break
    filter_strs = re.split(r'\s+AND\s+', where_)
    re_filter = re.compile(r'^(.+?)(\s*[>|>=|<|<=|=|!=]\s*)(.+)$')
    filters = []
    filter_attrs = []
    for f in filter_strs:
        fil_groups = re_filter.match(f).groups()
        for i, c in enumerate(columns):
            if c.upper() == fil_groups[0].upper():
                filter_attrs.append(i)
                filters.append([i, fil_groups[1].strip(), fil_groups[2]])
                break
    return query_attrs, filters, filter_attrs, order_attr


# sql里的属性涉及到的表
def getLinkedTables() -> List[Table]:
    all_attrs = set(query_attrs + filter_attrs + [order_attr, ])
    linked_attrs = set()
    linked_tables = []
    for table in table_list:
        if linked_attrs == all_attrs:
            break
        n = set(table.attr_list)
        inter_list = all_attrs.intersection(n)
        if not inter_list <= linked_attrs:
            linked_tables.append(table)
            linked_attrs = linked_attrs.union(inter_list)
    return linked_tables


# 构建left->right的连接谓词(pk是right的主键,right是单个表)
def buildJoinCondition(left, right, pks: List[str]) -> str:
    if isinstance(left, Table):
        condition = "{}.{}={}.{}".format(right.name, columns[pks[0]], left.name, columns[pks[0]])
        for i in range(1, len(pks)):
            pk = columns[pks[i]]
            condition += " and {}.{}={}.{}".format(right.name, pk, left.name, pk)
    else:
        table_list = left.getJoinOrder()
        for t in table_list:
            if pks[0] in t.attr_list:
                condition = "{}.{}={}.{}".format(right.name, columns[pks[0]], t.name, columns[pks[0]])
                break
        for i in range(1, len(pks)):
            for t in table_list:
                if pks[i] in t.attr_list:
                    condition += " and {}.{}={}.{}".format(right.name, columns[pks[i]], t.name, columns[pks[i]])
                    break
    return condition


# 为分解后的所有表构造左深连接树
def buildJoinTree() -> Join:
    # 等待连接的单表
    remainder = table_list[:]
    joinNode = None
    # 初始化joinNode
    for i, table_i in enumerate(remainder):
        for j, table_j in enumerate(remainder):
            if i != j and set(table_i.pk).issubset(set(table_j.attr_list)):
                joinNode = Join(table_i, table_j, buildJoinCondition(table_j, table_i, table_i.pk))
                if i > j:
                    remainder.pop(i)
                    remainder.pop(j)
                else:
                    remainder.pop(j)
                    remainder.pop(i)
                break
        if joinNode:
            break
    # 不断添加单个表到joinTree上
    for i in range(len(remainder) - 1, -1, -1):
        t = remainder[i]
        if set(t.pk).issubset(joinNode.getAttrs()):
            joinNode = Join(joinNode, t, buildJoinCondition(joinNode, t, t.pk))
            remainder.pop(i)
    return joinNode


# # 获取所有需要的表(按照连接顺序排列)
# def getSubJoinOrder()->List[Table]:
#     joinOrder = root.getJoinOrder()
#     i = 0
#     for i in range(len(joinOrder)-1,-1,-1):
#         t = joinOrder[i]
#         if t in linked_tables:
#             break
#     return joinOrder[:i+1]

# 返回所有表构建的joinTree里查询需要的子树的根节点
def getSubJoinTree(r):
    if len(linked_tables) == 1:
        return linked_tables[0]
    if isinstance(r, Table):
        return r
    t = r.right
    if t not in linked_tables:
        return getSubJoinTree(r.left)
    return r


# Functions have been changed
def combine(temp_list, n):
    """根据n获得列表中的所有可能组合（n个元素为一组）为找后续找最小需要表集合服务"""
    temp_list2 = []
    for c in combinations(temp_list, n):
        temp_list2.append(c)
    return temp_list2


def isLinkable(tables: list[Table]) -> bool:
    """If the input tables can be joined"""

    def linkable(order: list[Table]) -> bool:
        """If tables can be joined by this order"""
        for index in range(1, len(order)):
            pre_attrs = set()
            for table in order[:index]:
                pre_attrs = pre_attrs.union(set(table.attr_list))
            if not set(order[index].pk).issubset(pre_attrs):
                return False
        return True

    ans = False
    iters = itertools.permutations(tables, len(tables))
    for order in iters:
        if linkable(list(order)):
            ans = True
            for index in range(len(tables)):
                tables[index] = list(order)[index]
            break
    return ans


def getSubJoinOrder() -> List[Table]:
    """Find the minimum numbers of the needed tables"""
    joinOrder = root.getJoinOrder()
    i = 0
    for i in range(len(joinOrder) - 1, -1, -1):
        t = joinOrder[i]
        if t in linked_tables:
            break
    # 找出所有连接表范围的子集，如果现有的表连接不了就加入子集看是否可以连接
    tableScope = list(set(joinOrder[:i + 1]) - set(linked_tables))
    subSetList = []
    for i in range(len(tableScope)):
        for c in combinations(tableScope, i + 1):
            subSetList.append(c)
    tables = linked_tables
    index = 0
    while not isLinkable(tables):
        tables = linked_tables
        tables = list(set(tables).union(subSetList[index]))
        index = index + 1
    return tables


def buildSubTree(tables: list[Table]):
    """Construct the tree for the minimum needed tables"""
    if len(tables) == 1:
        return tables[0]
    # 等待连接的单表
    remainder = tables[:]
    joinNode = None
    # 初始化joinNode
    for i, table_i in enumerate(remainder):
        for j, table_j in enumerate(remainder):
            if i != j and set(table_i.pk).issubset(set(table_j.attr_list)):
                joinNode = Join(table_i, table_j, buildJoinCondition(table_j, table_i, table_i.pk))
                remainder.pop(max(i, j))
                remainder.pop(min(i, j))
                break
        if joinNode:
            break
    # 不断添加单个表到joinTree上
    for i in range(len(remainder) - 1, -1, -1):
        t = remainder[i]
        if set(t.pk).issubset(joinNode.getAttrs()):
            joinNode = Join(joinNode, t, buildJoinCondition(joinNode, t, t.pk))
            remainder.pop(i)
    return joinNode


# 获取改写后sql里的连接谓词
def getSqlJoinCondition() -> str:
    condition = []

    def postOrder(root, condition: List[str]):
        if isinstance(root, Table):
            return
        postOrder(root.left, condition)
        postOrder(root.right, condition)
        condition.append(root.condition)

    postOrder(subJoinTree, condition)
    return " and ".join(condition)


# 获取改写后sql里的过滤谓词
def getSqlFilterCondition() -> str:
    if len(filter_attrs) == 0:
        return ''
    idx = {}  # attr(int)-Table键值对
    # 构建attr-table索引
    for attr in filter_attrs:
        table = idx.get(attr)
        if not table:
            for t in linked_tables:
                if attr in t.attr_list:
                    idx[attr] = t
                    break
    condition = "{}.{}{}{}".format(idx[filters[0][0]].name, columns[filters[0][0]], filters[0][1], filters[0][2])
    for i in range(1, len(filters)):
        f = filters[i]
        condition += " and {}.{}{}{}".format(idx[f[0]].name, columns[f[0]], f[1], f[2])
    return condition


# 获取改写后sql里的select子句
def getSqlSelect() -> str:
    for t in linked_tables:
        if query_attrs[0] in t.attr_list:
            select = "{}.{}".format(t.name, columns[query_attrs[0]])
            break
    for i in range(1, len(query_attrs)):
        q = query_attrs[i]
        for t in linked_tables:
            if q in t.attr_list:
                select += ",{}.{}".format(t.name, columns[q])
                break
    return select


# 获取改写后sql里的from子句
def getSqlFrom() -> str:
    if isinstance(subJoinTree, Table):
        return subJoinTree.name
    ls = [t.name for t in subJoinTree.getJoinOrder()]
    return ",".join(ls)


# 获取改写后sql里的order by子句
def getSqlOrderBy() -> str:
    if order_attr:
        for t in linked_tables:
            if order_attr in t.attr_list:
                return "{}.{}".format(t.name, columns[order_attr])
    return ''


def execute(sql: str):
    user = "Sevent"
    database = "test"
    port = "5432"
    password = ""
    connect = psycopg2.connect(database=database,
                               user=user, password=password, port=port)
    cur = connect.cursor()
    cur.execute("set enable_bitmapscan = on;")
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

    filter_condition = getSqlFilterCondition()
    order_by = getSqlOrderBy()
    if len(linked_tables) == 1:
        targetSql = "EXPLAIN ANALYZE SELECT {} FROM {}".format(getSqlSelect(), getSqlFrom())
        if filter_condition != '':
            targetSql += " WHERE {}".format(filter_condition)
        if order_by != '':
            targetSql += " ORDER BY {}".format(order_by)
    else:
        targetSql = "EXPLAIN ANALYZE SELECT {} FROM {} WHERE {}".format(getSqlSelect(),
                                                                        getSqlFrom(), getSqlJoinCondition())
        if filter_condition != '':
            targetSql += " AND {}".format(filter_condition)
        if order_by != '':
            targetSql += " ORDER BY {}".format(order_by)
    print(targetSql)
    query_plan = pd.read_sql(targetSql, con=connect)
    line1 = query_plan.values[0][0]  # 查询计划的第一行
    with open("./tpcc/cost.txt", "a", encoding='utf-8') as f:
        f.write(line1 + '\n')
    cur.close()


def getInput() -> List[str]:
    input = []
    with open('./tpcc/query.txt', "r", encoding='utf-8') as f:
        for line in f.readlines():
            input.append(line)
    return input


if __name__ == '__main__':
    columns = ['S_DIST_05', 'S_REMOTE_CNT', 'OL_SUPPLY_W_ID', 'C_CITY', 'OL_W_ID', ' S_ORDER_CNT', ' W_CITY',
               ' OL_DIST_INFO', ' C_DELIVERY_CNT', ' C_CREDIT', ' C_DISCOUNT', ' C_CREDIT_LIM', ' H_C_D_ID', ' H_DATA',
               ' W_ZIP', ' O_D_ID', ' O_ALL_LOCAL', ' D_ID', ' S_DIST_10', 'H_DATE', 'O_CARRIER_ID', ' W_STREET_1',
               ' NO_W_ID', ' W_ID', ' C_DATA', ' D_ZIP', ' O_OL_CNT', ' OL_QUANTITY', ' OL_W_ID', ' S_DATA', ' O_W_ID',
               ' I_NAME', ' W_STREET_2', ' C_MIDDLE', ' H_C_W_ID', ' C_STREET_1', ' C_STREET_2', 'C_D_ID', ' W_NAME',
               ' NO_O_ID', ' OL_AMOUNT', 'OL_I_ID', ' D_CITY', ' S_DIST_03', ' S_DIST_02', ' D_STREET_2', ' H_C_ID',
               ' C_PAYMENT_CNT', ' D_W_ID', ' OL_NUMBER', ' D_NAME', ' O_C_ID', ' C_FIRST', ' S_DIST_04', ' OL_D_ID',
               ' C_YTD_PAYMENT', ' NO_D_ID', ' I_PRICE', ' S_DIST_07', ' D_YTD', ' D_STATE', 'C_W_ID', ' H_W_ID',
               ' D_TAX', ' S_DIST_09', ' W_STATE', ' S_W_ID', ' I_IM_ID', ' OL_O_ID', ' O_ENTRY_D', 'OL_D_ID', ' W_YTD',
               ' C_LAST', ' I_ID', 'O_ID', ' C_BALANCE', ' W_TAX', ' OL_DELIVERY_D', ' S_DIST_01', ' OL_I_ID',
               ' I_DATA', ' S_I_ID', ' C_ZIP', ' C_PHONE', ' D_STREET_1', ' D_NEXT_O_ID', ' S_DIST_08', ' S_YTD',
               ' S_DIST_06', ' H_D_ID', 'C_ID', 'H_AMOUNT', 'C_SINCE', 'S_QUANTITY', ' C_STATE']
    for i, column in enumerate(columns):
        columns[i] = column.strip()
    table_list = [Table(1, "table_1", [19, 91, 13, 54, 68, 49, 79], [19, 91, 13, 54, 68, 49, 79], []),
                  Table(2, "table_2", [70, 68, 49, 79, 40, 7], [70, 68, 49, 79], []),
                  Table(3, "table_3", [4, 54, 68, 79], [54, 68, 79], []),
                  Table(4, "table_4", [41, 40, 27], [41, 40], []),
                  Table(5, "table_5", [70, 68, 49, 77], [70, 68, 49], []),
                  Table(6, "table_6", [90, 10, 9, 72, 52, 75, 55, 47, 8, 35, 36, 3, 94, 82, 83, 92, 24, 46, 51, 26, 16, 69, 22, 56, 39, 28, 54, 68], [28, 54, 68], []),
                  Table(7, "table_7", [37, 17, 59, 63, 85, 50, 84, 45, 42, 60, 12, 89, 15, 28, 54], [28, 54], []),
                  Table(8, "table_8", [74, 28, 68], [28, 68], []),
                  Table(9, "table_9", [61, 11, 33, 48, 25, 23, 71, 76, 38, 21, 32, 6, 65, 14, 34, 62, 30, 28], [28], []),
                  Table(10, "table_10", [20, 54, 68], [54, 68], []),
                  Table(11, "table_11", [41, 81, 93, 5, 29, 78, 44, 43, 53, 0, 88, 58, 86, 64, 18, 73, 31, 57, 80, 67], [41], []),
                  Table(12, "table_12", [2, 66, 87, 5, 1, 57, 67], [5, 57, 67], []), ]

    input = getInput()
    print("共执行{}条sql".format(len(input)))

    for i, sql in enumerate(input):
        # try:
        query_attrs, filters, filter_attrs, order_attr = parseStmt(sql, columns)
        linked_tables = getLinkedTables()
        root = buildJoinTree()  # 所有表的Join树的根节点
        subJoinTree = buildSubTree(getSubJoinOrder())  # 需要连接的表的Join子树根节点
        execute(sql)
        # except Exception as e:
        #     print("第{}条sql出现问题:{}".format(i + 1, e))
        #     break

    # sql = """SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY,        C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,        C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE   FROM CUSTOMER WHERE C_W_ID = 2    AND C_D_ID = 3    AND C_LAST = 'OUGHTESEATION'  ORDER BY C_FIRST;"""
    # query_attrs,filters,filter_attrs,order_attr = parseStmt(sql,columns)
    # linked_tables = getLinkedTables()
    # root = buildJoinTree()  # 所有表的Join树的根节点
    # subJoinTree = buildSubTree(getSubJoinOrder())  # 需要连接的表的Join子树根节点
    # execute(sql)

# print(getSqlSelect())

# print(getSqlFilterCondition())

# print(getSqlJoinCondition())

# j = buildJoinTree(table_list)
# print(j.left.left.left.condition)


# j1 = Join(Table(2),Table(1),buildJoinCondition())
# j2 = Join(j1,Table(3))
# j3 = Join(j2,Table(4))
# j4 = Join(j3,Table(5))
# ls=j4.getJoinOrder()
# print(j1.condition)
