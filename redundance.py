import os
import re
import time
from itertools import combinations
from sklearn.cluster import DBSCAN

import psycopg2
import numpy as np
import pandas as pd

"""
workflow:
两个表->成功->添加下一个表。。。->失败  断开，返回主键从剩下的group中找
         ->失败->继续加...直到成功......

"""


def get_rd(attr_list, data_list):
    """
    计算冗余度
    :param attr_list:该组包括的属性名
    :param data_list: 数据列表，包含各个属性的数据
    :return: list[attr_list(list),data_list(df), rd_list(list)]
    """
    if len(attr_list) == 1:
        a = pd.value_counts(data_list) / len(data_list)
        entropy = sum(np.log2(a) * a * (-1))
        rd = np.log2(len(data_list)) - entropy
        if rd < 0.0005:
            rd = 0
        return rd
    else:
        rd_sum = 0
        for attribute in attr_list:
            # 冗余度计算：log2（n）-entropy
            a = pd.value_counts(data_list[attribute]) / len(data_list[attribute])
            entropy = sum(np.log2(a) * a * (-1))
            rd = np.log2(len(data_list[attribute])) - entropy
            if rd < 0.0005:
                rd = 0
            rd_sum = rd_sum + rd
        rd = rd_sum / len(attr_list)
        return rd


def get_attribute(database, user, password, port, table_name):
    """
    get data from database
    attribute: 要分解的表名
    :return: list[attr_list(list),data_list(df)]
    """
    connect = psycopg2.connect(database=database,
                               user=user,
                               password=password,
                               port=port
                               )
    # 创建一个cursor来执行数据库的操作
    cur = connect.cursor()
    all_list = []
    sql_data = "SELECT * FROM " + table_name
    sql_attributes = "SELECT a.attname FROM pg_attribute a, pg_class c where c.relname='" + table_name + "' and " \
                                                                                                         "a.attrelid" \
                                                                                                         "=c.oid " \
                                                                                                         "and " \
                                                                                                         "a.attnum>0 "
    data = pd.read_sql(sql_data, con=connect)
    attributes = pd.read_sql(sql_attributes, con=connect)
    all_list.append(attributes['attname'].tolist())
    all_list.append(data)
    cur.close()
    return all_list


# def table_exists(con, table_name):
#     """判断表是否存在，若存在就不需要创建了"""
#     sql = "show tables;"
#     con.execute(sql)
#     tables = [con.fetchall()]
#     table_list = re.findall('(\'.*?\')', str(tables))
#     table_list = [re.sub("'", '', each) for each in table_list]
#     if table_name in table_list:
#         return 1  # 存在返回1
#     else:
#         return 0


def sort_attr_by_rd(lt):
    """
    根据冗余度对属性进行排序，取最大的两个做合并
    :param lt: attrgroup list
    :return:
    """
    for i in range(len(lt) - 1):
        for j in range(i + 1, len(lt)):
            if lt[i].rd < lt[j].rd:
                t = lt[i]
                lt[i] = lt[j]
                lt[j] = t


def sort_table_by_rows(lt):
    for i in range(len(lt) - 1):
        for j in range(i + 1, len(lt)):
            if len(lt[i].data_list) > len(lt[j].data_list):
                t = lt[i]
                lt[i] = lt[j]
                lt[j] = t


def sort_table_by_key(table_list):
    for i in range(len(table_list[:-1])):
        for j in range(i, len(table_list[i:])):
            if set(table_list[i].key) <= set(table_list[j].attr_list):
                tmp = table_list[j]
                table_list[j] = table_list[i + 1]
                table_list[i + 1] = tmp
                break


def merge_attr(left_group, right_group, origin_data):
    """merge when merge successfully"""
    merge_attr = left_group.attr_list + right_group.attr_list
    merge_data = origin_data[merge_attr].drop_duplicates()
    merge = AttrGroup(merge_attr, merge_data)
    if merge.set_key() and merge.rd < 5:
        return [True, merge]
    return [False, left_group]


def merge_fail(left_group, right_group, origin_data):
    """merge when merge failure"""
    merge_attr = left_group.attr_list + right_group.attr_list
    merge_data = origin_data[merge_attr].drop_duplicates()
    merge = AttrGroup(merge_attr, merge_data)
    if merge.set_key():
        return [True, merge]
    return [False, merge]


def auto_merge(ans_list, group_list, origin_data):
    """ 输入各列属性，输出划分结果
    :param ans_list: 目标列表
    :param group_list: 输入的列表，包括各列数据结构
    :param origin_data: 原始数据
    :param flag: 判别组合成功过没，若成功过则失败时返回成功的和key，若没成功过则一直加
    :return: group_list
    """
    if len(group_list) > 2:
        sort_attr_by_rd(group_list)
        left = 0
        right = 1
        result = merge_attr(group_list[left], group_list[right], origin_data)
        if result[0]:
            while result[0] and right < len(group_list) - 1:
                right = right + 1
                result = merge_attr(result[1], group_list[right], origin_data)
            for i in range(right):
                group_list.pop(0)
            ans_list.append(result[1])
            result[1].set_key()
            key = result[1].key
            # print(key)
            if len(key) == 1:
                key_group = AttrGroup(key, origin_data[key[0]])
            else:
                key_group = AttrGroup(key, origin_data[key])
            group_list.append(key_group)
            auto_merge(ans_list, group_list, origin_data)
        else:
            while not result[0] and right < len(group_list) - 1:
                result = merge_fail(result[1], group_list[right], origin_data)
                right = right + 1
            for i in range(right):
                group_list.pop(0)
            ans_list.append(result[1])
            result[1].set_key()
            key = result[1].key
            if len(key) == 1:
                key_group = AttrGroup(key, origin_data[key[0]])
            else:
                key_group = AttrGroup(key, origin_data[key])
            group_list.append(key_group)
            auto_merge(ans_list, group_list, origin_data)
    pass


# def group_merge_one(attr_group, list, origin_data):
#     if len(list) != 0:
#         rd = attr_group.rd
#         merge = attr_group.attr_list.append(list[0].attr_list)
#         data = origin_data[attr_group.attr_list].drop_duplicates()
#         merge_group = AttrGroup(merge, data)
#         if merge_group.rd <= rd and merge_group.set_key():
#             list.pop(0)
#             attr_group = merge_group
#             group_merge_one(attr_group, list, origin_data)
#     pass


def combine(temp_list, n):
    """根据n获得列表中的所有可能组合（n个元素为一组）为找key服务"""
    temp_list2 = []
    for c in combinations(temp_list, n):
        temp_list2.append(c)
    return temp_list2


class config:
    def __init__(self, database, user, password, port, tablename):
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.tablename = tablename


class AttrGroup:
    """
    不同分组类，每个attrgroup中的属性被组合到一个表中
    可以访问到该表的冗余度，属性列，数据分布和主键（作为其他表的外键）
    """
    attr_list = []
    key = []
    rd = 0.0
    name = ""

    def __init__(self, attr_list, data_list):
        """
        初始化group
        :param attr_list: 列表，包含的所有属性
        :param data_list: dataframe
        """
        self.attr_list = attr_list
        self.data_list = data_list
        self.set_rd()

    def set_rd(self):
        """
        计算冗余度并设置rd
        :return: void
        """
        self.rd = get_rd(attr_list=self.attr_list, data_list=self.data_list)
        # if len(self.attr_list) == 1:
        #     a = pd.value_counts(self.data_list) / len(self.data_list)
        #     entropy = sum(np.log2(a) * a * (-1))
        #     rd = np.log2(len(self.data_list)) - entropy
        #     if rd < 0.0005:
        #         rd = 0
        #     self.rd = rd
        # else:
        #     rd_sum = 0
        #     for attribute in self.attr_list:
        #         # 冗余度计算：log2（n）-entropy
        #         a = pd.value_counts(self.data_list[attribute]) / len(self.data_list[attribute])
        #         entropy = sum(np.log2(a) * a * (-1))
        #         rd = np.log2(len(self.data_list[attribute])) - entropy
        #         if rd < 0.0005:
        #             rd = 0
        #         rd_sum = rd_sum + rd
        #         self.rd = rd_sum / len(self.attr_list)

    def set_attr_list(self, attr_list, data_list):
        """
        设置该组的属性列并存储数据
        :param attr_list: 属性列表
        :param data_list: 数据列表
        :return: void
        """
        self.attr_list = attr_list
        self.data_list = data_list
        pass

    def set_key(self):
        """
        确定该组主键
        :return: boolean
        """
        if len(self.attr_list) == 1:
            self.key = self.attr_list
            return True
        key_prob = []
        for i in range(len(self.attr_list) - 1):
            key_prob.extend(combine(self.attr_list, i + 1))
        for attrs in key_prob:
            if len(self.data_list[list(attrs)].drop_duplicates()) == len(self.data_list):
                self.key = list(attrs)
                return True
        return False

    """单独拉出一个函数（merge...）"""
    # def add(self, attr_group, all_data):
    #     self.attr_list = self.attr_list + attr_group.attr_list
    #     self.data_list = all_data[self.attr_list].drop_duplicates()
    #     pass
    """放到测试类中了"""
    # def create_table(self, tablename, user, port, password, database):
    #     """建表进行测试"""
    #     connect = psycopg2.connect(database=database,
    #                                user=user,
    #                                password=password,
    #                                port=port
    #                                )
    #     cur = connect.cursor()
    #     table_attr_str = self.attr_list[0]
    #     for i in self.attr_list[1:]:
    #         table_attr_str = table_attr_str + ", " + i
    #     new_table = table_attr_str.replace(", ", "_")
    #     sql_create = "CREATE TABLE " + new_table + " AS SELECT DISTINCT " + table_attr_str + " from " + tablename
    #     cur.execute(sql_create)
    #     start = time.time()
    #     sql = "SELECT * FROM h_w_id_h_c_w_id_h_amount_h_c_d_id"
    #     cur.execute(sql)
    #     connect.commit()
    #     end = time.time()
    #     print(end - start)
    #     connect.commit()
    #     cur.close()


class Mytest:
    """根据分解结果构建数据表并进行相关测试等"""
    table_list = []
    table_name_list = []
    key_list = []
    flag = True

    def __init__(self, config, tablelist):
        self.table_list = tablelist
        self.create_test_table(config)
        key_str = ""
        for table in self.table_list:
            table_key_str = table.key[0]
            for attr in table.key[1:]:
                key_str = key_str + ", " + attr
            self.key_list.append(key_str)

    def create_test_table(self, config):
        connect = psycopg2.connect(database=config.database,
                                   user=config.user,
                                   password=config.password,
                                   port=config.port
                                   )
        # 创建一个cursor来执行数据库的操作
        cur = connect.cursor()
        for table in self.table_list:
            table_attr_str = table.attr_list[0]
            for attr in table.attr_list[1:]:
                table_attr_str = table_attr_str + ", " + attr
            new_table = table_attr_str.replace(", ", "_")
            table.name = new_table
            self.table_name_list.append(new_table)
            table.name = new_table
            sql_create = "CREATE TABLE IF NOT EXISTS " + new_table + " AS SELECT DISTINCT " + table_attr_str + " from " + config.tablename
            cur.execute(sql_create)
            connect.commit()
        cur.close()

    def test_sql(self, config, query_attrs=[], filter_attrs=[], clause=[]):
        """找需要参与查询的分解后的表并改写sql语句。
        依次访问各组group找其与sql涉及的属性的交集并保留，与之前的交集做对比，
        如果都包含在之前的交集中则该group代表的表不需要参与查询，直到交集与sql涉及属性相同
        根据key调整连接顺序"""
        all_attrs = set(query_attrs + filter_attrs)
        linked_attrs = set()
        linked_tables = []
        for attrs in self.table_list:
            if linked_attrs == all_attrs:
                break
            n = set(attrs.attr_list)
            inter_list = all_attrs.intersection(n)
            if not inter_list <= linked_attrs:
                linked_tables.append(attrs)
                linked_attrs = linked_attrs.union(inter_list)
        for i in range(len(query_attrs)):
            for j in linked_tables:
                if query_attrs[i] in j.attr_list:
                    query_attrs[i] = j.name + "." + query_attrs[i]
                    print(query_attrs[i])
                    continue
        for i in range(len(filter_attrs)):
            for j in linked_tables:
                if filter_attrs[i] in j.attr_list:
                    filter_attrs[i] = j.name + "." + filter_attrs[i]
                    print(filter_attrs[i])
        print(query_attrs, filter_attrs)

        query_str = query_attrs[0]
        for i in query_attrs[1:]:
            query_str = query_str + "," + i
        filter_str = filter_attrs[0]
        for i in filter_attrs[1:]:
            filter_str = filter_str + "," + i
        table_str = linked_tables[0].name
        for i in linked_tables[1:]:
            table_str = table_str + "," + i.name
        sql_sel = "EXPLAIN SELECT " + query_str + " FROM " + table_str + " WHERE " + filter_str + ">1"
        print(sql_sel)
        connect = psycopg2.connect(database=config.database,
                                   user=config.user,
                                   password=config.password,
                                   port=config.port
                                   )
        # 创建一个cursor来执行数据库的操作
        cur = connect.cursor()
        cardinality = pd.read_sql(sql_sel, con=connect)
        print(cardinality)
        # sql = "SELECT "+query_str + from


# config:
table_name = "customer"
user = 'Sevent'
database = 'test'
port = '5432'
password = ''

# test:

group_list = []
data = get_attribute(database=database, user=user, password=password, port=port, table_name=table_name)
for i in range(len(data[0])):
    group_list.append(AttrGroup([data[0][i]], data[1][data[0][i]]))

for i in group_list:
    print(i.attr_list)
sort_attr_by_rd(group_list)
merge_attr(group_list[0], group_list[1], data[1])
# print(len(group_list))
# for i in group_list:
#     print(i.attr_list)
#     print(i.rd)
# group_list[0].add(group_list[1], data[1])
# group_list[0].set_rd()
# merge_group = merge_attr(merge_list, data[1])
# for i in group_list:
#     print(i.rd)
# print(merge_group.data_list)
# print(merge_group.rd)
# a = merge_attr(group_list[0], group_list[1], data[1])[1]
# print(a)
# b = merge_attr(a, group_list[2], data[1])[1]
# print(merge_attr(b, group_list[3], data[1])[1])
ans_list = []
auto_merge(ans_list, group_list, data[1])
sort_attr_by_rd(group_list)
for i in ans_list:
    print(i.attr_list)
    print(i.key)
    print(i.rd)

print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
sort_table_by_rows(ans_list)
for i in ans_list:
    print(len(i.data_list))
# for i in ans_list:
#     i.create_table(database=database, user=user, password=password, port=port, tablename=table_name)
# test for creating table
# config1 = config(database=database, user=user, port=port, password=password, tablename=table_name)
# my_test = Mytest(config1, ans_list)
# my_test.test_sql(config1, ['h_w_id'], ['h_w_id'], [])
