#
# Created on July 19, 2021
#
# @author: Yiqin Xiong
#

# TODO: 仿照oracle和postgres，从导出的metrics定义文件（oracle是json，postgres是csv，当然也可以自己定义metrics），转换为标准的metrics的json文件，拷贝到fixture目录中
# NOTE：当然也可以参考sap hana的添加：https://github.com/cmu-db/ottertune/pull/194/commits/10bdc3d378cf7c78110702dd85e317aa875ecf17
# NOTE：流程大概是 1.导出的metrics定义文件通过本文件得到fixture目录下标准的metrics的json文件 2.将json文件复制到fixtures文件夹下

import os
import shutil

import yaml
import json
import sys

sys.path.append(r'../../../../../website')
sys.path.append(r'../../../../../../client/driver')
from website.types import DBMSType, VarType, KnobUnitType, KnobResourceType
from driver_config import DB_HOST
import pymysql


def preprocess_metrics_data():
    # 查询数据库select type,instance from information_schema.cluster_info
    db = pymysql.connect(host=DB_HOST, port=4000)
    cursor = db.cursor()
    cursor.execute('select type,instance from information_schema.cluster_info;')
    data = cursor.fetchall()
    cursor.close()
    db.close()
    # 得到各组件的ip地址
    ip_dict = {'tidb': [], 'pd': [], 'tikv': []}
    for d in data:
        if d[0] in ip_dict.keys():
            ip_dict[d[0]].append(d[1].split(':')[0])
    # 固定值
    scope = "global"
    summary = ""
    dbms = DBMSType.TIDB
    default = None

    metrics_dict = {
        'tidb_qps': {
            'role': 'tidb',
            'port': 10080,
            'vartype': 3,
            'metric_type': 3,
        },
        'tidb_query_duration': {
            'role': 'tidb',
            'port': 10080,
            'vartype': 3,
            'metric_type': 3,
        },
        'tikv_thread_cpu': {
            'role': 'tikv',
            'port': 20180,
            'vartype': 3,
            'metric_type': 3,
        },
        'tikv_memory': {
            'role': 'tikv',
            'port': 20180,
            'vartype': 3,
            'metric_type': 3,
        },
        'node_disk_io_util': {
            'role': 'tikv',
            'port': 9100,
            'vartype': 3,
            'metric_type': 3,
        },
        'tikv_grpc_message_total_count': {
            'role': 'tikv',
            'port': 20180,
            'vartype': 3,
            'metric_type': 3,
        },
        'tikv_grpc_message_duration': {
            'role': 'tikv',
            'port': 20180,
            'vartype': 3,
            'metric_type': 3,
        },
    }

    yaml_data = []

    for metric_name in metrics_dict.keys():

        metric_item = metrics_dict[metric_name]
        port = metric_item['port']
        vartype = metric_item['vartype']
        metric_type = metric_item['metric_type']

        for role_ip in ip_dict[metric_item['role']]:
            temp_dict = dict()
            temp_dict["model"] = "website.MetricCatalog"

            temp_dict["fields"] = {
                "name": f'global.({metric_name},{role_ip}:{port})',
                "vartype": vartype,
                "metric_type": metric_type,
                "scope": scope,
                "summary": summary,
                "dbms": dbms,
                "default": default,
            }

            yaml_data.append(temp_dict)

    with open("tidb-50_metrics.json", "w") as jf:
        json.dump(yaml_data, jf, indent=4)


if __name__ == "__main__":
    preprocess_metrics_data()
    shutil.copy("tidb-50_metrics.json", "../../../../website/fixtures/tidb-50_metrics.json")
