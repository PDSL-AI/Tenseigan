#
# Created on July 19, 2021
#
# @author: Yiqin Xiong
#

# TODO: 仿照oracle和postgres，从导出的knobs定义文件，转换为标准的metrics的json文件，拷贝到fixture目录中
# NOTE：sap hana的添加中没有这一步骤（原因未知）

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


def is_real_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def convert_size_str(s):
    convert_dict = {
        'KiB': 1024 ** 1,
        'MiB': 1024 ** 2,
        'GiB': 1024 ** 3,
        'TiB': 1024 ** 4,
        'PiB': 1024 ** 5
    }
    factor = convert_dict[s[-3:]]
    base_value = int(s[:-3])
    return base_value * factor


def convert_time_str(s):
    return s


def single_process(temp_dict):
    fields = temp_dict["fields"]

    vartype = fields["vartype"]
    enumvals = fields["enumvals"]
    minval = fields["minval"]
    maxval = fields["maxval"]
    resource = fields["resource"]
    tunable = fields["tunable"]

    # tikv server 相关配置
    if "server." in fields["name"]:
        if "status-thread-pool-size" in fields["name"]:
            vartype = VarType.INTEGER
            resource = KnobResourceType.CPU
        elif "grpc-compression-type" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "none,deflate,gzip"
        elif "grpc-concurrency" in fields["name"]:
            minval = 1
            maxval = 16
            vartype = VarType.INTEGER
            resource = KnobResourceType.CPU
        elif "grpc-concurrent-stream" in fields["name"]:
            minval = 256
            maxval = 4096
        elif "grpc-raft-conn-num" in fields["name"]:
            minval = 1
            maxval = 8
        elif "raft-client-queue-size" in fields["name"]:
            minval = 2048
            maxval = 32768
        elif "grpc-memory-pool-quota" in fields["name"]:
            tunable = False
        elif "grpc-stream-initial-window-size" in fields["name"]:
            maxval = fields["maxval"] * 8
        elif "snap-max-write-bytes-per-sec" in fields["name"]:
            minval = 25 << 20
            maxval = 800 << 20
    # tikv readpool.unified 相关配置
    elif "readpool.unified." in fields["name"]:
        if "min-thread-count" in fields["name"]:
            vartype = VarType.INTEGER
            minval = 1
            maxval = 4
        elif "max-thread-count" in fields["name"]:
            vartype = VarType.INTEGER
            resource = KnobResourceType.CPU
        elif "stack-size" in fields["name"]:
            minval = 2 << 20
        elif "max-tasks-per-worker" in fields["name"]:
            minval = 500
            maxval = 4000
    # tikv readpool.storage 相关配置
    elif "readpool.storage." in fields["name"]:
        if "high-concurrency" in fields["name"]:
            minval = 4
            maxval = 32
            vartype = VarType.INTEGER
            resource = KnobResourceType.CPU
        elif "normal-concurrency" in fields["name"]:
            minval = 2
            maxval = 24
            vartype = VarType.INTEGER
            resource = KnobResourceType.CPU
        elif "low-concurrency" in fields["name"]:
            minval = 1
            maxval = 16
            vartype = VarType.INTEGER
            resource = KnobResourceType.CPU
    # tikv storage.block-cache 相关配置
    elif "storage.block-cache." in fields["name"]:
        if ".capacity" in fields["name"]:
            vartype = VarType.INTEGER
            minval = 8 << 20
            maxval = 64 << 30
            resource = KnobResourceType.MEMORY
    # tikv storage.flow-control 相关配置
    elif "storage.flow-control." in fields["name"]:
        if ".memtables-threshold" in fields["name"]:
            minval = 2
            maxval = 10
        elif "l0-files-threshold" in fields["name"]:
            minval = 8
            maxval = 32
    # tikv storage.io-rate-limit 相关配置
    elif "storage.io-rate-limit." in fields["name"]:
        if "compaction-priority" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "low,medium,high"
        elif "export-priority" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "low,medium,high"
        elif "flush-priority" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "low,medium,high"
        elif "foreground-read-priority" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "low,medium,high"
        elif "foreground-write-priority" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "low,medium,high"
        elif "gc-priority" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "low,medium,high"
        elif "import-priority" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "low,medium,high"
        elif "level-zero-compaction-priority" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "low,medium,high"
        elif "load-balance-priority" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "low,medium,high"
        elif "max-bytes-per-sec" in fields["name"]:
            minval = 50 << 20
            maxval = 5 << 30
        elif ".mode" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "write-only"
        elif "other-priority" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "low,medium,high"
        elif "replication-priority" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "low,medium,high"
    # tikv storage 相关配置
    elif "storage." in fields["name"]:
        if "scheduler-worker-pool-size" in fields["name"]:
            minval = 4
            maxval = 16
            vartype = VarType.INTEGER
            resource = KnobResourceType.CPU
        elif "scheduler-pending-write-threshold" in fields["name"]:
            minval = 100 << 20
        elif "reserve-space" in fields["name"]:
            minval = 5 << 30
            vartype = VarType.INTEGER
        elif "enable-ttl" in fields["name"]:
            tunable = False
    # tikv raftstore 相关配置
    elif "raftstore." in fields["name"]:
        if "apply-max-batch-size" in fields["name"]:
            minval = 128
            maxval = 2048
        elif "apply-pool-size" in fields["name"]:
            resource = KnobResourceType.CPU
        elif "store-max-batch-size" in fields["name"]:
            minval = 256
            maxval = 4096
        elif "store-pool-size" in fields["name"]:
            resource = KnobResourceType.CPU
        elif "future-poll-size" in fields["name"]:
            minval = 1
            maxval = 4
        elif "messages-per-tick" in fields["name"]:
            minval = 1024
            maxval = 16384
        elif "raft-entry-max-size" in fields["name"]:
            minval = 8 << 20
            maxval = 128 << 20
        elif "local-read-batch-size" in fields["name"]:
            minval = 512
            maxval = 8192
        elif "cmd-batch-concurrent-ready-max-count" in fields["name"]:
            minval = 1
            maxval = 10
    # tikv coprocessor 相关配置
    elif "coprocessor." in fields["name"]:
        if "region-max-keys" in fields["name"]:
            minval = int(minval / 8)
        elif "region-split-keys" in fields["name"]:
            minval = int(minval / 8)
    # tikv rocksdb.defaultcf.titan 相关配置
    elif "rocksdb.defaultcf.titan." in fields["name"]:
        if "min-blob-size" in fields["name"]:
            minval = 1 << 10
            maxval = 16 << 10
        elif "blob-file-compression" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "no,snappy,zlib,bz2,lz4,lz4hc,zstd"
            tunable = True
        elif "blob-cache-size" in fields["name"]:
            minval = 1 << 20
            maxval = 1 << 30
        elif "discardable-ratio" in fields["name"]:
            vartype = VarType.REAL
            minval = 0
            maxval = 1
        elif "sample-ratio" in fields["name"]:
            vartype = VarType.REAL
            minval = 0
            maxval = 1
        elif "blob-run-mode" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "normal,read-only,fallback"
            tunable = True
    # tikv defaultcf\writecf 相关配置
    elif "rocksdb.defaultcf." in fields["name"] or "rocksdb.writecf." in fields["name"]:
        if "lockcf" in fields["name"]:
            tunable = False
        elif "raftcf" in fields["name"]:
            tunable = False
        elif "ver-defaultcf" in fields["name"]:
            tunable = False
        elif "block-size" in fields["name"]:
            minval = 4 << 10
            maxval = 512 << 10
        elif "disable-block-cache" in fields["name"]:
            tunable = False
        elif "read-amp-bytes-per-bit" in fields["name"]:
            tunable = False
        elif "compression-per-level" in fields["name"]:
            tunable = False
        elif "bottommost-level-compression" in fields["name"]:
            tunable = False
        elif "block-cache-size" in fields["name"]:
            minval = 8 << 20
            maxval = 64 << 30
            resource = KnobResourceType.MEMORY
        elif "write-buffer-size" in fields["name"]:
            minval = 64 << 20
            maxval = 1 << 30
        elif "max-write-buffer-number" in fields["name"]:
            minval = 2
            maxval = 16
        elif "min-write-buffer-number-to-merge" in fields["name"]:
            minval = 1
            maxval = 16
        elif "max-bytes-for-level-base" in fields["name"]:
            minval = 256 << 20
            maxval = 8 << 30
        elif "target-file-size-base" in fields["name"]:
            minval = 64 << 20
            maxval = 1 << 30
        elif "level0-file-num-compaction-trigger" in fields["name"]:
            minval = 2
            maxval = 32
        elif "level0-slowdown-writes-trigger" in fields["name"]:
            minval = 5
            maxval = 80
        elif "level0-stop-writes-trigger" in fields["name"]:
            minval = 9
            maxval = 144
        elif "compaction-pri" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "0,1,2,3"
        elif "num-levels" in fields["name"]:
            minval = 5
            maxval = 10
        elif "max-bytes-for-level-multiplier" in fields["name"]:
            minval = 5
            maxval = 20
        elif "compaction-style" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "0,1"
        elif "soft-pending-compaction-bytes-limit" in fields["name"]:
            minval = 32 << 30
            maxval = 128 << 30
        elif "hard-pending-compaction-bytes-limit" in fields["name"]:
            minval = 128 << 30
            maxval = 512 << 30
        elif "compaction-guard-min-output-file-size" in fields["name"]:
            minval = 4 << 20
            maxval = 64 << 20
        elif "compaction-guard-max-output-file-size" in fields["name"]:
            minval = 64 << 20
            maxval = 1 << 30
    # tikv rocksdb.titan 相关配置
    elif "rocksdb.titan." in fields["name"]:
        if "max-background-gc" in fields["name"]:
            minval = 2
    # tikv rocksdb 相关配置
    elif "rocksdb." in fields["name"]:
        if "max-background-jobs" in fields["name"]:
            minval = 2
            vartype = VarType.INTEGER
            resource = KnobResourceType.CPU
        elif "max-background-flushes" in fields["name"]:
            minval = 2
            maxval = 16
        elif "max-sub-compactions" in fields["name"]:
            maxval = 16
        elif "create-if-missing" in fields["name"]:
            tunable = False
        elif "wal-recovery-mode" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "0,1,2,3"
        elif "enable-statistics" in fields["name"]:
            tunable = False
        elif "rate-limiter-mode" in fields["name"]:
            vartype = VarType.ENUM
            enumvals = "1,2,3"
    # raftdb相关配置
    elif "raftdb." in fields["name"]:
        if "max-background-jobs" in fields["name"]:
            minval = 2
            vartype = VarType.INTEGER
            resource = KnobResourceType.CPU
        elif "max-sub-compactions" in fields["name"]:
            minval = 1
            maxval = 16
    # global相关配置
    elif "tidb_build_stats_concurrency" in fields["name"]:
        minval = 1
        maxval = 16
    elif "tidb_distsql_scan_concurrency" in fields["name"]:
        minval = 1
        maxval = 255
    elif "tidb_executor_concurrency" in fields["name"]:
        minval = 1
        maxval = 20
    # TiDB相关配置
    elif "prepared-plan-cache.capacity" in fields["name"]:
        minval = 500
        maxval = 4000

    fields["vartype"] = vartype
    fields["minval"] = minval
    fields["maxval"] = maxval
    fields["enumvals"] = enumvals
    fields["resource"] = resource
    fields["tunable"] = tunable


def preprocess_knob_data():
    # 查询数据库show config和show global variables
    db = pymysql.connect(host=DB_HOST, port=4000)
    cursor = db.cursor()
    cursor.execute('show config')
    data = cursor.fetchall()
    cursor.execute('show global variables')
    data += cursor.fetchall()
    cursor.close()
    db.close()
    # 固定值
    category = ""
    context = "dynamic"
    scope = "global"
    dbms = DBMSType.TIDB
    summary = ""
    description = ""

    yaml_data = []

    for each in data:

        temp_dict = dict()
        temp_dict["model"] = "website.KnobCatalog"

        value = each[3] if len(each) == 4 else each[1]

        # 可变值
        tunable = True
        vartype = VarType.INTEGER
        default = value
        minval = None
        maxval = None
        unit = KnobUnitType.OTHER
        enumvals = None
        resource = KnobResourceType.OTHER

        if str.isdigit(value):
            default = int(value)
            if default > (1 << 60):
                minval = int(default) // 8
                maxval = default
            else:
                minval = int(default)
                maxval = int(default * 8)
        elif value in ('true', 'false'):
            vartype = VarType.BOOL
            default = True if value == 'true' else False
        elif value.endswith('iB'):
            default = convert_size_str(value)
            minval = int(default / 8)
            maxval = int(default * 8)
            unit = KnobUnitType.BYTES
        elif len(value) < 15 and (
                value.endswith('ms') or value.endswith('s') or value.endswith('m') or value.endswith('h')):
            # TODO: 暂时不考虑时间相关的knobs
            vartype = VarType.STRING
            tunable = False
            default = convert_time_str(value)
            unit = KnobUnitType.MILLISECONDS
        elif is_real_number(value):
            vartype = VarType.REAL
            if float(value) > 0:
                default = float(value)
                minval = default / 2.0
                maxval = default * 16.0
        else:
            vartype = VarType.STRING
            tunable = False
            # print(each)

        temp_dict["fields"] = {
            "category": category,
            "vartype": vartype,
            "default": default,
            "minval": minval,
            "maxval": maxval,
            "unit": unit,
            "enumvals": enumvals,
            "context": context,
            "scope": scope,
            "dbms": dbms,
            "name": f"global.({each[0]},{each[1]},{each[2]})" if len(each) == 4 else f"global.({each[0]})",
            "tunable": tunable,
            "summary": summary,
            "description": description,
            "resource": resource
        }

        single_process(temp_dict)

        yaml_data.append(temp_dict)

    with open("tidb-50_knobs.json", "w") as jf:
        json.dump(yaml_data, jf, indent=4)


# topology_file_path = '../../../../../../cluster/tidb-1.yaml'
#
# with open(topology_file_path, 'r') as f:
#     topology = yaml.load(f, Loader=yaml.FullLoader)

# rocksdb_cfs = [
#     "defaultcf",
#     ""
# ]

# rocksdb_global_knob_names = [
#     "max-background-jobs",
# ]


# rocksdb_cf_knobs = {
#     "write-buffer-size": {
#         "vartype": VarType.INTEGER,
#         "default": 67108864,
#         "minval": 67108864,
#         "maxval": 1073741824,
#         "unit": KnobUnitType.OTHER,
#         "enumvals": None,
#     },
#     "max-write-buffer-number": {
#         "vartype": VarType.INTEGER,
#         "default": 67108864,
#         "minval": 67108864,
#         "maxval": 1073741824,
#         "unit": KnobUnitType.OTHER,
#         "enumvals": None,
#         "resource": KnobResourceType.MEMORY
#     },
#     "max-bytes-for-level-base": {
#         "vartype": VarType.INTEGER,
#         "default": 67108864,
#         "minval": 67108864,
#         "maxval": 1073741824,
#         "unit": KnobUnitType.OTHER,
#         "enumvals": None,
#         "resource": KnobResourceType.MEMORY
#     },
#     "compaction-guard-max-output-file-size": {
#         "vartype": VarType.INTEGER,
#         "default": 67108864,
#         "minval": 67108864,
#         "maxval": 1073741824,
#         "unit": KnobUnitType.OTHER,
#         "enumvals": None,
#         "resource": KnobResourceType.MEMORY
#     },
#     "compaction-guard-min-output-file-size": {
#         "vartype": VarType.INTEGER,
#         "default": 67108864,
#         "minval": 67108864,
#         "maxval": 1073741824,
#         "unit": KnobUnitType.OTHER,
#         "enumvals": None,
#         "resource": KnobResourceType.MEMORY
#     },
#     "block-size": {
#         "vartype": VarType.INTEGER,
#         "default": 67108864,
#         "minval": 67108864,
#         "maxval": 1073741824,
#         "unit": KnobUnitType.OTHER,
#         "enumvals": None,
#         "resource": KnobResourceType.MEMORY
#     },
#     "whole-key-filtering": {
#         "vartype": VarType.INTEGER,
#         "default": 67108864,
#         "minval": 67108864,
#         "maxval": 1073741824,
#         "unit": KnobUnitType.OTHER,
#         "enumvals": None,
#         "resource": KnobResourceType.MEMORY
#     },
#     "optimize-filters-for-hits": {
#         "vartype": VarType.INTEGER,
#         "default": 67108864,
#         "minval": 67108864,
#         "maxval": 1073741824,
#         "unit": KnobUnitType.OTHER,
#         "enumvals": None,
#         "resource": KnobResourceType.MEMORY
#     },
#     "level0-file-num-compaction-trigger": {
#         "vartype": VarType.INTEGER,
#         "default": 67108864,
#         "minval": 67108864,
#         "maxval": 1073741824,
#         "unit": KnobUnitType.OTHER,
#         "enumvals": None,
#         "resource": KnobResourceType.MEMORY
#     },
#     "min-write-buffer-number-to-merge": {
#         "vartype": VarType.INTEGER,
#         "default": 67108864,
#         "minval": 67108864,
#         "maxval": 1073741824,
#         "unit": KnobUnitType.OTHER,
#         "enumvals": None,
#         "resource": KnobResourceType.MEMORY
#     },
#     "compaction-style": {
#         "vartype": VarType.INTEGER,
#         "default": 67108864,
#         "minval": 67108864,
#         "maxval": 1073741824,
#         "unit": KnobUnitType.OTHER,
#         "enumvals": None,
#         "resource": KnobResourceType.MEMORY
#     },
#     "compaction-pri": {
#         "vartype": VarType.INTEGER,
#         "default": 67108864,
#         "minval": 67108864,
#         "maxval": 1073741824,
#         "unit": KnobUnitType.OTHER,
#         "enumvals": None,
#         "resource": KnobResourceType.MEMORY
#     },
#     "block-cache-size": {
#         "vartype": VarType.INTEGER,
#         "default": 67108864,
#         "minval": 67108864,
#         "maxval": 1073741824,
#         "unit": KnobUnitType.OTHER,
#         "enumvals": None,
#         "resource": KnobResourceType.MEMORY
#     },
# }


# def get_rocksdb_knob_names():
#     # 先加global config
#     res = [f"rocksdb.{knob_name}" for knob_name in rocksdb_global_knob_names]
#     # 再加cf config
#     for cf in rocksdb_cfs:
#         for knob_name in rocksdb_cf_knob_names:
#             res.append(f"rocksdb.{cf}.{knob_name}")
#     return res

if __name__ == "__main__":
    preprocess_knob_data()
    shutil.copy("tidb-50_knobs.json", "../../../../website/fixtures/tidb-50_knobs.json")
