#
# OtterTune - set_default_knobs.py
#
# Copyright (c) 2017-18, Carnegie Mellon University Database Group
#
import os
import logging

from .models import KnobCatalog, SessionKnob
from .types import DBMSType, KnobResourceType, VarType

import sys

WEBSITE_HOME = os.path.dirname(os.path.realpath(__file__))
sys.path.append(f'{WEBSITE_HOME}/../../../client/driver')
from driver_config import DB_HOST
import pymysql

LOG = logging.getLogger(__name__)

# Path to this driver
WEBSITE_HOME = os.path.dirname(os.path.realpath(__file__))
CLUSTER_HOME = WEBSITE_HOME + '/../../../cluster'

# Default tunable knobs by DBMS. If a DBMS is not listed here, the set of
# tunable knobs in the KnobCatalog will be used instead.
DEFAULT_TUNABLE_KNOBS = {
    DBMSType.POSTGRES: {
        "global.autovacuum",
        "global.archive_mode",
        "global.effective_cache_size",
        "global.maintenance_work_mem",
        "global.max_wal_size",
        "global.max_worker_processes",
        "global.shared_buffers",
        "global.temp_buffers",
        "global.wal_buffers",
        "global.work_mem",
    },
    DBMSType.ORACLE: {
        "global.db_cache_size",
        "global.log_buffer",
        "global.hash_area_size",
        "global.open_cursors",
        "global.pga_aggregate_size",
        "global.shared_pool_reserved_size",
        "global.shared_pool_size",
        "global.sort_area_size",
    },
    DBMSType.MYSQL: {
        "global.innodb_buffer_pool_size",
        "global.innodb_thread_sleep_delay",
        "global.innodb_flush_method",
        "global.innodb_log_file_size",
        "global.innodb_max_dirty_pages_pct_lwm",
        "global.innodb_read_ahead_threshold",
        "global.innodb_adaptive_max_sleep_delay",
        "global.innodb_buffer_pool_instances",
        "global.thread_cache_size",
    },
    DBMSType.TIDB: {
        ## Global Variables
        "tidb_build_stats_concurrency",
        "tidb_distsql_scan_concurrency",
        "tidb_executor_concurrency",

        ## TiKV参数
        # server 相关
        "server.grpc-concurrency",
        "server.grpc-concurrent-stream",
        "server.grpc-raft-conn-num",
        "server.raft-client-queue-size",
        # readpool.unified相关
        "readpool.unified.min-thread-count",
        "readpool.unified.max-thread-count",
        "readpool.unified.max-tasks-per-worker",
        # storage相关
        "storage.scheduler-worker-pool-size",
        # storage.block-cache相关
        "storage.block-cache.capacity",
        # storage.flow-control相关
        "storage.flow-control.memtables-threshold",
        "storage.flow-control.l0-files-threshold",
        # storage.io-rate-limit相关
        "storage.io-rate-limit.compaction-priority",
        "storage.io-rate-limit.export-priority",
        "storage.io-rate-limit.flush-priority",
        "storage.io-rate-limit.foreground-read-priority",
        "storage.io-rate-limit.foreground-write-priority",
        "storage.io-rate-limit.gc-priority",
        "storage.io-rate-limit.import-priority",
        "storage.io-rate-limit.level-zero-compaction-priority",
        "storage.io-rate-limit.load-balance-priority",
        "storage.io-rate-limit.max-bytes-per-sec",
        "storage.io-rate-limit.other-priority",
        "storage.io-rate-limit.replication-priority",
        # raftstore相关
        "raftstore.apply-max-batch-size",
        "raftstore.apply-pool-size",
        "raftstore.store-max-batch-size",
        "raftstore.store-pool-size",
        "raftstore.future-poll-size",
        "raftstore.messages-per-tick",
        "raftstore.local-read-batch-size",
        "raftstore.cmd-batch-concurrent-ready-max-count",
        # coprocessor相关
        # rocksdb相关
        "rocksdb.max-background-jobs",
        "rocksdb.max-background-flushes",
        "rocksdb.max-sub-compactions",
        # "rocksdb.use-direct-io-for-flush-and-compaction",
        "rocksdb.rate-limiter-mode",
        "rocksdb.rate-limiter-auto-tuned",
        # rocksdb.defaultcf/writecf相关
        "rocksdb.defaultcf.block-size",
        "rocksdb.writecf.block-size",
        "rocksdb.defaultcf.write-buffer-size",
        "rocksdb.writecf.write-buffer-size",
        "rocksdb.defaultcf.max-write-buffer-number",
        "rocksdb.writecf.max-write-buffer-number",
        "rocksdb.defaultcf.max-bytes-for-level-base",
        "rocksdb.writecf.max-bytes-for-level-base",
        "rocksdb.defaultcf.whole-key-filtering",
        "rocksdb.writecf.whole-key-filtering",
        "rocksdb.defaultcf.optimize-filters-for-hits",
        "rocksdb.writecf.optimize-filters-for-hits",
        "rocksdb.defaultcf.level0-file-num-compaction-trigger",
        "rocksdb.writecf.level0-file-num-compaction-trigger",
        "rocksdb.defaultcf.min-write-buffer-number-to-merge",
        "rocksdb.writecf.min-write-buffer-number-to-merge",
        "rocksdb.defaultcf.compaction-style",
        "rocksdb.writecf.compaction-style",
        "rocksdb.defaultcf.compaction-pri",
        "rocksdb.writecf.compaction-pri",
        "rocksdb.defaultcf.compaction-guard-min-output-file-size",
        "rocksdb.writecf.compaction-guard-min-output-file-size",
        "rocksdb.defaultcf.compaction-guard-max-output-file-size",
        "rocksdb.writecf.compaction-guard-max-output-file-size",
        # raftdb相关
        "raftdb.max-background-jobs",
        "raftdb.max-sub-compactions",

        ## TiDB参数
        "prepared-plan-cache.enabled",
        "prepared-plan-cache.capacity",
    }
}

DEFAULT_TUNABLE_ROLES = {
    DBMSType.TIDB: {
        'tidb',
        'pd',
        'tikv',
    }
}

# Bytes in a GB
GB = 1024 ** 3

# Default minval when set to None
MINVAL = 0

# Default maxval when set to None
MAXVAL = 192 * GB

# Percentage of total CPUs to use for maxval
CPU_PERCENT = 2.0

# Percentage of total memory to use for maxval
MEMORY_PERCENT = 0.8

# Percentage of total storage to use for maxval
STORAGE_PERCENT = 0.8

# The maximum connections to the database
SESSION_NUM = 50.0

# Tune knobs by instance or together
TUNE_TIKV_BY_INSTANCE = False


def get_default_knobs(dbtype):
    default_tunable_knobs = DEFAULT_TUNABLE_KNOBS.get(dbtype)
    if not default_tunable_knobs:
        default_tunable_knobs = set(KnobCatalog.objects.filter(
            dbms=session.dbms, tunable=True).values_list('name', flat=True))
    elif dbtype == DBMSType.TIDB:
        # TODO: 暂时写死在代码里
        if TUNE_TIKV_BY_INSTANCE:
            tikv_instances = ['192.168.0.3', '192.168.0.4', '192.168.0.5', '192.168.0.6']
        else:
            tikv_instances = ['192.168.0.3']  # 先调节192.168.0.3，然后复制其参数给剩余tikv节点
        tikv_port = 20160
        real_default_tunable_knobs = []
        for knob in default_tunable_knobs:
            for tikv_inst in tikv_instances:
                real_default_tunable_knobs.append(f'global.(tikv,{tikv_inst}:{tikv_port},{knob})')
        default_tunable_knobs = real_default_tunable_knobs

    return default_tunable_knobs


def is_default_tunable_knobs(knob_full_name, roles, instances, knobs):
    if ',' not in knob_full_name:
        knob = knob_full_name[knob_full_name.find('(') + 1:len(knob_full_name) - 1]
        ans = knob in knobs if (knobs and len(knobs) > 0) else True
    else:
        role, instance, knob = knob_full_name[knob_full_name.find('(') + 1:len(knob_full_name) - 1].split(',')
        check_role = role in roles if (roles and len(roles) > 0) else True
        check_instance = instance in instances if (instances and len(instances) > 0) else True
        check_knob = knob in knobs if (knobs and len(knobs) > 0) else True
        ans = check_role and check_instance and check_knob
    return ans


def set_default_knobs(session, cascade=True):
    dbtype = session.dbms.type
    # default_tunable_knobs = get_default_knobs(dbtype)

    # 查询数据库select type,instance from information_schema.cluster_info
    db = pymysql.connect(host=DB_HOST, port=4000)
    cursor = db.cursor()
    cursor.execute('select type,instance from information_schema.cluster_info order by instance;')
    data = cursor.fetchall()
    cursor.close()
    db.close()
    # 得到各组件的ip地址
    ip_dict = {'tidb': [], 'pd': [], 'tikv': []}
    for d in data:
        if d[0] in ip_dict.keys():
            ip_dict[d[0]].append(d[1])

    if not TUNE_TIKV_BY_INSTANCE:
        instances = {ip_dict['tidb'][0], ip_dict['pd'][0], ip_dict['tikv'][0]}
    else:
        instances = set(ip_dict['tidb']) | set(ip_dict['pd']) | set(ip_dict['tikv'])

    for knob in KnobCatalog.objects.filter(dbms=session.dbms):
        # tunable = knob.name in default_tunable_knobs
        tunable = is_default_tunable_knobs(knob.name, DEFAULT_TUNABLE_ROLES.get(dbtype),
                                           instances,
                                           DEFAULT_TUNABLE_KNOBS.get(dbtype))
        minval = knob.minval

        # set session knob tunable in knob catalog
        if tunable and cascade:
            knob.tunable = True
            knob.save()

        if knob.vartype is VarType.ENUM:
            enumvals = knob.enumvals.split(',')
            minval = 0
            maxval = len(enumvals) - 1
        elif knob.vartype is VarType.BOOL:
            minval = 0
            maxval = 1
        elif knob.vartype in (VarType.INTEGER, VarType.REAL):
            vtype = int if knob.vartype == VarType.INTEGER else float

            minval = vtype(minval) if minval is not None else MINVAL
            knob_maxval = vtype(knob.maxval) if knob.maxval is not None else MAXVAL

            if knob.resource == KnobResourceType.CPU:
                maxval = session.hardware.cpu * CPU_PERCENT
            elif knob.resource == KnobResourceType.MEMORY:
                minval = session.hardware.memory * minval if session.hardware.memory > 32 else minval
                maxval = session.hardware.memory * GB * MEMORY_PERCENT
            elif knob.resource == KnobResourceType.STORAGE:
                minval = session.hardware.storage * minval if session.hardware.storage > 64 else minval
                maxval = session.hardware.storage * GB * STORAGE_PERCENT
            else:
                maxval = knob_maxval

            # Special cases
            if dbtype == DBMSType.POSTGRES:
                if knob.name in ('global.work_mem', 'global.temp_buffers'):
                    maxval /= SESSION_NUM

            if dbtype == DBMSType.MYSQL:
                if knob.name == 'global.innodb_log_file_size':
                    maxval /= 2

            if maxval > knob_maxval:
                maxval = knob_maxval

            if maxval < minval:
                LOG.warning(("Invalid range for session knob '%s': maxval <= minval "
                             "(minval: %s, maxval: %s). Setting maxval to the vendor setting: %s."),
                            knob.name, minval, maxval, knob_maxval)
                maxval = knob_maxval

            maxval = vtype(maxval)

        else:
            assert knob.resource == KnobResourceType.OTHER
            maxval = knob.maxval

        SessionKnob.objects.create(session=session,
                                   knob=knob,
                                   minval=minval,
                                   maxval=maxval,
                                   tunable=tunable)
