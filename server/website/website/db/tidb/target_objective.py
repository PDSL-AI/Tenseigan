#
# Created on July 19, 2021
#
# @author: Yiqin Xiong, Chen Ding
#

import logging

from website.types import DBMSType
from ..base.target_objective import (BaseTargetObjective, BaseUserDefinedTarget, LESS_IS_BETTER, MORE_IS_BETTER)

LOG = logging.getLogger(__name__)


class TiDBQPS(BaseTargetObjective):
    def __init__(self):
        super(TiDBQPS, self).__init__(name='tidb_qps', pprint='TiDB QPS', unit='queries / second',
                                      short_unit='qps', improvement=MORE_IS_BETTER)

    def compute(self, metrics, observation_time):
        return sum([float(metrics[x]) for x in metrics.keys() if 'tidb_qps' in x])


class TiDBQueryDuration99(BaseTargetObjective):
    def __init__(self):
        super(TiDBQueryDuration99, self).__init__(name='tidb_query_duration_99', pprint='TiDB Query Duration 99',
                                                  unit='millisecond', short_unit='ms', improvement=LESS_IS_BETTER)

    def compute(self, metrics, observation_time):
        for metrics_name in metrics.keys():
            if 'tidb_query_duration' in metrics_name:
                return float(metrics[metrics_name] * 1000)


class TiKVThreadCPU(BaseTargetObjective):
    def __init__(self):
        super(TiKVThreadCPU, self).__init__(name='tikv_thread_cpu', pprint='TiKV Thread CPU',
                                            unit='percent', short_unit='%', improvement=LESS_IS_BETTER)

    def compute(self, metrics, observation_time):
        tikv_thread_cpu_value = []
        for metrics_name in metrics.keys():
            if 'tikv_thread_cpu' in metrics_name and '20180' in metrics_name:
                tikv_thread_cpu_value.append(metrics[metrics_name])
        return float(sum(tikv_thread_cpu_value) * 100)


class TiKVMemory(BaseTargetObjective):
    def __init__(self):
        super(TiKVMemory, self).__init__(name='tikv_memory', pprint='TiKV Memory',
                                         unit='Million Bytes', short_unit='MB', improvement=LESS_IS_BETTER)

    def compute(self, metrics, observation_time):
        tikv_memory_value = []
        for metrics_name in metrics.keys():
            if 'tikv_memory' in metrics_name and '20180' in metrics_name:
                tikv_memory_value.append(metrics[metrics_name])
        return float(sum(tikv_memory_value) / (1024 * 1024))


class NodeDiskIOUtil(BaseTargetObjective):
    def __init__(self):
        super(NodeDiskIOUtil, self).__init__(name='node_disk_io_util', pprint='Node Disk IO Util',
                                             unit='percent', short_unit='%', improvement=LESS_IS_BETTER)

    def compute(self, metrics, observation_time):
        node_disk_io_util_value = []
        for metrics_name in metrics.keys():
            if 'node_disk_io_util' in metrics_name and '9100' in metrics_name:
                node_disk_io_util_value.append(metrics[metrics_name])
        return float(sum(node_disk_io_util_value) * 100)


class TiKVGrpcMessageTotalCount(BaseTargetObjective):
    def __init__(self):
        super(TiKVGrpcMessageTotalCount, self).__init__(name='tikv_grpc_message_total_count',
                                                        pprint='TiKV Grpc Message Total Count',
                                                        unit='operations / seconds', short_unit='ops',
                                                        improvement=MORE_IS_BETTER)

    def compute(self, metrics, observation_time):
        tikv_grpc_message_total_count_value = []
        for metrics_name in metrics.keys():
            if 'tikv_grpc_message_total_count' in metrics_name and '20180' in metrics_name:
                tikv_grpc_message_total_count_value.append(metrics[metrics_name])
        return float(sum(tikv_grpc_message_total_count_value) / 60)


class TiKVGrpcMessageDuration99(BaseTargetObjective):
    def __init__(self):
        super(TiKVGrpcMessageDuration99, self).__init__(name='tikv_grpc_message_duration_99',
                                                        pprint='TiKV Grpc Message Duration 99',
                                                        unit='millisecond', short_unit='ms', improvement=LESS_IS_BETTER)

    def compute(self, metrics, observation_time):
        tikv_grpc_message_duration_value = []
        for metrics_name in metrics.keys():
            if 'tikv_grpc_message_duration' in metrics_name and '20180' in metrics_name:
                tikv_grpc_message_duration_value.append(metrics[metrics_name])
        return float(max(tikv_grpc_message_duration_value) * 100)


target_objective_list = tuple((DBMSType.TIDB, target_obj) for target_obj in [  # pylint: disable=invalid-name
    TiDBQPS(),
    TiDBQueryDuration99(),
    TiKVThreadCPU(),
    TiKVMemory(),
    NodeDiskIOUtil(),
    TiKVGrpcMessageTotalCount(),
    TiKVGrpcMessageDuration99(),
    BaseUserDefinedTarget(target_name='tpcc_tpmC', improvement=MORE_IS_BETTER, unit='transaction / minute',
                          short_unit='txn/min'),
    BaseUserDefinedTarget(target_name='tpcc_efficiency', improvement=MORE_IS_BETTER, unit='percent', short_unit='%'),
    BaseUserDefinedTarget(target_name='latency_99', improvement=LESS_IS_BETTER,
                          unit='microseconds', short_unit='us'),
    BaseUserDefinedTarget(target_name='throughput', improvement=MORE_IS_BETTER,
                          unit='transactions / seconds', short_unit='txn/s'),
    BaseUserDefinedTarget(target_name='sysbench_qps', improvement=MORE_IS_BETTER,
                          unit='transaction / second', short_unit='txn/s'),
    BaseUserDefinedTarget(target_name='latency_avg', improvement=LESS_IS_BETTER,
                          unit='milliseconds', short_unit='ms'),
    BaseUserDefinedTarget(target_name='latency_95', improvement=LESS_IS_BETTER,
                          unit='milliseconds', short_unit='ms'),
])
