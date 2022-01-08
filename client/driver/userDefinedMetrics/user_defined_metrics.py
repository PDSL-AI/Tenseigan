import json
import sys
import copy
import argparse
import os

sys.path.append("../../../")
sys.path.append("../")
from server.website.website.types import \
    VarType  # pylint: disable=import-error,wrong-import-position,line-too-long  # noqa: E402
from driver_config import OLTPBENCH_HOME, \
    BENCHBASE_HOME  # pylint: disable=import-error,wrong-import-position  # noqa: E402
from driver_config import BENCH_TOOL, TIUP_BENCH_RUN_LOG, SYSBENCH_LOG

parser = argparse.ArgumentParser()  # pylint: disable=invalid-name
parser.add_argument("result_dir")
args = parser.parse_args()  # pylint: disable=invalid-name

if BENCH_TOOL == 'oltpbench':
    USER_DEINFED_METRICS = {
        "throughput": {
            "unit": "transaction / second",
            "short_unit": "txn/s",
            "type": VarType.INTEGER
        },
        "latency_99": {
            "unit": "milliseconds",
            "short_unit": "ms",
            "type": VarType.INTEGER
        },
        "latency_95": {
            "unit": "milliseconds",
            "short_unit": "ms",
            "type": VarType.INTEGER
        }
    }
elif BENCH_TOOL == 'tiup_bench':
    USER_DEINFED_METRICS = {
        "tpmC": {
            "unit": "transaction / minute",
            "short_unit": "txn/min",
            "type": VarType.INTEGER
        },
        "efficiency": {
            "unit": "percent",
            "short_unit": "%",
            "type": VarType.INTEGER
        }
    }
elif BENCH_TOOL == 'benchbase':
    USER_DEINFED_METRICS = {
        "throughput": {
            "unit": "transaction / second",
            "short_unit": "txn/s",
            "type": VarType.INTEGER
        },
        "latency_99": {
            "unit": "microseconds",
            "short_unit": "us",
            "type": VarType.INTEGER
        },
        "latency_95": {
            "unit": "microseconds",
            "short_unit": "us",
            "type": VarType.INTEGER
        }
    }
elif BENCH_TOOL == 'sysbench':
    USER_DEINFED_METRICS = {
        "sysbench_qps": {
            "unit": "transaction / second",
            "short_unit": "txn/s",
            "type": VarType.INTEGER
        },
        "latency_avg": {
            "unit": "milliseconds",
            "short_unit": "ms",
            "type": VarType.INTEGER
        },
        "latency_95": {
            "unit": "milliseconds",
            "short_unit": "ms",
            "type": VarType.INTEGER
        }
    }
else:
    USER_DEINFED_METRICS = {}


def is_real_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def get_udm():
    metrics = copy.deepcopy(USER_DEINFED_METRICS)
    if BENCH_TOOL == 'oltpbench':
        summary_path = OLTPBENCH_HOME + '/results/outputfile.summary'
        with open(summary_path, 'r') as f:
            info = json.load(f)
        metrics["throughput"]["value"] = info["Throughput (requests/second)"]
        metrics["latency_99"]["value"] = \
            info["Latency Distribution"]["99th Percentile Latency (milliseconds)"]
        metrics["latency_95"]["value"] = \
            info["Latency Distribution"]["95th Percentile Latency (milliseconds)"]
    elif BENCH_TOOL == 'tiup_bench':
        summary_path = TIUP_BENCH_RUN_LOG
        with open(summary_path, 'r') as f:
            content = f.readlines()
            if 'tpmC' in content[-1] and 'efficiency' in content[-1]:
                tpm, efficiency = [x.split(':')[-1].strip().strip('%') for x in content[-1].split(',')]
            else:
                tpm = efficiency = ''
        metrics["tpmC"]["value"] = float(tpm) if is_real_number(tpm) else 0
        metrics["efficiency"]["value"] = float(efficiency) if is_real_number(efficiency) else 0
    elif BENCH_TOOL == 'benchbase':
        res_dir = os.path.join(BENCHBASE_HOME, 'results')
        summary_path = os.path.join(res_dir,
                                    sorted([s for s in os.listdir(res_dir) if 'summary' in s], reverse=True)[0])
        with open(summary_path, 'r') as f:
            info = json.load(f)
        metrics["throughput"]["value"] = info["Throughput (requests/second)"]
        metrics["latency_99"]["value"] = \
            info["Latency Distribution"]["99th Percentile Latency (microseconds)"]
        metrics["latency_95"]["value"] = \
            info["Latency Distribution"]["95th Percentile Latency (microseconds)"]
    elif BENCH_TOOL == 'sysbench':
        summary_path = SYSBENCH_LOG
        with open(summary_path, 'r') as f:
            content = f.readlines()
            qps = [float(x.split('(')[1].split()[0]) for x in content if 'queries:' in x][0]
            lat_avg = [float(x.split()[1]) for x in content if 'avg:' in x][0]
            lat_95 = [float(x.split()[2]) for x in content if '95th percentile:' in x][0]
        metrics["sysbench_qps"]["value"] = qps
        metrics["latency_avg"]["value"] = lat_avg
        metrics["latency_95"]["value"] = lat_95

    return metrics


def write_udm():
    metrics = get_udm()
    result_dir = args.result_dir
    path = os.path.join(result_dir, 'user_defined_metrics.json')
    with open(path, 'w') as f:
        json.dump(metrics, f, indent=4)


if __name__ == "__main__":
    write_udm()
