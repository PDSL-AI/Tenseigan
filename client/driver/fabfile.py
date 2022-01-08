#
# OtterTune - fabfile.py
#
# Copyright (c) 2017-18, Carnegie Mellon University Database Group
#
'''
Created on Mar 23, 2018

@author: bohan
'''
import glob
import json
import os
import re
import time
import yaml
from collections import OrderedDict
from multiprocessing import Process

import logging
from logging.handlers import RotatingFileHandler

import requests
from fabric.api import env, lcd, local, settings, show, task, hide
from fabric.state import output as fabric_output

from utils import (file_exists, get, get_content, load_driver_conf, parse_bool,
                   put, run, run_sql_script, sudo, FabricException, file_exists_local)

# Loads the driver config file (defaults to driver_config.py)
dconf = load_driver_conf()  # pylint: disable=invalid-name

# Fabric settings
fabric_output.update({
    'running': True,
    'stdout': True,
})
env.abort_exception = FabricException
env.hosts = [dconf.LOGIN]
# xyq add
env.warn_only = True
# if password is empty, use ssh-key instead
if dconf.LOGIN_KEY:
    env.key_filename = dconf.LOGIN_KEY
else:
    env.password = dconf.LOGIN_PASSWORD
# xyq add

# Create local directories
for _d in (dconf.RESULT_DIR, dconf.LOG_DIR, dconf.TEMP_DIR):
    os.makedirs(_d, exist_ok=True)

# Configure logging
LOG = logging.getLogger(__name__)
LOG.setLevel(getattr(logging, dconf.LOG_LEVEL, logging.DEBUG))
Formatter = logging.Formatter(  # pylint: disable=invalid-name
    fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S')
ConsoleHandler = logging.StreamHandler()  # pylint: disable=invalid-name
ConsoleHandler.setFormatter(Formatter)
LOG.addHandler(ConsoleHandler)
FileHandler = RotatingFileHandler(  # pylint: disable=invalid-name
    dconf.DRIVER_LOG, maxBytes=50000, backupCount=2)
FileHandler.setFormatter(Formatter)
LOG.addHandler(FileHandler)


@task
def check_disk_usage():
    partition = dconf.DATABASE_DISK
    disk_use = 0
    if partition:
        cmd = "df -h {}".format(partition)
        out = run(cmd).splitlines()[1]
        m = re.search(r'\d+(?=%)', out)
        if m:
            disk_use = int(m.group(0))
        LOG.info("Current Disk Usage: %s%s", disk_use, '%')
    return disk_use


@task
def check_memory_usage():
    run('free -m -h')


@task
def create_controller_config():
    if dconf.DB_TYPE == 'postgres':
        dburl_fmt = 'jdbc:postgresql://{host}:{port}/{db}'.format
    elif dconf.DB_TYPE == 'oracle':
        dburl_fmt = 'jdbc:oracle:thin:@{host}:{port}:{db}'.format
    elif dconf.DB_TYPE == 'tidb':
        dburl_fmt = 'jdbc:mysql://{host}:{port}/{db}?useSSL=false'.format
    elif dconf.DB_TYPE == 'mysql':
        if dconf.DB_VERSION in ['5.6', '5.7']:
            dburl_fmt = 'jdbc:mysql://{host}:{port}/{db}?useSSL=false'.format
        elif dconf.DB_VERSION == '8.0':
            dburl_fmt = ('jdbc:mysql://{host}:{port}/{db}?'
                         'allowPublicKeyRetrieval=true&useSSL=false').format
        else:
            raise Exception("MySQL Database Version {} Not Implemented !".format(dconf.DB_VERSION))
    else:
        raise Exception("Database Type {} Not Implemented !".format(dconf.DB_TYPE))

    bench_type = ''

    if dconf.BENCH_TOOL == 'oltpbench':
        bench_type = dconf.OLTPBENCH_BENCH
    elif dconf.BENCH_TOOL == 'tiup_bench':
        bench_type = 'tpcc'
    elif dconf.BENCH_TOOL == 'benchbase':
        bench_type = dconf.BENCHBASE_BENCH
    elif dconf.BENCH_TOOL == 'sysbench':
        bench_type = dconf.SYSBENCH_BENCH

    config = dict(
        database_type=dconf.DB_TYPE,
        database_url=dburl_fmt(host=dconf.DB_HOST, port=dconf.DB_PORT, db=dconf.DB_NAME),
        username=dconf.DB_USER,
        password=dconf.DB_PASSWORD,
        upload_code='DEPRECATED',
        upload_url='DEPRECATED',
        workload_name=bench_type
    )

    with open(dconf.CONTROLLER_CONFIG, 'w') as f:
        json.dump(config, f, indent=2)


@task
def restart_database(check_if_need_to_restore):
    LOG.info('Begin to restart database...')
    if dconf.DB_TYPE == 'postgres':
        if dconf.HOST_CONN == 'docker':
            # Restarting the docker container here is the cleanest way to do it
            # becaues there's no init system running and the only process running
            # in the container is postgres itself
            local('docker restart {}'.format(dconf.CONTAINER_NAME))
        elif dconf.HOST_CONN == 'remote_docker':
            run('docker restart {}'.format(dconf.CONTAINER_NAME), remote_only=True)
        else:
            sudo('pg_ctl -D {} -w -t 600 restart -m fast'.format(
                dconf.PG_DATADIR), user=dconf.ADMIN_USER, capture=False)
    elif dconf.DB_TYPE == 'tidb':
        if dconf.HOST_CONN == 'docker':
            local('docker restart {}'.format(dconf.CONTAINER_NAME))
        elif dconf.HOST_CONN == 'remote_docker':
            run('docker restart {}'.format(dconf.CONTAINER_NAME), remote_only=True)
        else:
            if check_if_need_to_restore:
                run_sql_script('reload_tidb_cnf_ignore_check_skip_restart.sh', dconf.TIDB_CLUSTER_NAME)
            else:
                run_sql_script('reload_tidb_cnf_ignore_check.sh', dconf.TIDB_CLUSTER_NAME)
    elif dconf.DB_TYPE == 'mysql':
        if dconf.HOST_CONN == 'docker':
            local('docker restart {}'.format(dconf.CONTAINER_NAME))
        elif dconf.HOST_CONN == 'remote_docker':
            run('docker restart {}'.format(dconf.CONTAINER_NAME), remote_only=True)
        else:
            sudo('service mysql restart')
    elif dconf.DB_TYPE == 'oracle':
        db_log_path = os.path.join(os.path.split(dconf.DB_CONF)[0], 'startup.log')
        local_log_path = os.path.join(dconf.LOG_DIR, 'startup.log')
        local_logs_path = os.path.join(dconf.LOG_DIR, 'startups.log')
        run_sql_script('restartOracle.sh', db_log_path)
        get(db_log_path, local_log_path)
        with open(local_log_path, 'r') as fin, open(local_logs_path, 'a') as fout:
            lines = fin.readlines()
            for line in lines:
                if line.startswith('ORACLE instance started.'):
                    return True
                if not line.startswith('SQL>'):
                    fout.write(line)
            fout.write('\n')
        return False
    else:
        raise Exception("Database Type {} Not Implemented !".format(dconf.DB_TYPE))
    return True


@task
def drop_database():
    if dconf.DB_TYPE == 'postgres':
        run("PGPASSWORD={} dropdb -e --if-exists {} -U {} -h {}".format(
            dconf.DB_PASSWORD, dconf.DB_NAME, dconf.DB_USER, dconf.DB_HOST))
    elif dconf.DB_TYPE == 'tidb':
        # local(
        #     "mysql --user={} --password={} -h {} -P {} -e 'drop database if exists {}'".format(dconf.DB_USER,
        #                                                                                        dconf.DB_PASSWORD,
        #                                                                                        dconf.DB_HOST,
        #                                                                                        dconf.DB_PORT,
        #                                                                                        dconf.DB_NAME))
        # local("tiup cluster clean {} --all --ignore-role prometheus --yes".format(dconf.TIDB_CLUSTER_NAME))
        # local("tiup cluster start {}".format(dconf.TIDB_CLUSTER_NAME))
        run_sql_script('clean_tidb_data.sh', dconf.TIDB_CLUSTER_NAME)
        run_sql_script('start_cluster.sh', dconf.TIDB_CLUSTER_NAME)
    elif dconf.DB_TYPE == 'mysql':
        run(
            "mysql --user={} --password={} -e 'drop database if exists {}'".format(dconf.DB_USER, dconf.DB_PASSWORD,
                                                                                   dconf.DB_NAME))
    else:
        raise Exception("Database Type {} Not Implemented !".format(dconf.DB_TYPE))


@task
def create_database():
    if dconf.DB_TYPE == 'postgres':
        run("PGPASSWORD={} createdb -e {} -U {} -h {}".format(
            dconf.DB_PASSWORD, dconf.DB_NAME, dconf.DB_USER, dconf.DB_HOST))
    elif dconf.DB_TYPE == 'tidb':
        local("mysql -u {} -p {} -h {} -P {} -e 'create database {}'".format(
            dconf.DB_USER, dconf.DB_PASSWORD, dconf.DB_HOST, dconf.DB_PORT, dconf.DB_NAME))
    elif dconf.DB_TYPE == 'mysql':
        run("mysql --user={} --password={} -e 'create database {}'".format(
            dconf.DB_USER, dconf.DB_PASSWORD, dconf.DB_NAME))
    else:
        raise Exception("Database Type {} Not Implemented !".format(dconf.DB_TYPE))


@task
def create_user():
    if dconf.DB_TYPE == 'postgres':
        sql = "CREATE USER {} SUPERUSER PASSWORD '{}';".format(dconf.DB_USER, dconf.DB_PASSWORD)
        run("PGPASSWORD={} psql -c \\\"{}\\\" -U postgres -h {}".format(
            dconf.DB_PASSWORD, sql, dconf.DB_HOST))
    elif dconf.DB_TYPE == 'oracle':
        run_sql_script('createUser.sh', dconf.DB_USER, dconf.DB_PASSWORD)
    else:
        raise Exception("Database Type {} Not Implemented !".format(dconf.DB_TYPE))


@task
def drop_user():
    if dconf.DB_TYPE == 'postgres':
        sql = "DROP USER IF EXISTS {};".format(dconf.DB_USER)
        run("PGPASSWORD={} psql -c \\\"{}\\\" -U postgres -h {}".format(
            dconf.DB_PASSWORD, sql, dconf.DB_HOST))
    elif dconf.DB_TYPE == 'oracle':
        run_sql_script('dropUser.sh', dconf.DB_USER)
    else:
        raise Exception("Database Type {} Not Implemented !".format(dconf.DB_TYPE))


@task
def reset_conf(always=True):
    if always:
        change_conf()
        return

    if dconf.DB_TYPE == 'tidb':
        return
    else:
        # reset the config only if it has not been changed by Ottertune,
        # i.e. OtterTune signal line is not in the config file.
        signal = "# configurations recommended by ottertune:\n"
        tmp_conf_in = os.path.join(dconf.TEMP_DIR, os.path.basename(dconf.DB_CONF) + '.in')
        get(dconf.DB_CONF, tmp_conf_in)
        with open(tmp_conf_in, 'r') as f:
            lines = f.readlines()
        if signal not in lines:
            change_conf()


@task
def change_conf(next_conf=None):
    signal = "# configurations recommended by ottertune:\n"
    next_conf = next_conf or {}

    # Tune knobs by instance or together
    TUNE_TIKV_BY_INSTANCE = False

    if dconf.DB_TYPE == 'tidb':
        if isinstance(next_conf, str):
            with open(next_conf, 'r') as f:
                recommendation = json.load(
                    f, encoding="UTF-8", object_pairs_hook=OrderedDict)['recommendation']
        else:
            recommendation = next_conf

        assert isinstance(recommendation, dict)

        # 1. 修改config类参数
        local('cp {0} {0}.ottertune.bak'.format(dconf.DB_CONF))

        if dconf.BASE_DB_CONF:
            assert isinstance(dconf.BASE_DB_CONF, dict)
            recommendation.update(dconf.BASE_DB_CONF)

        with open(dconf.DB_CONF, 'r') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            # preprocess
            for key, value in recommendation.items():
                if ',' not in key:
                    continue
                role, instance, name = key[1:len(key) - 1].split(',')
                servers = data[role + '_servers']
                for s in servers:
                    if 'config' not in s.keys():
                        s['config'] = {}
                    else:
                        s['config'].clear()
            # write conf
            for key, value in recommendation.items():
                if ',' not in key:
                    continue
                role, instance, name = key[1:len(key) - 1].split(',')
                servers = data[role + '_servers']
                for s in servers:
                    if not TUNE_TIKV_BY_INSTANCE or instance == str(s['host']) + ':' + str(s['port']):

                        if type(value) == str:
                            if str.isdigit(value):
                                value = int(value)
                            elif value in ('true', 'false'):
                                value = True if value == 'true' else False

                        s['config'][name] = value

        with open(dconf.DB_CONF, 'w') as f:
            yaml.dump(data, f)

        run_sql_script('change_tidb_cnf.sh', dconf.TIDB_CLUSTER_NAME, dconf.DB_CONF)

        # 2. 修改global variables类参数
        for key, value in recommendation.items():
            if ',' in key:
                continue
            name = key[1:len(key) - 1]
            if type(value) == str:
                if str.isdigit(value):
                    value = int(value)
                elif value in ('true', 'false'):
                    value = True if value == 'true' else False
            local(
                "mysql --user={} --password={} -h {} -P {} -e 'set @@global.{}={};'".format(
                    'root',
                    '',
                    dconf.DB_HOST,
                    '4000',
                    name,
                    value))
            LOG.info(f'global.{name} changed to {value}.')

    else:
        tmp_conf_in = os.path.join(dconf.TEMP_DIR, os.path.basename(dconf.DB_CONF) + '.in')
        get(dconf.DB_CONF, tmp_conf_in)
        with open(tmp_conf_in, 'r') as f:
            lines = f.readlines()

        if signal not in lines:
            lines += ['\n', signal]

        signal_idx = lines.index(signal)
        lines = lines[0:signal_idx + 1]

        if dconf.DB_TYPE == 'mysql':
            lines.append('[mysqld]\n')

        if dconf.BASE_DB_CONF:
            assert isinstance(dconf.BASE_DB_CONF, dict), \
                (type(dconf.BASE_DB_CONF), dconf.BASE_DB_CONF)
            for name, value in sorted(dconf.BASE_DB_CONF.items()):
                if value is None:
                    lines.append('{}\n'.format(name))
                else:
                    lines.append('{} = {}\n'.format(name, value))

        if isinstance(next_conf, str):
            with open(next_conf, 'r') as f:
                recommendation = json.load(
                    f, encoding="UTF-8", object_pairs_hook=OrderedDict)['recommendation']
        else:
            recommendation = next_conf

        assert isinstance(recommendation, dict)

        for name, value in recommendation.items():
            if dconf.DB_TYPE == 'oracle' and isinstance(value, str):
                value = value.strip('B')
            # If innodb_flush_method is set to NULL on a Unix-like system,
            # the fsync option is used by default.
            if name == 'innodb_flush_method' and value == '':
                value = "fsync"
            lines.append('{} = {}\n'.format(name, value))
        lines.append('\n')

        tmp_conf_out = os.path.join(dconf.TEMP_DIR, os.path.basename(dconf.DB_CONF) + '.out')
        with open(tmp_conf_out, 'w') as f:
            f.write(''.join(lines))

        sudo('cp {0} {0}.ottertune.bak'.format(dconf.DB_CONF), remote_only=True)
        put(tmp_conf_out, dconf.DB_CONF, use_sudo=True)
        local('rm -f {} {}'.format(tmp_conf_in, tmp_conf_out))


@task
def load_oltpbench():
    if os.path.exists(dconf.OLTPBENCH_CONFIG) is False:
        msg = 'oltpbench config {} does not exist, '.format(dconf.OLTPBENCH_CONFIG)
        msg += 'please double check the option in driver_config.py'
        raise Exception(msg)
    set_oltpbench_config()
    cmd = "./oltpbenchmark -b {} -c {} --create=true --load=true". \
        format(dconf.OLTPBENCH_BENCH, dconf.OLTPBENCH_CONFIG)
    with lcd(dconf.OLTPBENCH_HOME):  # pylint: disable=not-context-manager
        local(cmd)


@task
def load_tiup_bench():
    cmd = "tiup bench tpcc -H {} -P {} -D {} --warehouses {} --threads {} prepare > {} 2>&1 &".format(
        dconf.DB_HOST, dconf.DB_PORT, dconf.DB_NAME, dconf.TIUP_BENCH_WAREHOUSE, dconf.TIUP_BENCH_THREADS,
        dconf.TIUP_BENCH_LOAD_LOG)
    local(cmd)


@task
def load_benchbase():
    if os.path.exists(dconf.BENCHBASE_CONFIG) is False:
        msg = 'benchbase config {} does not exist, '.format(dconf.BENCHBASE_CONFIG)
        msg += 'please double check the option in driver_config.py'
        raise Exception(msg)
    # set oltpbench config, including db username, password, url
    set_benchbase_config()
    cmd = "java -jar benchbase.jar -b {} -c {} --create=true --load=true". \
        format(dconf.BENCHBASE_BENCH, dconf.BENCHBASE_CONFIG)
    with lcd(dconf.BENCHBASE_HOME):  # pylint: disable=not-context-manager
        local(cmd)


@task
def run_oltpbench():
    if os.path.exists(dconf.OLTPBENCH_CONFIG) is False:
        msg = 'oltpbench config {} does not exist, '.format(dconf.OLTPBENCH_CONFIG)
        msg += 'please double check the option in driver_config.py'
        raise Exception(msg)
    set_oltpbench_config()
    cmd = "./oltpbenchmark -b {} -c {} --execute=true -s 5 -o outputfile". \
        format(dconf.OLTPBENCH_BENCH, dconf.OLTPBENCH_CONFIG)
    with lcd(dconf.OLTPBENCH_HOME):  # pylint: disable=not-context-manager
        local(cmd)


@task
def run_bench_tool_bg():
    if dconf.BENCH_TOOL == 'oltpbench':
        if os.path.exists(dconf.OLTPBENCH_CONFIG) is False:
            msg = 'oltpbench config {} does not exist, '.format(dconf.OLTPBENCH_CONFIG)
            msg += 'please double check the option in driver_config.py'
            raise Exception(msg)
        # set oltpbench config, including db username, password, url
        set_oltpbench_config()
        cmd = "./oltpbenchmark -b {} -c {} --execute=true -s 5 -o outputfile > {} 2>&1 &". \
            format(dconf.OLTPBENCH_BENCH, dconf.OLTPBENCH_CONFIG, dconf.OLTPBENCH_LOG)
        with lcd(dconf.OLTPBENCH_HOME):  # pylint: disable=not-context-manager
            local(cmd)
    elif dconf.BENCH_TOOL == 'tiup_bench':
        # cmd = "tiup bench tpcc -H {} -P {} -D {} --warehouses {} --threads {} --time {} run > {} 2>&1 &".format(
        #     dconf.DB_HOST, dconf.DB_PORT, dconf.DB_NAME, dconf.TIUP_BENCH_WAREHOUSE, dconf.TIUP_BENCH_THREADS,
        #     dconf.TIUP_BENCH_TIME, dconf.TIUP_BENCH_RUN_LOG)
        # local(cmd)
        run_sql_script('tiup_bench_run.sh', dconf.DB_HOST, dconf.DB_PORT, dconf.DB_NAME, dconf.TIUP_BENCH_WAREHOUSE,
                       dconf.TIUP_BENCH_THREADS,
                       dconf.TIUP_BENCH_TIME)
    elif dconf.BENCH_TOOL == 'benchbase':
        if os.path.exists(dconf.BENCHBASE_CONFIG) is False:
            msg = 'benchbase config {} does not exist, '.format(dconf.BENCHBASE_CONFIG)
            msg += 'please double check the option in driver_config.py'
            raise Exception(msg)
        # set oltpbench config, including db username, password, url
        set_benchbase_config()
        cmd = "java -jar benchbase.jar -b {} -c {} --execute=true -s 5 > {} 2>&1 &". \
            format(dconf.BENCHBASE_BENCH, dconf.BENCHBASE_CONFIG, dconf.BENCHBASE_LOG)
        with lcd(dconf.BENCHBASE_HOME):  # pylint: disable=not-context-manager
            local(cmd)
    elif dconf.BENCH_TOOL == 'sysbench':
        cmd = f"sysbench --config-file={dconf.SYSBENCH_CONFIG} {dconf.SYSBENCH_BENCH} " \
              f"--tables=32 --table-size=10000000 {dconf.SYSBENCH_PARAM} run > {dconf.SYSBENCH_LOG} 2>&1 &"
        LOG.info(f'Run test:\n {cmd}.')
        local(cmd)


@task
def run_controller(interval_sec=-1):
    LOG.info('Controller config path: %s', dconf.CONTROLLER_CONFIG)
    create_controller_config()
    cmd = 'gradle run -PappArgs="-c {} -t {} -d output/" --no-daemon > {}'. \
        format(dconf.CONTROLLER_CONFIG, interval_sec, dconf.CONTROLLER_LOG)
    with lcd(dconf.CONTROLLER_HOME):  # pylint: disable=not-context-manager
        local(cmd)


@task
def signal_controller():
    pidfile = os.path.join(dconf.CONTROLLER_HOME, 'pid.txt')
    with open(pidfile, 'r') as f:
        pid = int(f.read())
    cmd = 'kill -2 {}'.format(pid)
    with lcd(dconf.CONTROLLER_HOME):  # pylint: disable=not-context-manager
        local(cmd)


@task
def save_dbms_result():
    t = int(time.time())
    files = ['knobs.json', 'metrics_after.json', 'metrics_before.json', 'summary.json']
    if dconf.ENABLE_UDM:
        files.append('user_defined_metrics.json')
    for f_ in files:
        srcfile = os.path.join(dconf.CONTROLLER_HOME, 'output', f_)
        t_str = time.strftime("%m%d-%H%M", time.localtime(t))
        dstfile = os.path.join(dconf.RESULT_DIR, '{}__{}'.format(t_str, f_))
        local('cp {} {}'.format(srcfile, dstfile))
    return t


@task
def save_next_config(next_config, t=None):
    if not t:
        t = int(time.time())
    t_str = time.strftime("%m%d-%H%M", time.localtime(t))
    with open(os.path.join(dconf.RESULT_DIR, '{}__next_config.json'.format(t_str)), 'w') as f:
        json.dump(next_config, f, indent=2)
    return t


@task
def free_cache():
    if dconf.HOST_CONN not in ['docker', 'remote_docker']:
        with show('everything'), settings(warn_only=True):  # pylint: disable=not-context-manager
            res = sudo("sh -c \"echo 3 > /proc/sys/vm/drop_caches\"")
            if res.failed:
                LOG.error('%s (return code %s)', res.stderr.strip(), res.return_code)
    else:
        res = sudo("sh -c \"echo 3 > /proc/sys/vm/drop_caches\"", remote_only=True)
        # TODO: 如何清除tidb的缓存？（按上述方式只清除了tidb-pd-1节点的缓存）


@task
def upload_result(result_dir=None, prefix=None, upload_code=None):
    result_dir = result_dir or os.path.join(dconf.CONTROLLER_HOME, 'output')
    prefix = prefix or ''
    upload_code = upload_code or dconf.UPLOAD_CODE
    files = {}
    bases = ['summary', 'knobs', 'metrics_before', 'metrics_after']
    if dconf.ENABLE_UDM:
        bases.append('user_defined_metrics')
    for base in bases:
        fpath = os.path.join(result_dir, prefix + base + '.json')

        # Replaces the true db version with the specified version to allow for
        # testing versions not officially supported by OtterTune
        if base == 'summary' and dconf.OVERRIDE_DB_VERSION:
            with open(fpath, 'r') as f:
                summary = json.load(f)
            summary['real_database_version'] = summary['database_version']
            summary['database_version'] = dconf.OVERRIDE_DB_VERSION
            with open(fpath, 'w') as f:
                json.dump(summary, f, indent=1)

        files[base] = open(fpath, 'rb')

    response = requests.post(dconf.WEBSITE_URL + '/new_result/', files=files,
                             data={'upload_code': upload_code})
    if response.status_code != 200:
        raise Exception('Error uploading result.\nStatus: {}\nMessage: {}\n'.format(
            response.status_code, get_content(response)))

    for f in files.values():  # pylint: disable=not-an-iterable
        f.close()

    LOG.info(get_content(response))

    return response


@task
def get_result(max_time_sec=180, interval_sec=5, upload_code=None):
    max_time_sec = int(max_time_sec)
    interval_sec = int(interval_sec)
    upload_code = upload_code or dconf.UPLOAD_CODE
    url = dconf.WEBSITE_URL + '/query_and_get/' + upload_code
    elapsed = 0
    response_dict = None
    rout = ''

    while elapsed <= max_time_sec:
        rsp = requests.get(url)
        response = get_content(rsp)
        assert response != 'null'
        rout = json.dumps(response, indent=4) if isinstance(response, dict) else response

        LOG.debug('%s\n\n[status code: %d, type(response): %s, elapsed: %ds, %s]', rout,
                  rsp.status_code, type(response), elapsed,
                  ', '.join(['{}: {}'.format(k, v) for k, v in rsp.headers.items()]))

        if rsp.status_code == 200:
            # Success
            response_dict = response
            break

        elif rsp.status_code == 202:
            # Not ready
            time.sleep(interval_sec)
            elapsed += interval_sec

        elif rsp.status_code == 400:
            # Failure
            raise Exception(
                "Failed to download the next config.\nStatus code: {}\nMessage: {}\n".format(
                    rsp.status_code, rout))

        elif rsp.status_code == 404:
            # No Tuning Session
            response_dict = response
            break

        elif rsp.status_code == 500:
            # Failure
            msg = rout
            if isinstance(response, str):
                savepath = os.path.join(dconf.LOG_DIR, 'error.html')
                with open(savepath, 'w') as f:
                    f.write(response)
                msg = "Saved HTML error to '{}'.".format(os.path.relpath(savepath))
            raise Exception(
                "Failed to download the next config.\nStatus code: {}\nMessage: {}\n".format(
                    rsp.status_code, msg))

        else:
            raise NotImplementedError(
                "Unhandled status code: '{}'.\nMessage: {}".format(rsp.status_code, rout))

    if not response_dict:
        assert elapsed > max_time_sec, \
            'response={} but elapsed={}s <= max_time={}s'.format(
                rout, elapsed, max_time_sec)
        raise Exception(
            'Failed to download the next config in {}s: {} (elapsed: {}s)'.format(
                max_time_sec, rout, elapsed))

    LOG.info('Downloaded the next config in %ds', elapsed)

    return response_dict


@task
def download_debug_info(pprint=False):
    pprint = parse_bool(pprint)
    url = '{}/dump/{}'.format(dconf.WEBSITE_URL, dconf.UPLOAD_CODE)
    params = {'pp': int(True)} if pprint else {}
    rsp = requests.get(url, params=params)

    if rsp.status_code != 200:
        raise Exception('Error downloading debug info.')

    filename = rsp.headers.get('Content-Disposition').split('=')[-1]
    file_len, exp_len = len(rsp.content), int(rsp.headers.get('Content-Length'))
    assert file_len == exp_len, 'File {}: content length != expected length: {} != {}'.format(
        filename, file_len, exp_len)

    with open(filename, 'wb') as f:
        f.write(rsp.content)
    LOG.info('Downloaded debug info to %s', filename)

    return filename


@task
def add_udm(result_dir=None):
    result_dir = result_dir or os.path.join(dconf.CONTROLLER_HOME, 'output')
    with lcd(dconf.UDM_DIR):  # pylint: disable=not-context-manager
        local('python3 user_defined_metrics.py {}'.format(result_dir))


@task
def upload_batch(result_dir=None, sort=True, upload_code=None):
    result_dir = result_dir or dconf.RESULT_DIR
    sort = parse_bool(sort)
    results = glob.glob(os.path.join(result_dir, '*__summary.json'))
    if sort:
        results = sorted(results)
    count = len(results)

    LOG.info('Uploading %d samples from %s...', count, result_dir)
    for i, result in enumerate(results):
        prefix = os.path.basename(result)
        prefix_len = os.path.basename(result).rfind('__') + 2
        prefix = prefix[:prefix_len]
        upload_result(result_dir=result_dir, prefix=prefix, upload_code=upload_code)
        LOG.info('Uploaded result %d/%d: %s__*.json', i + 1, count, prefix)


@task
def dump_database():
    dumpfile = os.path.join(dconf.DB_DUMP_DIR, dconf.DB_NAME + '.dump')
    if dconf.DB_TYPE == 'oracle':
        if not dconf.ORACLE_FLASH_BACK and file_exists(dumpfile):
            LOG.info('%s already exists ! ', dumpfile)
            return False
    elif dconf.DB_TYPE == 'mysql' and dconf.MYSQL_USE_MYDUMPER:
        if file_exists(os.path.join(dconf.DB_DUMP_DIR, dconf.DB_NAME + '.WAREHOUSE.sql')):
            LOG.info('%s already exists ! ', os.path.join(dconf.DB_DUMP_DIR, dconf.DB_NAME + '.*.sql'))
            return False
    elif dconf.DB_TYPE == 'tidb':
        if file_exists_local(os.path.join(dconf.DB_DUMP_DIR, dconf.DB_NAME + '-schema-create.sql')):
            LOG.info('%s already exists ! ', os.path.join(dconf.DB_DUMP_DIR, dconf.DB_NAME + '-schema-create.sql'))
            return False
    else:
        if file_exists(dumpfile):
            LOG.info('%s already exists ! ', dumpfile)
            return False

    if dconf.DB_TYPE == 'oracle' and dconf.ORACLE_FLASH_BACK:
        LOG.info('create restore point %s for database %s in %s', dconf.RESTORE_POINT,
                 dconf.DB_NAME, dconf.RECOVERY_FILE_DEST)

    if dconf.DB_TYPE == 'oracle':
        LOG.info('Dump database %s to %s', dconf.DB_NAME, dumpfile)
        if dconf.ORACLE_FLASH_BACK:
            run_sql_script('createRestore.sh', dconf.RESTORE_POINT,
                           dconf.RECOVERY_FILE_DEST_SIZE, dconf.RECOVERY_FILE_DEST)
        else:
            run_sql_script('dumpOracle.sh', dconf.DB_USER, dconf.DB_PASSWORD,
                           dconf.DB_NAME, dconf.DB_DUMP_DIR)

    elif dconf.DB_TYPE == 'postgres':
        LOG.info('Dump database %s to %s', dconf.DB_NAME, dumpfile)
        run('PGPASSWORD={} pg_dump -U {} -h {} -F c -d {} > {}'.format(
            dconf.DB_PASSWORD, dconf.DB_USER, dconf.DB_HOST, dconf.DB_NAME,
            dumpfile))
    elif dconf.DB_TYPE == 'mysql':
        if dconf.MYSQL_USE_MYDUMPER:
            LOG.info('Dump database %s to %s by using mydumper', dconf.DB_NAME, dconf.DB_DUMP_DIR)
            sudo('mydumper -B {} -o {}'.format(dconf.DB_NAME, dconf.DB_DUMP_DIR))
        else:
            LOG.info('Dump database %s to %s', dconf.DB_NAME, dumpfile)
            sudo('mysqldump --user={} --password={} --databases {} > {}'.format(
                dconf.DB_USER, dconf.DB_PASSWORD, dconf.DB_NAME, dumpfile))
    elif dconf.DB_TYPE == 'tidb':
        LOG.info('Dump database %s to %s by using tiup dumpling', dconf.DB_NAME, dconf.DB_DUMP_DIR)
        local('tiup dumpling -u {} --host {} -P {} -F {} -t {} -o {}'.format(dconf.DB_USER, dconf.DB_HOST,
                                                                             dconf.DB_PORT,
                                                                             dconf.TIDB_DUMP_SPLIT_FILE_SIZE,
                                                                             dconf.TIDB_DUMP_THREADS,
                                                                             dconf.DB_DUMP_DIR))
    else:
        raise Exception("Database Type {} Not Implemented !".format(dconf.DB_TYPE))
    return True


@task
def clean_recovery():
    run_sql_script('removeRestore.sh', dconf.RESTORE_POINT)
    cmds = ("""rman TARGET / <<EOF\nDELETE ARCHIVELOG ALL;\nexit\nEOF""")
    run(cmds)


@task
def restore_database():
    dumpfile = os.path.join(dconf.DB_DUMP_DIR, dconf.DB_NAME + '.dump')
    if not dconf.ORACLE_FLASH_BACK and not file_exists(dumpfile) and not file_exists_local(
            os.path.join(dconf.DB_DUMP_DIR, dconf.DB_NAME + '-schema-create.sql')):
        raise FileNotFoundError("Database dumpfile '{}' does not exist!".format(dumpfile))

    LOG.info('Start restoring database')
    if dconf.DB_TYPE == 'oracle':
        if dconf.ORACLE_FLASH_BACK:
            run_sql_script('flashBack.sh', dconf.RESTORE_POINT)
            clean_recovery()
        else:
            drop_user()
            create_user()
            run_sql_script('restoreOracle.sh', dconf.DB_USER, dconf.DB_NAME)
    elif dconf.DB_TYPE == 'postgres':
        drop_database()
        create_database()
        run('PGPASSWORD={} pg_restore -U {} -h {} -n public -j 8 -F c -d {} {}'.format(
            dconf.DB_PASSWORD, dconf.DB_USER, dconf.DB_HOST, dconf.DB_NAME, dumpfile))
    elif dconf.DB_TYPE == 'mysql':
        # 导入前先删除原有数据库
        drop_database()
        LOG.info('Database %s has been dropped before restore %s.', dconf.DB_NAME, dumpfile)
        if dconf.MYSQL_USE_MYDUMPER:
            run('myloader -B {} -d {} -t {}'.format(dconf.DB_NAME, dconf.DB_DUMP_DIR, dconf.MYSQL_MYDUMPER_THREADS))
        else:
            run('mysql --user={} --password={} < {}'.format(dconf.DB_USER, dconf.DB_PASSWORD, dumpfile))
    elif dconf.DB_TYPE == 'tidb':
        # 导入前先删除原有数据库
        drop_database()
        LOG.info('Database %s has been dropped before restore %s.', dconf.DB_NAME, dumpfile)
        # 然后使用tidb_lightning导入数据
        lightning_checkpoint_path = '/tmp/tidb_lightning_checkpoint-3.pb'
        if file_exists(lightning_checkpoint_path):
            local(f"rm -rf {lightning_checkpoint_path}")
        res = run_sql_script('run_tidb_lightning.sh', dconf.TIDB_LIGHTNING_CONF)
        err_times = 0
        while res.failed and err_times < 3:
            # run_sql_script('tidb_lightning_switch_mode.sh')
            drop_database()
            LOG.info('Database %s has been dropped before restore %s.', dconf.DB_NAME, dumpfile)
            local(f"rm -rf {lightning_checkpoint_path}")
            LOG.info('Cleaned tidb_lightning error check points! Now try it again...')
            res = run_sql_script('run_tidb_lightning.sh', dconf.TIDB_LIGHTNING_CONF)
            err_times += 1
    else:
        raise Exception("Database Type {} Not Implemented !".format(dconf.DB_TYPE))
    LOG.info('Wait %s seconds after restoring database', dconf.RESTORE_SLEEP_SEC)
    time.sleep(dconf.RESTORE_SLEEP_SEC)
    LOG.info('Finish restoring database')


@task
def is_ready_db(interval_sec=10):
    if dconf.DB_TYPE not in ('mysql', 'tidb'):
        LOG.info('database %s connecting function is not implemented, sleep %s seconds and return',
                 dconf.DB_TYPE, dconf.RESTART_SLEEP_SEC)
        time.sleep(dconf.RESTART_SLEEP_SEC)
        LOG.info('Wait %s seconds after restarting database', dconf.RESTART_SLEEP_SEC)
        return

    test_sql_query = "SELECT instance, sum(value) from METRICS_SCHEMA.node_disk_io_util " \
                     "where device='vdc' and time=now() group by instance;"

    with hide('everything'), settings(warn_only=True):  # pylint: disable=not-context-manager
        while True:
            res = run(
                "mysql --user={} --password={} "
                "-e 'exit'".format(dconf.DB_USER,
                                   dconf.DB_PASSWORD)) if dconf.DB_TYPE == 'mysql' else local(
                "mysql --user={} --password={} -h {} -P {} "
                "-e \"{}\"".format(dconf.DB_USER, dconf.DB_PASSWORD,
                                   dconf.DB_HOST, dconf.DB_PORT,
                                   test_sql_query))
            if res.failed:
                LOG.info('Database %s is not ready, wait for %s seconds',
                         dconf.DB_TYPE, interval_sec)
                time.sleep(interval_sec)
            else:
                LOG.info('Database %s is ready.', dconf.DB_TYPE)
                return


def _ready_to_start_benchmark():
    ready = False
    if os.path.exists(dconf.CONTROLLER_LOG):
        with open(dconf.CONTROLLER_LOG, 'r') as f:
            content = f.read()
        ready = 'Output the process pid to' in content
    return ready


def _ready_to_start_controller():
    ready = False
    if dconf.BENCH_TOOL == 'oltpbench' and os.path.exists(dconf.OLTPBENCH_LOG):
        with open(dconf.OLTPBENCH_LOG, 'r') as f:
            content = f.read()
        ready = 'Warmup complete, starting measurements' in content
    elif dconf.BENCH_TOOL == 'tiup_bench' and os.path.exists(dconf.TIUP_BENCH_RUN_LOG):
        with open(dconf.TIUP_BENCH_RUN_LOG, 'r') as f:
            content = f.read()
        ready = 'Starting component `bench`' in content
    elif dconf.BENCH_TOOL == 'benchbase' and os.path.exists(dconf.BENCHBASE_LOG):
        with open(dconf.BENCHBASE_LOG, 'r') as f:
            content = f.read()
        ready = 'Warmup complete, starting measurements' in content
    elif dconf.BENCH_TOOL == 'sysbench' and os.path.exists(dconf.SYSBENCH_LOG):
        with open(dconf.SYSBENCH_LOG, 'r') as f:
            content = f.read()
        ready = 'Threads started!' in content
    return ready


def _ready_to_shut_down_controller():
    pidfile = os.path.join(dconf.CONTROLLER_HOME, 'pid.txt')
    ready = False
    if os.path.exists(pidfile):
        if dconf.BENCH_TOOL == 'oltpbench' and os.path.exists(dconf.OLTPBENCH_LOG):
            with open(dconf.OLTPBENCH_LOG, 'r') as f:
                content = f.read()
            if 'Failed' in content:
                m = re.search('\n.*Failed.*\n', content)
                error_msg = m.group(0)
                LOG.error('OLTPBench Failed!')
                return True, error_msg
            if dconf.DB_TYPE == 'tidb':
                # TODO: 跳过oltpbench的collect过程，比较trick的做法。更优雅的做法是修改oltpbench源码使其支持tidb
                ready = 'Starting WatchDogThread' in content
            else:
                ready = 'Output throughput samples into file' in content
        elif dconf.BENCH_TOOL == 'tiup_bench' and os.path.exists(dconf.TIUP_BENCH_RUN_LOG):
            with open(dconf.TIUP_BENCH_RUN_LOG, 'r') as f:
                content = f.read()
            if 'Failed' in content:
                m = re.search('\n.*Failed.*\n', content)
                error_msg = m.group(0)
                LOG.error('TIUP_Bench Failed!')
                return True, error_msg
            ready = 'Finished' in content
        elif dconf.BENCH_TOOL == 'benchbase' and os.path.exists(dconf.BENCHBASE_LOG):
            with open(dconf.BENCHBASE_LOG, 'r') as f:
                content = f.read()
            if 'Failed' in content:
                m = re.search('\n.*Failed.*\n', content)
                error_msg = m.group(0)
                LOG.error('BenchBase Failed!')
                return True, error_msg
            ready = 'Output summary data into file' in content
        elif dconf.BENCH_TOOL == 'sysbench' and os.path.exists(dconf.SYSBENCH_LOG):
            with open(dconf.SYSBENCH_LOG, 'r') as f:
                content = f.read()
            if 'Failed' in content:
                m = re.search('\n.*Failed.*\n', content)
                error_msg = m.group(0)
                LOG.error('Sysbench Failed!')
                return True, error_msg
            ready = 'Threads fairness:' in content
    return ready, None


def clean_logs():
    if dconf.BENCH_TOOL == 'oltpbench':
        # remove oltpbench and controller log files
        local('rm -f {} {}'.format(dconf.OLTPBENCH_LOG, dconf.CONTROLLER_LOG))
    elif dconf.BENCH_TOOL == 'tiup_bench':
        local('rm -f {} {}'.format(dconf.TIUP_BENCH_RUN_LOG, dconf.CONTROLLER_LOG))
    elif dconf.BENCH_TOOL == 'benchbase':
        local('rm -f {} {}'.format(dconf.BENCHBASE_LOG, dconf.CONTROLLER_LOG))
    elif dconf.BENCH_TOOL == 'sysbench':
        local('rm -f {} {}'.format(dconf.SYSBENCH_LOG, dconf.CONTROLLER_LOG))


@task
def clean_bench_tool_results():
    if dconf.BENCH_TOOL == 'oltpbench':
        # remove oltpbench result files
        local('rm -f {}/results/outputfile*'.format(dconf.OLTPBENCH_HOME))
    elif dconf.BENCH_TOOL == 'tiup_bench':
        pass
    elif dconf.BENCH_TOOL == 'benchbase':
        # No need to clean benchbase results.
        # Because we always pick the latest summary log file to get UDM.
        # local('rm -f {}/results/*'.format(dconf.BENCHBASE_HOME))
        pass
    elif dconf.BENCH_TOOL == 'sysbench':
        pass


@task
def clean_controller_results():
    # remove benchmark result files
    local('rm -f {}/output/*.json'.format(dconf.CONTROLLER_HOME))


def _set_oltpbench_property(name, line):
    if name == 'username':
        ss = line.split('username')
        new_line = ss[0] + 'username>{}</username'.format(dconf.DB_USER) + ss[-1]
    elif name == 'password':
        ss = line.split('password')
        new_line = ss[0] + 'password>{}</password'.format(dconf.DB_PASSWORD) + ss[-1]
    elif name == 'DBUrl':
        ss = line.split('DBUrl')
        if dconf.DB_TYPE == 'postgres':
            dburl_fmt = 'jdbc:postgresql://{host}:{port}/{db}'.format
        elif dconf.DB_TYPE == 'oracle':
            dburl_fmt = 'jdbc:oracle:thin:@{host}:{port}:{db}'.format
        elif dconf.DB_TYPE == 'tidb':
            dburl_fmt = 'jdbc:mysql://{host}:{port}/{db}?useSSL=false'.format
        elif dconf.DB_TYPE == 'mysql':
            if dconf.DB_VERSION in ['5.6', '5.7']:
                dburl_fmt = 'jdbc:mysql://{host}:{port}/{db}?useSSL=false'.format
            elif dconf.DB_VERSION == '8.0':
                dburl_fmt = ('jdbc:mysql://{host}:{port}/{db}?'
                             'allowPublicKeyRetrieval=true&amp;useSSL=false').format
            else:
                raise Exception("MySQL Database Version {} "
                                "Not Implemented !".format(dconf.DB_VERSION))
        else:
            raise Exception("Database Type {} Not Implemented !".format(dconf.DB_TYPE))
        database_url = dburl_fmt(host=dconf.DB_HOST, port=dconf.DB_PORT, db=dconf.DB_NAME)
        new_line = ss[0] + 'DBUrl>{}</DBUrl'.format(database_url) + ss[-1]
    else:
        raise Exception("OLTPBench Config Property {} Not Implemented !".format(name))
    return new_line


def _set_benchbase_property(name, line):
    if name == 'username':
        ss = line.split('username')
        new_line = ss[0] + 'username>{}</username'.format(dconf.DB_USER) + ss[-1]
    elif name == 'password':
        ss = line.split('password')
        new_line = ss[0] + 'password>{}</password'.format(dconf.DB_PASSWORD) + ss[-1]
    elif name == 'url':
        ss = line.split('url')
        if dconf.DB_TYPE == 'postgres':
            dburl_fmt = 'jdbc:postgresql://{host}:{port}/{db}'.format
        elif dconf.DB_TYPE == 'oracle':
            dburl_fmt = 'jdbc:oracle:thin:@{host}:{port}:{db}'.format
        elif dconf.DB_TYPE == 'tidb':
            dburl_fmt = 'jdbc:mysql://{host}:{port}/{db}?useSSL=false'.format
        elif dconf.DB_TYPE == 'mysql':
            if dconf.DB_VERSION in ['5.6', '5.7']:
                dburl_fmt = 'jdbc:mysql://{host}:{port}/{db}?useSSL=false'.format
            elif dconf.DB_VERSION == '8.0':
                dburl_fmt = ('jdbc:mysql://{host}:{port}/{db}?'
                             'allowPublicKeyRetrieval=true&amp;useSSL=false').format
            else:
                raise Exception("MySQL Database Version {} "
                                "Not Implemented !".format(dconf.DB_VERSION))
        else:
            raise Exception("Database Type {} Not Implemented !".format(dconf.DB_TYPE))
        database_url = dburl_fmt(host=dconf.DB_HOST, port=dconf.DB_PORT, db=dconf.DB_NAME)
        new_line = ss[0] + 'url>{}</url'.format(database_url) + ss[-1]
    else:
        raise Exception("BenchBase Config Property {} Not Implemented !".format(name))
    return new_line


@task
def set_oltpbench_config():
    # set database user, password, and connection url in oltpbench config
    lines = None
    text = None
    with open(dconf.OLTPBENCH_CONFIG, 'r') as f:
        lines = f.readlines()
        text = ''.join(lines)
    lid = 10
    for i, line in enumerate(lines):
        if 'dbtype' in line:
            dbtype = line.split('dbtype')[1][1:-2].strip()
            if dbtype != dconf.DB_TYPE:
                raise Exception("dbtype {} in OLTPBench config != DB_TYPE {}"
                                "in driver config !".format(dbtype, dconf.DB_TYPE))
        if 'username' in line:
            lines[i] = _set_oltpbench_property('username', line)
        elif 'password' in line:
            lines[i] = _set_oltpbench_property('password', line)
            lid = i + 1
        elif 'DBUrl' in line:
            lines[i] = _set_oltpbench_property('DBUrl', line)
    if dconf.ENABLE_UDM:
        # add the empty uploadCode and uploadUrl so that OLTPBench will output the summary file,
        # which contains throughput, 99latency, 95latency, etc.
        if 'uploadUrl' not in text:
            line = '    <uploadUrl></uploadUrl>\n'
            lines.insert(lid, line)
        if 'uploadCode' not in text:
            line = '    <uploadCode></uploadCode>\n'
            lines.insert(lid, line)
    text = ''.join(lines)
    with open(dconf.OLTPBENCH_CONFIG, 'w') as f:
        f.write(text)
    LOG.info('oltpbench config is set: %s', dconf.OLTPBENCH_CONFIG)


@task
def set_benchbase_config():
    # set database user, password, and connection url in benchbase config
    with open(dconf.BENCHBASE_CONFIG, 'r') as f:
        lines = f.readlines()
    for i, line in enumerate(lines):
        if '<type>' in line:
            dbtype = line.split('type')[1][1:-2].strip()
            if dbtype.lower() != dconf.DB_TYPE.lower():
                raise Exception("dbtype {} in BenchBase config != DB_TYPE {}"
                                "in driver config !".format(dbtype, dconf.DB_TYPE))
        if '<username>' in line:
            lines[i] = _set_benchbase_property('username', line)
        elif '<password>' in line:
            lines[i] = _set_benchbase_property('password', line)
        elif '<url>' in line:
            lines[i] = _set_benchbase_property('url', line)
    text = ''.join(lines)
    with open(dconf.BENCHBASE_CONFIG, 'w') as f:
        f.write(text)
    LOG.info('benchbase config is set: %s', dconf.BENCHBASE_CONFIG)


@task
def loop(i):
    i = int(i)

    # free cache
    free_cache()

    # remove oltpbench log and controller log
    clean_logs()

    if dconf.ENABLE_UDM is True:
        clean_bench_tool_results()

    # check disk usage
    if check_disk_usage() > dconf.MAX_DISK_USAGE:
        LOG.warning('Exceeds max disk usage %s', dconf.MAX_DISK_USAGE)

    # run controller from another process
    p = Process(target=run_controller, args=())
    p.start()
    LOG.info('Run the controller')

    # run oltpbench/tiup_bench as a background job
    while not _ready_to_start_benchmark():
        time.sleep(1)
    run_bench_tool_bg()

    # the controller starts the first collection
    while not _ready_to_start_controller():
        time.sleep(1)
    signal_controller()
    LOG.info('Start the first collection')

    # stop the experiment
    ready_to_shut_down = False
    error_msg = None
    while not ready_to_shut_down:
        ready_to_shut_down, error_msg = _ready_to_shut_down_controller()
        time.sleep(1)

    signal_controller()
    LOG.info('Start the second collection, shut down the controller')

    p.join()
    if error_msg:
        raise Exception('Benchmark Failed: ' + error_msg)
    # add user defined metrics
    if dconf.ENABLE_UDM is True:
        add_udm()

    # save result
    result_timestamp = save_dbms_result()

    if i >= dconf.WARMUP_ITERATIONS:
        # upload result
        upload_result()

        # get result
        response = get_result()

        # save next config
        save_next_config(response, t=result_timestamp)

        # change config
        if not dconf.IGNORE_CHANGE_CONF:
            change_conf(response['recommendation'])


@task
def set_dynamic_knobs(recommendation, context):
    DYNAMIC = 'dynamic'  # pylint: disable=invalid-name
    RESTART = 'restart'  # pylint: disable=invalid-name
    UNKNOWN = 'unknown'  # pylint: disable=invalid-name
    if dconf.DB_TYPE == 'mysql':
        cmd_fmt = "mysql --user={} --password={} -e 'SET GLOBAL {}={}'".format
    else:
        raise Exception('database {} set dynamic knob function is not implemented.'.format(
            dconf.DB_TYPE))

    LOG.info('Start setting knobs dynamically.')
    with hide('everything'), settings(warn_only=True):  # pylint: disable=not-context-manager
        for knob, value in recommendation.items():
            if value is None:
                LOG.warning('Cannot set knob %s dynamically, skip this.', knob)
                continue
            mode = context.get(knob, UNKNOWN)
            if mode == DYNAMIC:
                res = run(cmd_fmt(dconf.DB_USER, dconf.DB_PASSWORD, knob, value))
            elif mode == RESTART:
                LOG.error('Knob %s cannot be set dynamically, restarting database is required, '
                          'skip this knob.', knob)
                continue
            elif mode == UNKNOWN:
                LOG.warning('It is unclear whether knob %s can be set dynamically or not, '
                            'still set it to value %s', knob, value)
                res = run(cmd_fmt(dconf.DB_USER, dconf.DB_PASSWORD, knob, value))

            if res.failed:
                LOG.error('Failed to set knob %s to value %s', knob, value)
                LOG.error(res)
    LOG.info('Finish setting knobs dynamically.')


@task
def clean_conf():
    if dconf.DB_TYPE == 'mysql' and dconf.HOST_CONN == 'remote':
        sudo('/etc/mysql/clean_my_cnf.sh')
        LOG.info('my.cnf cleaned!!! all configs are reseted to default.')
    elif dconf.DB_TYPE == 'tidb':
        global_cnfs = {
            "tidb_build_stats_concurrency": 4,
            "tidb_distsql_scan_concurrency": 15,
            "tidb_executor_concurrency": 5,
        }
        for cnf_name in global_cnfs.keys():
            local(
                "mysql --user={} --password={} -h {} -P {} -e 'set @@global.{}={};'".format(
                    'root',
                    '',
                    dconf.DB_HOST,
                    '4000',
                    cnf_name,
                    global_cnfs[cnf_name]))
        time.sleep(3)
        run_sql_script('clean_tidb_cnf.sh', dconf.TIDB_CLUSTER_NAME)
        LOG.info('config cleaned!!! all configs are reseted to default.')


@task
def run_loops(max_iter=10, clean_config=False):
    # dump database if it's not done before.
    dump = dump_database()
    # clean config to default
    if clean_config:
        clean_conf()
    # put the BASE_DB_CONF in the config file
    # e.g., mysql needs to set innodb_monitor_enable to track innodb metrics
    reset_conf(False)
    for i in range(int(max_iter)):
        # check if need to reload
        check_if_need_to_reload = dconf.RELOAD_INTERVAL > 0 and i % dconf.RELOAD_INTERVAL == 0
        check_if_need_to_reload = check_if_need_to_reload and ((i == 0 and dump is False) or i > 0)
        # restart database
        restart_succeeded = restart_database(check_if_need_to_reload)
        if not restart_succeeded:
            LOG.warning('restart not succeeded!')
            files = {'summary': b'{"error": "DB_RESTART_ERROR"}',
                     'knobs': b'{}',
                     'metrics_before': b'{}',
                     'metrics_after': b'{}'}
            if dconf.ENABLE_UDM:
                files['user_defined_metrics'] = b'{}'
            response = requests.post(dconf.WEBSITE_URL + '/new_result/', files=files,
                                     data={'upload_code': dconf.UPLOAD_CODE})
            response = get_result()
            result_timestamp = int(time.time())
            save_next_config(response, t=result_timestamp)
            change_conf(response['recommendation'])
            continue

        # reload database periodically
        if check_if_need_to_reload:
            is_ready_db(interval_sec=3)
            restore_database()

        is_ready_db(interval_sec=3)
        LOG.info('The %s-th Loop Starts / Total Loops %s', i + 1, max_iter)
        loop(i % dconf.RELOAD_INTERVAL if dconf.RELOAD_INTERVAL > 0 else i)
        LOG.info('The %s-th Loop Ends / Total Loops %s', i + 1, max_iter)
    # 转移并重命名result文件夹中的文件
    cmd = f'mv results res__{dconf.UPLOAD_CODE} && mkdir results'
    result_home = os.path.dirname(dconf.RESULT_DIR)
    new_res_dir = os.path.join(result_home, f'res__{dconf.UPLOAD_CODE}')
    with lcd(result_home):  # pylint: disable=not-context-manager
        local(cmd)
    rename_batch(result_dir=new_res_dir)


@task
def monitor(max_iter=1):
    # Monitor the database, without tuning. OLTPBench is also disabled
    for i in range(int(max_iter)):
        LOG.info('The %s-th Monitor Loop Starts / Total Loops %s', i + 1, max_iter)
        clean_controller_results()
        run_controller(interval_sec=dconf.CONTROLLER_OBSERVE_SEC)
        upload_result()
        LOG.info('The %s-th Monitor Loop Ends / Total Loops %s', i + 1, max_iter)


@task
def monitor_tune(max_iter=1):
    # Monitor the database, with tuning. OLTPBench is also disabled

    # Set base config
    set_dynamic_knobs(dconf.BASE_DB_CONF, {})

    for i in range(int(max_iter)):
        LOG.info('The %s-th Monitor Loop (with Tuning) Starts / Total Loops %s', i + 1, max_iter)
        clean_controller_results()
        run_controller(interval_sec=dconf.CONTROLLER_OBSERVE_SEC)
        upload_result()
        response = get_result()
        set_dynamic_knobs(response['recommendation'], response['context'])
        LOG.info('The %s-th Monitor Loop (with Tuning) Ends / Total Loops %s', i + 1, max_iter)


@task
def rename_batch(result_dir=None):
    result_dir = result_dir or dconf.RESULT_DIR
    results = glob.glob(os.path.join(result_dir, '*__summary.json'))
    results = sorted(results)
    for i, result in enumerate(results):
        prefix = os.path.basename(result)
        prefix_len = os.path.basename(result).rfind('__') + 2
        prefix = prefix[:prefix_len]
        new_prefix = '%03d' % i + '__' + prefix
        bases = ['summary', 'knobs', 'metrics_before', 'metrics_after', 'next_config']
        if dconf.ENABLE_UDM:
            bases.append('user_defined_metrics')
        for base in bases:
            fpath = os.path.join(result_dir, prefix + base + '.json')
            if os.path.exists(fpath):
                rename_path = os.path.join(result_dir, new_prefix + base + '.json')
                os.rename(fpath, rename_path)


def _http_content_to_json(content):
    if isinstance(content, bytes):
        content = content.decode('utf-8')
    try:
        json_content = json.loads(content)
        decoded = True
    except (TypeError, json.decoder.JSONDecodeError):
        json_content = None
        decoded = False

    return json_content, decoded


def _modify_website_object(obj_name, action, verbose=False, **kwargs):
    verbose = parse_bool(verbose)
    if obj_name == 'project':
        valid_actions = ('create', 'edit')
    elif obj_name == 'session':
        valid_actions = ('create', 'edit')
    elif obj_name == 'user':
        valid_actions = ('create', 'delete')
    else:
        raise ValueError('Invalid object: {}. Valid objects: project, session'.format(obj_name))

    if action not in valid_actions:
        raise ValueError('Invalid action: {}. Valid actions: {}'.format(
            action, ', '.join(valid_actions)))

    data = {}
    for k, v in kwargs.items():
        if isinstance(v, (dict, list, tuple)):
            v = json.dumps(v)
        data[k] = v

    url_path = '/{}/{}/'.format(action, obj_name)
    response = requests.post(dconf.WEBSITE_URL + url_path, data=data)

    content = response.content.decode('utf-8')
    if response.status_code != 200:
        raise Exception("Failed to {} {}.\nStatus: {}\nMessage: {}\n".format(
            action, obj_name, response.status_code, content))

    json_content, decoded = _http_content_to_json(content)
    if verbose:
        if decoded:
            LOG.info('\n%s_%s = %s', action.upper(), obj_name.upper(),
                     json.dumps(json_content, indent=4))
        else:
            LOG.warning("Content could not be decoded.\n\n%s\n", content)

    return response, json_content, decoded


@task
def create_website_user(**kwargs):
    return _modify_website_object('user', 'create', **kwargs)


@task
def delete_website_user(**kwargs):
    return _modify_website_object('user', 'delete', **kwargs)


@task
def create_website_project(**kwargs):
    return _modify_website_object('project', 'create', **kwargs)


@task
def edit_website_project(**kwargs):
    return _modify_website_object('project', 'edit', **kwargs)


@task
def create_website_session(**kwargs):
    return _modify_website_object('session', 'create', **kwargs)


@task
def edit_website_session(**kwargs):
    return _modify_website_object('session', 'edit', **kwargs)


def wait_pipeline_data_ready(max_time_sec=800, interval_sec=10):
    max_time_sec = int(max_time_sec)
    interval_sec = int(interval_sec)
    elapsed = 0
    ready = False

    while elapsed <= max_time_sec:
        response = requests.get(dconf.WEBSITE_URL + '/test/pipeline/')
        content = get_content(response)
        LOG.info("%s (elapsed: %ss)", content, elapsed)
        if 'False' in content:
            time.sleep(interval_sec)
            elapsed += interval_sec
        else:
            ready = True
            break

    return ready


@task
def integration_tests_simple():
    # Create test website
    response = requests.get(dconf.WEBSITE_URL + '/test/create/')
    LOG.info(get_content(response))

    # Upload training data
    LOG.info('Upload training data to no tuning session')
    upload_batch(result_dir='./integrationTests/data/', upload_code='ottertuneTestNoTuning')

    # periodic tasks haven't ran, lhs result returns.
    LOG.info('Test no pipeline data, LHS returned')
    upload_result(result_dir='./integrationTests/data/', prefix='0__',
                  upload_code='ottertuneTestTuningGPR')
    response = get_result(upload_code='ottertuneTestTuningGPR')
    assert response['status'] == 'lhs'

    # wait celery periodic task finishes
    assert wait_pipeline_data_ready(), "Pipeline data failed"

    # Test DNN
    LOG.info('Test DNN (deep neural network)')
    upload_result(result_dir='./integrationTests/data/', prefix='0__',
                  upload_code='ottertuneTestTuningDNN')
    response = get_result(upload_code='ottertuneTestTuningDNN')
    assert response['status'] == 'good'

    # Test GPR
    LOG.info('Test GPR (gaussian process regression)')
    upload_result(result_dir='./integrationTests/data/', prefix='0__',
                  upload_code='ottertuneTestTuningGPR')
    response = get_result(upload_code='ottertuneTestTuningGPR')
    assert response['status'] == 'good'

    # Test DDPG
    LOG.info('Test DDPG (deep deterministic policy gradient)')
    upload_result(result_dir='./integrationTests/data/', prefix='0__',
                  upload_code='ottertuneTestTuningDDPG')
    response = get_result(upload_code='ottertuneTestTuningDDPG')
    assert response['status'] == 'good'

    # Test DNN: 2rd iteration
    upload_result(result_dir='./integrationTests/data/', prefix='1__',
                  upload_code='ottertuneTestTuningDNN')
    response = get_result(upload_code='ottertuneTestTuningDNN')
    assert response['status'] == 'good'

    # Test GPR: 2rd iteration
    upload_result(result_dir='./integrationTests/data/', prefix='1__',
                  upload_code='ottertuneTestTuningGPR')
    response = get_result(upload_code='ottertuneTestTuningGPR')
    assert response['status'] == 'good'

    # Test DDPG: 2rd iteration
    upload_result(result_dir='./integrationTests/data/', prefix='1__',
                  upload_code='ottertuneTestTuningDDPG')
    response = get_result(upload_code='ottertuneTestTuningDDPG')
    assert response['status'] == 'good'

    LOG.info("\n\nIntegration Tests: PASSED!!\n")

    # Test task status UI
    task_status_ui_test()


@task
def task_status_ui_test():
    # Test GPR
    upload_code = 'ottertuneTestTuningGPR'
    response = requests.get(dconf.WEBSITE_URL + '/test/task_status/' + upload_code)
    LOG.info(get_content(response))
    assert 'Success:' in get_content(response)

    # Test DNN:
    upload_code = 'ottertuneTestTuningDNN'
    response = requests.get(dconf.WEBSITE_URL + '/test/task_status/' + upload_code)
    LOG.info(get_content(response))
    assert 'Success:' in get_content(response)

    # Test DDPG:
    upload_code = 'ottertuneTestTuningDDPG'
    response = requests.get(dconf.WEBSITE_URL + '/test/task_status/' + upload_code)
    LOG.info(get_content(response))
    assert 'Success:' in get_content(response)

    LOG.info("\n\nTask Status UI Tests: PASSED!!\n")


def simulate_db_run(i, next_conf):
    # Using 1 knob to decide performance; simple but effective
    gain = int(next_conf['effective_cache_size'].replace('MB', '000').replace('kB', ''))

    with open('./integrationTests/data/x__metrics_after.json', 'r') as fin:
        metrics_after = json.load(fin)
        metrics_after['local']['database']['pg_stat_database']['tpcc']['xact_commit'] = gain
    with open('./integrationTests/data/x__metrics_after.json', 'w') as fout:
        json.dump(metrics_after, fout)
    with open('./integrationTests/data/x__knobs.json', 'r') as fin:
        knobs = json.load(fin)
        for knob in next_conf:
            knobs['global']['global'][knob] = next_conf[knob]
    with open('./integrationTests/data/x__knobs.json', 'w') as fout:
        json.dump(knobs, fout)
    with open('./integrationTests/data/x__summary.json', 'r') as fin:
        summary = json.load(fin)
        summary['start_time'] = i * 20000000
        summary['observation_time'] = 100
        summary['end_time'] = (i + 1) * 20000000
    with open('./integrationTests/data/x__summary.json', 'w') as fout:
        json.dump(summary, fout)
    return gain


@task
def integration_tests():
    # Create test website
    response = requests.get(dconf.WEBSITE_URL + '/test/create/')
    LOG.info(get_content(response))

    # Upload training data
    LOG.info('Upload training data to no tuning session')
    upload_batch(result_dir='./integrationTests/data/', upload_code='ottertuneTestNoTuning')

    # periodic tasks haven't ran, lhs result returns.
    LOG.info('Test no pipeline data, LHS returned')
    upload_result(result_dir='./integrationTests/data/', prefix='1__',
                  upload_code='ottertuneTestTuningGPR')
    response = get_result(upload_code='ottertuneTestTuningGPR')
    assert response['status'] == 'lhs'

    # wait celery periodic task finishes
    assert wait_pipeline_data_ready(), "Pipeline data failed"

    total_n = 20
    last_n = 10

    max_gain = 0
    simulate_db_run(1, {'effective_cache_size': '0kB'})
    for i in range(2, total_n + 2):
        LOG.info('Test GPR (gaussian process regression)')
        upload_result(result_dir='./integrationTests/data/', prefix='x__',
                      upload_code='ottertuneTestTuningGPR')
        response = get_result(upload_code='ottertuneTestTuningGPR')
        assert response['status'] == 'good'
        gain = simulate_db_run(i, response['recommendation'])
        if max_gain < gain:
            max_gain = gain
        elif i > total_n - last_n + 2:
            assert gain > max_gain / 2.0

    max_gain = 0
    simulate_db_run(1, {'effective_cache_size': '0kB'})
    for i in range(2, total_n + 2):
        LOG.info('Test DNN (deep neural network)')
        upload_result(result_dir='./integrationTests/data/', prefix='x__',
                      upload_code='ottertuneTestTuningDNN')
        response = get_result(upload_code='ottertuneTestTuningDNN')
        assert response['status'] == 'good'
        gain = simulate_db_run(i, response['recommendation'])
        if max_gain < gain:
            max_gain = gain
        elif i > total_n - last_n + 2:
            assert gain > max_gain / 2.0

    max_gain = 0
    simulate_db_run(1, {'effective_cache_size': '0kB'})
    for i in range(2, total_n + 2):
        upload_result(result_dir='./integrationTests/data/', prefix='x__',
                      upload_code='ottertuneTestTuningDDPG')
        response = get_result(upload_code='ottertuneTestTuningDDPG')
        assert response['status'] == 'good'
        gain = simulate_db_run(i, response['recommendation'])
        if max_gain < gain:
            max_gain = gain
        elif i > total_n - last_n + 2:
            assert gain > max_gain / 2.0

    # ------------------- concurrency test ----------------------
    upload_result(result_dir='./integrationTests/data/', prefix='2__',
                  upload_code='ottertuneTestTuningDNN')
    upload_result(result_dir='./integrationTests/data/', prefix='3__',
                  upload_code='ottertuneTestTuningDNN')

    upload_result(result_dir='./integrationTests/data/', prefix='2__',
                  upload_code='ottertuneTestTuningGPR')
    upload_result(result_dir='./integrationTests/data/', prefix='3__',
                  upload_code='ottertuneTestTuningGPR')

    upload_result(result_dir='./integrationTests/data/', prefix='2__',
                  upload_code='ottertuneTestTuningDDPG')
    upload_result(result_dir='./integrationTests/data/', prefix='3__',
                  upload_code='ottertuneTestTuningDDPG')

    response = get_result(upload_code='ottertuneTestTuningDNN')
    assert response['status'] == 'good'
    response = get_result(upload_code='ottertuneTestTuningGPR')
    assert response['status'] == 'good'
    response = get_result(upload_code='ottertuneTestTuningDDPG')
    assert response['status'] == 'good'

    LOG.info("\n\nIntegration Tests: PASSED!!\n")

    # Test task status UI
    task_status_ui_test()
