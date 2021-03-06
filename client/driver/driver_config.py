import os

# ==========================================================
#  DEBUG OPTIONS
# ==========================================================

# do not change config
IGNORE_CHANGE_CONF = False

# ==========================================================
#  DRIVER OPTIONS
# ==========================================================

# Path to this driver
DRIVER_HOME = os.path.dirname(os.path.realpath(__file__))

# Path to scripts
SCRIPT_HOME = DRIVER_HOME + '/../script'

# Path to the directory for storing results
RESULT_DIR = os.path.join(DRIVER_HOME, 'results')

# Set this to add user defined metrics
ENABLE_UDM = True

# Path to the User Defined Metrics (UDM), only required when ENABLE_UDM is True
UDM_DIR = os.path.join(DRIVER_HOME, 'userDefinedMetrics')

# Path to temp directory
TEMP_DIR = '/tmp/driver'

# Reload the database after running this many iterations
RELOAD_INTERVAL = 10

# The maximum allowable disk usage percentage. Reload the database
# whenever the current disk usage exceeds this value.
MAX_DISK_USAGE = 90

# Execute this many warmup iterations before uploading the next result
# to the website
WARMUP_ITERATIONS = 0

# Let the database initialize for this many seconds after it restarts
RESTART_SLEEP_SEC = 30

# Let the database initialize for this many seconds after it restores
RESTORE_SLEEP_SEC = 30

# The bench tool you want to use. Choose from ['oltpbench', 'tiup_bench', 'benchbase', 'sysbench']
BENCH_TOOL = 'tiup_bench'

# ==========================================================
#  SPECIFIC OPTIONS
# ==========================================================

# POSTGRES-SPECIFIC OPTIONS >>>
PG_DATADIR = '/var/lib/postgresql/9.6/main'

# ORACLE-SPECIFIC OPTIONS >>>
ORACLE_AWR_ENABLED = False
ORACLE_FLASH_BACK = True
RESTORE_POINT = 'tpcc_point'
RECOVERY_FILE_DEST = '/opt/oracle/oradata/ORCL'
RECOVERY_FILE_DEST_SIZE = '15G'

# MYSQL-SPECIFIC OPTIONS >>>
MYSQL_USE_MYDUMPER = True
MYSQL_MYDUMPER_THREADS = 16

# TIDB-SPECIFIC OPTIONS >>>
TIDB_CLUSTER_NAME = 'tidb-3'
TIDB_DUMP_THREADS = 16
TIDB_DUMP_SPLIT_FILE_SIZE = '256MiB'
TIDB_LIGHTNING_CONF = os.path.join(SCRIPT_HOME, f'{TIDB_CLUSTER_NAME}/tidb-lightning.toml')
TIUP_BENCH_WAREHOUSE = '1000'
TIUP_BENCH_THREADS = '200'
TIUP_BENCH_TIME = '300s'

# ==========================================================
#  HOST LOGIN
# ==========================================================

# Location of the database host relative to this driver
# Valid values: local, remote, docker or remote_docker
HOST_CONN = 'remote'

# The name of the Docker container for the target database
# (only required if HOST_CONN = docker)
CONTAINER_NAME = None  # e.g., 'postgres_container'

# Host SSH login credentials (only required if HOST_CONN=remote)
LOGIN_NAME = 'root'
LOGIN_HOST = 'tidb-pd-3'
LOGIN_PASSWORD = None
LOGIN_PORT = None  # Set when using a port other than the SSH default
# xyq add
LOGIN_KEY = '/root/.ssh/kp-x5oszr3r'  # Set when using ssh-key-file other than ssh-password

# ==========================================================
#  DATABASE OPTIONS
# ==========================================================

# Postgres, Oracle or Mysql
DB_TYPE = 'tidb'

# Database version
DB_VERSION = '5.0'

# Name of the database
DB_NAME = 'tpcc'

# Database username
DB_USER = 'root'

# Password for DB_USER
DB_PASSWORD = ''

# Database admin username (for tasks like restarting the database)
ADMIN_USER = DB_USER

# Database host address
DB_HOST = 'tidb-pd-3'

# Database port
DB_PORT = '4000'

# If set to True, DB_CONF file is mounted to database container file
# Only available when HOST_CONN is docker or remote_docker
DB_CONF_MOUNT = False

# Path to the configuration file on the database server
# If DB_CONF_MOUNT is True, the path is on the host server, not docker
DB_CONF = os.path.join(SCRIPT_HOME, f'{TIDB_CLUSTER_NAME}/config.yaml')

# Path to the directory for storing database dump files
DB_DUMP_DIR = f'/data1/{TIDB_CLUSTER_NAME}/'

# Path to the directory for storing database dump files
if DB_DUMP_DIR is None:
    if HOST_CONN == 'local':
        DB_DUMP_DIR = os.path.join(DRIVER_HOME, 'dumpfiles')
        if not os.path.exists(DB_DUMP_DIR):
            os.mkdir(DB_DUMP_DIR)
    else:
        DB_DUMP_DIR = os.path.expanduser('~/')

# Base config settings to always include when installing new configurations
if DB_TYPE == 'mysql' and DB_VERSION == '8.0':
    BASE_DB_CONF = {
        'innodb_monitor_enable': 'all',
        # Do not generate binlog, otherwise the disk space will grow continuely during the tuning
        # Be careful about it when tuning a production database, it changes your binlog behavior.
        'skip-log-bin': None,
    }
elif DB_TYPE == 'mysql':
    BASE_DB_CONF = {
        'innodb_monitor_enable': 'all',
    }
elif DB_TYPE == 'postgres':
    BASE_DB_CONF = {
        'track_counts': 'on',
        'track_functions': 'all',
        'track_io_timing': 'on',
        'autovacuum': 'off',
    }
elif DB_TYPE == 'tidb':
    BASE_DB_CONF = {
        # '(tikv,192.168.0.3:20160,storage.block-cache.shared)': 'false',
        # '(tikv,192.168.0.4:20160,storage.block-cache.shared)': 'false',
        # '(tikv,192.168.0.5:20160,storage.block-cache.shared)': 'false',
        # '(tikv,192.168.0.6:20160,storage.block-cache.shared)': 'false',
    }
else:
    BASE_DB_CONF = None

# Name of the device on the database server to monitor the disk usage, or None to disable
DATABASE_DISK = None

# Set this to a different database version to override the current version
OVERRIDE_DB_VERSION = None

# ==========================================================
#  OLTPBENCHMARK OPTIONS
# ==========================================================

# Path to OLTPBench directory
OLTPBENCH_HOME = '/data1/workspace/oltpbench/'

# Path to the OLTPBench configuration file
OLTPBENCH_CONFIG = os.path.join(OLTPBENCH_HOME, f'config/tpcc_config_{DB_TYPE}.xml')

# Name of the benchmark to run
OLTPBENCH_BENCH = 'tpcc'

# ==========================================================
#  BENCHBASE OPTIONS
# ==========================================================

# Path to BenchBase directory
BENCHBASE_HOME = '/data1/workspace/benchbase/target/benchbase-2021-SNAPSHOT/'

# Name of the benchmark to run
BENCHBASE_BENCH = 'tpcc'

# Path to the BenchBase configuration file
BENCHBASE_CONFIG = os.path.join(BENCHBASE_HOME, f'config/{DB_TYPE}/{TIDB_CLUSTER_NAME}/{BENCHBASE_BENCH}_config.xml')

# ==========================================================
#  SYSBENCH OPTIONS
# ==========================================================

# Path to Sysbench directory
SYSBENCH_HOME = '/data1/workspace/benchbase/target/benchbase-2021-SNAPSHOT/'

# Name of the benchmark to run
SYSBENCH_BENCH = 'select_random_ranges'

# Path to the BenchBase configuration file
SYSBENCH_CONFIG = os.path.join(SYSBENCH_HOME, f'config/{DB_TYPE}/{TIDB_CLUSTER_NAME}/{SYSBENCH_BENCH}_config_run')

# Parameters of Sysbench
SYSBENCH_PARAM = '--delta=5000000 --time=300'

# ==========================================================
#  CONTROLLER OPTIONS
# ==========================================================

# Controller observation time, OLTPBench will be disabled for
# monitoring if the time is specified
CONTROLLER_OBSERVE_SEC = 100

# Path to the controller directory
CONTROLLER_HOME = DRIVER_HOME + '/../controller'

# Path to the controller configuration file
CONTROLLER_CONFIG = os.path.join(CONTROLLER_HOME, f'config/{DB_TYPE}_config.json')

# ==========================================================
#  LOGGING OPTIONS
# ==========================================================

LOG_LEVEL = 'DEBUG'

# Path to log directory
LOG_DIR = os.path.join(DRIVER_HOME, 'log')

# Log files
DRIVER_LOG = os.path.join(LOG_DIR, 'driver.log')
OLTPBENCH_LOG = os.path.join(LOG_DIR, 'oltpbench.log')
BENCHBASE_LOG = os.path.join(LOG_DIR, 'benchbase.log')
SYSBENCH_LOG = os.path.join(LOG_DIR, f'sysbench_{SYSBENCH_BENCH}.log')
CONTROLLER_LOG = os.path.join(LOG_DIR, 'controller.log')
TIUP_BENCH_RUN_LOG = os.path.join(LOG_DIR, 'tiup_bench_run.log')
TIUP_BENCH_LOAD_LOG = os.path.join(LOG_DIR, 'tiup_bench_load.log')

# ==========================================================
#  WEBSITE OPTIONS
# ==========================================================

# OtterTune website URL
WEBSITE_URL = 'http://tuner:8002'

# Code for uploading new results to the website
UPLOAD_CODE = '5TEV4AE98KK1EBFAFYDX'
