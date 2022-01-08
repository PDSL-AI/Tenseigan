#
# Created on July 19, 2021
#
# @author: Yiqin Xiong, Chen Ding
#

from ..base.parser import BaseParser
from website.utils import ConversionUtil


class TidbParser(BaseParser):
    def __init__(self, dbms_obj):
        super().__init__(dbms_obj)

        self.valid_true_val = ("on", "true", "yes", '1', 'enabled')
        self.valid_false_val = ("off", "false", "no", '0', 'disabled')
        self.true_value = 'true'
        self.false_value = 'false'

        self.bytes_system = ConversionUtil.TIDB_BYTES_SYSTEM
        self.time_system = ConversionUtil.TIDB_TIME_SYSTEM
        self.min_bytes_unit = 'KiB'
        self.min_time_unit = 'ms'

    def parse_version_string(self, version_string):
        return version_string