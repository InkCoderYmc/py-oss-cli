import os

import yaml

from .utils import check_file_exists


class OSSConfig:
    def __init__(self, config_path=None, config_name=None):
        self.config_path = (
            config_path
            if config_path
            else os.path.join(
                os.path.expanduser("~"), ".config", "oss_cli", "config.yaml"
            )
        )
        self.config_name = config_name if config_name else "default"
        self.config = self.load_config()

    def load_config(self):
        """
        从yaml配置文件加载oss配置
        目前仅支持yaml文件
        """
        if not check_file_exists(self.config_path):
            print(f"Config file not found: {self.config_path}")
            return {}

        with open(self.config_path, "r") as f:
            config = yaml.safe_load(f)
        return config[self.config_name] if self.config_name in config else {}
