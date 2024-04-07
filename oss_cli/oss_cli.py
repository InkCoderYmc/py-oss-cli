import os
import argparse
from .oss_config import OSSConfig
from .oss_operator import OSSOperator

def get_args():
    # 参数读取
    parser = argparse.ArgumentParser(description='Simple OSS CLI')
    # 支持上传、下载、删除三种操作
    parser.add_argument('--action', help='upload, download, delete', choices=['upload', 'download', 'delete'], default=False, required=False)
    # 是否启用目录操作
    parser.add_argument('--dir-enable', help='enable dir operation', )
    # 源文件路径
    parser.add_argument('--source-path', type=str, help='source_path')
    # 目标文件路径
    parser.add_argument('--target-path', type=str, help='target_path', required=False)
    # 配置文件路径 非必须
    parser.add_argument('--config-path', type=str, help='config_path', default=None, required=False)
    # 配置文件名称 非必须 默认为default
    parser.add_argument('--config-name', type=str, help='config_name', default=None, required=False)

    args = parser.parse_args()

    return args

def main():
    # 参数读取
    args = get_args()

    config_store = OSSConfig(config_name=args.config_name)
    operator = OSSOperator(config_store.config)

    operator.check_args()

    if args.action == 'upload':
        if args.dir_enable:
            operator.upload_dir(args.source_path, args.target_path)
        else:
            operator.upload_single_file(args.source_path, args.target_path)
    elif args.action == 'download':
        if args.dir_enable:
            operator.download_dir(args.source_path, args.target_path)
        else:
            operator.download_single_file(args.source_path, args.
            target_path)
    elif args.action == 'delete':
        if args.dir_enable:
            operator.delete_dir(args.source_path)
        else:
            operator.delete_file(args.source_path)