a oss cli project

提供一个cli工具用于管理oss文件，可通过配置文件设置简化oss连接，并提供了一个oss operator

config文件默认路径: ~/.config/oss_cli/config.yaml

install: clone repo and run 
'''
python install -e .
'''

参数解释:
 --action: 包含uopload,download,delete
 --source-path: 源路径
 --target-path: 目标路径
 --config-path: 支持定制config路径（后续会增加环境变量支持）
 --config-name: 读取config文件中对应的配置项，方便多bucket的管理（后续考虑增加环境变量支持和可视化支持）

oss operator功能:（oss operator后续会单独作为一个类上传至pypi）
  - 支持上传、下载、删除单个文件
  - 支持按文件夹粒度上传、下载、删除文件
  - todo：
    - 支持文件移动操作
    - 支持文件压缩操作
