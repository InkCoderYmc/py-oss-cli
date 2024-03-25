import os
import re
from io import BytesIO
from boto3 import Session
from botocore.config import Config
from botocore.exceptions import ClientError

class BossOperator:
    """
    作用: 提供session层面的管理,支持下载上传删除等多项操作
    """
    def __init__(
        self,
        access_key: str,
        secret_access_key: str,
        region: str,
        endpoint_url: str,
        buckets: str,
    ):
        self.access_key = access_key
        self.secret_access_key = secret_access_key
        self.region = region
        self.endpoint_url = endpoint_url
        self.buckets = buckets
        self.session = Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
        )
        self.error_message = ""

        # 配置 config
        self.session_config = Config(
            s3={"addressing_style": "path"}, signature_version="s3v4"
        )

        self.client = self.session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            config=self.session_config,
        )

        self.resource = self.session.resource(
            "s3",
            endpoint_url=self.endpoint_url,
            config=self.session_config,
        )

    @property
    def get_client(self) -> str:
        return self.client

    @property
    def get_resource(self) -> str:
        return self.resource

    @property
    def get_buckets(self) -> str:
        return self.buckets

    def get_last_modified(self, path):
        return self.resource.Object(self.buckets, path).last_modified

    # TODO 增加 list 限制
    # !已废弃,不建议使用
    @property
    def list_files(self) -> list:
        file_paths = []
        for item in self.resource.Bucket(self.buckets).objects.all():
            file_paths.append(item.key)
        return file_paths

    # 通过list_objects_v2实现文件列表获取
    def list_dir_files(self, boss_dir: str) -> list:
        """
        通过list_objects_v2实现文件列表获取

        Args:
            boss_dir (str): 要获取的文件夹名称(可以只给出前缀,不强制要求写全)
            =>为保证只获取到对应的文件夹中的文件列表,使用时boss_dir尽量以"/"结尾

        Returns:
            list: boss_dir文件夹下的所有文件的完整路径
        """
        files = []
        is_truncated = True
        continuation_token = ""
        # 去除对前缀的支持，强制要求传入参数为完整的文件夹名
        boss_dir = boss_dir.rstrip("/")+"/"
        while is_truncated:
            resp = self.client.list_objects_v2(
                Bucket=self.buckets,
                ContinuationToken=continuation_token,
                MaxKeys=100, # 通过每次 100 个请求以及偏移量的支持，优化list性能
                StartAfter=boss_dir,
            )
            contents = resp["Contents"]
            for item in contents:
                if item["Key"].startswith(boss_dir):
                    files.append(item["Key"])
            if contents[-1]["Key"].startswith(boss_dir):
                is_truncated = resp["IsTruncated"]
            else:
                is_truncated = False
            continuation_token = resp["NextContinuationToken"] if is_truncated else ""
        return files

    def check_args(self) -> bool:
        """
        通过list函数判断连接状态
        =>原因为list_objects_v2开销较小

        Returns:
            bool
        """
        try:
            self.client.list_objects_v2(
                Bucket=self.buckets,
                ContinuationToken="",
                MaxKeys=100,
                StartAfter="",
            )
            return True
        except Exception as e:
            self.error_message = e
            return False

    def upload_single_file(self, file_path: str, upload_path: str) -> bool:
        """
        上传单个文件到boss

        Args:
            file_path (str): 上传文件的本地路径
            upload_path (str): 上传的boss路径

        Raises:
            Exception: 所有异常

        Returns:
            bool: 文件上传是否成功
        """
        try:
            if not self.__local_file_exist(file_path):
                raise Exception(f"File {file_path} not found")
            if self.__boss_file_exist(upload_path):
                self.resource.Object(self.buckets, upload_path).delete()
            print(f"upload {file_path} -> {upload_path}")
            self.resource.Object(self.buckets, upload_path).upload_file(file_path)
            return True
        except Exception as e:
            print(f"upload single file failed: {e=}")
            return False

    def upload_files(self, file_list: list, path_list: list) -> bool:
        """
        上传多个文件到boss

        Args:
            file_list (list): 上传文件的本地路径列表
            path_list (list): 上传的boss路径列表
            =>上述两个参数长度需保持一致,文件路径需一一对应

        Returns:
            bool: 是否所有文件都上传成功

        Raises:
            Exception: 参数检查失败和上传失败
        """
        try:
            # 输入两个参数的长度检查
            if len(file_list) != len(path_list):
                raise Exception("file_list and path_list length not match")
            for file, path in zip(file_list, path_list):
                self.upload_single_file(file, path)
            return True
        except Exception as e:
            print(f"upload file failed: {e=}")
            return False

    def upload_dir(self, file_dir: str, upload_dir: str, ignore_re: str = None) -> bool:
        """
        上传本地文件夹到boss

        Args:
            file_dir (str): 本地文件夹名
            upload_dir (str): boss文件夹名
            ignore_re (str, optional): 忽略规则,None为不忽略
            =>

        Returns:
            bool: 是否所有文件上传成功
        """
        try:
            file_names = self.__get_file_names_in_folder(file_dir)
            upload_list = []
            file_list = []
            for file_name in file_names:
                # 使用正则判断忽略文件，并跳过
                if ignore_re:
                    is_ignore = False
                    ignore_rules = ignore_re.split(",")
                    for ignore_rule in ignore_rules:
                        if self.__match_file_name(file_name, ignore_rule):
                            is_ignore = True
                            break
                    if is_ignore:
                        continue
                upload_list.append(upload_dir + "/" + file_name)
                file_list.append(file_dir + "/" + file_name)
            self.upload_files(file_list, upload_list)
            return True
        except Exception as e:
            print(f"upload dir failed: {e=}")
            return False

    def download_single_file(self, file: str, path: str) -> bool:
        """
        下载boss文件到本地

        Args:
            file (str): boss上的文件路径
            path (str): 下载到本地的路径

        Returns:
            bool: 文件下载是否成功
        """
        try:
            if not self.__local_file_exist(path) and self.__boss_file_exist(file):
                self.resource.Object(self.buckets, file).download_file(path)
                print(f"download success: {file} -> {path}")
            elif self.__local_file_exist(path):
                os.remove(path)
                self.resource.Object(self.buckets, file).download_file(path)
                print(f"download success: {file} -> {path}")
            else:
                print(f"download failed: boss file {file} not exist")
            return True
        except Exception as e:
            print(f"download single file failed: {e=}")
            return False

    def download_dir(self, boss_dir: str, local_dir: str) -> bool:
        """
        下载boss文件夹到本地

        Args:
            boss_dir (str): boss上的文件夹路径
            local_dir (str): 下载到本地的路径

        Returns:
            bool: 文件夹下载是否成功
        """
        try:
            boss_files = self.list_dir_files(boss_dir)
            self.__mkdir(local_dir)
            for file in boss_files:
                local_file = file.replace(boss_dir, local_dir)
                file_dir = "/".join(local_file.split("/")[:-1])
                self.__mkdir(file_dir)
                self.download_single_file(file, local_file)
            return True
        except Exception as e:
            print(f"download dir failed: {e=}")
            return False


    def download_dir_with_ignore(self, boss_dir, local_dir, ignore) -> bool:
        """
        下载boss文件夹到本地，忽略满足以 ignore 结尾的文件
        目前仅支持后缀匹配
        TODO: 增加正则支持

        Args:
            boss_dir (str): boss上的文件夹路径
            local_dir (str): 下载到本地的路径

        Returns:
            bool: 文件夹下载是否成功
        """
        try:
            boss_files = self.list_dir_files(boss_dir)
            # mkdir 保证本地下载文件夹存在，不创建直接下载会失败
            self.__mkdir(local_dir)
            for file in boss_files:
                if file.endswith(ignore):
                    print(f"{file} ignore")
                    continue
                local_file = file.replace(boss_dir, local_dir)
                file_dir = "/".join(local_file.split("/")[:-1])
                # 用于子文件夹的创建
                self.__mkdir(file_dir)
                self.download_single_file(file, local_file)
            return True
        except Exception as e:
            print(f"downloda dir with ignore failed: {e=}")
            return False


    def delete_single_file(self, file_path: str) -> bool:
        """
        删除boss文件

        Args:
            file_path (str): boss文件路径

        Returns:
            bool: 删除是否成功
        """
        try:
            self.resource.Object(self.buckets, file_path).delete()
            if self.__boss_file_exist(file_path):
                print(f"boss file {file_path} delete fail")
                return False
            else:
                print(f"boss file {file_path} delete success")
                return True
        except Exception as e:
            print(f"delete single file failed: {e=}")
            return False

    def delete_files(self, file_list: list) -> bool:
        """
        删除boss文件列表

        Args:
            file_list (list): boss文件列表

        Returns:
            bool: 删除是否成功
        """
        try:
            for file in file_list:
                self.delete_single_file(file)
            return True
        except Exception as e:
            print(f"delete files failed: {e=}")
            return False

    def delete_dir(self, boss_dir:str) -> bool:
        """
        删除boss文件夹

        Args:
            boss_dir (str): boss文件夹路径

        Returns:
            bool: 删除是否成功
        """
        try:
            files = self.list_dir_files(boss_dir)
            self.delete_files(files)
            print(f"delete {boss_dir} success")
            return True
        except Exception as e:
            print(f"delete dir failed: {e=}")
            return False


    def __mkdir(self, path:str):
        """
        创建文件夹，用于本地文件存储

        Args:
            path (str): 需要创建的文件夹路径
        """
        # 判断文件夹是否存在
        folder = os.path.exists(path)
        # 不存在则创建
        if not folder:
            os.makedirs(path)


    def __boss_file_exist(self, file_path:str) -> bool:
        """
        判断 boss 文件是否存在

        Args:
            file_path (str): boss文件路径

        Returns:
            bool: boss文件存在与否,存在返回True

        Raises:
            ClientError:404表示文件对象不存在
        """
        try:
            self.resource.Object(self.buckets, file_path).load()
            print(f"archimedes: file {file_path} exist!")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False

    def boss_file_exist(self, file_path: str) -> bool:
        try:
            self.resource.Object(self.buckets, file_path).load()
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False


    def __local_file_exist(self, file_path: str) -> bool:
        """
        判断本地文件是否存在

        Args:
            file_path (str): 本地文件路径

        Returns:
            bool: 本地文件存在与否,存在返回True
        """
        if os.path.exists(file_path):
            return True
        else:
            return False


    def __get_file_names_in_folder(self, folder_path: str) -> list:
        """
        获取文件夹中所有文件的文件名列表

        Args:
            folder_path (str): 文件夹路径

        Returns:
            list: 文件名列表
        """
        file_paths = []
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                file_paths.append(os.path.relpath(file_path, folder_path))
        return file_paths


    def __match_file_name(self, file_name: str, pattern: str) -> bool:
        """
        正则匹配文件名

        Args:
            file_name (str): 文件名
            pattern (str): 正则表达式

        Returns:
            bool: 是否能匹配到结果
        """
        # 使用正则表达式匹配文件名
        match = re.match(pattern, file_name)
        return bool(match)

