# Description: 七牛云文件迁移脚本
# Author: 初音过去
import requests
import os
from qiniu import Auth, BucketManager, put_file, config

# 配置
SOURCE_ACCESS_KEY = 'SAK'
SOURCE_SECRET_KEY = 'SSK'
SOURCE_BUCKET = 'SOURCE_BUCKET'
SOURCE_PREFIX = '/'  # 源文件夹前缀
SOURCE_REGION = 'z1'  # 源文件夹区域
SOURCE_DOMAIN = 'https://example.com'  # 源文件下载域名

TARGET_ACCESS_KEY = 'TAK'
TARGET_SECRET_KEY = 'TSK'
TARGET_BUCKET = 'TARGET_BUCKET'
TARGET_PREFIX = '/'  # 目标文件夹前缀
TARGET_REGION = 'z2'  # 目标文件夹区域

TEMP_DIR = './temp'  # 本地临时文件目录

source_auth = Auth(SOURCE_ACCESS_KEY, SOURCE_SECRET_KEY)
target_auth = Auth(TARGET_ACCESS_KEY, TARGET_SECRET_KEY)

def list_files(auth, bucket_name, prefix):
    bucket = BucketManager(auth)
    marker = None
    eof = False
    files = []

    while not eof:
        ret, eof, info = bucket.list(bucket_name, prefix, marker=marker)
        if ret is None:
            print(f"列举文件失败：{info.error}")
            break

        files.extend(ret['items'])
        marker = ret.get('marker', None)

    return files

def download_file(auth, bucket_name, key, local_path):
    """
    下载文件到本地
    """
    if local_path.endswith('/'):
        print(f"错误：local_path 指向目录，而非文件路径：{local_path}")
        return False

    dir_path = os.path.dirname(local_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    base_url = f"{SOURCE_DOMAIN}/{key}"
    private_url = auth.private_download_url(base_url)
    response = requests.get(private_url, stream=True)
    if response.status_code == 200:
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        print(f"成功下载文件：{key} -> {local_path}")
        return True
    else:
        print(f"下载失败：{key}，错误码：{response.status_code}")
        return False

def upload_file(auth, bucket_name, key, local_path):
    """
    上传文件到目标空间
    """
    token = auth.upload_token(bucket_name, key)
    ret, info = put_file(token, key, local_path)
    if info.status_code == 200:
        print(f"成功上传文件：{local_path} -> {key}")
        return True
    else:
        print(f"上传失败：{local_path}，错误信息：{info.error}")
        return False

def migrate_files():
    """
    执行文件迁移逻辑
    """
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)

    for file in list_files(source_auth, SOURCE_BUCKET, SOURCE_PREFIX):
        source_key = file['key'] 
        local_path = os.path.join(TEMP_DIR, source_key)
        target_key = source_key.replace(SOURCE_PREFIX, TARGET_PREFIX, 1)

        print(f"开始迁移文件：{source_key} -> {target_key}")

        try:
            if download_file(source_auth, SOURCE_BUCKET, source_key, local_path):
                upload_file(target_auth, TARGET_BUCKET, target_key, local_path)

            if os.path.exists(local_path):
                os.remove(local_path)

        except Exception as e:
            print(f"迁移文件失败：{source_key}，错误信息：{e}")


if __name__ == '__main__':
    migrate_files()