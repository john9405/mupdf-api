import os
import logging
import uuid
from datetime import datetime

import tos
import pymupdf
from fastapi import FastAPI, Body

logging.basicConfig(level=logging.INFO, filename='run.log')
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_ROOT = os.path.join(BASE_DIR, 'static')

app = FastAPI()
# 从环境变量获取 AK 和 SK 信息。
ak = os.getenv('TOS_ACCESS_KEY')
sk = os.getenv('TOS_SECRET_KEY')
# your endpoint 和 your region 填写Bucket 所在区域对应的Endpoint。# 以华北2(北京)为例，your endpoint 填写 tos-cn-beijing.volces.com，your region 填写 cn-beijing。
endpoint = "your endpoint"
region = "your region"
bucket_name = "bucket-test"

def download_file(object_key, file_name):
    try:
        # 创建 TosClientV2 对象，对桶和对象的操作都通过 TosClientV2 实现
        client = tos.TosClientV2(ak, sk, endpoint, region)
        # 若 file_name 为目录则将对象下载到此目录下, 文件名为对象名
        client.get_object_to_file(bucket_name, object_key, file_name)
        return True
    except tos.exceptions.TosClientError as e:
        # 操作失败，捕获客户端异常，一般情况为非法请求参数或网络异常
        logging.info('fail with client error, message:{}, cause: {}'.format(e.message, e.cause))
    except tos.exceptions.TosServerError as e:
        # 操作失败，捕获服务端异常，可从返回信息中获取详细错误信息
        logging.info('fail with server error, code: {}'.format(e.code))
        # request id 可定位具体问题，强烈建议日志中保存
        logging.info('error with request id: {}'.format(e.request_id))
        logging.info('error with message: {}'.format(e.message))
        logging.info('error with http code: {}'.format(e.status_code))
        logging.info('error with ec: {}'.format(e.ec))
        logging.info('error with request url: {}'.format(e.request_url))
    except Exception as e:
        logging.info('fail with unknown error: {}'.format(e))
    return False

def upload_file(object_key, file_name):
    try:
        # 创建 TosClientV2 对象，对桶和对象的操作都通过 TosClientV2 实现
        client = tos.TosClientV2(ak, sk, endpoint, region)
        # 将本地文件上传到目标桶中
        # file_name为本地文件的完整路径。
        client.put_object_from_file(bucket_name, object_key, file_name)
        return True
    except tos.exceptions.TosClientError as e:
        # 操作失败，捕获客户端异常，一般情况为非法请求参数或网络异常
        logging.info('fail with client error, message:{}, cause: {}'.format(e.message, e.cause))
    except tos.exceptions.TosServerError as e:
        # 操作失败，捕获服务端异常，可从返回信息中获取详细错误信息
        logging.info('fail with server error, code: {}'.format(e.code))
        # request id 可定位具体问题，强烈建议日志中保存
        logging.info('error with request id: {}'.format(e.request_id))
        logging.info('error with message: {}'.format(e.message))
        logging.info('error with http code: {}'.format(e.status_code))
        logging.info('error with ec: {}'.format(e.ec))
        logging.info('error with request url: {}'.format(e.request_url))
    except Exception as e:
        logging.info('fail with unknown error: {}'.format(e))
    return False

def convert_file(input_path, output_path):
    try:
        file = pymupdf.open(input_path)
        pdf_bytes = file.convert_to_pdf()
        pdf = pymupdf.open("pdf", pdf_bytes)
        pdf.save(output_path)
        return True
    except Exception as e:
        logging.info('fail with unknown error: {}'.format(e))
        return False

@app.post("/convert")
async def convert(file_url: str = Body(...),
                  file_ext: str = Body(...)):
    """
    Convert a docx/xlsx file to a pdf file.
    """
    filename = str(uuid.uuid1())
    res = download_file(file_url, os.path.join(STATIC_ROOT, f"{filename}.{file_ext}"))
    if not res:
        return {"code":1, "msg": "download file failed"}
    res = convert_file(os.path.join(STATIC_ROOT, f"{filename}.{file_ext}"), os.path.join(STATIC_ROOT, f"{filename}.pdf"))
    if not res:
        return {"code":1, "msg": "convert file fail"}
    file_key = f"convert/{datetime.now().strftime('%Y%m%d')}/{filename}.pdf"
    res = upload_file(file_key, os.path.join(STATIC_ROOT, f"{filename}.pdf"))
    if not res:
        return {"code":1, "msg": "upload file fail"}
    return {"code":0, "msg": "",  "data": {"file_key": file_key}}
