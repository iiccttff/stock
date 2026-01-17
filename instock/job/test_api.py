#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import sys
import os

# 配置日志输出到控制台
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import requests
import time

def test_basic_connection():
    """测试基本网络连接"""
    logging.info("========== 测试1: 基本网络连接 ==========")
    urls = [
        "http://www.baidu.com",
        "http://82.push2.eastmoney.com",
        "http://push2his.eastmoney.com",
    ]

    for url in urls:
        try:
            logging.info(f"尝试连接: {url}")
            response = requests.get(url, timeout=10)
            logging.info(f"✓ 连接成功，状态码: {response.status_code}")
        except Exception as e:
            logging.error(f"✗ 连接失败: {e}")

def test_api_with_request():
    """直接测试东方财富 API"""
    logging.info("\n========== 测试2: 直接调用东方财富 API ==========")

    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": 10,
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f14,f15,f16,f17,f18,f20,f21,f22,f23,f24,f25,f26,f37,f38,f39,f40,f41,f45,f46,f48,f49,f57,f61,f100,f112,f113,f114,f115,f221",
        "_": "1623833739532",
    }

    try:
        logging.info(f"请求 URL: {url}")
        logging.info(f"参数: {params}")

        start_time = time.time()
        response = requests.get(url, params=params, timeout=30)
        elapsed_time = time.time() - start_time

        logging.info(f"✓ 请求成功，耗时: {elapsed_time:.2f} 秒")
        logging.info(f"状态码: {response.status_code}")

        data = response.json()
        if data.get("data") and data["data"].get("diff"):
            stock_count = len(data["data"]["diff"])
            logging.info(f"✓ 获取到 {stock_count} 只股票的数据")

            # 显示前3只股票
            stocks = data["data"]["diff"][:3]
            for stock in stocks:
                code = stock.get("f12", "N/A")
                name = stock.get("f14", "N/A")
                logging.info(f"  股票: {code} - {name}")
            return True
        else:
            logging.error("✗ 响应数据格式异常")
            logging.error(f"响应内容: {data}")
            return False

    except Exception as e:
        logging.error(f"✗ 请求失败: {e}")
        import traceback
        logging.error(f"详细错误: {traceback.format_exc()}")
        return False

def test_with_fetcher():
    """使用 eastmoney_fetcher 测试"""
    logging.info("\n========== 测试3: 使用 eastmoney_fetcher ==========")

    try:
        from instock.core.eastmoney_fetcher import eastmoney_fetcher

        logging.info("创建 fetcher 实例...")
        fetcher = eastmoney_fetcher()
        logging.info("✓ fetcher 创建成功")

        url = "http://82.push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1,
            "pz": 10,
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
            "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f14,f15,f16,f17,f18,f20,f21,f22,f23,f24,f25,f26,f37,f38,f39,f40,f41,f45,f46,f48,f49,f57,f61,f100,f112,f113,f114,f115,f221",
            "_": "1623833739532",
        }

        logging.info("调用 make_request...")
        start_time = time.time()
        response = fetcher.make_request(url, params=params, retry=1, timeout=30)
        elapsed_time = time.time() - start_time

        logging.info(f"✓ 请求成功，耗时: {elapsed_time:.2f} 秒")
        return True

    except Exception as e:
        logging.error(f"✗ 测试失败: {e}")
        import traceback
        logging.error(f"详细错误: {traceback.format_exc()}")
        return False

def main():
    """主测试函数"""
    logging.info("开始 API 诊断测试...\n")

    # 测试1: 基本网络连接
    test_basic_connection()

    # 测试2: 直接调用 API
    result2 = test_api_with_request()

    # 测试3: 使用 fetcher
    result3 = test_with_fetcher()

    logging.info("\n========== 测试总结 ==========")
    if result2 and result3:
        logging.info("✓ 所有 API 测试通过")
        return True
    else:
        logging.error("✗ API 测试失败")
        logging.error("\n可能的原因：")
        logging.error("1. 网络连接问题 - Docker 容器无法访问外网")
        logging.error("2. Cookie 过期 - 需要更新 instock/config/eastmoney_cookie.txt")
        logging.error("3. API 限流 - 东方财富限制了访问频率")
        logging.error("4. 需要配置代理 - 编辑 instock/config/proxy.txt")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
