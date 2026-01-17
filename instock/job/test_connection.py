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

import instock.core.stockfetch as stf
from instock.core.singleton_stock import stock_data, stock_hist_data
import instock.lib.trade_time as trd

def test_network():
    """测试网络连接"""
    logging.info("========== 开始测试网络连接 ==========")
    try:
        import requests
        response = requests.get("http://82.push2.eastmoney.com", timeout=10)
        logging.info(f"✓ 可以连接到东方财富服务器，状态码: {response.status_code}")
    except Exception as e:
        logging.error(f"✗ 无法连接到东方财富服务器: {e}")
        return False
    return True

def test_fetch_stocks():
    """测试获取股票列表"""
    logging.info("========== 测试获取股票列表 ==========")
    try:
        run_date, _ = trd.get_trade_date_last()
        logging.info(f"测试日期: {run_date}")
        data = stf.fetch_stocks(run_date)
        if data is not None and len(data) > 0:
            logging.info(f"✓ 成功获取 {len(data)} 只股票的列表")
            return True
        else:
            logging.warning("✗ 获取股票列表失败或数据为空")
            return False
    except Exception as e:
        logging.error(f"✗ 获取股票列表异常: {e}")
        import traceback
        logging.error(f"详细错误: {traceback.format_exc()}")
        return False

def test_stock_hist_data():
    """测试获取股票历史数据（仅获取少量股票）"""
    logging.info("========== 测试获取股票历史数据（前5只股票） ==========")
    try:
        run_date, _ = trd.get_trade_date_last()
        logging.info(f"测试日期: {run_date}")

        # 获取股票列表
        stock_data_obj = stock_data(run_date)
        stock_list = stock_data_obj.get_data()

        if stock_list is None or len(stock_list) == 0:
            logging.warning("无法获取股票列表，跳过历史数据测试")
            return False

        logging.info(f"获取到 {len(stock_list)} 只股票，测试前5只")

        import instock.core.tablestructure as tbs
        _subset = stock_list[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
        stocks = [tuple(x) for x in _subset.values[:5]]  # 只测试前5只

        logging.info(f"开始获取 {len(stocks)} 只股票的历史数据...")
        hist_data = stock_hist_data(run_date, stocks=stocks, workers=5)

        if hist_data is not None and len(hist_data) > 0:
            logging.info(f"✓ 成功获取 {len(hist_data)} 只股票的历史数据")
            return True
        else:
            logging.warning("✗ 获取股票历史数据失败或数据为空")
            return False
    except Exception as e:
        logging.error(f"✗ 获取股票历史数据异常: {e}")
        import traceback
        logging.error(f"详细错误: {traceback.format_exc()}")
        return False

def main():
    """主测试函数"""
    logging.info("开始诊断测试...")

    # 测试1: 网络连接
    if not test_network():
        logging.error("")
        logging.error("网络连接失败！请检查：")
        logging.error("1. Docker 容器是否可以访问外网")
        logging.error("2. 是否需要配置代理（编辑 instock/config/proxy.txt）")
        logging.error("3. 防火墙或网络策略是否限制了访问")
        return

    # 测试2: 获取股票列表
    if not test_fetch_stocks():
        logging.error("")
        logging.error("获取股票列表失败！请检查：")
        logging.error("1. Cookie 是否过期（编辑 instock/config/eastmoney_cookie.txt）")
        logging.error("2. API 地址是否被限制")
        logging.error("3. 超时设置是否合理")
        return

    # 测试3: 获取历史数据
    if not test_stock_hist_data():
        logging.error("")
        logging.error("获取历史数据失败！请检查：")
        logging.error("1. 数据库中是否有该日期的股票数据")
        logging.error("2. 缓存目录权限是否正确")
        logging.error("3. 网络连接是否稳定")
        return

    logging.info("")
    logging.info("========== 所有测试通过！可以正常运行 ==========")

if __name__ == '__main__':
    main()
