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
import instock.lib.trade_time as trd

def main():
    """快速测试获取股票列表"""
    logging.info("========== 开始测试获取股票列表 ==========")

    try:
        run_date, _ = trd.get_trade_date_last()
        logging.info(f"测试日期: {run_date}")
        logging.info("正在调用 fetch_stocks()...")
        logging.info("这可能需要 30-90 秒（3 次重试，每次 30 秒超时）")

        import time
        start_time = time.time()

        data = stf.fetch_stocks(run_date)

        elapsed_time = time.time() - start_time
        logging.info(f"请求耗时: {elapsed_time:.2f} 秒")

        if data is not None and len(data) > 0:
            logging.info(f"✓ 成功获取 {len(data)} 只股票的列表")
            logging.info(f"前5行数据:\n{data.head()}")
            return True
        else:
            logging.warning("✗ 获取股票列表失败或数据为空")
            return False
    except Exception as e:
        logging.error(f"✗ 获取股票列表异常: {e}")
        import traceback
        logging.error(f"详细错误: {traceback.format_exc()}")
        return False

if __name__ == '__main__':
    success = main()
    if success:
        logging.info("")
        logging.info("========== 测试成功！可以继续运行 ==========")
    else:
        logging.error("")
        logging.error("========== 测试失败！需要检查网络或配置 ==========")
        sys.exit(1)
