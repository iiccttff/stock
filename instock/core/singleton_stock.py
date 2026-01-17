#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import concurrent.futures
import instock.core.stockfetch as stf
import instock.core.tablestructure as tbs
import instock.lib.trade_time as trd
from instock.lib.singleton_type import singleton_type

__author__ = 'myh '
__date__ = '2023/3/10 '


# 读取当天股票数据
class stock_data(metaclass=singleton_type):
    def __init__(self, date):
        logging.info(f"正在获取日期 {date} 的股票列表...")
        try:
            self.data = stf.fetch_stocks(date)
            if self.data is not None:
                logging.info(f"成功获取 {len(self.data)} 只股票的列表数据")
            else:
                logging.warning("获取股票列表失败，返回 None")
        except Exception as e:
            logging.error(f"singleton.stock_data处理异常：{e}")
            import traceback
            logging.error(f"详细错误：{traceback.format_exc()}")
            self.data = None

    def get_data(self):
        return self.data


# 读取股票历史数据
class stock_hist_data(metaclass=singleton_type):
    def __init__(self, date=None, stocks=None, workers=16):
        logging.info(f"正在初始化股票历史数据，日期: {date}, workers: {workers}")
        if stocks is None:
            logging.info("正在获取股票列表数据...")
            _stock_data = stock_data(date).get_data()
            if _stock_data is None or len(_stock_data) == 0:
                logging.warning(f"stock_hist_data: 日期 {date} 没有获取到股票数据")
                self.data = None
                return
            _subset = _stock_data[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
            stocks = [tuple(x) for x in _subset.values]
            logging.info(f"获取到 {len(stocks)} 只股票")
        if stocks is None or len(stocks) == 0:
            logging.warning("股票列表为空")
            self.data = None
            return
        date_start, is_cache = trd.get_trade_hist_interval(stocks[0][0])  # 提高运行效率，只运行一次
        logging.info(f"开始获取 {len(stocks)} 只股票的历史数据，起始日期: {date_start}, 使用缓存: {is_cache}")
        _data = {}
        try:
            # max_workers是None还是没有给出，将默认为机器cup个数*5
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_stock = {executor.submit(stf.fetch_stock_hist, stock, date_start, is_cache): stock for stock
                                   in stocks}
                for future in concurrent.futures.as_completed(future_to_stock):
                    stock = future_to_stock
                    try:
                        __data = future.result()
                        if __data is not None:
                            _data[stock] = __data
                    except Exception as e:
                        logging.error(f"singleton.stock_hist_data处理异常：{stock[1] if len(stock) > 1 else '未知'}代码{e}")
        except Exception as e:
            logging.error(f"singleton.stock_hist_data处理异常：{e}")
        logging.info(f"成功获取 {len(_data)} 只股票的历史数据")
        if not _data:
            self.data = None
        else:
            self.data = _data

    def get_data(self):
        return self.data
