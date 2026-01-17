#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import logging
import concurrent.futures
import pandas as pd
import os.path
import sys
from tqdm import tqdm

# 配置日志输出到控制台
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
from instock.core.singleton_stock import stock_hist_data
import instock.core.pattern.pattern_recognitions as kpr

__author__ = 'myh '
__date__ = '2023/3/10 '


def prepare(date):
    try:
        logging.info(f"开始处理日期 {date} 的K线模式数据")
        stocks_data = stock_hist_data(date=date).get_data()
        if stocks_data is None:
            logging.warning(f"日期 {date} 没有获取到股票数据")
            return

        logging.info(f"获取到 {len(stocks_data)} 只股票的历史数据")
        results = run_check(stocks_data, date=date)
        if results is None:
            logging.warning(f"日期 {date} 没有识别到任何K线模式")
            return

        logging.info(f"识别到 {len(results)} 只股票存在K线模式")
        table_name = tbs.TABLE_CN_STOCK_KLINE_PATTERN['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_KLINE_PATTERN['columns'])

        dataKey = pd.DataFrame(results.keys())
        _columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        dataKey.columns = _columns

        dataVal = pd.DataFrame(results.values())

        data = pd.merge(dataKey, dataVal, on=['code'], how='left')
        # 单例，时间段循环必须改时间
        date_str = date.strftime("%Y-%m-%d")
        if date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
            data['date'] = date_str
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        logging.info(f"日期 {date} 的K线模式数据处理完成，共插入 {len(data)} 条记录")

    except Exception as e:
        logging.error(f"klinepattern_data_daily_job.prepare处理异常：{e}")


def run_check(stocks, date=None, workers=40):
    data = {}
    columns = tbs.STOCK_KLINE_PATTERN_DATA['columns']
    data_column = columns
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_data = {executor.submit(kpr.get_pattern_recognition, k, stocks[k], data_column, date=date): k for k in stocks}
            total = len(future_to_data)
            with tqdm(total=total, desc=f"处理K线模式 {date}", unit="只") as pbar:
                for future in concurrent.futures.as_completed(future_to_data):
                    stock = future_to_data[future]
                    try:
                        _data_ = future.result()
                        if _data_ is not None:
                            data[stock] = _data_
                    except Exception as e:
                        logging.error(f"klinepattern_data_daily_job.run_check处理异常：{stock[1]}代码{e}")
                    finally:
                        pbar.update(1)
    except Exception as e:
        logging.error(f"klinepattern_data_daily_job.run_check处理异常：{e}")
    if not data:
        return None
    else:
        return data


def main():
    # 使用方法传递。
    logging.info("========== K线模式数据处理任务启动 ==========")
    runt.run_with_args(prepare)
    logging.info("========== K线模式数据处理任务结束 ==========")


# main函数入口
if __name__ == '__main__':
    main()
