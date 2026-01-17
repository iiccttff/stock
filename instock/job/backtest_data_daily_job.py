#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import logging
import concurrent.futures
import pandas as pd
import os.path
import sys
import datetime
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
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.backtest.rate_stats as rate
from instock.core.singleton_stock import stock_hist_data

__author__ = 'myh '
__date__ = '2023/3/10 '


# 股票策略回归测试。
def prepare():
    logging.info("========== 回测数据处理任务启动 ==========")
    logging.info("开始准备回测数据...")

    tables = [tbs.TABLE_CN_STOCK_INDICATORS_BUY, tbs.TABLE_CN_STOCK_INDICATORS_SELL]
    tables.extend(tbs.TABLE_CN_STOCK_STRATEGIES)
    backtest_columns = list(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
    backtest_columns.insert(0, 'code')
    backtest_columns.insert(0, 'date')
    backtest_column = backtest_columns

    logging.info("正在获取股票历史数据...")
    stocks_data = stock_hist_data().get_data()
    if stocks_data is None:
        logging.warning("未获取到股票历史数据，退出")
        return

    for k in stocks_data:
        date = k[0]
        break

    logging.info(f"使用数据日期: {date}")
    logging.info(f"待处理的表数量: {len(tables)}")

    # 回归测试表
    logging.info("开始批量处理回测表...")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for idx, table in enumerate(tables, 1):
            logging.info(f"提交任务 {idx}/{len(tables)}: {table['name']}")
            future = executor.submit(process, table, stocks_data, date, backtest_column)
            futures.append(future)

        # 等待所有任务完成
        with tqdm(total=len(futures), desc="处理回测表", unit="个") as pbar:
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"回测表处理异常: {e}")
                finally:
                    pbar.update(1)

    logging.info("========== 回测数据处理任务结束 ==========")


def process(table, data_all, date, backtest_column):
    table_name = table['name']
    logging.info(f"开始处理表: {table_name}")

    if not mdb.checkTableIsExist(table_name):
        logging.warning(f"表 {table_name} 不存在，跳过")
        return

    column_tail = tuple(table['columns'])[-1]
    now_date = datetime.datetime.now().date()
    sql = f"SELECT * FROM `{table_name}` WHERE `date` < '{now_date}' AND `{column_tail}` is NULL"

    try:
        data = pd.read_sql(sql=sql, con=mdb.engine())
        if data is None or len(data.index) == 0:
            logging.info(f"表 {table_name} 无需回测数据")
            return

        logging.info(f"表 {table_name} 需回测股票数: {len(data)}")
        subset = data[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
        subset = subset.astype({'date': 'string'})
        stocks = [tuple(x) for x in subset.values]

        results = run_check(stocks, data_all, date, backtest_column)
        if results is None:
            logging.warning(f"表 {table_name} 回测结果为空")
            return

        data_new = pd.DataFrame(results.values())
        mdb.update_db_from_df(data_new, table_name, ('date', 'code'))
        logging.info(f"表 {table_name} 回测完成，更新 {len(data_new)} 条记录")

    except Exception as e:
        logging.error(f"backtest_data_daily_job.process处理异常：{table}表{e}")


def run_check(stocks, data_all, date, backtest_column, workers=40):
    data = {}
    try:
        logging.info(f"开始回测 {len(stocks)} 只股票，workers={workers}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_data = {executor.submit(rate.get_rates, stock,
                                              data_all.get((date, stock[1], stock[2])), backtest_column,
                                              len(backtest_column) - 1): stock for stock in stocks}

            with tqdm(total=len(future_to_data), desc="回测股票", unit="只") as pbar:
                for future in concurrent.futures.as_completed(future_to_data):
                    stock = future_to_data[future]
                    try:
                        _data_ = future.result()
                        if _data_ is not None:
                            data[stock] = _data_
                    except Exception as e:
                        logging.error(f"backtest_data_daily_job.run_check处理异常：{stock[1]}代码{e}")
                    finally:
                        pbar.update(1)
    except Exception as e:
        logging.error(f"backtest_data_daily_job.run_check处理异常：{e}")

    if not data:
        return None
    else:
        logging.info(f"回测完成，有效结果: {len(data)} 只股票")
        return data


def main():
    prepare()


# main函数入口
if __name__ == '__main__':
    main()

