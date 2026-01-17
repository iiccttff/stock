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
from instock.core.stockfetch import fetch_stock_top_entity_data

__author__ = 'myh '
__date__ = '2023/3/10 '


def prepare(date, strategy):
    table_name = strategy['name']
    logging.info(f"开始处理策略 {table_name} 日期 {date}...")
    try:
        logging.info("正在获取股票历史数据...")
        stocks_data = stock_hist_data(date=date).get_data()
        if stocks_data is None:
            logging.warning(f"日期 {date} 没有股票历史数据")
            return

        logging.info(f"获取到 {len(stocks_data)} 只股票的历史数据")
        strategy_func = strategy['func']
        results = run_check(strategy_func, table_name, stocks_data, date)
        if results is None:
            logging.info(f"策略 {table_name} 无符合条件股票")
            return

        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_STRATEGIES[0]['columns'])

        data = pd.DataFrame(results)
        columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        data.columns = columns
        _columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
        # 单例，时间段循环必须改时间
        date_str = date.strftime("%Y-%m-%d")
        if len(data) > 0 and date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
            data['date'] = date_str
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        logging.info(f"策略 {table_name} 处理完成，共 {len(data)} 只股票")

    except Exception as e:
        logging.error(f"strategy_data_daily_job.prepare处理异常：{strategy}策略{e}")
        import traceback
        logging.error(f"详细错误: {traceback.format_exc()}")


def run_check(strategy_fun, table_name, stocks, date, workers=40):
    is_check_high_tight = False
    if strategy_fun.__name__ == 'check_high_tight':
        stock_tops = fetch_stock_top_entity_data(date)
        if stock_tops is not None:
            is_check_high_tight = True
    data = []
    try:
        logging.info(f"开始执行策略 {table_name}，workers={workers}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            if is_check_high_tight:
                future_to_data = {executor.submit(strategy_fun, k, stocks[k], date=date, istop=(k[1] in stock_tops)): k for k in stocks}
            else:
                future_to_data = {executor.submit(strategy_fun, k, stocks[k], date=date): k for k in stocks}

            with tqdm(total=len(future_to_data), desc=f"执行策略 {table_name}", unit="只") as pbar:
                for future in concurrent.futures.as_completed(future_to_data):
                    stock = future_to_data[future]
                    try:
                        if future.result():
                            data.append(stock)
                    except Exception as e:
                        logging.error(f"strategy_data_daily_job.run_check处理异常：{stock[1]}代码{e}策略{table_name}")
                    finally:
                        pbar.update(1)
    except Exception as e:
        logging.error(f"strategy_data_daily_job.run_check处理异常：{e}策略{table_name}")

    if not data:
        return None
    else:
        logging.info(f"策略 {table_name} 筛选到 {len(data)} 只股票")
        return data


def main():
    logging.info("========== 策略数据处理任务启动 ==========")
    # 使用方法传递。
    logging.info(f"待处理的策略数量: {len(tbs.TABLE_CN_STOCK_STRATEGIES)}")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for idx, strategy in enumerate(tbs.TABLE_CN_STOCK_STRATEGIES, 1):
            logging.info(f"提交策略任务 {idx}/{len(tbs.TABLE_CN_STOCK_STRATEGIES)}: {strategy['name']}")
            executor.submit(runt.run_with_args, prepare, strategy)
    logging.info("========== 所有策略任务已提交 ==========")


# main函数入口
if __name__ == '__main__':
    main()

