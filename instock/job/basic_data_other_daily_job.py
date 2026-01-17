#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import concurrent.futures
import os.path
import sys
import pandas as pd
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
import instock.core.stockfetch as stf

__author__ = 'myh '
__date__ = '2023/3/10 '

# 每日股票龙虎榜
def save_nph_stock_lhb_data(date, before=True):
    if before:
        return

    logging.info(f"开始处理龙虎榜数据 {date}...")
    try:
        data = stf.fetch_stock_lhb_data(date)
        if data is None or len(data.index) == 0:
            logging.info(f"日期 {date} 无龙虎榜数据")
            return

        table_name = tbs.TABLE_CN_STOCK_lHB['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_lHB['columns'])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        logging.info(f"龙虎榜数据处理完成，共 {len(data)} 条记录")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_stock_lhb_data处理异常：{e}")
    stock_spot_buy(date)

# 每日股票龙虎榜(新浪)
def save_nph_stock_top_data(date, before=True):
    if before:
        return

    logging.info(f"开始处理龙虎榜(新浪)数据 {date}...")
    try:
        data = stf.fetch_stock_top_data(date)
        if data is None or len(data.index) == 0:
            logging.info(f"日期 {date} 无龙虎榜(新浪)数据")
            return

        table_name = tbs.TABLE_CN_STOCK_TOP['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_TOP['columns'])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        logging.info(f"龙虎榜(新浪)数据处理完成，共 {len(data)} 条记录")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_stock_top_data处理异常：{e}")
    stock_spot_buy(date)


# 每日股票资金流向
def save_nph_stock_fund_flow_data(date, before=True):
    if before:
        return

    logging.info(f"开始处理股票资金流向数据 {date}...")
    try:
        times = tuple(range(4))
        logging.info(f"获取 {len(times)} 个指标的资金流向数据...")
        results = run_check_stock_fund_flow(times)
        if results is None:
            return

        for t in times:
            if t == 0:
                data = results.get(t)
            else:
                r = results.get(t)
                if r is not None:
                    r.drop(columns=['name', 'new_price'], inplace=True)
                    data = pd.merge(data, r, on=['code'], how='left')

        if data is None or len(data.index) == 0:
            logging.info(f"日期 {date} 无资金流向数据")
            return

        data.insert(0, 'date', date.strftime("%Y-%m-%d"))

        table_name = tbs.TABLE_CN_STOCK_FUND_FLOW['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_FUND_FLOW['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        logging.info(f"股票资金流向数据处理完成，共 {len(data)} 条记录")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_nph_stock_fund_flow_data处理异常：{e}")


def run_check_stock_fund_flow(times):
    data = {}
    try:
        logging.info(f"开始获取资金流向，共 {len(times)} 个指标")
        for idx, k in enumerate(times, 1):
            logging.info(f"获取资金流向指标 {idx}/{len(times)}")
            _data = stf.fetch_stocks_fund_flow(k)
            if _data is not None:
                data[k] = _data
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.run_check_stock_fund_flow处理异常：{e}")
    if not data:
        return None
    else:
        logging.info(f"成功获取 {len(data)} 个指标的资金流向数据")
        return data


# 每日行业资金流向
def save_nph_stock_sector_fund_flow_data(date, before=True):
    if before:
        return

    logging.info(f"开始处理行业资金流向数据 {date}...")
    # times = tuple(range(2))
    # with concurrent.futures.ThreadPoolExecutor(max_workers=len(times)) as executor:
    #     {executor.submit(stock_sector_fund_flow_data, date, k): k for k in times}
    stock_sector_fund_flow_data(date, 0)
    stock_sector_fund_flow_data(date, 1)


def stock_sector_fund_flow_data(date, index_sector):
    sector_type = "行业" if index_sector == 0 else "概念"
    logging.info(f"开始处理{sector_type}资金流向数据 {date}...")
    try:
        times = tuple(range(3))
        logging.info(f"获取 {len(times)} 个指标的{sector_type}资金流向...")
        results = run_check_stock_sector_fund_flow(index_sector, times)
        if results is None:
            return

        for t in times:
            if t == 0:
                data = results.get(t)
            else:
                r = results.get(t)
                if r is not None:
                    data = pd.merge(data, r, on=['name'], how='left')

        if data is None or len(data.index) == 0:
            logging.info(f"日期 {date} 无{sector_type}资金流向数据")
            return

        data.insert(0, 'date', date.strftime("%Y-%m-%d"))

        if index_sector == 0:
            tbs_table = tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY
        else:
            tbs_table = tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT
        table_name = tbs_table['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs_table['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`name`")
        logging.info(f"{sector_type}资金流向数据处理完成，共 {len(data)} 条记录")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_sector_fund_flow_data处理异常：{e}")


def run_check_stock_sector_fund_flow(index_sector, times):
    sector_type = "行业" if index_sector == 0 else "概念"
    data = {}
    try:
        logging.info(f"开始获取{sector_type}资金流向，共 {len(times)} 个指标")
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(times)) as executor:
            future_to_data = {executor.submit(stf.fetch_stocks_sector_fund_flow, index_sector, k): k for k in times}
            with tqdm(total=len(future_to_data), desc=f"获取{sector_type}资金流向", unit="个") as pbar:
                for future in concurrent.futures.as_completed(future_to_data):
                    _time = future_to_data[future]
                    try:
                        _data_ = future.result()
                        if _data_ is not None:
                            data[_time] = _data_
                    except Exception as e:
                        logging.error(f"basic_data_other_daily_job.run_check_stock_sector_fund_flow处理异常：{e}")
                    finally:
                        pbar.update(1)
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.run_check_stock_sector_fund_flow处理异常：{e}")
    if not data:
        return None
    else:
        logging.info(f"成功获取 {len(data)} 个指标的{sector_type}资金流向")
        return data


# 每日股票分红配送
def save_nph_stock_bonus(date, before=True):
    if before:
        return

    logging.info(f"开始处理股票分红配送数据 {date}...")
    try:
        data = stf.fetch_stocks_bonus(date)
        if data is None or len(data.index) == 0:
            logging.info(f"日期 {date} 无分红配送数据")
            return

        table_name = tbs.TABLE_CN_STOCK_BONUS['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_BONUS['columns'])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        logging.info(f"股票分红配送数据处理完成，共 {len(data)} 条记录")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_nph_stock_bonus处理异常：{e}")


# 基本面选股
def stock_spot_buy(date):
    logging.info("开始基本面选股...")
    try:
        _table_name = tbs.TABLE_CN_STOCK_SPOT['name']
        if not mdb.checkTableIsExist(_table_name):
            logging.warning(f"表 {_table_name} 不存在，跳过选股")
            return

        sql = f'''SELECT * FROM `{_table_name}` WHERE `date` = '{date}' and
                `pe9` > 0 and `pe9` <= 20 and `pbnewmrq` <= 10 and `roe_weight` >= 15'''
        data = pd.read_sql(sql=sql, con=mdb.engine())
        data = data.drop_duplicates(subset="code", keep="last")
        if len(data.index) == 0:
            logging.info("基本面选股结果为空")
            return

        table_name = tbs.TABLE_CN_STOCK_SPOT_BUY['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SPOT_BUY['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        logging.info(f"基本面选股完成，共 {len(data)} 只股票")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_spot_buy处理异常：{e}")


# 每日早盘抢筹
def stock_chip_race_open_data(date):
    logging.info(f"开始处理早盘抢筹数据 {date}...")
    try:
        data = stf.fetch_stock_chip_race_open(date)
        if data is None or len(data.index) == 0:
            logging.info(f"日期 {date} 无早盘抢筹数据")
            return

        table_name = tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_CHIP_RACE_OPEN['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        logging.info(f"早盘抢筹数据处理完成，共 {len(data)} 条记录")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_chip_race_open_data：{e}")


# 每日涨停原因
def stock_imitup_reason_data(date):
    logging.info(f"开始处理涨停原因数据 {date}...")
    try:
        data = stf.fetch_stock_limitup_reason(date)
        if data is None or len(data.index) == 0:
            logging.info(f"日期 {date} 无涨停原因数据")
            return

        table_name = tbs.TABLE_CN_STOCK_LIMITUP_REASON['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_LIMITUP_REASON['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        logging.info(f"涨停原因数据处理完成，共 {len(data)} 条记录")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_imitup_reason_data：{e}")

def main():
    logging.info("========== 基础数据非实时作业任务启动 ==========")
    runt.run_with_args(save_nph_stock_lhb_data)
    runt.run_with_args(save_nph_stock_bonus)
    runt.run_with_args(save_nph_stock_fund_flow_data)
    runt.run_with_args(save_nph_stock_sector_fund_flow_data)
    runt.run_with_args(stock_chip_race_open_data)
    runt.run_with_args(stock_imitup_reason_data)
    logging.info("========== 基础数据非实时作业任务结束 ==========")


# main函数入口
if __name__ == '__main__':
    main()

