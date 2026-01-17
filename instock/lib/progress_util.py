#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
通用进度显示工具
为所有作业提供统一的进度显示和日志功能
"""

import logging
from tqdm import tqdm
import sys

# 配置日志输出到控制台
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def show_progress_bar(iterable, desc="", unit="", total=None):
    """
    显示进度条
    :param iterable: 可迭代对象
    :param desc: 进度条描述
    :param unit: 单位
    :param total: 总数
    :return: 带进度的迭代器
    """
    return tqdm(iterable, desc=desc, unit=unit, total=total)

def log_task_start(task_name):
    """记录任务开始"""
    logging.info(f"========== {task_name}任务启动 ==========")

def log_task_end(task_name):
    """记录任务结束"""
    logging.info(f"========== {task_name}任务结束 ==========")

def log_processing_step(step_name, details=""):
    """记录处理步骤"""
    if details:
        logging.info(f"[{step_name}] {details}")
    else:
        logging.info(f"开始执行: {step_name}")

def log_data_fetch(count, data_type="数据"):
    """记录数据获取"""
    logging.info(f"成功获取 {count} 条{data_type}")

def log_data_save(count, table_name, date=None):
    """记录数据保存"""
    if date:
        logging.info(f"日期 {date} - 保存 {count} 条数据到表 {table_name}")
    else:
        logging.info(f"保存 {count} 条数据到表 {table_name}")

def log_error(error_msg, exception=None):
    """记录错误"""
    if exception:
        logging.error(f"{error_msg}: {exception}")
    else:
        logging.error(error_msg)

def log_warning(warning_msg):
    """记录警告"""
    logging.warning(warning_msg)

def log_info(info_msg):
    """记录信息"""
    logging.info(info_msg)
