#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
from pathlib import Path
import time
import random
import logging
from instock.core.singleton_proxy import proxys

__author__ = 'myh '
__date__ = '2025/12/31 '

class eastmoney_fetcher:
    """
    东方财富网数据获取器
    封装了Cookie管理、会话管理和请求发送功能
    """

    def __init__(self):
        """初始化获取器"""
        self.base_dir = os.path.dirname(os.path.dirname(__file__))
        self.session = self._create_session()

    def _get_cookie(self):
        """
        获取东方财富网的Cookie
        优先级：环境变量 > 文件 > 默认Cookie
        """
        # 1. 尝试从环境变量获取
        cookie = os.environ.get('EAST_MONEY_COOKIE')
        if cookie:
            # print("环境变量中的Cookie: 已设置")
            return cookie

        # 2. 尝试从文件获取
        cookie_file = Path(os.path.join(self.base_dir, 'config', 'eastmoney_cookie.txt'))
        if cookie_file.exists():
            with open(cookie_file, 'r') as f:
                cookie = f.read().strip()
            if cookie:
                # print("文件中的Cookie: 已设置")
                return cookie

        # 3. 默认Cookie（可能过期，仅作为备选）
        return 'fullscreengg=1; fullscreengg2=1; qgqp_b_id=76670de7aee4283d73f88b9c543a53f0; st_si=52987000764549; st_sn=1; st_psi=20251231162316664-113200301321-0046286479; st_asi=delete; st_pvi=43436093393372; st_sp=2025-12-31%2016%3A23%3A16; st_inirUrl='

    def _create_session(self):
        """创建并配置会话"""
        session = requests.Session()
        # 增加连接池大小，支持更多并发连接
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=50,
            pool_maxsize=50,
            max_retries=3
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        # 随机选择 User-Agent
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
        ]
        # 设置请求头
        headers = {
            'User-Agent': random.choice(user_agents),
            'Referer': 'https://quote.eastmoney.com/',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            # 不设置 Accept-Encoding，让 requests 自动处理压缩
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
        }
        session.headers.update(headers)
        # 设置Cookie
        session.cookies.update({'Cookie': self._get_cookie()})
        return session

    def make_request(self, url, params=None, retry=3, timeout=60, use_proxy=True):
        """
        发送请求
        :param url: 请求URL
        :param params: 请求参数
        :param retry: 重试次数（默认3次）
        :param timeout: 超时时间（默认60秒）
        :param use_proxy: 是否使用代理（默认True）
        :return: 响应对象
        """
        # 移除每次请求的延迟，改用随机 User-Agent 轮换防止反爬虫

        for i in range(retry):
            try:
                # 随机更换 User-Agent
                user_agents = [
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
                ]
                self.session.headers['User-Agent'] = random.choice(user_agents)

                # 根据参数决定是否使用代理
                proxies = proxys().get_proxies() if use_proxy else None

                response = self.session.get(
                    url,
                    proxies=proxies,
                    params=params,
                    timeout=timeout
                )
                response.raise_for_status()  # 检查HTTP错误
                return response
            except requests.exceptions.RequestException as e:
                logging.warning(f"请求错误: {e}, URL: {url}, 第 {i + 1}/{retry} 次重试")
                if i < retry - 1:
                    # 随机延迟后重试，延迟时间逐渐增加
                    retry_delay = random.uniform(2, 5) * (i + 1)
                    time.sleep(retry_delay)
                else:
                    raise

    def update_cookie(self, new_cookie):
        """
        更新Cookie
        :param new_cookie: 新的Cookie值
        """
        self.session.cookies.update({'Cookie': new_cookie})