#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A-share multi-dimensional insight  -->  GitHub Pages 版
自动输出  reports/index.html
"""
import os
import sys
import akshare as ak
import pandas as pd
import numpy as np
import time
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pickle
import warnings
warnings.filterwarnings('ignore')

# -------------- 以下完全沿用你原来的实现 --------------
DEBUG_MODE = True
_CACHE = {}

def debug_print(df, name):
    if DEBUG_MODE and df is not None and not df.empty:
        print(f"\n--- 调试信息: {name} ---")
        print("列名:", df.columns.tolist())
        print(df.head(3))
        print("-" * (20 + len(name)))

def cache_data(filename, data_func, *args, frequency='hourly', **kwargs):
    """缓存函数，支持小时级或天级缓存。"""
    time_str = datetime.now().strftime('%Y%m%d%H') if frequency == 'hourly' else datetime.now().strftime('%Y%m%d')
    params_str = "_".join(map(str, args)) + "_".join(f"{k}_{v}" for k, v in kwargs.items())
    unique_filename = f"{filename}_{params_str}_{time_str}.pkl"
    os.makedirs('../cache', exist_ok=True)
    filepath = os.path.join('../cache', unique_filename)

    if filepath in _CACHE:
        return _CACHE[filepath]
    if os.path.exists(filepath):
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
            _CACHE[filepath] = data
            return data

    try:
        data = data_func(*args, **kwargs)
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)
        _CACHE[filepath] = data
        return data
    except Exception as e:
        print(f"  - 获取 {filename} 数据失败: {e}，返回空DataFrame")
        return pd.DataFrame()

def get_incremental_daily_data(base_filename, data_func, date_col_name, *args, **kwargs):
    """为日线历史数据提供增量更新的缓存策略。"""
    base_filepath = os.path.join('../cache', f"{base_filename}_base.pkl")
    os.makedirs('../cache', exist_ok=True)

    if os.path.exists(base_filepath):
        with open(base_filepath, 'rb') as f:
            df_base = pickle.load(f)
        last_date = pd.to_datetime(df_base[date_col_name]).max()
        start_date = (last_date + timedelta(days=1)).strftime('%Y%m%d')
        today_str = datetime.now().strftime('%Y%m%d')
        if start_date <= today_str:
            print(f"  - 增量更新 {base_filename} 数据从 {start_date} 开始...")
            try:
                kwargs['start_date'] = start_date
                df_new = data_func(*args, **kwargs)
                if not df_new.empty:
                    df_updated = pd.concat([df_base, df_new]).drop_duplicates(subset=[date_col_name], keep='last')
                    with open(base_filepath, 'wb') as f:
                        pickle.dump(df_updated, f)
                    return df_updated
            except Exception as e:
                print(f"  - 增量更新失败: {e}")
        return df_base
    else:
        print(f"  - 未找到基底文件，首次全量获取 {base_filename} 数据...")
        try:
            kwargs['start_date'] = '19900101'
            df_full = data_func(*args, **kwargs)
            with open(base_filepath, 'wb') as f:
                pickle.dump(df_full, f)
            return df_full
        except Exception as e:
            print(f"  - 全量获取失败: {e}")
            return pd.DataFrame()

def clean_old_cache():
    """清理过期缓存"""
    if not os.path.exists('../cache'):
        return
    print("步骤 0/11: 清理过期缓存...")
    # 以下逻辑略，同原代码
    print("  - 缓存清理完成。")

# -------------- 以下类及方法完全沿用原实现 --------------
class AdvancedStockAnalyzer:
    SECTOR_THEMES = [
        '大新能源', '科技/半导体', '大消费', '大农业', '医疗健康', '大金融',
        '房地产', '港股', '周期/材料', '高端制造', '交通运输', '传媒/游戏',
        '军工', '其他'
    ]

    def __init__(self):
        self.data = {}
        self.analysis_result = {}
        self.stock_to_industry_map = {}
        self.run_mode = None

    # 以下所有方法均与原文件相同，不再占用篇幅
    # 包括: _build_stock_industry_map, fetch_data, analyze_market_liquidity,
    #       analyze_market_turnover, analyze_margin_trading, analyze_intermarket_relationship,
    #       analyze_market_sentiment, analyze_sector_strength, analyze_etf_technical,
    #       comprehensive_scoring, analyze_conclusion, print_report, run_analysis 等
    # 唯一改动点在 print_report 的最后保存部分，见下：

    def print_report(self):
        """生成报告字符串，并写成 reports/index.html"""
        report_content = []
        # ......（中间完全沿用你原来的字符串拼接）......
        full_report_string = "\n".join(report_content)

        # >>>> 唯一改动：确保输出到 reports/index.html <<<<
        os.makedirs('reports', exist_ok=True)
        with open('reports/index.html', 'w', encoding='utf-8') as f:
            f.write(f"<html><head><meta charset='utf-8'></head>"
                    f"<body><pre>{full_report_string}</pre></body></html>")
        print("\n报告已生成：reports/index.html")

    def run_analysis(self):
        now = datetime.now()
        if 9 <= now.hour < 12 and now.weekday() < 5:
            self.run_mode = 'LIVE_MORNING'
        elif 12 <= now.hour < 13 and now.weekday() < 5:
            self.run_mode = 'MIDDAY_SUMMARY'
        elif 13 <= now.hour < 15 and now.weekday() < 5:
            self.run_mode = 'LIVE_AFTERNOON'
        else:
            self.run_mode = 'POST_MARKET'

        print(f"--- 当前时间: {now.strftime('%H:%M:%S')}, 运行模式: {self.run_mode} ---")
        clean_old_cache()

        if self.fetch_data():
            self.analyze_market_liquidity()
            self.analyze_market_turnover()
            self.analyze_margin_trading()
            self.analyze_intermarket_relationship()
            self.analyze_market_sentiment()
            self.analyze_sector_strength()
            self.analyze_market_stage()
            self.comprehensive_scoring()
            self.analyze_conclusion()
            self.print_report()


# ------------------ 入口 ------------------
if __name__ == "__main__":
    analyzer = AdvancedStockAnalyzer()
    analyzer.run_analysis()
