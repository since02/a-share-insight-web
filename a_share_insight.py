#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A-Share 多维度复盘  →  GitHub Pages 版
运行后自动生成  reports/index.html
"""
import os
import akshare as ak
import pandas as pd
import numpy as np
import time
import re
import pickle
from datetime import datetime, timedelta
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# ---------- 工具 ----------
CACHE_DIR = 'cache'
os.makedirs(CACHE_DIR, exist_ok=True)

def cache_pkl(name: str, func, *a, **kw):
    """简易缓存装饰器（天级）"""
    path = os.path.join(CACHE_DIR, f"{name}_{datetime.now():%Y%m%d}.pkl")
    if os.path.exists(path):
        return pickle.load(open(path, 'rb'))
    try:
        data = func(*a, **kw)
        pickle.dump(data, open(path, 'wb'))
        return data
    except Exception as e:
        print(f'  - 缓存失败 {name}: {e}')
        return pd.DataFrame()

# ---------- 核心类 ----------
class AdvancedStockAnalyzer:
    def __init__(self):
        self.data = {}
        self.analysis_result = {}
        self.stock_to_industry_map = {}
        self.run_mode = 'POST_MARKET'

    # ---------------- 数据获取 ----------------
    def fetch_data(self):
        print('开始获取基础数据...')
        try:
            # 1. 股票-行业映射
            self.stock_to_industry_map = cache_pkl(
                'industry_map', self._build_industry_map
            )

            # 2. 市场资金流
            self.data['market_fund_flow'] = ak.stock_market_fund_flow()
            self.data['industry_fund_flow'] = ak.stock_sector_fund_flow_rank(
                indicator='今日', sector_type='行业资金流'
            )

            # 3. 指数行情
            for name, symbol in [('sh', '000001'), ('sz', '399001')]:
                df = ak.index_zh_a_hist(symbol=symbol, period='daily', start_date='19900101')
                df = df.rename(columns={
                    '日期': 'date', '收盘': 'close', '开盘': 'open',
                    '最高': 'high', '最低': 'low', '成交额': 'amount', '涨跌幅': 'pct_chg'
                })
                self.data[f'{name}_index'] = df

            # 4. 国债ETF
            self.data['bond_etf'] = ak.fund_etf_hist_em(symbol='511260')

            # 5. 情绪指标
            self.data['market_activity'] = ak.stock_market_activity_legu()
            self.data['congestion'] = ak.stock_a_congestion_lg()
            self.data['rank_ljqs'] = ak.stock_rank_ljqs_ths()

            # 6. 个股快照 & 两融
            self.data['all_a_spot'] = ak.stock_zh_a_spot_em()
            self.data['sh_margin'] = ak.macro_china_market_margin_sh()
            self.data['sz_margin'] = ak.macro_china_market_margin_sz()

            return True
        except Exception as e:
            print(f'数据获取失败: {e}')
            return False

    def _build_industry_map(self):
        """股票→行业字典"""
        industry_df = ak.stock_board_industry_name_em()
        mapping = {}
        for industry in tqdm(industry_df['板块名称'], desc='构建行业映射'):
            try:
                cons = ak.stock_board_industry_cons_em(symbol=industry)
                for code in cons['代码']:
                    mapping[code] = industry
            except Exception:
                time.sleep(0.2)
        return mapping

    # ---------------- 分析 ----------------
    def analyze_market_liquidity(self):
        try:
            sh = self.data['sh_index'].iloc[-1]
            sh_y = self.data['sh_index'].iloc[-2]
            turnover = (sh['amount'] + sh_y['amount']) / 1e8
            main_in = self.data['market_fund_flow'].iloc[-1]['主力净流入-净额'] / 1e8
            self.analysis_result['liquidity'] = {
                'total_volume': f'{turnover:.2f}亿元',
                'main_net_inflow': f'{main_in:.2f}亿元'
            }
        except Exception as e:
            print('流动性分析失败:', e)
            self.analysis_result['liquidity'] = {}

    def analyze_sentiment(self):
        try:
            act = self.data['market_activity']
            up = act[act['item'] == '上涨']['value'].iloc[0]
            down = act[act['item'] == '下跌']['value'].iloc[0]
            profit = round(up / (up + down) * 100, 2)
            self.analysis_result['sentiment'] = {
                '综合情绪': '贪婪' if profit > 65 else '中性',
                '赚钱效应': f'{profit}%'
            }
        except Exception as e:
            print('情绪分析失败:', e)
            self.analysis_result['sentiment'] = {}

    def analyze_sector(self):
        try:
            df = self.data['industry_fund_flow'].copy()
            df['热力值'] = (df['今日主力净流入-净额'].rank(pct=True) * 100).round(2)
            self.analysis_result['sector_heat_map'] = df.sort_values('热力值', ascending=False)
        except Exception as e:
            print('板块分析失败:', e)
            self.analysis_result['sector_heat_map'] = pd.DataFrame()

    # ---------------- 生成报告 ----------------
    def print_report(self):
        report = []
        report.append('=' * 80)
        report.append(f"A-Share 每日复盘  {datetime.now():%Y-%m-%d %H:%M}")
        report.append('=' * 80)

        # 1. 市场阶段
        stage = self.analysis_result.get('market_stage', {})
        if stage:
            report.append(f"\n【市场阶段】{stage.get('stage_description', '暂无')}")

        # 2. 流动性 & 情绪
        liq = self.analysis_result.get('liquidity', {})
        sent = self.analysis_result.get('sentiment', {})
        report.append(f"\n【流动性】{liq.get('total_volume', '暂无')}")
        report.append(f"【情绪】{sent.get('综合情绪', '暂无')}  |  赚钱效应: {sent.get('赚钱效应', '暂无')}")

        # 3. 板块
        heat = self.analysis_result.get('sector_heat_map', pd.DataFrame())
        if not heat.empty:
            report.append("\n【板块热力 TOP5】")
            for _, row in heat.head(5).iterrows():
                report.append(f"  - {row['名称']}  热力值 {row['热力值']}")

        report.append("\n" + "=" * 80)
        report.append("免责声明: 仅供参考，不构成投资建议。")
        report.append("=" * 80)
        full_text = "\n".join(report)

        # 输出网页
        os.makedirs('reports', exist_ok=True)
        with open('reports/index.html', 'w', encoding='utf-8') as f:
            f.write(f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>A-Share 每日复盘</title>
<style>
body{{font-family:Consolas,monospace;font-size:14px;line-height:1.6;margin:2rem;}}
pre{{white-space:pre-wrap;word-break:break-all;}}
</style>
</head>
<body><pre>{full_text}</pre></body>
</html>""")
        print('报告已生成：reports/index.html')

    # ---------------- 运行入口 ----------------
    def run(self):
        print(f'--- 运行时间: {datetime.now():%H:%M:%S} ---')
        if self.fetch_data():
            self.analyze_market_liquidity()
            self.analyze_sentiment()
            self.analyze_sector()
            self.print_report()


# ---------------- 启动 ----------------
if __name__ == '__main__':
    AdvancedStockAnalyzer().run()
