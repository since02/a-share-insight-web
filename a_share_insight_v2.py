#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
腾讯备用字段 + 本地缓存兜底版
GitHub Actions 不断连，无数据也出报告
"""
import os
import requests
import pandas as pd
from datetime import datetime

CACHE_DIR = 'cache'
os.makedirs(CACHE_DIR, exist_ok=True)

# ---------- 工具 ----------
def get_tx_daily_cached(market: str = 'sh'):
    """腾讯指数日K，失败读本地缓存"""
    cache_file = os.path.join(CACHE_DIR, f'tx_{market}.pkl')
    url = f'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={market}000001,day,,,5,qfq'
    try:
        r = requests.get(url, timeout=10).json()
        # 字段可能变化，用 try 拿
        kline = r['data'].get(f'{market}000001', {}).get('day', [])
        if not kline:
            raise RuntimeError('no kline')
        df = pd.DataFrame(kline, columns=['day', 'open', 'close', 'high', 'low', 'volume'])
        df = df.astype({'close': float, 'volume': float})
        df['amount'] = df['volume'] * 1e4
        df = df[['day', 'close', 'amount']]
        # 写缓存
        df.to_pickle(cache_file)
        return df
    except Exception:
        # 读缓存保底
        if os.path.exists(cache_file):
            return pd.read_pickle(cache_file)
        # 缓存也没有，返回空表
        return pd.DataFrame(columns=['day', 'close', 'amount']).astype({'close': float, 'amount': float})

def get_up_down_tx():
    """腾讯涨跌家数，失败返回 0,0"""
    try:
        url = 'http://web.ifzq.gtimg.cn/appstock/app/hq/get?type=adratio&callback='
        r = requests.get(url, timeout=10).json()
        up = r['data']['adratio']['up']
        down = r['data']['adratio']['down']
        return int(up), int(down)
    except Exception:
        return 0, 0

def get_bk_tx():
    """腾讯板块涨跌前5，失败返回空表"""
    try:
        url = 'http://web.ifzq.gtimg.cn/appstock/app/hq/get?type=bd&callback='
        r = requests.get(url, timeout=10).json()
        bk = pd.DataFrame(r['data']['bd'])
        bk = bk[['n', 'zd']].head(5)
        bk.columns = ['name', 'zd']
        return bk
    except Exception:
        return pd.DataFrame(columns=['name', 'zd'])

# ---------- 分析 ----------
def analyze():
    print(f'--- 运行时间: {datetime.now():%H:%M:%S} ---')
    sh_df = get_tx_daily_cached('sh')
    sz_df = get_tx_daily_cached('sz')
    up, down = get_up_down_tx()
    bk = get_bk_tx()

    # 保底值
    sh_close = sh_df['close'].iloc[-1] if not sh_df.empty else 0.0
    sh_amount = sh_df['amount'].iloc[-1] if not sh_df.empty else 0.0

    report = []
    report.append('=' * 80)
    report.append(f'A-Share 每日复盘  {datetime.now():%Y-%m-%d %H:%M}')
    report.append('=' * 80)
    report.append(f'\n【大盘】上证最新 {sh_close:.2f}  成交额 {sh_amount/1e8:.1f} 亿')
    report.append(f'【涨跌】上涨 {up} 家  下跌 {down} 家')
    if not bk.empty:
        report.append('\n【腾讯板块涨跌 TOP5】')
        for _, r in bk.iterrows():
            report.append(f'  - {r["name"]}  {r["zd"]:.2f}%')
    else:
        report.append('\n【板块】接口暂不可用')
    report.append('\n' + '=' * 80)
    full_text = '\n'.join(report)

    os.makedirs('reports', exist_ok=True)
    with open('reports/index.html', 'w', encoding='utf-8') as f:
        f.write(f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<title>A-Share Report</title>
<style>body{{font-family:Consolas,monospace;font-size:14px;line-height:1.6;margin:2rem;}}</style>
</head><body><pre>{full_text}</pre></body></html>""")
    print('报告已生成：reports/index.html')

if __name__ == '__main__':
    analyze()
