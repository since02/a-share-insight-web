#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
腾讯接口版 - GitHub Actions 不断连
"""
import os
import requests
import pandas as pd
from datetime import datetime

# ---------- 工具 ----------
def get_tx_daily(market: str = 'sh'):
    """腾讯指数日K  market=sh 上证  sz 深证  返回近5日"""
    url = f'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={market}000001,day,,,5,qfq'
    r = requests.get(url, timeout=10).json()
    kline = r['data'][f'{market}000001']['day']
    df = pd.DataFrame(kline, columns=['day', 'open', 'close', 'high', 'low', 'volume'])
    df = df.astype({'close': float, 'volume': float})
    df['amount'] = df['volume'] * 1e4   # 手→金额近似
    return df[['day', 'close', 'amount']]

def get_up_down_tx():
    """腾讯涨跌家数"""
    url = 'http://web.ifzq.gtimg.cn/appstock/app/hq/get?type=adratio&callback='
    r = requests.get(url, timeout=10).json()
    up = r['data']['adratio']['up']
    down = r['data']['adratio']['down']
    return int(up), int(down)

def get_bk_tx():
    """腾讯板块涨跌（取前 5）"""
    url = 'http://web.ifzq.gtimg.cn/appstock/app/hq/get?type=bd&callback='
    r = requests.get(url, timeout=10).json()
    bk = pd.DataFrame(r['data']['bd'])
    bk = bk[['n', 'zd']].head(5)   # n=名称 zd=涨跌%
    bk.columns = ['name', 'zd']
    return bk

# ---------- 分析 ----------
def analyze():
    print(f'--- 运行时间: {datetime.now():%H:%M:%S} ---')
    sh_df = get_tx_daily('sh')
    sz_df = get_tx_daily('sz')
    up, down = get_up_down_tx()
    bk = get_bk_tx()

    sh_close = sh_df['close'].iloc[-1]
    sh_amount = sh_df['amount'].iloc[-1]

    report = []
    report.append('=' * 80)
    report.append(f'A-Share 每日复盘  {datetime.now():%Y-%m-%d %H:%M}')
    report.append('=' * 80)
    report.append(f'\n【大盘】上证最新 {sh_close:.2f}  成交额 {sh_amount/1e8:.1f} 亿')
    report.append(f'【涨跌】上涨 {up} 家  下跌 {down} 家')
    report.append('\n【腾讯板块涨跌 TOP5】')
    for _, r in bk.iterrows():
        report.append(f'  - {r["name"]}  {r["zd"]:.2f}%')
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
