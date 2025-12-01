#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AkShare 被断 → 改用东方财富 + 新浪 纯 requests 版
GitHub Actions 直接跑，无需本地
"""
import os
import requests
import pandas as pd
from datetime import datetime

# ---------- 工具 ----------
def get_index_daily(secid: str, limit: int = 120):
    """东方财富指数日K  secid=1.000001 上证  0.399001 深证"""
    url = (f'https://push2his.eastmoney.com/api/qt/stock/kline/get?'
           f'secid={secid}&klt=101&fqt=1&lmt={limit}')
    data = requests.get(url, timeout=10).json()['data']['klines']
    df = pd.DataFrame([r.split(',') for r in data],
                      columns=['date', 'open', 'close', 'high', 'low', 'amount', '_'])
    df = df[['date', 'close', 'amount']].astype({'close': float, 'amount': float})
    return df

def get_up_down_count():
    """新浪实时涨跌家数"""
    url = 'https://vip.stock.finance.sina.com.cn/quotesService/view/qInfo.php?format=json&node=adratio'
    r = requests.get(url, timeout=10).json()
    return int(r['up']), int(r['down'])

def get_main_fund():
    """同花顺板块主力净流入 TOP20（含金额）"""
    url = 'http://q.10jqka.com.cn/interface/stock/dhq/bk/page/dp/order/zdf/desc/1/'
    data = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10).json()
    df = pd.DataFrame(data['data'])
    df['zdf'] = pd.to_numeric(df['zdf'])
    df['zljlr'] = pd.to_numeric(df['zljlr'])
    return df[['name', 'zdf', 'zljlr']].head(20)

# ---------- 分析 ----------
def analyze():
    print(f'--- 运行时间: {datetime.now():%H:%M:%S} ---')
    sh_df = get_index_daily('1.000001', 5)      # 上证 5 日
    sz_df = get_index_daily('0.399001', 5)      # 深证 5 日
    up, down = get_up_down_count()
    ban_kuai = get_main_fund()

    # 拼报告
    report = []
    report.append('=' * 80)
    report.append(f'A-Share 每日复盘  {datetime.now():%Y-%m-%d %H:%M}')
    report.append('=' * 80)
    report.append(f'\n【大盘】上证最新 {sh_df["close"].iloc[-1]:.2f}  成交额 {sh_df["amount"].iloc[-1]/1e8:.1f} 亿')
    report.append(f'【涨跌】上涨 {up} 家  下跌 {down} 家')
    report.append('\n【板块主力净流入 TOP5】')
    for _, r in ban_kuai.head(5).iterrows():
        report.append(f'  - {r["name"]}  {r["zdf"]:.2f}%  {r["zljlr"]/1e8:.1f} 亿')
    report.append('\n' + '=' * 80)
    full_text = '\n'.join(report)

    # 写网页
    os.makedirs('reports', exist_ok=True)
    with open('reports/index.html', 'w', encoding='utf-8') as f:
        f.write(f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<title>A-Share Report</title>
<style>body{{font-family:Consolas,monospace;font-size:14px;line-height:1.6;margin:2rem;}}</style>
</head><body><pre>{full_text}</pre></body></html>""")
    print('报告已生成：reports/index.html')

# ---------- 入口 ----------
if __name__ == '__main__':
    analyze()
