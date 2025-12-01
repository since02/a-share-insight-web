#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新浪保底版 - 接口空也照常出报告
"""
import os
import requests
import pandas as pd
from datetime import datetime

# ---------- 工具 ----------
def get_sina_daily(symbol: str = 's_sh000001'):
    """新浪指数近5日，失败返回空表"""
    url = f'https://quotes.sina.cn/cn/api/jsonp.php/var_{symbol}=/CN_MarketDataService.getKLineData?symbol={symbol}&scale=240&ma=5&datalen=5'
    try:
        r = requests.get(url, timeout=10).text
        import re, json
        json_str = re.search(r'\((.*?)\)', r).group(1)
        data = json.loads(json_str)          # 可能 []
    except Exception:
        data = []
    # 保底：没数据也返回带字段的空表
    if not data:
        return pd.DataFrame(columns=['day', 'close', 'amount']).astype({'close': float, 'amount': float})
    df = pd.DataFrame(data)
    # 只拿前6列，防止字段变化
    df = df.iloc[:, :6]
    df.columns = ['day', 'open', 'close', 'high', 'low', 'volume']
    df = df.astype({'close': float, 'volume': float})
    df['amount'] = df['volume'] * 1e4    # 手→金额近似
    return df[['day', 'close', 'amount']]

def get_up_down_count():
    """新浪涨跌家数，失败返回 0,0"""
    try:
        url = 'https://vip.stock.finance.sina.com.cn/quotesService/view/qInfo.php?format=json&node=adratio'
        r = requests.get(url, timeout=10).json()
        return int(r['up']), int(r['down'])
    except Exception:
        return 0, 0

def get_main_fund():
    """同花顺板块，失败返回空表"""
    try:
        url = 'http://q.10jqka.com.cn/interface/stock/dhq/bk/page/dp/order/zdf/desc/1/'
        data = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10).json()
        df = pd.DataFrame(data['data'])
        df['zdf'] = pd.to_numeric(df['zdf'])
        df['zljlr'] = pd.to_numeric(df['zljlr'])
        return df[['name', 'zdf', 'zljlr']].head(20)
    except Exception:
        return pd.DataFrame(columns=['name', 'zdf', 'zljlr'])

# ---------- 分析 ----------
def analyze():
    print(f'--- 运行时间: {datetime.now():%H:%M:%S} ---')
    sh_df = get_sina_daily('s_sh000001')
    sz_df = get_sina_daily('s_sz399001')
    up, down = get_up_down_count()
    ban = get_main_fund()

    # 保底值
    sh_close = sh_df['close'].iloc[-1] if not sh_df.empty else 0.0
    sh_amount = sh_df['amount'].iloc[-1] if not sh_df.empty else 0.0

    report = []
    report.append('=' * 80)
    report.append(f'A-Share 每日复盘  {datetime.now():%Y-%m-%d %H:%M}')
    report.append('=' * 80)
    report.append(f'\n【大盘】上证最新 {sh_close:.2f}  成交额 {sh_amount/1e8:.1f} 亿')
    report.append(f'【涨跌】上涨 {up} 家  下跌 {down} 家')
    if not ban.empty:
        report.append('\n【板块主力净流入 TOP5】')
        for _, r in ban.head(5).iterrows():
            report.append(f'  - {r["name"]}  {r["zdf"]:.2f}%  {r["zljlr"]/1e8:.1f} 亿')
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
