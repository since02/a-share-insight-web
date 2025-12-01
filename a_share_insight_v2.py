#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
a_share_insight_v2.py  全面替代 AkShare 版
数据源：腾讯 + 新浪 + 东财  纯 requests
GitHub Actions 无风控，空数据保底
"""
import os
import requests
import pandas as pd
from datetime import datetime

CACHE_DIR = 'cache'
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs('reports', exist_ok=True)

# ---------- 工具 ----------
def cache_pkl(name: str, func, *a, **kw):
    """天级缓存装饰器"""
    path = os.path.join(CACHE_DIR, f"{name}_{datetime.now():%Y%m%d}.pkl")
    if os.path.exists(path):
        return pd.read_pickle(path)
    try:
        data = func(*a, **kw)
        data.to_pickle(path)
        return data
    except Exception as e:
        print(f'  - {name} 获取失败: {e}，返回空表')
        return pd.DataFrame()

# ① 指数日K + 成交额
def get_index_tx(market: str = 'sh'):
    url = f'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={market}000001,day,,,5,qfq'
    try:
        r = requests.get(url, timeout=10).json()
        kline = r['data'].get(f'{market}000001', {}).get('day', [])
        if not kline: raise RuntimeError('no kline')
        df = pd.DataFrame(kline, columns=['day', 'open', 'close', 'high', 'low', 'volume'])
        df = df.astype({'close': float, 'volume': float})
        df['amount'] = df['volume'] * 1e4   # 手→金额
        return df[['day', 'close', 'amount']]
    except Exception:
        # 缓存兜底
        cache_file = os.path.join(CACHE_DIR, f'tx_{market}.pkl')
        if os.path.exists(cache_file):
            return pd.read_pickle(cache_file)
        return pd.DataFrame(columns=['day', 'close', 'amount']).astype({'close': float, 'amount': float})

# ② 涨跌家数
def get_market_activity_tx():
    url = 'http://web.ifzq.gtimg.cn/appstock/app/hq/get?type=adratio&callback='
    try:
        r = requests.get(url, timeout=10).json()
        up = int(r['data']['adratio']['up'])
        down = int(r['data']['adratio']['down'])
        return up, down
    except Exception:
        return 0, 0

# ③ 板块涨跌
def get_sector_tx():
    url = 'http://web.ifzq.gtimg.cn/appstock/app/hq/get?type=bd&callback='
    try:
        r = requests.get(url, timeout=10).json()
        bk = pd.DataFrame(r['data']['bd'])[['n', 'zd']].head(10)
        bk.columns = ['name', 'zd']
        return bk
    except Exception:
        return pd.DataFrame(columns=['name', 'zd'])

# ④ 融资融券（新浪）
def get_margin_sina():
    try:
        # 沪市
        sh_url = 'https://vip.stock.finance.sina.com.cn/quotesService/view/qInfo.php?format=json&node=margin'
        sh = requests.get(sh_url, timeout=10).json()
        # 深市
        sz_url = 'https://vip.stock.finance.sina.com.cn/quotesService/view/qInfo.php?format=json&node=margin_sz'
        sz = requests.get(sz_url, timeout=10).json()
        total = float(sh['balance']) + float(sz['balance'])
        change = float(sh['change']) + float(sz['change'])
        return total / 1e8, change / 1e8
    except Exception:
        return 0.0, 0.0

# ⑤ 北向资金（东方财富）
def get_north_money_em():
    try:
        url = 'http://push2.eastmoney.com/api/qt/kamt.rtmin/get?fields1=f1,f3&fields2=f51,f53&ut=b2884a393a59ad64002292a3e90d46a5'
        r = requests.get(url, timeout=10).json()
        today = r['data']['s2n']
        return float(today) / 1e8   # 亿元
    except Exception:
        return 0.0

# ---------- 分析 ----------
def analyze():
    print(f'--- 运行时间: {datetime.now():%H:%M:%S} ---')
    # 1. 大盘
    sh_df = get_index_tx('sh')
    sz_df = get_index_tx('sz')
    # 2. 情绪
    up, down = get_market_activity_tx()
    # 3. 两融
    margin_bal, margin_chg = get_margin_sina()
    # 4. 北向
    north = get_north_money_em()
    # 5. 板块
    sector = get_sector_tx()

    # 保底值
    sh_close = sh_df['close'].iloc[-1] if not sh_df.empty else 0.0
    sh_amount = sh_df['amount'].iloc[-1] if not sh_df.empty else 0.0
    profit = round(up / (up + down) * 100, 2) if (up + down) > 0 else 50.0

    # 拼装报告
    report = []
    report.append('=' * 80)
    report.append(f'A-Share 每日复盘  {datetime.now():%Y-%m-%d %H:%M}')
    report.append('=' * 80)

    # 1. 大盘与成交
    report.append(f'\n【大盘】上证最新 {sh_close:.2f}  成交额 {sh_amount/1e8:.1f} 亿')
    report.append(f'【情绪】赚钱效应 {profit}%  （上涨 {up} 家  下跌 {down} 家）')
    report.append(f'【两融】余额 {margin_bal:.2f} 亿  较前日 {margin_chg:+.2f} 亿')
    report.append(f'【北向】净流入 {north:+.2f} 亿')

    # 2. 板块
    if not sector.empty:
        report.append('\n【板块涨跌 TOP10】')
        for _, r in sector.iterrows():
            report.append(f'  - {r["name"]}  {r["zd"]:+.2f}%')
    else:
        report.append('\n【板块】接口暂不可用')

    report.append('\n' + '=' * 80)
    report.append('免责声明: 仅供参考，不构成投资建议。')
    report.append('=' * 80)
    full_text = '\n'.join(report)

    # 写网页
    with open('reports/index.html', 'w', encoding='utf-8') as f:
        f.write(f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<title>A-Share 全面复盘</title>
<style>
body{{font-family:Consolas,monospace;font-size:14px;line-height:1.6;margin:2rem;}}
h3{{color:#444;}}
</style>
</head><body><pre>{full_text}</pre></body></html>""")
    print('全面报告已生成：reports/index.html')

# ---------- 入口 ----------
if __name__ == '__main__':
    analyze()
