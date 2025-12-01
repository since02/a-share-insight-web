#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI 决策 + ETF 评分 全面替代版
数据源：腾讯/新浪/东财 纯 requests
"""
import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime

CACHE_DIR = 'cache'
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs('reports', exist_ok=True)

# ---------- 工具 ----------
def cache_pkl(name: str, func, *a, **kw):
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

# ① 指数日线
def get_index_tx(market: str = 'sh'):
    url = f'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={market}000001,day,,,5,qfq'
    try:
        r = requests.get(url, timeout=10).json()
        kline = r['data'].get(f'{market}000001', {}).get('day', [])
        if not kline: raise RuntimeError('no kline')
        df = pd.DataFrame(kline, columns=['day', 'open', 'close', 'high', 'low', 'volume'])
        df = df.astype({'close': float, 'volume': float})
        df['amount'] = df['volume'] * 1e4
        return df[['day', 'close', 'amount']]
    except Exception:
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

# ③ 板块
def get_sector_tx():
    url = 'http://web.ifzq.gtimg.cn/appstock/app/hq/get?type=bd&callback='
    try:
        r = requests.get(url, timeout=10).json()
        bk = pd.DataFrame(r['data']['bd'])[['n', 'zd']].head(10)
        bk.columns = ['name', 'zd']
        return bk
    except Exception:
        return pd.DataFrame(columns=['name', 'zd'])

# ④ 两融
def get_margin_sina():
    try:
        sh = requests.get('https://vip.stock.finance.sina.com.cn/quotesService/view/qInfo.php?format=json&node=margin', timeout=10).json()
        sz = requests.get('https://vip.stock.finance.sina.com.cn/quotesService/view/qInfo.php?format=json&node=margin_sz', timeout=10).json()
        total = float(sh['balance']) + float(sz['balance'])
        change = float(sh['change']) + float(sz['change'])
        return total / 1e8, change / 1e8
    except Exception:
        return 0.0, 0.0

# ⑤ 北向
def get_north_money_em():
    try:
        url = 'http://push2.eastmoney.com/api/qt/kamt.rtmin/get?fields1=f1,f3&fields2=f51,f53&ut=b2884a393a59ad64002292a3e90d46a5'
        r = requests.get(url, timeout=10).json()
        today = r['data']['s2n']
        return float(today) / 1e8
    except Exception:
        return 0.0

# ⑥ ETF 技术面评分（模拟）
def get_etf_tech():
    """模拟 30 只主流 ETF 技术打分"""
    etf_list = [
        ('510300', '沪深300ETF'),
        ('510500', '中证500ETF'),
        ('512880', '证券ETF'),
        ('512480', '半导体ETF'),
        ('515790', '光伏ETF'),
        ('512690', '酒ETF'),
        ('512760', '芯片ETF'),
        ('512000', '券商ETF'),
        ('512170', '医疗ETF'),
        ('512010', '医药ETF'),
        ('515030', '新能源车ETF'),
        ('512660', '军工ETF'),
        ('512980', '传媒ETF'),
        ('512800', '银行ETF'),
        ('512700', '银行龙头ETF'),
        ('515220', '煤炭ETF'),
        ('512400', '有色ETF'),
        ('512200', '房地产ETF'),
        ('512880', '证券ETF'),
        ('515210', '钢铁ETF'),
        ('512010', '医药ETF'),
        ('512170', '医疗ETF'),
        ('515030', '新能源车ETF'),
        ('512660', '军工ETF'),
        ('512980', '传媒ETF'),
        ('512800', '银行ETF'),
        ('512700', '银行龙头ETF'),
        ('515220', '煤炭ETF'),
        ('512400', '有色ETF'),
        ('512200', '房地产ETF'),
    ]
    records = []
    for code, name in etf_list:
        try:
            # 腾讯 ETF 行情
            url = f'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code},day,,,21,qfq'
            r = requests.get(url, timeout=5).json()
            kline = r['data'][code]['day']
            df = pd.DataFrame(kline, columns=['day', 'open', 'close', 'high', 'low', 'volume']).astype({'close': float})
            if len(df) < 21:
                raise RuntimeError('too short')
            # 简单技术打分
            close = df['close'].iloc[-1]
            ma5 = df['close'].iloc[-5:].mean()
            ma20 = df['close'].iloc[-20:].mean()
            score = 5.0
            if close > ma20 and ma5 > ma20:
                score = 4.5 if close > ma5 else 3.5
            elif close < ma20:
                score = 2.0
            records.append({'code': code, 'name': name, 'close': close, 'score': score})
        except Exception:
            records.append({'code': code, 'name': name, 'close': 0.0, 'score': 2.0})
    return pd.DataFrame(records).sort_values('score', ascending=False)

# ⑦ AI 复盘（模拟 JSON 结论）
def ai_conclusion(data: dict):
    """假装调用火山，返回结构化 JSON"""
    # 这里用规则模拟，后续可替换为真实 chat_volces 调用
    liquidity = data['liquidity']
    sentiment = data['sentiment']
    north = data['north']
    margin = data['margin']

    core = {
        "核心矛盾解读": {
            "量价背离": f"总成交 {liquidity:.1f} 亿，较昨日小幅变化，主力分歧加大",
            "多空博弈": f"赚钱效应 {sentiment:.1f}%，北向资金{north:+.1f}亿，杠杆资金{margin:+.1f}亿",
            "风格割裂": "科技板块强势，金融权重承压，市场呈结构性分化",
            "技术面冲突": "指数站上5日线但MACD顶背离，短期震荡需求增加"
        },
        "操作建议": {
            "仓位管理": "维持60%中性仓位，保留10%现金应对波动",
            "持仓结构调整": {
                "增持方向": "半导体、光伏、军工等趋势板块回踩5日线机会",
                "减持方向": "融资余额占比超2.5%的高杠杆品种及破位金融股"
            },
            "风险对冲": "可配置10%仓位的国债ETF对冲波动风险",
            "关键观察点": [
                "关注科创50能否守住5日线",
                "跟踪两市融资余额单日变化是否超±100亿",
                "北向资金连续3日流出需警惕"
            ]
        },
        "情景推演": {
            "标题": "明日走势推演",
            "基准情景": "60%概率维持3350-3400区间震荡，量能回落至3500亿以下",
            "乐观情景": "30%概率放量突破3420点（需成交超4500亿且北向+80亿）",
            "悲观情景": "10%概率跌破3330点引发技术抛盘（关注券商是否领跌）"
        }
    }
    return core

# ---------- 分析 ----------
def analyze():
    print(f'--- 运行时间: {datetime.now():%H:%M:%S} ---')
    # 1. 大盘
    sh_df = get_index_tx('sh')
    sz_df = get_index_tx('sz')
    # 2. 情绪
    up, down = get_market_activity_tx()
    profit = round(up / (up + down) * 100, 2) if (up + down) > 0 else 50.0
    # 3. 两融
    margin_bal, margin_chg = get_margin_sina()
    # 4. 北向
    north = get_north_money_em()
    # 5. 板块
    sector = get_sector_tx()
    # 6. ETF 技术评分
    etf_rank = get_etf_tech()
    # 7. AI 结论
    ai_data = {
        'liquidity': (sh_df['amount'].iloc[-1] if not sh_df.empty else 0) / 1e8,
        'sentiment': profit,
        'north': north,
        'margin': margin_chg
    }
    ai_json = ai_conclusion(ai_data)

    # 保底值
    sh_close = sh_df['close'].iloc[-1] if not sh_df.empty else 0.0
    sh_amount = sh_df['amount'].iloc[-1] if not sh_df.empty else 0.0

    # 拼装全文
    report = []
    report.append('=' * 80)
    report.append(f'A-Share 全面复盘+AI决策  {datetime.now():%Y-%m-%d %H:%M}')
    report.append('=' * 80)

    # 1. 市场阶段 & AI 结论
    report.append('\n一、AI 结构化决策（模拟）')
    report.append(f"【核心矛盾】{ai_json['核心矛盾解读']['多空博弈']}")
    report.append(f"【操作建议】{ai_json['操作建议']['仓位管理']}")
    report.append(f"【情景推演】{ai_json['情景推演']['基准情景']}")

    # 2. 纯数据
    report.append(f'\n二、纯数据速览')
    report.append(f'【大盘】上证 {sh_close:.2f}  成交额 {sh_amount/1e8:.1f} 亿')
    report.append(f'【情绪】赚钱效应 {profit}%  （上涨 {up} 家  下跌 {down} 家）')
    report.append(f'【两融】余额 {margin_bal:.2f} 亿  较前日 {margin_chg:+.2f} 亿')
    report.append(f'【北向】净流入 {north:+.2f} 亿')

    # 3. 板块
    if not sector.empty:
        report.append('\n三、板块涨跌 TOP10')
        for _, r in sector.iterrows():
            report.append(f'  - {r["name"]}  {r["zd"]:+.2f}%')
    else:
        report.append('\n三、板块 接口暂不可用')

    # 4. ETF 评分
    if not etf_rank.empty:
        report.append('\n四、ETF 技术评分（TOP 10）')
        for _, r in etf_rank.head(10).iterrows():
            report.append(f'  - {r["name"]}({r["code"]})  得分 {r["score"]:.1f}  现价 {r["close"]:.2f}')

    report.append('\n' + '=' * 80)
    report.append('免责声明: 仅供参考，不构成投资建议。')
    report.append('=' * 80)
    full_text = '\n'.join(report)

    # 写网页
    with open('reports/index.html', 'w', encoding='utf-8') as f:
        f.write(f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<title>A-Share 全面复盘+AI决策</title>
<style>
body{{font-family:Consolas,monospace;font-size:14px;line-height:1.6;margin:2rem;}}
h3{{color:#444;}}
</style>
</head><body><pre>{full_text}</pre></body></html>""")
    print('全面复盘+AI 报告已生成：reports/index.html')

# ---------- 入口 ----------
if __name__ == '__main__':
    analyze()
