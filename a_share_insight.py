import akshare as ak
import pandas as pd
import numpy as np
import time
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import os
import pickle
 
try:
    from volces_chat import chat_volces
 
    AI_TOOL_AVAILABLE = True
except ImportError:
    AI_TOOL_AVAILABLE = False
 
from tqdm import tqdm
 
# 关闭不必要的警告
import warnings
 
warnings.filterwarnings('ignore')
 
DEBUG_MODE = True # 开启调试模式，会打印每个接口的返回列名
 
def debug_print(df, name): 
    if DEBUG_MODE:
        print(f"\n--- 调试信息: {name} ---")
        if df is not None and not df.empty:
            print("列名:", df.columns.tolist())
            print("数据预览:")
            print(df.head(3))
        else:
            print("数据为空或不存在。")
        print("-" * (20 + len(name)))
 
_cache = {}
 
def cache_data(filename, data_func, *args, frequency='hourly', **kwargs):
    """
    缓存函数，支持小时级或天级缓存。
    frequency: 'hourly' 或 'daily'
    """
    if frequency == 'hourly':
        time_str = datetime.now().strftime('%Y%m%d%H')
    else: # daily
        time_str = datetime.now().strftime('%Y%m%d')
 
    params_str = "_".join(map(str, args)) + "_".join(f"{k}_{v}" for k, v in kwargs.items())
    unique_filename = f"{filename}_{params_str}_{time_str}.pkl"
    filepath = os.path.join('../cache', unique_filename)
 
    if filepath in _cache:
        return _cache[filepath]
 
    if not os.path.exists('../cache'):
        os.makedirs('../cache')
    if os.path.exists(filepath):
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
            _cache[filepath] = data
            return data
 
    try:
        data = data_func(*args, **kwargs)
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)
        _cache[filepath] = data
        return data
    except Exception as e:
        print(f"  - 获取 {filename} 数据失败: {e}，返回空DataFrame")
        return pd.DataFrame()
 
def get_incremental_daily_data(base_filename, data_func, date_col_name, *args, **kwargs):
    """
    为日线历史数据提供增量更新的缓存策略。
    """
    base_filepath = os.path.join('../cache', f"{base_filename}_base.pkl")
 
    if not os.path.exists('../cache'):
        os.makedirs('../cache')
 
    if os.path.exists(base_filepath):
        with open(base_filepath, 'rb') as f:
            df_base = pickle.load(f)
 
        last_date = pd.to_datetime(df_base[date_col_name]).max()
        start_date = (last_date + timedelta(days=1)).strftime('%Y%m%d')
        today_str = datetime.now().strftime('%Y%m%d')
 
        if start_date <= today_str:
            print(f"  - 增量更新 {base_filename} 数据从 {start_date} 开始...")
            try:
                # 传递 start_date 给 akshare 函数
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
            # 首次获取，从一个较早的日期开始
            kwargs['start_date'] = '19900101'
            df_full = data_func(*args, **kwargs)
            with open(base_filepath, 'wb') as f:
                pickle.dump(df_full, f)
            return df_full
        except Exception as e:
            print(f"  - 全量获取失败: {e}")
            return pd.DataFrame()
 
def clean_old_cache():
    """清理过期的小时级和天级缓存文件，保留基底文件。"""
    if not os.path.exists('../cache'):
        return
 
    print("步骤 0/11: 清理过期缓存...")
    current_hour_str = datetime.now().strftime('%Y%m%d%H')
    current_day_str = datetime.now().strftime('%Y%m%d')
 
    for filename in os.listdir('../cache'):
        if '_base.pkl' in filename:
            continue
        match_hourly = re.search(r'_(\d{10})\.pkl$', filename)
        if match_hourly and match_hourly.group(1) != current_hour_str:
            os.remove(os.path.join('../cache', filename))
            if DEBUG_MODE: print(f"  - 已删除过期小时缓存: {filename}")
            continue
        match_daily = re.search(r'_(\d{8})\.pkl$', filename)
        if match_daily and match_daily.group(1) != current_day_str:
            os.remove(os.path.join('../cache', filename))
            if DEBUG_MODE: print(f"  - 已删除过期每日缓存: {filename}")
    print("  - 缓存清理完成。")
 
 
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
 
    def _build_stock_industry_map(self):
        """构建股票代码到行业名称的精确映射字典"""
        print("步骤 1/11: 构建股票与行业的精确映射...")
        try:
            self.stock_to_industry_map = cache_data("stock_industry_map", self._fetch_full_industry_map, frequency='daily')
            print(f"  - 股票行业映射构建完成，共映射 {len(self.stock_to_industry_map)} 只股票。")
        except Exception as e:
            print(f"  - 构建股票行业映射失败: {e}")
 
    def _fetch_full_industry_map(self):
        """遍历所有行业板块获取其成分股，构建完整映射"""
        all_industries = ak.stock_board_industry_name_em()
        full_map = {}
        for industry_name in tqdm(all_industries['板块名称'], desc="  - 正在遍历行业板块"):
            try:
                cons_df = ak.stock_board_industry_cons_em(symbol=industry_name)
                for code in cons_df['代码']:
                    full_map[code] = industry_name
            except Exception:
                time.sleep(0.5); continue
        return full_map
 
    def fetch_data(self):
        """获取所有需要的基础数据"""
        print("开始获取基础数据...")
        try:
            self._build_stock_industry_map()
 
            print("步骤 2/11: 获取市场总体资金流历史...")
            self.data['market_fund_flow'] = ak.stock_market_fund_flow()
            debug_print(self.data['market_fund_flow'], "市场总体资金流")
 
            print("步骤 3/11: 获取行业板块当日资金流...")
            self.data['industry_fund_flow'] = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
 
            print("步骤 4/11: 获取大盘与国债历史行情 (增量更新)...")
            # 上证指数
            sh_index_df = get_incremental_daily_data("sh_index", ak.index_zh_a_hist, '日期', symbol="000001", period="daily")
            sh_index_df.rename(columns={'收盘': 'close', '开盘': 'open', '最高': 'high', '最低': 'low', '成交额': 'amount', '涨跌幅': 'pct_chg'}, inplace=True)
            self.data['sh_index'] = sh_index_df
 
            # 深证成指
            sz_index_df = get_incremental_daily_data("sz_index", ak.index_zh_a_hist, '日期', symbol="399001", period="daily")
            sz_index_df.rename(columns={'收盘': 'close', '开盘': 'open', '最高': 'high', '最低': 'low', '成交额': 'amount', '涨跌幅': 'pct_chg'}, inplace=True)
            self.data['sz_index'] = sz_index_df
 
            self.data['bond_etf'] = cache_data("bond_etf", ak.fund_etf_hist_em, symbol="511260", frequency='daily')
            debug_print(self.data['sh_index'], "上证指数历史行情")
            debug_print(self.data['sz_index'], "深证成指历史行情")
 
            # print("步骤 5/11: 获取ETF列表并筛选...")
            # etf_spot = cache_data("etf_spot", ak.fund_etf_spot_em)
            # foreign_keywords = ['纳斯达克', '纳指', '标普', '日经', '德国', '法国', '印度', '沙特', '美国', '海外', '全球', '原油', '黄金']
            # mask_to_drop = etf_spot['名称'].str.contains('|'.join(foreign_keywords), na=False)
            # etf_spot = etf_spot[~mask_to_drop]
            # self.data['etf_spot'] = etf_spot[etf_spot['成交额'] > 50000000]
 
            print("步骤 6/11: 获取市场情绪数据(赚钱效应)...")
            self.data['market_activity'] = cache_data("market_activity", ak.stock_market_activity_legu)
 
            print("步骤 7/11: 获取市场情绪数据(拥挤度)...")
            self.data['congestion'] = cache_data("congestion", ak.stock_a_congestion_lg, frequency='daily')
 
            print("步骤 8/11: 获取市场热点数据(量价齐升)...")
            self.data['rank_ljqs'] = cache_data("rank_ljqs", ak.stock_rank_ljqs_ths)
 
            print("步骤 9/11: 获取A股全体数据(用于换手率和杠杆率计算)...")
            self.data['all_a_spot'] = cache_data("all_a_spot", ak.stock_zh_a_spot_em)
            debug_print(self.data['all_a_spot'], "A股全体实时数据")
 
            print("步骤 10/11: 获取两市融资融券数据...")
            sh_margin_df = cache_data("sh_margin", ak.macro_china_market_margin_sh, frequency='daily')
            sz_margin_df = cache_data("sz_margin", ak.macro_china_market_margin_sz, frequency='daily')
            self.data['sh_margin'] = sh_margin_df
            self.data['sz_margin'] = sz_margin_df
            debug_print(self.data['sh_margin'].tail(), "沪市融资融券余额")
            debug_print(self.data['sz_margin'].tail(), "深市融资融券余额")
 
            # print(f"\n基础数据获取完成。筛选出 {len(self.data['etf_spot'])} 个活跃ETF进行分析。")
            return True
        except Exception as e:
            print(f"\n数据获取过程中出现严重错误: {e}")
            return False
 
    def _get_elapsed_trading_minutes(self):
        """计算当天已过的交易分钟数"""
        now = datetime.now()
        if now.time() < datetime(2000, 1, 1, 9, 30).time(): return 0
        if now.time() <= datetime(2000, 1, 1, 11, 30).time():
            start = now.replace(hour=9, minute=30, second=0, microsecond=0)
            return (now - start).total_seconds() / 60
        if now.time() < datetime(2000, 1, 1, 13, 0).time(): return 120
        if now.time() <= datetime(2000, 1, 1, 15, 0).time():
            start = now.replace(hour=13, minute=0, second=0, microsecond=0)
            return 120 + (now - start).total_seconds() / 60
        return 240
 
    def analyze_market_liquidity(self):
        print("正在分析市场流动性与主力行为...")
        try:
            mff = self.data.get('market_fund_flow')
            sh_index = self.data.get('sh_index')
            sz_index = self.data.get('sz_index')
            if mff is None or mff.empty or sh_index is None or sh_index.empty or sz_index is None or sz_index.empty:
                raise ValueError("市场资金流或大盘历史行情数据为空")
 
            total_turnover = (sh_index.iloc[-1]['amount'] + sz_index.iloc[-1]['amount']) / 1e8
            yesterday_turnover = (sh_index.iloc[-2]['amount'] + sz_index.iloc[-2]['amount']) / 1e8
 
            volume_analysis_turnover = total_turnover
            estimated_turnover_str = ""
            volume_desc_prefix = ""
 
            if self.run_mode != 'POST_MARKET':
                elapsed_minutes = self._get_elapsed_trading_minutes()
                if elapsed_minutes > 0:
                    estimated_turnover = total_turnover * (240 / elapsed_minutes)
                    volume_analysis_turnover = estimated_turnover
                    volume_desc_prefix = "预估"
                    estimated_turnover_str = f" (预估全天: {estimated_turnover:.2f}亿元)"
 
            latest_flow = mff.iloc[-1]
            main_net_inflow = latest_flow['主力净流入-净额'] / 1e8
            retail_net_inflow = latest_flow['小单净流入-净额'] / 1e8
 
            avg_5d_turnover = (sh_index.iloc[-6:-1]['amount'].mean() + sz_index.iloc[-6:-1]['amount'].mean()) / 1e8
            volume_level = "高于5日均量" if volume_analysis_turnover > avg_5d_turnover else "低于5日均量"
 
            volume_change = volume_analysis_turnover - yesterday_turnover
            volume_change_desc = f"{volume_desc_prefix}缩量 {abs(volume_change):.2f}亿" if volume_change < 0 else f"{volume_desc_prefix}放量 {volume_change:.2f}亿"
 
            AVG_TURNOVER = 10000
            if volume_analysis_turnover < AVG_TURNOVER * 0.7: volume_qualitative_level = "地量水平"
            elif volume_analysis_turnover <= AVG_TURNOVER * 1.5: volume_qualitative_level = "平量水平"
            elif volume_analysis_turnover <= AVG_TURNOVER * 2.5: volume_qualitative_level = "天量水平"
            else: volume_qualitative_level = "巨量水平"
 
            inflow_percentage = (main_net_inflow / volume_analysis_turnover) * 100 if volume_analysis_turnover > 0 else 0
 
            self.analysis_result['liquidity'] = {
                'total_volume': f"{total_turnover:.2f}亿元", 'estimated_turnover_str': estimated_turnover_str,
                'volume_level': volume_level, 'volume_change_desc': volume_change_desc,
                'main_net_inflow': f"{main_net_inflow:.2f}亿元", 'retail_net_inflow': f"{retail_net_inflow:.2f}亿元",
                'inflow_percentage': inflow_percentage,
                'volume_qualitative_level': volume_qualitative_level
            }
        except Exception as e:
            print(f"  - 市场流动性分析失败: {e}")
            self.analysis_result['liquidity'] = {}
        print("市场流动性与主力行为分析完成。")
 
    def analyze_market_turnover(self):
        print("正在分析市场换手率...")
        try:
            all_a_spot = self.data.get('all_a_spot')
            if all_a_spot is None or all_a_spot.empty: raise ValueError("A股实时行情数据为空")
            valid_stocks = all_a_spot[(all_a_spot['流通市值'] > 0) & (all_a_spot['换手率'] > 0)]
            weighted_turnover = (valid_stocks['换手率'] * valid_stocks['流通市值']).sum() / valid_stocks['流通市值'].sum()
            turnover_level = "极高(过热)" if weighted_turnover > 3.5 else "较高(活跃)" if weighted_turnover > 2.0 else "中等(温和)" if weighted_turnover > 1.0 else "较低(谨慎)"
            self.analysis_result['turnover'] = {"market_turnover_rate": f"{weighted_turnover:.2f}%", "turnover_level": turnover_level}
        except Exception as e:
            print(f"  - 市场换手率分析失败: {e}")
            self.analysis_result['turnover'] = {}
        print("市场换手率分析完成。")
 
    def analyze_margin_trading(self):
        """分析市场杠杆率(融资余额/总流通市值)"""
        print("正在分析市场杠杆率...")
        try:
            sh_margin = self.data.get('sh_margin')
            sz_margin = self.data.get('sz_margin')
            all_a_spot = self.data.get('all_a_spot')
            if sh_margin is None or sh_margin.empty or sz_margin is None or sz_margin.empty or all_a_spot is None or all_a_spot.empty:
                raise ValueError("融资融券或A股实时行情数据为空")
 
            # 计算总融资余额
            latest_sh_balance = sh_margin.iloc[-1]['融资余额']
            latest_sz_balance = sz_margin.iloc[-1]['融资余额']
            total_margin_balance = latest_sh_balance + latest_sz_balance
 
            # 计算融资余额变化
            prev_sh_balance = sh_margin.iloc[-2]['融资余额']
            prev_sz_balance = sz_margin.iloc[-2]['融资余额']
            prev_total_margin_balance = prev_sh_balance + prev_sz_balance
            change = (total_margin_balance - prev_total_margin_balance) / 1e8
            change_desc = f"净买入 {change:.2f}亿元" if change > 0 else f"净偿还 {abs(change):.2f}亿元"
 
            # 计算总流通市值
            total_circulating_market_cap = all_a_spot['流通市值'].sum()
 
            # 计算杠杆率
            leverage_ratio = (total_margin_balance / total_circulating_market_cap) * 100 if total_circulating_market_cap > 0 else 0
 
            # 对杠杆率进行定性描述
            if leverage_ratio < 1.8:
                leverage_level = "较低"
            elif 1.8 <= leverage_ratio < 2.2:
                leverage_level = "中等"
            elif 2.2 <= leverage_ratio < 2.5:
                leverage_level = "偏高"
            else: # >= 2.5%
                leverage_level = "风险区"
 
            self.analysis_result['margin_trading'] = {
                "total_balance": f"{total_margin_balance / 1e8:.2f}亿元",
                "change_desc": change_desc,
                "leverage_ratio": f"{leverage_ratio:.2f}%",
                "leverage_level": leverage_level
            }
        except Exception as e:
            print(f"  - 市场杠杆率分析失败: {e}")
            self.analysis_result['margin_trading'] = {}
        print("市场杠杆率分析完成。")
 
    def analyze_intermarket_relationship(self):
        print("正在分析股债关系与大盘趋势...")
        try:
            sh_df = self.data.get('sh_index')
            bond_df = self.data.get('bond_etf')
            if sh_df is None or sh_df.empty or bond_df is None or bond_df.empty: raise ValueError("大盘或国债行情数据为空")
 
            sh_latest = sh_df.iloc[-1]
            sh_ma5 = sh_df['close'].iloc[-5:].mean()
            market_trend = "站上5日线" if sh_latest['close'] > sh_ma5 else "跌破5日线"
            last_60_days = sh_df.iloc[-60:]
            high_60d, low_60d = last_60_days['high'].max(), last_60_days['low'].min()
            position = (sh_latest['close'] - low_60d) / (high_60d - low_60d) if (high_60d - low_60d) > 0 else 0.5
            position_desc = "高位区域" if position > 0.8 else "低位区域" if position < 0.2 else "震荡中枢"
            bond_latest = bond_df.iloc[-1]
            relation = "分歧"
            if sh_latest['pct_chg'] > 0.2 and bond_latest['涨跌幅'] < -0.05: relation = "股强债弱 (Risk-On)"
            elif sh_latest['pct_chg'] < -0.2 and bond_latest['涨跌幅'] > 0.05: relation = "股弱债强 (Risk-Off)"
            elif sh_latest['pct_chg'] < -0.2 and bond_latest['涨跌幅'] < -0.05: relation = "股债双杀 (流动性收紧)"
            elif sh_latest['pct_chg'] > 0.2 and bond_latest['涨跌幅'] > 0.05: relation = "股债双强 (流动性宽松)"
            self.analysis_result['intermarket'] = {
                "market_trend": market_trend, "relation": relation, "sh_index_close": f"{sh_latest['close']:.2f}",
                "sh_pct_chg": sh_latest['pct_chg'], "position_desc": position_desc
            }
        except Exception as e:
            print(f"  - 股债关系分析失败: {e}")
            self.analysis_result['intermarket'] = {}
        print("股债关系与大盘趋势分析完成。")
 
    def analyze_market_sentiment(self):
        print("正在分析市场情绪温度...")
        try:
            activity_df = self.data.get('market_activity')
            if activity_df is None or activity_df.empty: raise ValueError("赚钱效应数据为空")
            up_series = activity_df[activity_df['item'] == '上涨']['value']
            down_series = activity_df[activity_df['item'] == '下跌']['value']
            if up_series.empty or down_series.empty: raise ValueError("未能从数据中找到'上涨'或'下跌'家数")
            up_count, down_count = up_series.iloc[0], down_series.iloc[0]
            profit_effect = round((up_count / (up_count + down_count)) * 100, 2) if (up_count + down_count) > 0 else 50.0
            zt_count = len(self.data.get('rank_ljqs', pd.DataFrame()))
            congestion_df = self.data.get('congestion')
            if congestion_df is None or congestion_df.empty: raise ValueError("拥挤度数据为空")
            congestion = congestion_df.iloc[-1]['congestion'] * 100
            sentiment, sentiment_reason = "中性", []
            if profit_effect > 65 and zt_count > 80: sentiment, _ = "贪婪", sentiment_reason.append(f"赚钱效应强({profit_effect}%)")
            elif profit_effect > 50: sentiment, _ = "乐观", sentiment_reason.append(f"赚钱效应较好({profit_effect}%)")
            elif 40 <= profit_effect <= 50: sentiment, _ = "中性偏冷", sentiment_reason.append(f"赚钱效应一般({profit_effect}%)")
            elif profit_effect < 40: sentiment, _ = "恐慌", sentiment_reason.append(f"赚钱效应差({profit_effect}%)")
            if congestion > 90: sentiment_reason.append(f"拥挤度过高({congestion:.1f}%),警惕回调")
            elif congestion < 20: sentiment_reason.append(f"拥挤度较低({congestion:.1f}%),或存机会")
            self.analysis_result['sentiment'] = { "综合情绪": sentiment, "情绪摘要": " | ".join(sentiment_reason), "赚钱效应": f"{profit_effect}%", "量价齐升家数": zt_count, "大盘拥挤度": f"{congestion:.2f}%" }
        except Exception as e:
            print(f"  - 市场情绪分析失败: {e}")
            self.analysis_result['sentiment'] = {}
        print("市场情绪温度分析完成。")
 
    def analyze_sector_strength(self):
        print("正在分析板块相对强弱度...")
        industry_flow = self.data.get('industry_fund_flow')
        if industry_flow is None or industry_flow.empty:
            print("  - 行业资金流数据为空，跳过板块强度分析。")
            self.analysis_result['sector_heat_map'] = pd.DataFrame()
            return
        industry_flow = industry_flow.copy()
        rename_dict = {'名称': '板块名称', '涨跌幅': '板块涨跌幅', '今日主力净流入-净额': '主力净流入'}
        industry_flow.rename(columns=rename_dict, inplace=True)
        ljqs_df = self.data.get('rank_ljqs', pd.DataFrame())
        if not ljqs_df.empty and self.stock_to_industry_map:
            ljqs_df['代码'] = ljqs_df['股票代码'].astype(str).str.zfill(6)
            ljqs_df['精确行业'] = ljqs_df['代码'].map(self.stock_to_industry_map)
            ljqs_counts = ljqs_df.groupby('精确行业')['代码'].count().reset_index().rename(columns={'代码': '量价齐升家数'})
            industry_flow = pd.merge(industry_flow, ljqs_counts, left_on='板块名称', right_on='精确行业', how='left')
            industry_flow['量价齐升家数'].fillna(0, inplace=True)
        else: industry_flow['量价齐升家数'] = 0
        if '主力净流入' not in industry_flow.columns: industry_flow['资金强度分'] = 0
        else: industry_flow['资金强度分'] = industry_flow['主力净流入'].rank(pct=True) * 100
        industry_flow['人气强度分'] = industry_flow['量价齐升家数'].rank(pct=True) * 100
        industry_flow['热力值'] = (0.7 * industry_flow['人气强度分'] + 0.3 * industry_flow['资金强度分']).round(2)
        self.analysis_result['sector_heat_map'] = industry_flow.sort_values(by='热力值', ascending=False)
        print("板块相对强弱度分析完成。")
 
    def analyze_etf_technical(self):
        etf_spot_df = self.data['etf_spot']
        print("正在对活跃ETF进行技术面分析 (支持断点续传)...")
        today_str = datetime.now().strftime('%Y%m%d')
        progress_file = os.path.join('../cache', f'etf_tech_progress_{today_str}.pkl')
        try:
            with open(progress_file, 'rb') as f: all_etf_analysis = pickle.load(f)
            print(f"  - 检测到已有进度，已加载 {len(all_etf_analysis)} 条分析结果。")
        except FileNotFoundError: all_etf_analysis = []
        analyzed_codes = {item['code'] for item in all_etf_analysis}
        etfs_to_process = [(row['代码'], row['名称']) for _, row in etf_spot_df.iterrows() if row['代码'] not in analyzed_codes]
        if not etfs_to_process:
            print("  - 所有ETF均已分析完毕。")
            self.analysis_result['etf_technical'] = all_etf_analysis
            return
        print(f"  - 需分析 {len(etfs_to_process)} 个新ETF...")
        with ThreadPoolExecutor(max_workers=5) as executor, tqdm(total=len(etfs_to_process), desc="分析ETF进度", mininterval=1.0) as pbar:
            futures = [executor.submit(self._analyze_single_etf, etf_info) for etf_info in etfs_to_process]
            for future in futures:
                result = future.result()
                if result:
                    all_etf_analysis.append(result)
                    with open(progress_file, 'wb') as f: pickle.dump(all_etf_analysis, f)
                pbar.update(1)
        self.analysis_result['etf_technical'] = all_etf_analysis
        print(f"\n完成 {len(all_etf_analysis)} 个ETF的技术面分析。")
 
    def _analyze_single_etf(self, etf_info):
        etf_code, etf_name = etf_info
        try:
            etf_hist = ak.fund_etf_hist_em(symbol=etf_code, period="daily", adjust="qfq").tail(30)
            if len(etf_hist) < 21: return None
            etf_hist.loc[:, 'MA5'] = etf_hist['收盘'].rolling(window=5).mean()
            etf_hist.loc[:, 'MA20'] = etf_hist['收盘'].rolling(window=20).mean()
            status, base_score = "观察", 2.0
            latest = etf_hist.iloc[-1]
            if latest['收盘'] > latest['MA20'] and latest['MA5'] > latest['MA20']:
                if latest['涨跌幅'] < 0: status, base_score = "上涨趋势中的回调", 3.0
                else: status, base_score = "强势加速上涨", 2.5
            elif latest['收盘'] < latest['MA20']: status, base_score = "弱势下跌通道", 1.0
            return {"name": etf_name, "code": etf_code, "technical_status": status, "base_score": base_score, "change_pct": latest['涨跌幅']}
        except Exception as e:
            if DEBUG_MODE: print(f"\n  - [错误] 分析ETF {etf_name}({etf_code}) 失败: {e}")
            return None
 
    def comprehensive_scoring(self):
        print("开始进行综合评分...")
        final_scores = []
        if not self.analysis_result.get('etf_technical') or self.analysis_result.get('sector_heat_map', pd.DataFrame()).empty: return
        heat_map = self.analysis_result['sector_heat_map']
        theme_to_heat = {row['板块名称']: row['热力值'] for _, row in heat_map.iterrows()}
        for etf in self.analysis_result['etf_technical']:
            score = etf['base_score']
            reasons = [f"技术面: {etf['technical_status']} (基础分: {score:.1f})"]
            specific_sector, broad_theme = self.get_etf_sector_and_theme(etf['name'])
            heat_value = theme_to_heat.get(specific_sector, 50)
            heat_score = (heat_value - 50) / 15
            score += heat_score
            reasons.append(f"板块热力: {heat_score:.1f} (热力值: {heat_value})")
            final_score = np.clip(score, 0, 5)
            if final_score >= 3.5 and etf['change_pct'] < 0: reasons.append("回调但趋势未破，或为低吸机会")
            final_scores.append({"名称": etf['name'], "代码": etf['code'], "涨跌幅": etf['change_pct'], "最终得分": final_score, "分析摘要": " | ".join(reasons), "主题": broad_theme})
        df = pd.DataFrame(final_scores).sort_values(by="最终得分", ascending=False).reset_index(drop=True)
        self.analysis_result['final_ranking'] = df
        print("综合评分完成。")
 
    def get_etf_sector_and_theme(self, etf_name):
        map_rules = {
            ('证券', '大金融'): ['证券', '券商'], ('保险', '大金融'): ['保险'], ('银行', '大金融'): ['银行'],
            ('半导体', '科技/半导体'): ['半导体', '芯片'], ('计算机', '科技/半导体'): ['计算机', '信创', '软件', '云计算', 'AI', '人工智能'], ('消费电子', '科技/半导体'): ['消费电子'], ('通信设备', '科技/半导体'): ['5G', '通信'], ('光学光电子', '科技/半导体'): ['光学'],
            ('光伏设备', '大新能源'): ['光伏'], ('电池', '大新能源'): ['电池', '锂电', '新能车', '电动车'], ('电网设备', '大新能源'): ['电网', '特高压'],
            ('食品饮料', '大消费'): ['食品', '饮料', '白酒', '消费'], ('家电行业', '大消费'): ['家电'], ('美容护理', '大消费'): ['医美', '美容'],
            ('医药商业', '医疗健康'): ['医药'], ('医疗服务', '医疗健康'): ['医疗'], ('中药', '医疗健康'): ['中药'],
            ('房地产开发', '房地产'): ['地产', '房地产'],
            ('游戏', '传媒/游戏'): ['游戏'], ('文化传媒', '传媒/游戏'): ['传媒', '影视'],
            ('国防军工', '军工'): ['军工', '国防'],
            ('煤炭行业', '周期/材料'): ['煤炭'], ('有色金属', '周期/材料'): ['有色'], ('钢铁行业', '周期/材料'): ['钢铁'], ('化学原料', '周期/材料'): ['化工'],
            ('工程机械', '高端制造'): ['机械', '制造'], ('专用设备', '高端制造'): ['设备'],
            ('物流行业', '交通运输'): ['运输', '物流', '航运', '港口'], ('农牧饲渔', '大农业'): ['农业', '养殖', '畜牧']
        }
        if any(kw in etf_name for kw in ['恒生', 'H股', '港股', '中概', '互联网']): return '港股', '港股'
        for (sector, theme), keywords in map_rules.items():
            if any(keyword in etf_name for keyword in keywords): return sector, theme
        return '其他', '其他'
 
    def analyze_market_stage(self):
        print("正在进行市场阶段定性分析...")
        try:
            position_desc = self.analysis_result.get('intermarket', {}).get('position_desc', '未知')
            volume_level = self.analysis_result.get('liquidity', {}).get('volume_qualitative_level', '未知')
            main_inflow_str = self.analysis_result.get('liquidity', {}).get('main_net_inflow', '0亿元')
            main_inflow_value = float(re.findall(r'-?\d+\.?\d*', main_inflow_str)[0])
            stage_desc, risk_type = "市场阶段特征不明显。", "趋势性风险与技术性风险均需关注。"
            if position_desc == "高位区域":
                if volume_level in ["天量水平", "巨量水平"]:
                    if main_inflow_value < -50:
                        stage_desc, risk_type = "当前市场处于【上涨趋势末期】的巨量换手阶段，主力资金分歧加大，离场意愿明显。", "趋势性风险上升，技术性回调风险剧增。"
                    else:
                        stage_desc, risk_type = "当前市场处于【牛市中期】的巨量换手阶段，资金承接良好但波动加剧。", "趋势保持但技术性回调风险加剧。"
                else:
                    stage_desc, risk_type = "当前市场处于【高位震荡】阶段，短期上攻动能减弱，进入存量博弈。", "趋势面临考验，技术性风险较高。"
            elif position_desc == "低位区域":
                if volume_level == "地量水平":
                    stage_desc, risk_type = "当前市场处于【熊市末期或牛市初期】的筑底阶段，成交持续低迷，市场关注度低。", "趋势性风险已大幅释放，但仍需警惕技术性探底风险。"
                else:
                    stage_desc, risk_type = "当前市场处于【低位反弹】阶段，资金尝试抄底，但趋势反转仍需确认。", "趋势性风险仍存，技术性反弹非反转。"
            else:
                stage_desc, risk_type = "当前市场处于【震荡整固】阶段，多空双方力量均衡，等待方向选择。", "趋势不明朗，主要为技术性波动风险。"
            self.analysis_result['market_stage'] = {"stage_description": stage_desc, "risk_type": risk_type}
        except Exception as e:
            print(f"  - 市场阶段定性分析失败: {e}")
            self.analysis_result['market_stage'] = {}
        print("市场阶段定性分析完成。")
 
    def analyze_conclusion(self):
        print("正在调用AI进行动态复盘与决策建议...")
        if not AI_TOOL_AVAILABLE:
            print("  - AI工具不可用，跳过动态复盘。")
            self.analysis_result['conclusion_raw'] = "{}" # 返回一个空的JSON字符串
            return
        try:
            liquidity = self.analysis_result.get('liquidity', {})
            sentiment = self.analysis_result.get('sentiment', {})
            intermarket = self.analysis_result.get('intermarket', {})
            heat_map = self.analysis_result.get('sector_heat_map', pd.DataFrame())
            ranking = self.analysis_result.get('final_ranking', pd.DataFrame())
            turnover = self.analysis_result.get('turnover', {})
            market_stage = self.analysis_result.get('market_stage', {})
            margin_trading = self.analysis_result.get('margin_trading', {})
            top_sectors = heat_map.head(5)['板块名称'].tolist() if not heat_map.empty else []
            bottom_sectors = heat_map.tail(5)['板块名称'].tolist() if not heat_map.empty else []
            opportunities = []
            if not ranking.empty:
                opp_df = ranking[ranking['最终得分'] >= 3.5].head(3)
                opportunities = [f"{row['主题']}({row['代码']}, 现价涨跌幅: {row['涨跌幅']:.2f}%)" for _, row in opp_df.iterrows()]
 
            system_prompt = """
角色：你是一位顶级的A股市场分析师，风格冷静、客观、一针见血。
任务：根据我提供的结构化数据，生成一份专业的JSON格式分析报告。
严格要求：
1.  **JSON结构**: 返回内容必须是严格的JSON，且必须包含以下三个顶级键: "核心矛盾解读", "操作建议", "情景推演"。
    **参考格式如下**:
    ```json
    {
      "核心矛盾解读": {
        "量价背离": "总成交额3.17万亿创历史天量但指数下跌1.76%，显示资金在高位激烈换手但承接不足",
        "多空博弈": "主力资金净流出1536亿与散户净流入1294亿形成尖锐对立，杠杆资金逆势加仓191亿加剧市场波动",
        "风格割裂": "科技半导体ETF维持强势(588780涨2.29%)与金融权重板块(银行/保险)领跌形成冰火格局",
        "技术面冲突": "60日线维持多头排列但日线MACD顶背离，短期超买修复需求与中期趋势形成矛盾"
      },
      "操作建议": {
        "仓位管理": "将总仓位降至60%以下，保留10%现金应对可能的技术性反抽",
        "持仓结构调整": {
          "增持方向": "半导体ETF(588780/516920)的回调机会，关注5日线支撑",
          "减持方向": "融资余额占比超2.3%的高杠杆品种及破位金融股"
        },
        "风险对冲": "可配置20%仓位的国债ETF或黄金ETF对冲股债性价比恶化风险",
        "关键观察点": [
          "明日若反弹至3850点附近且量能不足2.8万亿，建议减仓至50%",
          "关注科创50指数能否守住2700点关键技术位",
          "跟踪两市融资余额变化，若单日减少超300亿需警惕杠杆资金撤离"
        ]
      },
      "情景推演": {
        "标题": "明日走势推演",
        "基准情景": "60%概率维持3800-3850点震荡，量能回落至2.8万亿以下(延续今日尾盘弱势)",
        "乐观情景": "30%概率放量反包今日阴线(需成交额超3.2万亿且北向净流入超80亿)",
        "悲观情景": "10%概率有效跌破3780点引发技术抛盘(关注券商板块是否破位)"
      }
    }
    ```
2.  **内容要求**:
    -   **核心矛盾解读**: 深入分析数据间的背离和矛盾点。
    -   **操作建议**: 必须清晰、可执行、结构化。
    -   **情景推演**: 根据【报告类型】和【推演标题】生成，包含概率、描述和关键观察点。
3.  **语言风格**: 保持冷静、客观、数据驱动的风格。
"""
            report_type_map = {"LIVE_MORNING": "盘中实时分析 (早盘)", "MIDDAY_SUMMARY": "午间总结", "LIVE_AFTERNOON": "盘中实时分析 (午盘)", "POST_MARKET": "盘后复盘"}
            forecast_title_map = {
                "LIVE_MORNING": "上午收盘推演", "MIDDAY_SUMMARY": "下午走势推演",
                "LIVE_AFTERNOON": "收盘走势推演", "POST_MARKET": "明日走势推演"
            }
            report_type = report_type_map.get(self.run_mode, "盘后复盘")
            forecast_title = forecast_title_map.get(self.run_mode, "明日走势推演")
 
            inflow_perc = liquidity.get('inflow_percentage', 0)
            inflow_perc_str = f"净流入占比 {inflow_perc:.2f}%" if inflow_perc > 0 else f"净流出占比 {abs(inflow_perc):.2f}%"
 
            user_prompt = f"""
请根据以下今日A股数据，生成一份【{report_type}】报告，推演标题请使用【{forecast_title}】:
【市场阶段定性】
- 宏观判断: {market_stage.get('stage_description', '未知')}
- 主要风险: {market_stage.get('risk_type', '未知')}
【宏观与流动性分析】
- 上证指数: {intermarket.get('sh_index_close', '未知')}点 ({intermarket.get('sh_pct_chg', 0.00):.2f}%), 处于60日{intermarket.get('position_desc', '未知')}
- 大盘趋势: {intermarket.get('market_trend', '未知')}
- 股债关系: {intermarket.get('relation', '未知')}
- A股总成交额: {liquidity.get('total_volume', '未知')}{liquidity.get('estimated_turnover_str', '')} ({liquidity.get('volume_change_desc', '')}, {liquidity.get('volume_qualitative_level', '')})
- 市场换手率: {turnover.get('market_turnover_rate', '未知')} ({turnover.get('turnover_level', '未知')})
- 主力与散户行为: 主力净流入 {liquidity.get('main_net_inflow', '未知')} ({inflow_perc_str}) | 散户净流入 {liquidity.get('retail_net_inflow', '未知')}
- 杠杆资金动态: 两市融资余额 {margin_trading.get('total_balance', '未知')}，较前一日{margin_trading.get('change_desc', '变化未知')}。市场杠杆率 {margin_trading.get('leverage_ratio', '未知')} (当前处于 {margin_trading.get('leverage_level', '未知')} 水平)
【情绪分析】
- 综合情绪: {sentiment.get('综合情绪', '未知')}
- 赚钱效应: {sentiment.get('赚钱效应', '未知')}
- 量价齐升家数: {sentiment.get('量价齐升家数', '未知')}
- 大盘拥挤度: {sentiment.get('大盘拥挤度', '未知')}
【板块热力】
- 当前最强板块: {', '.join(top_sectors)}
- 当前最弱板块: {', '.join(bottom_sectors)}
- 潜在机会ETF(含实时价格): {', '.join(opportunities)}
"""
            content_string = chat_volces(system=system_prompt, user=user_prompt)
            # [V21.0] 直接展示AI的原始内容，不做解析
            self.analysis_result['conclusion_raw'] = content_string
            print("AI动态复盘与决策建议生成完毕。")
        except Exception as e:
            print(f"  - AI动态复盘失败: {e}")
            self.analysis_result['conclusion_raw'] = "AI分析模块出现异常，请检查日志。"
 
    def print_report(self):
        report_content = []
 
        report = self.analysis_result
        time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        report_title_map = {
            "LIVE_MORNING": "A股市场多维度实时分析报告 (早盘)", "MIDDAY_SUMMARY": "A股市场午间总结报告",
            "LIVE_AFTERNOON": "A股市场多维度实时分析报告 (午盘)", "POST_MARKET": "A股市场多维度综合复盘报告"
        }
        report_title = report_title_map.get(self.run_mode, "A股市场分析报告")
 
        # --- 开始构建报告字符串 ---
        report_content.append("=" * 80)
        report_content.append(f"{report_title} ({time_str})")
        report_content.append("=" * 80)
 
        market_stage = report.get('market_stage', {})
        if market_stage:
            report_content.append("\n一、市场阶段定性")
            report_content.append(f"  - 宏观判断: {market_stage.get('stage_description', '未能生成')}")
            report_content.append(f"  - 主要风险: {market_stage.get('risk_type', '未能生成')}")
 
        conclusion_raw = report.get('conclusion_raw', '')
        report_content.append("\n二、核心观点与操作建议 (由AI生成)")
        if conclusion_raw:
            # 直接展示AI返回的原始内容，不做解析
            report_content.append(conclusion_raw)
        else:
            report_content.append("未能生成AI分析。")
 
 
        intermarket = report.get('intermarket', {})
        liquidity = report.get('liquidity', {})
        turnover = report.get('turnover', {})
        margin_trading = report.get('margin_trading', {})
        report_content.append("\n三、大盘与跨市场分析 (纯数据)")
        report_content.append(f"  - 上证指数: 【{intermarket.get('sh_index_close', '未知')} ({intermarket.get('sh_pct_chg', 0.00):.2f}%)】，目前处于60日【{intermarket.get('position_desc', '未知')}】")
        report_content.append(f"  - 大盘趋势: 【{intermarket.get('market_trend', '未知')}】 | 股债关系: 【{intermarket.get('relation', '未知')}】")
        report_content.append(f"  - 成交量:   【{liquidity.get('total_volume', '未知')}{liquidity.get('estimated_turnover_str', '')}】，{liquidity.get('volume_level', '')} ({liquidity.get('volume_change_desc', '')})，属于【{liquidity.get('volume_qualitative_level', '未知')}】")
        report_content.append(f"  - 换手率:   【{turnover.get('market_turnover_rate', '未知')}】，当前市场活跃度【{turnover.get('turnover_level', '未知')}】")
        inflow_perc_str = f"占比{abs(liquidity.get('inflow_percentage', 0)):.2f}%"
        report_content.append(f"  - 主力行为: 主力净流入 {liquidity.get('main_net_inflow', '未知')} ({inflow_perc_str}) | 散户净流入 {liquidity.get('retail_net_inflow', '未知')}")
        report_content.append(f"  - 杠杆资金: 两市融资余额【{margin_trading.get('total_balance', '未知')}】，较前一日【{margin_trading.get('change_desc', '未知')}】")
        report_content.append(f"  - 市场杠杆率: 【{margin_trading.get('leverage_ratio', '未知')}】，当前处于【{margin_trading.get('leverage_level', '未知')}】水平")
 
 
        sentiment = report.get('sentiment', {})
        report_content.append("\n四、市场情绪温度计")
        report_content.append(f"  - 综合情绪: 【{sentiment.get('综合情绪', '未知')}】 | {sentiment.get('情绪摘要', '无')}")
        report_content.append(f"  - 核心指标: 赚钱效应: {sentiment.get('赚钱效应', '未知')}, 量价齐升家数: {sentiment.get('量价齐升家数', '未知')}, 大盘拥挤度: {sentiment.get('大盘拥挤度', '未知')}")
 
        if 'sector_heat_map' in report and not report['sector_heat_map'].empty:
            heat_map = report['sector_heat_map']
            report_content.append("\n五、板块热力追踪 (人气+资金)")
            report_content.append("  --- 【当前最强板块 (TOP 5)】 ---")
            top5 = heat_map.head(5)
            for _, row in top5.iterrows():
                report_content.append(f"  - 热力值: {row['热力值']:.1f} | {row['板块名称']} (量价齐升: {int(row.get('量价齐升家数', 0))})")
            report_content.append("\n  --- 【当前最弱板块 (BOTTOM 5)】 ---")
            bot5 = heat_map.tail(5)
            for _, row in bot5.sort_values(by='热力值', ascending=True).iterrows():
                report_content.append(f"  - 热力值: {row['热力值']:.1f} | {row['板块名称']} (量价齐升: {int(row.get('量价齐升家数', 0))})")
 
        if 'final_ranking' in report and not report['final_ranking'].empty:
            ranking_df = report['final_ranking']
            report_content.append("\n六、ETF综合评分排名 (基于技术面与板块热力)")
            report_content.append("\n  --- 【机会清单 (TOP 5 主题)】 ---")
            opportunities = ranking_df[ranking_df['最终得分'] >= 3.5]
            if not opportunities.empty:
                displayed_themes, count = set(), 0
                for _, row in opportunities.iterrows():
                    if row['主题'] not in displayed_themes:
                        report_content.append(f"  - 得分: {row['最终得分']:.1f} | {row['名称']} ({row['代码']}) | 主题: {row['主题']} | 实时涨跌: {row['涨跌幅']:.2f}%")
                        report_content.append(f"    摘要: {row['分析摘要']}")
                        displayed_themes.add(row['主题'])
                        count += 1
                    if count >= 5: break
                if count == 0: report_content.append("    当前市场未发现得分高于3.5的显著机会。")
            else: report_content.append("    当前市场未发现得分高于3.e5的显著机会。")
 
            report_content.append("\n  --- 【风险清单 (BOTTOM 5 主题)】 ---")
            risks = ranking_df[ranking_df['最终得分'] <= 1.5].sort_values(by="最终得分", ascending=True)
            if not risks.empty:
                displayed_themes, count = set(), 0
                for _, row in risks.iterrows():
                    if row['主题'] not in displayed_themes:
                        report_content.append(f"  - 得分: {row['最终得分']:.1f} | {row['名称']} ({row['代码']}) | 主题: {row['主题']} | 实时涨跌: {row['涨跌幅']:.2f}%")
                        report_content.append(f"    摘要: {row['分析摘要']}")
                        displayed_themes.add(row['主题'])
                        count += 1
                    if count >= 5: break
                if count == 0: report_content.append("    当前市场未发现得分低于1.5的显著风险。")
            else: report_content.append("    当前市场未发现得分低于1.5的显著风险。")
 
        report_content.append("\n" + "=" * 80)
        report_content.append("免责声明: 本报告基于公开数据和量化模型生成，所有结论仅供参考，不构成任何投资建议。")
        report_content.append("=" * 80)
 
        # --- 将报告内容整合为单一字符串 ---
        full_report_string = "\n".join(report_content)
 
        # --- 打印到控制台 ---
        print("\n\n" + full_report_string)
 
        # --- 保存到文件 ---
        if not os.path.exists('../reports'):
            os.makedirs('../reports')
 
        # 创建一个安全的文件名
        safe_time_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        os.makedirs('reports', exist_ok=True)
        report_filename = f"reports/index.html"
        with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(f"<pre>{full_report_string}</pre>")
 
        try:
            with open(report_filename, 'w', encoding='utf-8') as f:
                f.write(full_report_string)
            print(f"\n报告已成功保存至: {report_filename}")
        except Exception as e:
            print(f"\n[错误] 报告保存失败: {e}")
 
 
    def run_analysis(self):
        now = datetime.now()
        if 9 <= now.hour < 12 and now.weekday() < 5: self.run_mode = 'LIVE_MORNING'
        elif 12 <= now.hour < 13 and now.weekday() < 5: self.run_mode = 'MIDDAY_SUMMARY'
        elif 13 <= now.hour < 15 and now.weekday() < 5: self.run_mode = 'LIVE_AFTERNOON'
        else: self.run_mode = 'POST_MARKET'
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
            # self.analyze_etf_technical()
            self.comprehensive_scoring()
            self.analyze_conclusion()
            self.print_report()
 
if __name__ == "__main__":
    analyzer = AdvancedStockAnalyzer()

    analyzer.run_analysis()
