# volces_chat.py
import requests
import json
import re
 
 
VOLCES_API_KEY = "xxxxxxxxx"   # 替换成自己的 key
ENDPOINT = "https://ark.cn-beijing.volces.com/api/v3/bots/chat/completions"
# MODEL = "deepseek-v3-250324"
MODEL = "xxxxxxxx" # 模型名称
 
 
def chat_volces(system: str, user: str, timeout: int = 30) -> str:
    """
    调用火山方舟 DeepSeek 模型的同步接口
    :param system: system prompt
    :param user:   user prompt
    :param timeout: 请求超时时间（秒）
    :return: 模型返回文本
    """
    headers = {
        "Authorization": f"Bearer {VOLCES_API_KEY}",
        "Content-Type": "application/json"
    }
 
    payload = {
        "model": MODEL,
        "stream": False,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user}
        ],
        "thinking": {"type": "disabled"}
    }
 
    try:
        print(f"正在调用火山方舟模型...请求参数为{system};{user}")
        resp = requests.post(ENDPOINT, json=payload, headers=headers, timeout=timeout)
        resp.raise_for_status()
        content_str = resp.json()["choices"][0]["message"]["content"]
        print(f"返回结果为：{content_str}")
        return content_str
        # # 2. 智能清洗内容，提取纯净的JSON部分
        # json_match = re.search(r'\[[\s\S]*\]', content_str)
        # if json_match:
        #     clean_json_str = json_match.group(0)
        #     # 尝试验证一下提取出的是否是合法的JSON，防止意外
        #     try:
        #         json.loads(clean_json_str)
        #         return clean_json_str
        #     except json.JSONDecodeError:
        #         return json.dumps([{"error": "AI返回了格式错误的JSON", "details": clean_json_str}])
        # else:
        #     return json.dumps([{"error": "AI返回内容中未找到有效的JSON数组", "details": content_str}])
    except Exception as e:
        return str(e)
 
 
# 当脚本直接运行时给出一个最小示例
if __name__ == "__main__":
    print(chat_volces("""
    角色：你是专业的股市新闻筛选分析大师，根据我要求的新闻内容检索，分析后，返回我指定的严格JSON格式内容，不包含任何解释、注释或其他无关字符。
    响应示例：[{"summary": "国务院印发《新一轮千亿斤粮食产能提升行动方案》", "sentiment": "利好", "impact_score": 1.5, "sector": ["大农业"], "type": "长线", "expiry_date": "2030-12-31"}]
    强调：不要包含除了json响应示例以外任何内容
  """, """
  总结近2-3个月发布的、影响至今且未来一段时间内仍然有效的【长线】宏观或行业政策（例如XX规划、XX活动）。对每条新闻，请提供：1. 摘要(summary) 2. 情绪(sentiment: '利好'/'利空') 3. 影响权重(impact_score: 0-2分) 4. 影响的板块标签(sector: 从[{sector_themes_str}]中选择的列表) 5. 影响过期日(expiry_date: 'YYYY-MM-DD'格式)
  """))