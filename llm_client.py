# -*- coding: utf-8 -*-
"""LLM 客户端：走本机 settings.json env 块的 Anthropic 兼容端点（kimi-for-coding）。

密钥纪律：只从环境变量 / settings.json env 块读取，绝不打印、绝不落盘。
"""
import json, os, re, threading, time
import requests

_lock = threading.Lock()
_cached = {}

def _settings_env(path):
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f).get('env', {}) or {}
    except (OSError, json.JSONDecodeError):
        return {}

def _resolve(cfg):
    with _lock:
        if _cached:
            return _cached
    env_blk = _settings_env(cfg['llm']['settings_env']) if cfg.get('llm', {}).get('settings_env') else {}
    token = os.environ.get('ANTHROPIC_AUTH_TOKEN') or env_blk.get('ANTHROPIC_AUTH_TOKEN') or ''
    base = (os.environ.get('ANTHROPIC_BASE_URL') or env_blk.get('ANTHROPIC_BASE_URL')
            or cfg.get('llm', {}).get('base_url') or 'https://api.kimi.com/coding/').rstrip('/')
    model = (os.environ.get('ANTHROPIC_MODEL') or env_blk.get('ANTHROPIC_MODEL')
             or cfg.get('llm', {}).get('model') or 'kimi-for-coding')
    model = re.sub(r'\[.*?\]', '', model).strip() or 'kimi-for-coding'
    with _lock:
        _cached.update({'token': token, 'base': base, 'model': model,
                        'max_tokens': int(cfg.get('llm', {}).get('max_tokens', 4096))})
    return _cached

def ready(cfg):
    return bool(_resolve(cfg)['token'])

def chat(cfg, system, user, timeout=180):
    """单轮对话，返回文本；失败抛 RuntimeError（不泄露密钥）。"""
    r = _resolve(cfg)
    if not r['token']:
        raise RuntimeError('LLM 未配置：settings.json env 块无 ANTHROPIC_AUTH_TOKEN')
    url = r['base'] + '/v1/messages'
    headers = {'x-api-key': r['token'], 'anthropic-version': '2023-06-01',
               'content-type': 'application/json'}
    body = {'model': r['model'], 'max_tokens': r['max_tokens'],
            'messages': [{'role': 'user', 'content': user}]}
    if system:
        body['system'] = system
    last = None
    for attempt in range(3):
        try:
            resp = requests.post(url, headers=headers, json=body, timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                return ''.join(b.get('text', '') for b in data.get('content', []) if isinstance(b, dict))
            last = f'HTTP {resp.status_code}'
            if resp.status_code in (401, 403):
                raise RuntimeError(f'LLM 鉴权失败（{last}），请检查 settings env 密钥是否被供应商切换覆盖')
        except requests.RequestException as e:
            last = f'{type(e).__name__}: {e}'
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f'LLM 调用失败：{last}')

def chat_json(cfg, system, user, timeout=180):
    """要求严格 JSON 输出；解析失败抛 RuntimeError。"""
    text = chat(cfg, system, user + '\n\n只输出 JSON 本体，不要输出 ``` 代码块或任何解释文字。', timeout)
    text = text.strip()
    text = re.sub(r'^```(json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    m = re.search(r'\{.*\}', text, re.S)
    if not m:
        raise RuntimeError(f'LLM 输出非 JSON：{text[:120]}')
    return json.loads(m.group(0))
