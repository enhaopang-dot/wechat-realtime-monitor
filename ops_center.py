# -*- coding: utf-8 -*-
"""实时微信与录音监控中台 v2（2026-10-12 重建）——关注参谋版

定位：打开即关注对象；私聊/被@即时分析+备选回复；群消息聚批分析；
九离命理私域号全账号通配关注；非关注消息按"近7天我主动发言过的会话"扫重点；
微信语音实时转写；录音导入分析；21:00 日报；全分析永久存档。

数据纪律：只读微信；本机 127.0.0.1；聊天内容不出本机；密钥只从 settings env 块读。

用法:
  python ops_center.py run | daemon | install | uninstall | status
"""
import hashlib, io, json, os, re, signal, sqlite3, sys, threading, time, queue, glob, subprocess
import urllib.request
from datetime import datetime
from flask import Flask, request, jsonify, send_from_directory

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)
os.makedirs(os.path.join(BASE, 'logs'), exist_ok=True)

def log(msg):
    line = f'[{time.strftime("%m-%d %H:%M:%S")}] {msg}'
    print(line, flush=True)
    try:
        with open(os.path.join(BASE, 'logs', 'ops_center.log'), 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except OSError:
        pass

CFG = json.load(open(os.path.join(BASE, 'config.json'), encoding='utf-8'))
DATA = os.path.join(BASE, CFG.get('data_dir', 'data'))
for d in ('analyses', 'daily', 'recordings'):
    os.makedirs(os.path.join(DATA, d), exist_ok=True)
RT = CFG['monitor_runtime']
PORT = int(CFG.get('port', 5690))

import llm_client, voice_transcribe, mp_scan
voice_transcribe.init(os.path.join(DATA, 'voice_cache.json'))

# ---------------- 监控实例 ----------------

def load_monitors():
    try:
        pm = json.load(open(os.path.join(RT, 'port_map.json'), encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        pm = {}
    out = []
    for wxid_dir, lab in CFG.get('accounts', {}).items():
        port = pm.get(wxid_dir)
        if not port:
            continue
        label = lab.get('label') if isinstance(lab, dict) else lab
        out.append({'account': wxid_dir, 'label': label, 'port': port,
                    'runtime': os.path.join(RT, wxid_dir),
                    'cache': os.path.join(RT, wxid_dir, 'decrypted', '_monitor_cache'),
                    'url': f'http://127.0.0.1:{port}'})
    return out

MONITORS = load_monitors()
M_BY_ACCOUNT = {m['account']: m for m in MONITORS}

def mon_api(mon, path, timeout=8):
    with urllib.request.urlopen(mon['url'] + path, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))

def mon_ok(mon):
    try:
        mon_api(mon, '/api/sessions')
        return True
    except Exception:
        return False

def wechat_logged_in(mon):
    """读上游 watchdog 写的 login_state.json，判断该账号微信是否已登录。
    无记录时保守返回 True（避免误报离线）。"""
    try:
        st = json.load(open(os.path.join(RT, 'login_state.json'), encoding='utf-8'))
        return bool(st.get(mon['account'], True))
    except (OSError, json.JSONDecodeError):
        return True

def monitor_status(mon):
    """三态：'online'=微信已登录+监控正常；'standby'=监控在跑但微信未登录(只能看历史)；'offline'=监控挂了。"""
    if not mon_ok(mon):
        return 'offline'
    return 'online' if wechat_logged_in(mon) else 'standby'

# ---------------- 通用存储 ----------------

def _json_file(name, default):
    p = os.path.join(DATA, name)
    try:
        return json.load(open(p, encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return default

def _json_save(name, obj):
    tmp = os.path.join(DATA, name + '.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)
    os.replace(tmp, os.path.join(DATA, name))

SETTINGS = _json_file('settings.json', {})
for k, v in CFG.items():
    if k not in SETTINGS and k in ('daily_time', 'daily_on', 'notify_high', 'recording_keep_days'):
        SETTINGS[k] = v

# ---------------- 关注名单（支持账号通配 *） ----------------

DEFAULT_WATCH = [
    {'username': '12345678@chatroom', 'label': '示例群', 'account': 'wxid_example', 'note': '示例备注'},
    {'username': '*', 'label': '示例关注项', 'account': 'wxid_example', 'note': '全账号会话通配'},
]

def get_watchlist():
    wl = _json_file('watchlist.json', None)
    if wl is None:
        wl = list(DEFAULT_WATCH)
        save_watchlist(wl)
        return wl
    # 兜底：把 v2 默认项补齐（如九离全账号通配关注），幂等
    changed = False
    for d in DEFAULT_WATCH:
        if not any(w.get('username') == d['username'] and w.get('account') == d['account'] for w in wl):
            wl.append(dict(d))
            changed = True
    if changed:
        save_watchlist(wl)
    return wl

def save_watchlist(wl):
    _json_save('watchlist.json', wl)

# ---------------- 忽略名单（不再监控：瀑布/自动分析里隐藏该会话） ----------------

def get_ignore():
    return _json_file('ignore.json', {'items': []})


# 自动纳入报告时按名字直接跳过的噪声群（招聘/通告/资源群，不是业务会话）
NOISE_LABELS = ('招聘', '通告', '主播资源', '带货群', '兼职', '约拍', '模特群', '返点')

def save_ignore(ig):
    _json_save('ignore.json', ig)

def is_ignored(account, username, sender=''):
    """不再监控判定：sender 条目=跨会话屏蔽该发送人；无 sender 条目=屏蔽整个会话。"""
    for i in get_ignore().get('items', []):
        if i.get('account') != account:
            continue
        if i.get('sender'):
            if sender and i['sender'] == sender:
                return True
        elif i.get('username') == username:
            return True
    return False

def get_pins():
    """关注对象置顶集合：data/pins.json {"keys": ["account|username", ...]}"""
    return set(_json_file('pins.json', {'keys': []}).get('keys', []))

def save_pins(pins):
    _json_save('pins.json', {'keys': sorted(pins)})

# ---------------- Get笔记（录音自动链路） ----------------

def _biji_env():
    env_blk = {}
    p = CFG.get('llm', {}).get('settings_env')
    if p and os.path.isfile(p):
        try:
            env_blk = json.load(open(p, encoding='utf-8')).get('env', {})
        except (OSError, json.JSONDecodeError):
            pass
    key = os.environ.get('GET_BIJI_API_KEY') or env_blk.get('GET_BIJI_API_KEY')
    cid = os.environ.get('GET_BIJI_CLIENT_ID') or env_blk.get('GET_BIJI_CLIENT_ID')
    return key, cid

def biji_list(keyword='', since_ts=0, until_ts=4102444800, limit=30):
    key, cid = _biji_env()
    if not key or not cid:
        raise RuntimeError('Get笔记凭证缺失（settings env 块 GET_BIJI_API_KEY/GET_BIJI_CLIENT_ID）')
    out, cursor = [], '0'
    for _ in range(15):
        url = f'https://openapi.biji.com/open/api/v1/resource/note/list?cursor={cursor}'
        req = urllib.request.Request(url, headers={'Authorization': key, 'X-Client-ID': cid})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode('utf-8'))
        if not data.get('success'):
            raise RuntimeError(f"Get笔记 list 失败：{str(data)[:120]}")
        d = data.get('data', {})
        notes = d.get('notes', [])
        stop = False
        for n in notes:
            if n.get('note_type') != 'recorder_audio':
                continue
            try:
                ts = int(time.mktime(time.strptime(n.get('created_at', '2000-01-01')[:19], '%Y-%m-%d %H:%M:%S')))
            except (ValueError, OverflowError):
                continue
            if ts < since_ts:
                stop = True
                break
            if ts > until_ts:
                continue
            if keyword and keyword not in (n.get('title', '') + n.get('content', '')):
                continue
            out.append({'id': str(n.get('id', n.get('note_id', ''))),
                        'title': n.get('title', ''), 'created': n.get('created_at', ''), 'ts': ts,
                        'summary': (n.get('content') or '')[:240]})
            if len(out) >= limit:
                stop = True
                break
        if stop or not d.get('has_more'):
            break
        cursor = str(d.get('next_cursor', '0'))
        if cursor == '0':
            break
    out.sort(key=lambda x: x['ts'], reverse=True)
    return out[:limit]

def biji_detail(note_id):
    key, cid = _biji_env()
    url = f'https://openapi.biji.com/open/api/v1/resource/note/detail?id={note_id}'
    req = urllib.request.Request(url, headers={'Authorization': key, 'X-Client-ID': cid})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read().decode('utf-8'))
    if not data.get('success'):
        raise RuntimeError(f"Get笔记 detail 失败：{str(data)[:120]}")
    n = data.get('data', {}).get('note', {})
    return {'title': n.get('title', ''), 'content': n.get('content', ''),
            'original': (n.get('audio') or {}).get('original', ''),
            'duration_ms': ((n.get('attachments') or [{}])[0] or {}).get('duration', 0),
            'url': ((n.get('attachments') or [{}])[0] or {}).get('url', ''),
            'created': n.get('created_at', '')}

SUGGEST_STYLES = ['简约', '正常', '缓和', '婉拒']
SUGGEST_STYLE_GUIDE = ('四条备选回复风格（第一人称、可直接微信发送）：'
    '「简约」=最精简，1句直接回应；「正常」=标准完整，1-3句口语体（最常用）；'
    '「缓和」=语气更软更暖、多铺垫体贴；「婉拒」=缓兵/挡白嫖/留余地，礼貌不把话说死。')

RECORD_RELATE_SYSTEM = (
    '你是庞恩浩的录音参谋。下面是一段录音的带说话人转写原文，以及他最近关注的若干微信会话（标题+各自最近消息摘要）。'
    '输出严格 JSON：{"level":"high|mid|low","summary":"录音核心结论一句话",\n'
    ' "advice":"给庞恩浩的参谋建议（不回发）",\n'
    ' "related":"录音里与其中某个/某些微信会话直接相关的内容：发生了什么、对应的承诺/报价/待办/分歧是什么；与所有会话都无关则为空字符串",\n'
    ' "suggestions":[{"style":"简约","text":"..."},{"style":"正常","text":"..."},{"style":"缓和","text":"..."},{"style":"婉拒","text":"..."}]}\n'
    '- level：high=涉及价格/合同/承诺/纠纷/明确商机；mid=事项推进；low=日常闲聊。\n'
    '- suggestions 四条按 简约/正常/缓和/婉拒 风格，用庞恩浩第一人称写可直接微信发送的回复（与相关会话的最新话题衔接）；' + SUGGEST_STYLE_GUIDE + '不相关时可为空数组。\n'
    '- 只依据录音原文与会话摘要，不编造。')

def analyze_recording(note_id, note_title, note_content, original, watch_items):
    """录音智能分析：与关注会话关联 + 级别 + 建议 + 备选回复，存档。"""
    conv_brief = []
    wl = get_watchlist()
    for item in watch_items[:12]:
        mon = M_BY_ACCOUNT.get(item.get('account'))
        if not mon or item.get('username') == '*':
            continue
        try:
            msgs = mon_api(mon, f"/api/history?chat={urllib.request.quote(item['username'])}&limit=12")
            msgs = [m for m in msgs if m.get('username') == item['username']]
            msgs.sort(key=lambda m: m.get('timestamp', 0))
            if msgs:
                lines = [f"{m.get('time', '')} {_resolve_sender(mon, m)}: {(m.get('content') or '')[:60]}" for m in msgs[-8:]]
                conv_brief.append(f"### {item.get('label')}\n" + '\n'.join(lines))
        except Exception:
            continue
    body = (f"录音《{note_title}》（{note_content[:1500] if note_content else ''}）\n\n"
            f"【转写原文】\n{original[:20000]}\n\n"
            f"【庞恩浩关注的微信会话近况】\n" + ('\n\n'.join(conv_brief) if conv_brief else '（暂无可关联会话）')
            + '\n\n输出 JSON。')
    r = llm_client.chat_json(CFG, RECORD_RELATE_SYSTEM, body, timeout=300)
    level = str(r.get('level', 'low')).lower()
    if level not in ('high', 'mid', 'low'):
        level = 'low'
    suggestions = []
    for s in (r.get('suggestions') or [])[:4]:
        if isinstance(s, dict) and s.get('text'):
            suggestions.append({'style': s.get('style', '正常'), 'text': str(s['text']).strip()})
    md = (f"# {note_title}\n\n- 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n- 级别: {level}\n\n---\n\n"
          f"## 转写原文\n\n{original}\n\n---\n\n"
          f"**摘要**: {r.get('summary', '')}\n\n**参谋**: {r.get('advice', '')}\n\n"
          f"**与会话关联**: {r.get('related', '')}\n\n")
    for s in suggestions:
        md += f"**备选·{s['style']}**: {s['text']}\n\n"
    adir = os.path.join(DATA, 'analyses', time.strftime('%Y-%m'))
    os.makedirs(adir, exist_ok=True)
    name = f"{time.strftime('%Y%m%d_%H%M%S')}_recording_{re.sub(r'[^\\w\\u4e00-\\u9fff]+', '_', note_title)[:20]}"
    with open(os.path.join(adir, name + '.md'), 'w', encoding='utf-8') as f:
        f.write(md)
    meta = {'kind': 'recording', 'label': note_title, 'account': 'getnote', 'username': note_id,
            'level': level, 'md': os.path.relpath(os.path.join(adir, name + '.md'), DATA),
            'created': time.time(), 'note_id': note_id,
            'summary': r.get('summary', ''), 'advice': r.get('advice', ''),
            'related': r.get('related', ''), 'suggestions': suggestions,
            'duration_ms': 0}
    with open(os.path.join(adir, name + '.json'), 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=1)
    return meta

def watch_match(wl, account, username):
    """返回匹配的 watch item 或 None。通配 * 只匹配该账号会话，排除群助手等系统号。"""
    for w in wl:
        if w.get('account') != account:
            continue
        if w.get('username') == '*':
            if username and not username.startswith('gh_'):
                return w
        elif w.get('username') == username:
            return w
    return None

# ---------------- focus 状态（对象级未读/级别/建议） ----------------

FOCUS = _json_file('focus.json', {})   # key=f"{account}|{username}" -> state

def fkey(account, username):
    return f'{account}|{username}'

def focus_get(account, username):
    return FOCUS.get(fkey(account, username), {})

def focus_update(account, username, **kw):
    k = fkey(account, username)
    st = FOCUS.setdefault(k, {})
    st.update(kw)
    st['account'], st['username'] = account, username
    _json_save('focus.json', FOCUS)
    return st

GENERIC_LABELS = {'', '对方', '我'}
# 监控快照阶段 LLM 对群昵称的猜测（如「Kral」）不可信；命中这些一律回退联系人库真名，
# 防群昵称猜测长期固化在 focus.json 里覆盖显示。
BAD_SNAPSHOT_LABELS = {'Kral'}

def focus_label(mon, username, item=None):
    """会话显示名：联系人库真实名称（备注优先、其次昵称）为最高权威；
    名单 label / 监控快照 label 仅在非占位、非坏快照名时作补充。"""
    real = contact_names(mon).get(username)
    if item and item.get('username') == '*':
        return real or username
    if item:
        lab = (item.get('label') or '').strip()
        if lab and lab not in GENERIC_LABELS and lab not in BAD_SNAPSHOT_LABELS:
            return lab
    return real or username

def _resolve_sender(mon, m):
    """消息行对端显示名：monitor 私聊消息 sender 常为空 → 用联系人库备注/昵称兜底。"""
    if m.get('is_self'):
        return '我'
    s = (m.get('sender') or '').strip()
    if s and s not in GENERIC_LABELS:
        return s
    if not m.get('is_group'):
        return contact_names(mon).get(m.get('username', '')) or s or '对方'
    return s or '对方'

# ---------------- 历史库读取 ----------------
_zstd = None
def _zstd_dctx():
    global _zstd
    if _zstd is None:
        import zstandard
        _zstd = zstandard.ZstdDecompressor(max_output_size=8 * 1024 * 1024)
    return _zstd

_name_caches = {}

def contact_names(mon):
    if mon['account'] in _name_caches:
        return _name_caches[mon['account']]
    names = {}
    db = os.path.join(mon['cache'], 'contact_contact.db')
    try:
        conn = sqlite3.connect('file:%s?mode=ro' % db, uri=True, timeout=8)
        tbls = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        tbl = next((t for t in tbls if 'contact' in t.lower()), None)
        if tbl:
            cols = [r[1].lower() for r in conn.execute(f"PRAGMA table_info({tbl})")]
            def pick(*cands):
                return next((c for c in cands if c in cols), None)
            c_user = pick('username', 'user_name', 'wxid')
            c_nick, c_remark = pick('nickname', 'nick_name'), pick('remark')
            if c_user:
                sel = f"SELECT {c_user}, {c_nick or c_user}, {c_remark or c_nick or c_user} FROM {tbl}"
                for row in conn.execute(sel):
                    names[row[0]] = (row[2] or row[1]) if len(row) > 2 else row[1]
        conn.close()
    except Exception as e:
        log(f'contact_names 失败 {mon["account"]}: {e}')
    _name_caches[mon['account']] = names
    return names

def self_wxid(mon):
    return re.sub(r'_\d+$', '', mon['account'])

def _name2id(conn):
    out = {}
    try:
        cols = [r[1].lower() for r in conn.execute("PRAGMA table_info(Name2Id)")]
        ucol = next((c for c in ('user_name', 'username') if c in cols), cols[-1] if cols else None)
        if ucol:
            for row in conn.execute(f"SELECT rowid, {ucol} FROM Name2Id"):
                out[row[0]] = row[1]
    except sqlite3.Error:
        pass
    return out

def msg_table(username):
    return 'Msg_' + hashlib.md5(username.encode('utf-8')).hexdigest()

def render_row(mon, raw, n2id, names):
    local_id, ts, lt, mc, ct, is_sender, sender_id = raw
    base = lt % 4294967296 if lt and lt > 4294967296 else lt
    if isinstance(mc, bytes):
        if ct == 4 and mc[:4] == b'\x28\xb5\x2f\xfd':
            try:
                mc = _zstd_dctx().decompress(mc).decode('utf-8', 'replace')
            except Exception:
                mc = mc.decode('utf-8', 'replace')
        else:
            mc = mc.decode('utf-8', 'replace')
    mc = mc or ''
    me = self_wxid(mon)
    who = '我' if is_sender else None
    if who is None and sender_id is not None:
        wx = n2id.get(sender_id, '')
        who = '我' if wx == me else (names.get(wx) or wx or '对方')
    if who is None:
        who = '对方'
    kind = {1: 'text', 3: 'image', 34: 'voice', 43: 'video', 47: 'emoji'}.get(base, 'other')
    text = mc
    if base == 1:
        text = re.sub(r'^wxid[^\n:：]{0,40}[:：]\s*\n?', '', mc)
        # 引用消息头："Duan: b745240160:" / "猎海@老洋: IWEIBOO51:" —— 逐行剥
        lines = text.split('\n')
        while len(lines) > 1:
            head = lines[0].strip()
            if len(head) <= 40 and head.endswith((':', '：')) and not re.search(r'[，。！？!?]', head):
                lines.pop(0)
            else:
                break
        text = '\n'.join(lines)
        if mc.lstrip().startswith('<sysmsg'):
            text = _appmsg_text(mc)
    elif base == 49:
        m = re.search(r'<title>([^<]{1,120})</title>', mc)
        text = ('[链接/文件] ' + m.group(1)) if m and m.group(1).strip() else '[应用消息]'
    elif base == 34:
        text = ''
    elif base not in (3, 43, 47):
        text = mc[:80] if mc else f'[类型{base}]'
    if text and text.count('�') >= 2:
        text = '[非文本内容]'
    return {'ts': int(ts), 'local_id': local_id, 'who': who, 'text': text.strip(),
            'kind': kind, 'base': base, 'is_self': bool(is_sender)}

def history_rows(mon, username, start_ts=0, end_ts=4102444800, limit=500, keyword=''):
    names = contact_names(mon)
    rows_out, kw = [], keyword.strip().lower()
    tbl = msg_table(username)
    for db in sorted(glob.glob(os.path.join(mon['cache'], 'message_message_*.db'))):
        try:
            conn = sqlite3.connect('file:%s?mode=ro' % db, uri=True, timeout=8)
            if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (tbl,)).fetchone():
                conn.close()
                continue
            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({tbl})")]
            low = {c.lower(): c for c in cols}
            def col(*cands):
                return next((low[c] for c in cands if c in low), None)
            c_id, c_ts, c_lt = col('local_id'), col('create_time'), col('local_type')
            c_mc, c_ct = col('message_content'), col('content_type')
            c_self, c_sender = col('issender', 'is_sender'), col('real_sender_id', 'sender_id')
            if not (c_ts and c_lt and c_mc):
                conn.close()
                continue
            sel = [c_id or 'NULL', c_ts, c_lt, c_mc, c_ct or '0', c_self or '0', c_sender or 'NULL']
            sql = (f"SELECT {', '.join(sel)} FROM {tbl} WHERE {c_ts} >= ? AND {c_ts} <= ? "
                   f"ORDER BY {c_ts} DESC LIMIT ?")
            n2id = _name2id(conn)
            for raw in conn.execute(sql, (start_ts, end_ts, limit * 3)):
                item = render_row(mon, raw, n2id, names)
                if kw and kw not in item['text'].lower() and kw not in item['who'].lower():
                    continue
                rows_out.append(item)
            conn.close()
        except sqlite3.Error as e:
            log(f'history 读库失败 {db}: {e}')
    # 分片重叠去重：同一会话消息可能同时落在相邻 message_*.db，按 (ts, who, text) 去重
    seen = set()
    dedup = []
    for r in sorted(rows_out, key=lambda x: x['ts']):
        sig = (r['ts'], r['who'], r['text'])
        if sig in seen:
            continue
        seen.add(sig)
        dedup.append(r)
    return dedup[-limit:]


def active_chats(mon, start_ts, end_ts, min_rows=3, limit=60):
    """枚举时间段内**所有**会话的活跃度，不限关注名单。

    日报只能覆盖关注名单，新建的群、临时拉起的对接群永远进不了报告。
    这里反查 Msg_<md5(会话名)> → 会话名，按消息条数排序返回，供报告自动纳入。
    """
    names = contact_names(mon)
    hit = {}
    for db in sorted(glob.glob(os.path.join(mon['cache'], 'message_message_*.db'))):
        try:
            conn = sqlite3.connect('file:%s?mode=ro' % db, uri=True, timeout=8)
            tabs = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Msg_%'")]
            if not tabs:
                conn.close()
                continue
            rev = {}
            for uid, uname in _name2id(conn).items():
                rev[msg_table(uname)] = uname
            for tab in tabs:
                uname = rev.get(tab)
                if not uname:
                    continue
                cols = [r[1].lower() for r in conn.execute("PRAGMA table_info(%s)" % tab)]
                if 'create_time' not in cols:
                    continue
                try:
                    n = conn.execute(
                        "SELECT COUNT(*) FROM %s WHERE create_time >= ? AND create_time <= ?" % tab,
                        (start_ts, end_ts)).fetchone()[0]
                except sqlite3.Error:
                    continue
                if n:
                    hit[uname] = hit.get(uname, 0) + n
            conn.close()
        except sqlite3.Error as e:
            log(f'active_chats 读库失败 {db}: {e}')
    out = [{'username': u, 'label': names.get(u) or u, 'rows': n}
           for u, n in hit.items() if n >= min_rows]
    out.sort(key=lambda x: -x['rows'])
    return out[:limit]

# ---------------- 语音 ----------------
_voice_rt = {}   # (account,username,ts) -> text 实时转写缓存（进程内）
_voice_pending = set()
_voice_retry = {}   # (account,username,ts) -> 已重试次数（撞 keys 重建窗口等瞬时故障用）
_VOICE_RETRY_MAX = 3
_vlock = threading.Lock()

def is_voice_msg(m):
    t = (m.get('type') or '')
    c = (m.get('content') or '')
    return t == '语音' or 'voicemsg' in c

def ensure_voice(mon, username, ts):
    """语音消息：命中缓存直接返回；否则登记转写任务，先返回 None（前端显示[语音]）。"""
    k = (mon['account'], username, int(ts))
    with _vlock:
        if k in _voice_rt:
            return _voice_rt[k]
        _voice_pending.add(k)
    return None

def voice_worker():
    while True:
        with _vlock:
            todo = sorted(_voice_pending)[:4]
            for k in todo:
                _voice_pending.discard(k)
        for account, username, ts in todo:
            mon = M_BY_ACCOUNT.get(account)
            if not mon:
                continue
            try:
                text = voice_transcribe.transcribe_wechat(mon['runtime'], username, ts)
            except Exception as e:
                log(f'实时转写失败 {username}@{ts}: {e}')
                text = None
            with _vlock:
                if text:
                    _voice_rt[(account, username, ts)] = text
                    _voice_retry.pop((account, username, ts), None)
                else:
                    # 失败重试：仅 1 小时内的新消息、最多 3 次（keys 重建窗口/malformed 恢复后有机会补上）
                    n = _voice_retry.get((account, username, ts), 0)
                    if n < _VOICE_RETRY_MAX and time.time() - ts < 3600:
                        _voice_retry[(account, username, ts)] = n + 1
                        _voice_pending.add((account, username, ts))
        time.sleep(2)

def voice_text(mon, username, ts):
    return _voice_rt.get((mon['account'], username, int(ts)))

# ---------------- 商务金额预警 ----------------
# 双命中规则：金额模式 + 商务词表同时出现才弹（2026-10-16 用户拍板"宁缺勿滥"）。
# 挂在 tick_poll 入口逐条扫（与是否关注无关，is_ignored 已在调用前过滤），秒级、不依赖 LLM。
_BIZ_AMOUNT_RE = re.compile(r'\d+(?:\.\d+)?\s*(?:万元|万|萬|千元|千块|元|块|k|K|w|W)')
_BIZ_WORDS = ('合作', '报价', '报个价', '定金', '尾款', '预算', '分成', '佣金', '年框', '合同', '洽谈',
              '签约', '服务费', '意向金', '加盟', '入驻', '渠道费', '介绍费', '返点', '垫资', '回款')
_BIZ_COOLDOWN = 600      # 同一会话命中冷却（秒），防连发刷屏
_biz_last = {}           # (account, username) -> 上次弹出 ts

def _biz_money_hit(text):
    """双命中返回 (金额串, 商务词)，否则 None。"""
    if not text or len(text) < 4:
        return None
    amt = _BIZ_AMOUNT_RE.search(text)
    if not amt:
        return None
    for w in _BIZ_WORDS:
        if w in text:
            return amt.group(0), w
    return None

def biz_money_check(mon, m):
    """tick_poll 逐条调用。命中 → 冷却去重 → high 红色预警（detail 存原文供前端定位）。"""
    if is_voice_msg(m):
        return
    text = _strip_wxid_prefix(_appmsg_text(m.get('content') or ''))
    hit = _biz_money_hit(text)
    if not hit:
        return
    key = (mon['account'], m.get('username', ''))
    now = time.time()
    if now - _biz_last.get(key, 0) < _BIZ_COOLDOWN:
        return
    _biz_last[key] = now
    amt, word = hit
    who = '我' if m.get('is_self') else (m.get('sender') or '对方')
    tstr = m.get('time') or datetime.fromtimestamp(int(m.get('timestamp', now))).strftime('%m-%d %H:%M')
    pos = text.find(amt)
    lo, hi = max(0, pos - 30), min(len(text), pos + len(amt) + 40)
    snippet = ('…' if lo else '') + text[lo:hi].strip() + ('…' if hi < len(text) else '')
    label = contact_names(mon).get(m.get('username', ''), '') or m.get('username', '')
    add_alert('high', label, mon['account'], m.get('username', ''), f'命中「{amt}」+「{word}」：{snippet}',
              ['商务金额', word], None,
              detail={'text': text, 'sender': who, 'msg_time': tstr, 'amount': amt, 'word': word})
    log(f'[商务金额] {label}: {amt}+{word} → 红色预警')

# ---------------- 存档 ----------------

def archive_write(kind, label, account, username, text_md, meta, level=None):
    month = time.strftime('%Y-%m')
    adir = os.path.join(DATA, 'analyses', month)
    os.makedirs(adir, exist_ok=True)
    slug = re.sub(r'[^\w一-鿿]+', '_', label)[:20] or username[:16]
    name = f"{time.strftime('%Y%m%d_%H%M%S')}_{kind}_{slug}"
    md_path = os.path.join(adir, name + '.md')
    kind_cn = {'auto': '自动分析', 'manual': '手动分析', 'upload': '录音分析', 'daily': '每日汇总'}.get(kind, kind)
    head = [f"# {label} — {kind_cn}",
            f"- 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"- 账号: {M_BY_ACCOUNT.get(account, {}).get('label', account)}  会话: {username}",
            f"- 级别: {level or '-'}",
            '', '---', '']
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(head) + text_md)
    meta_path = os.path.join(adir, name + '.json')
    meta.update({'kind': kind, 'label': label, 'account': account, 'username': username,
                 'level': level, 'md': os.path.relpath(md_path, DATA), 'created': time.time()})
    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=1)
    return os.path.relpath(md_path, BASE)

def archive_read(rel):
    # meta['md'] 以 DATA 为基（analyses/...），旧实现拼到 BASE 下必 404；两个基都试，且只允许落在 DATA 内
    rel = (rel or '').replace('\\', '/').lstrip('/')
    for root in (DATA, BASE):
        p = os.path.realpath(os.path.join(root, rel))
        if p.startswith(os.path.realpath(DATA)) and os.path.isfile(p):
            try:
                return open(p, encoding='utf-8').read()
            except OSError:
                return None
    return None

def list_archives(month=None, username=None, kind=None, level=None):
    out = []
    adir = os.path.join(DATA, 'analyses')
    months = [month] if month else sorted(os.listdir(adir), reverse=True) if os.path.isdir(adir) else []
    for mo in months:
        d = os.path.join(adir, mo)
        if not os.path.isdir(d):
            continue
        for fn in sorted(os.listdir(d), reverse=True):
            if not fn.endswith('.json'):
                continue
            try:
                m = json.load(open(os.path.join(d, fn), encoding='utf-8'))
            except (OSError, json.JSONDecodeError):
                continue
            if username and m.get('username') != username:
                continue
            if kind and m.get('kind') != kind:
                continue
            if level and m.get('level') != level:
                continue
            m['json'] = fn
            out.append(m)
    return out[:300]

# ---------------- LLM 分析（v2：带备选回复） ----------------

ANALYZE_SYSTEM = (
    '你是庞恩浩的微信沟通参谋。庞恩浩是电商运营/命理咨询从业者，正在用手机微信对接客户与合作方。'
    '基于给定聊天记录做判断并输出严格 JSON：\n'
    '{"level":"high|mid|low","tags":[...],"summary":"一句话","advice":"给庞恩浩的参谋建议（不回发）",\n'
    ' "suggestions":[{"style":"简约","text":"..."},{"style":"正常","text":"..."},{"style":"缓和","text":"..."},{"style":"婉拒","text":"..."}],\n'
    ' "related":"这条录音里与下面这些微信会话相关的内容（无关联则为空字符串）"}\n'
    '规则：\n'
    '- level：high=砍价/压价、投诉不满、明确商机推进、承诺待办、风险异常；mid=有事项推进；low=日常闲聊。\n'
    '- tags 从封闭集合选：砍价,投诉,承诺,商机,风险,日常。\n'
    '- suggestions 四条按 简约/正常/缓和/婉拒 风格，' + SUGGEST_STYLE_GUIDE +
    '根据最新消息内容给（如对方问了事就回事项、砍价就守价或给条件）；low 级别或纯对方自言自语时 suggestions 可为空数组。\n'
    '- 绝不编造聊天记录里没有的事实。')

NONWATCH_SYSTEM = (
    '你是微信消息巡检员。下面是庞恩浩近7天主动发言过的会话里、他尚未关注的新消息。'
    '输出严格 JSON：{"items":[{"level":"high|mid","tags":[...],"summary":"一句话","advice":"一句行动建议"}]}\n'
    '只保留需要庞恩浩知道的：待办/承诺未闭环/商务推进/风险/家人学校急事；闲聊直接丢弃（items 可为空数组）。'
    'tags 封闭集合：砍价,投诉,承诺,商机,风险,待办,日常。')

def _lines_from_monitor(mon, item, msgs, with_voice=True):
    """monitor 消息 dict → 转写行 + 预览"""
    username = item['username'] if item.get('username') != '*' else None
    lines = []
    for m in msgs[-80:]:
        text = _appmsg_text(m.get('content')) or f"[{m.get('type', '消息')}]"
        if is_voice_msg(m) and m.get('timestamp'):
            vt = ensure_voice(mon, m['username'] if m.get('is_group') else m['username'], int(m['timestamp']))
            if not vt:
                try:
                    vt = voice_transcribe.transcribe_wechat(
                        mon['runtime'], m['username'], int(m['timestamp']))
                    with _vlock:
                        _voice_rt[(mon['account'], m['username'], int(m['timestamp']))] = vt
                except Exception:
                    vt = None
            text = f'[语音] {vt}' if vt else '[语音]'
        who = '我' if m.get('is_self') else _resolve_sender(mon, m)
        lines.append(f"{m.get('time', '')} {who}: {text}")
    return lines

def analyze_conversation(mon, item, msgs, kind='auto'):
    """v2 主分析：级别+标签+摘要+参谋建议+三条备选回复。结果入 focus 状态+存档+警报。"""
    username = item['username']
    # 通配名单的 eff_item.username 已被改写为会话 wxid，必须回查原始名单项才能按会话取联系人名
    orig = watch_match(get_watchlist(), mon['account'], username) or item
    label = focus_label(mon, username, orig)
    lines = _lines_from_monitor(mon, item, msgs)
    if not lines:
        return None
    user = (f"会话「{label}」新到 {len(lines)} 条消息（时间倒序最近的在后）：\n" + '\n'.join(lines)
            + '\n\n输出 JSON。')
    try:
        r = llm_client.chat_json(CFG, ANALYZE_SYSTEM, user, timeout=180)
    except Exception as e:
        log(f'分析 LLM 失败 {label}: {e}')
        return None
    level = str(r.get('level', 'low')).lower()
    if level not in ('high', 'mid', 'low'):
        level = 'low'
    suggestions = []
    for s in (r.get('suggestions') or [])[:4]:
        if isinstance(s, dict) and s.get('text'):
            style = s.get('style', '')
            style = style if style in SUGGEST_STYLES else SUGGEST_STYLES[len(suggestions) % len(SUGGEST_STYLES)]
            suggestions.append({'style': style, 'text': str(s['text']).strip()})
    ts_now = time.time()
    last_ts = max((int(m.get('timestamp', 0)) for m in msgs), default=ts_now)
    focus_update(mon['account'], username,
                 label=label, last_msg_ts=last_ts, last_level=level,
                 last_summary=r.get('summary', ''), last_advice=r.get('advice', ''),
                 suggestions=suggestions, last_analyze_ts=ts_now,
                 msg_preview=lines[-1][:80] if lines else '')
    md = '\n'.join(lines) + f"\n\n---\n**级别**: {level}  **标签**: {', '.join(r.get('tags', []))}\n\n**摘要**: {r.get('summary', '')}\n\n**参谋**: {r.get('advice', '')}\n\n"
    for s in suggestions:
        md += f"**备选·{s['style']}**: {s['text']}\n\n"
    path = archive_write(kind, label, mon['account'], username, md, {'msg_count': len(lines)}, level)
    alert = None
    if level in ('high', 'mid'):
        alert = add_alert(level, label, mon['account'], username,
                          r.get('summary', ''), r.get('tags', []), path)
    return {'level': level, 'tags': r.get('tags', []), 'summary': r.get('summary', ''),
            'advice': r.get('advice', ''), 'suggestions': suggestions, 'path': path,
            'alert': alert}

def analyze_manual(mon, item, hours=24):
    username = item['username']
    label = focus_label(mon, username, item)
    end, start = int(time.time()), int(time.time()) - int(hours * 3600)
    rows = history_rows(mon, username, start, end, 400)
    if not rows:
        raise RuntimeError('该时间段没有消息')
    lines = []
    for r in rows:
        text = r['text']
        if r['kind'] == 'voice':
            text = '[语音]'
            try:
                t = voice_transcribe.transcribe_wechat(mon['runtime'], username, r['ts'])
                if t:
                    text = f'[语音] {t}'
            except Exception:
                pass
        elif r['kind'] == 'image':
            text = '[图片]'
        elif r['kind'] == 'video':
            text = '[视频]'
        lines.append(f"{datetime.fromtimestamp(r['ts']).strftime('%m-%d %H:%M')} {r['who']}: {text}")
    user = f"会话「{label}」近 {hours} 小时聊天记录：\n" + '\n'.join(lines) + '\n\n输出 JSON（含 suggestions）。'
    r = llm_client.chat_json(CFG, ANALYZE_SYSTEM, user, timeout=240)
    level = str(r.get('level', 'low')).lower()
    suggestions = [{'style': s.get('style', '正常'), 'text': str(s.get('text', '')).strip()}
                   for s in (r.get('suggestions') or [])[:4] if s.get('text')]
    md = '\n'.join(lines) + f"\n\n---\n**级别**: {level}  **标签**: {', '.join(r.get('tags', []))}\n\n**摘要**: {r.get('summary', '')}\n\n**参谋**: {r.get('advice', '')}\n\n"
    for s in suggestions:
        md += f"**备选·{s['style']}**: {s['text']}\n\n"
    path = archive_write('manual', label, mon['account'], username, md,
                         {'msg_count': len(lines), 'hours': hours}, level)
    focus_update(mon['account'], username, label=label, last_level=level,
                 last_summary=r.get('summary', ''), last_advice=r.get('advice', ''),
                 suggestions=suggestions, last_analyze_ts=time.time(),
                 last_msg_ts=rows[-1]['ts'])
    return {'path': path, 'level': level, 'summary': r.get('summary', ''),
            'advice': r.get('advice', ''), 'suggestions': suggestions, 'msg_count': len(lines)}

def analyze_upload(saved_path, orig_name, match_username=None, match_account=None):
    text = voice_transcribe.transcribe_upload(saved_path)
    if not text:
        raise RuntimeError('未能从音频中识别出文字')
    user = (f"录音文件「{orig_name}」语音转写全文：\n\n{text}\n\n"
            '输出 Markdown：## 转写全文 / ## 内容分析 / ## 风险与待办 / ## 建议。')
    md = llm_client.chat(CFG, '你是录音内容分析顾问，输出客观、具体、不臆测。', user, timeout=240)
    full = f"**转写**:\n\n{text}\n\n---\n{md}"
    label = orig_name
    path = archive_write('upload', label, match_account or 'upload', match_username or orig_name,
                         '\n' + full, {'source': saved_path, 'orig': orig_name})
    return {'path': path, 'text': full}

def daily_report(force=False):
    if not SETTINGS.get('daily_on', True):
        return None
    today = time.strftime('%Y-%m-%d')
    seen = get_seen()
    if not force and seen.get('daily_done') == today:
        return None
    hh, mm = str(SETTINGS.get('daily_time', '21:00')).split(':')
    now = datetime.now()
    if not force and (now.hour, now.minute) < (int(hh), int(mm)):
        return None
    start, end = int(time.mktime(time.strptime(today, '%Y-%m-%d'))), int(time.time())
    parts, wl = [], get_watchlist()
    covered = set()          # 已纳入报告的会话，自动发现时跳过，避免重复
    for item in wl:
        mon = M_BY_ACCOUNT.get(item.get('account')) or (MONITORS[0] if MONITORS else None)
        if not mon:
            continue
        if item.get('username') == '*':
            continue  # 通配账号的对象动态在 focus.json 里聚合
        covered.add((mon['account'], item['username']))
        try:
            rows = history_rows(mon, item['username'], start, end, 300)
        except Exception:
            continue
        if rows:
            lines = [f"{datetime.fromtimestamp(r['ts']).strftime('%H:%M')} {r['who']}: {r['text'][:100]}"
                     for r in rows]
            parts.append(f"### {item.get('label')}\n" + '\n'.join(lines))
    for k, st in FOCUS.items():
        acc, _, uname = k.partition('|')
        if not st.get('last_msg_ts') or st['last_msg_ts'] < start:
            continue
        mon = M_BY_ACCOUNT.get(acc)
        if not mon or any(p.get('username') == uname and p.get('username') != '*' for p in wl):
            continue
        covered.add((acc, uname))
        try:
            rows = history_rows(mon, uname, start, end, 200)
        except Exception:
            continue
        if rows:
            lines = [f"{datetime.fromtimestamp(r['ts']).strftime('%H:%M')} {r['who']}: {r['text'][:100]}"
                     for r in rows]
            parts.append(f"### {st.get('label', uname)}\n" + '\n'.join(lines))

    # 自动发现：关注名单之外、当天有新消息的会话也纳入，防止新群/临时群被漏掉
    auto_added = 0
    for mon in MONITORS:
        if auto_added >= 8:
            break
        try:
            cands = active_chats(mon, start, end, min_rows=6, limit=20)
        except Exception as e:
            log(f'自动发现失败 {mon["label"]}: {e}')
            continue
        for c in cands:
            if auto_added >= 8:
                break
            if (mon['account'], c['username']) in covered:
                continue
            if is_ignored(mon['account'], c['username']):
                continue
            if any(k in c['label'] for k in NOISE_LABELS):
                continue
            try:
                rows = history_rows(mon, c['username'], start, end, 120)
            except Exception:
                continue
            if not rows:
                continue
            covered.add((mon['account'], c['username']))
            lines = [f"{datetime.fromtimestamp(r['ts']).strftime('%H:%M')} {r['who']}: {r['text'][:100]}"
                     for r in rows]
            parts.append(f"### {c['label']}（自动发现·{c['rows']}条）\n" + '\n'.join(lines))
            auto_added += 1
    body = '今日关注会话暂无消息。' if not parts else None
    if parts:
        try:
            body = llm_client.chat(CFG, ANALYZE_SYSTEM,
                                   '今日关注会话汇总：\n\n' + '\n\n'.join(parts[:12])
                                   + '\n\n输出今日汇总（概览/要点/风险与待办/明日关注），suggestions 为空数组。', timeout=300)
            if isinstance(body, str):
                pass
        except Exception as e:
            log(f'日报 LLM 失败: {e}')
            body = None
        if not body:
            body = '\n\n'.join(parts)
    ddir = os.path.join(DATA, 'daily')
    os.makedirs(ddir, exist_ok=True)
    dp = os.path.join(ddir, f'{today}.md')
    with open(dp, 'w', encoding='utf-8') as f:
        f.write(f"# 每日汇总 {today}\n\n---\n\n" + (body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)))
    path = archive_write('daily', f'每日汇总 {today}', 'all', '-', '\n' + (body if isinstance(body, str) else ''), {'date': today})
    seen['daily_done'] = today
    save_seen(seen)
    log(f'日报已生成 {dp}')
    return path

# ---------------- 警报 ----------------

ALERTS = []
_alerts_lock = threading.Lock()
_next_alert_id = [0]

# 被用户手动关掉的异常（存 (id,ts) 指纹；alerts.jsonl 原文不动，防丢数据）
# 注：历史数据存在 id 重复（10-13 公众号两条从 1 重新计数），故不能单按 id 过滤
DISMISSED_ALERTS = _json_file('dismissed_alerts.json', {'items': []})

def _dismissed_pairs():
    return {(i.get('id'), i.get('ts')) for i in DISMISSED_ALERTS.get('items', [])}

def load_alerts():
    p = os.path.join(DATA, 'alerts.jsonl')
    if os.path.exists(p):
        try:
            for line in open(p, encoding='utf-8').readlines()[-200:]:
                a = json.loads(line)
                ALERTS.append(a)
                _next_alert_id[0] = max(_next_alert_id[0], a['id'])
        except (OSError, ValueError):
            pass
    # dismissed 只保留仍存在于内存的指纹（jsonl 仅留最近 200 条，旧指纹无意义）
    alive = {(a['id'], a['ts']) for a in ALERTS}
    trimmed = [i for i in DISMISSED_ALERTS.get('items', []) if (i.get('id'), i.get('ts')) in alive]
    if len(trimmed) != len(DISMISSED_ALERTS.get('items', [])):
        DISMISSED_ALERTS['items'] = trimmed
        _json_save('dismissed_alerts.json', DISMISSED_ALERTS)

def add_alert(level, label, account, username, summary, tags, path, detail=None):
    with _alerts_lock:
        _next_alert_id[0] += 1
        a = {'id': _next_alert_id[0], 'ts': int(time.time()), 'level': level, 'label': label,
             'account': account, 'username': username, 'summary': summary, 'tags': tags, 'path': path}
        if detail:
            a['detail'] = detail
        ALERTS.append(a)
        del ALERTS[:-200]
        try:
            with open(os.path.join(DATA, 'alerts.jsonl'), 'a', encoding='utf-8') as f:
                f.write(json.dumps(a, ensure_ascii=False) + '\n')
        except OSError:
            pass
        return a

# ---------------- 非关注扫描（近7天主动发言会话） ----------------

NONWATCH = _json_file('nonwatch.json', {'items': [], 'last_scan_ts': 0, 'known': []})

def save_nonwatch():
    _json_save('nonwatch.json', NONWATCH)

def self_active_conversations(mon, days=7):
    """近 N 天我有主动发言的会话 username 列表（按最近发言时间倒序）"""
    since = int(time.time()) - days * 86400
    names = contact_names(mon)
    out = []
    for db in sorted(glob.glob(os.path.join(mon['cache'], 'message_message_*.db'))):
        try:
            conn = sqlite3.connect('file:%s?mode=ro' % db, uri=True, timeout=8)
            tbls = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Msg_%'")]
            for tbl in tbls:
                try:
                    cols = {r[1].lower() for r in conn.execute(f"PRAGMA table_info({tbl})")}
                    c_ts = next((c for c in ('create_time',) if c in cols), None)
                    c_self = next((c for c in ('issender', 'is_sender') if c in cols), None)
                    if not (c_ts and c_self):
                        continue
                    row = conn.execute(
                        f"SELECT MAX({c_ts}) FROM {tbl} WHERE {c_self} = 1 AND {c_ts} > ?", (since,)).fetchone()
                    if row and row[0]:
                        md5h = tbl[4:]
                        for wxid, display in names.items():
                            if hashlib.md5(wxid.encode('utf-8')).hexdigest() == md5h:
                                out.append((int(row[0]), wxid, display))
                                break
                except sqlite3.Error:
                    continue
            conn.close()
        except sqlite3.Error:
            continue
    out.sort(reverse=True)
    return out

def nonwatch_scan(force=False):
    if not force and time.time() - NONWATCH.get('last_scan_ts', 0) < int(CFG.get('nonwatch_scan_minutes', 30)) * 60:
        return None
    wl = get_watchlist()
    items_new = []
    for mon in MONITORS:
        try:
            convs = self_active_conversations(mon)
        except Exception as e:
            log(f'主动发言扫描失败 {mon["account"]}: {e}')
            continue
        known = set(NONWATCH.get('known', []))
        for last_ts, wxid, display in convs[:40]:
            if watch_match(wl, mon['account'], wxid):
                continue
            key = f'{mon["account"]}|{wxid}'
            since_ts = last_ts - 3600  # 取最近一小时上下文
            try:
                rows = history_rows(mon, wxid, since_ts, int(time.time()), 60)
            except Exception:
                continue
            if not rows:
                continue
            lines = [f"{datetime.fromtimestamp(r['ts']).strftime('%m-%d %H:%M')} {r['who']}: {r['text'][:120]}"
                     for r in rows if not r['is_self']][-30:]
            if not lines:
                continue
            try:
                r = llm_client.chat_json(CFG, NONWATCH_SYSTEM,
                                         f"会话「{display}」：\n" + '\n'.join(lines), timeout=150)
            except Exception as e:
                log(f'非关注分析失败 {display}: {e}')
                continue
            for it in (r.get('items') or [])[:2]:
                if not isinstance(it, dict) or not it.get('summary'):
                    continue
                items_new.append({'ts': int(time.time()), 'account': mon['account'], 'username': wxid,
                                  'label': display, 'level': it.get('level', 'mid'),
                                  'tags': it.get('tags', []), 'summary': it.get('summary', ''),
                                  'advice': it.get('advice', '')})
            known.add(key)
    if items_new:
        NONWATCH['items'] = (items_new + NONWATCH.get('items', []))[:50]
    NONWATCH['known'] = sorted(known)[-500:]
    NONWATCH['last_scan_ts'] = int(time.time())
    save_nonwatch()
    return len(items_new)

# ---------------- 后台引擎 v2 ----------------

def get_seen():
    return _json_file('seen.json', {})

def save_seen(s):
    _json_save('seen.json', s)

class Engine:
    """私聊/被@ → 即时队列；群消息 → 2分钟静默聚批。"""
    def __init__(self):
        self.buffers = {}      # (account, username) -> {'msgs': [], 'deadline': float, 'item': {}}
        self.lock = threading.Lock()
        self.immediate = queue.Queue()
        self.tasks = queue.Queue()
        self.stop = threading.Event()

    def is_at_me(self, mon, m):
        if m.get('is_self'):
            return False
        c = m.get('content') or ''
        if '<atuserlist>' in c:
            me = self_wxid(mon)
            if me in c or me.strip('wxid_') in c:
                return True
        return False

    def tick_poll(self):
        seen = get_seen()
        wl = get_watchlist()
        for mon in MONITORS:
            if not mon_ok(mon):
                continue
            cursor = int(seen.get(mon['account'], int(time.time()) - 3600))
            try:
                msgs = mon_api(mon, f'/api/history?since={cursor}&limit=2000')
            except Exception:
                continue
            if msgs:
                seen[mon['account']] = max(int(m.get('timestamp', 0)) for m in msgs)
            for m in msgs:
                if is_ignored(mon['account'], m.get('username', ''), m.get('sender') or ''):
                    continue
                if is_voice_msg(m) and m.get('timestamp'):
                    ensure_voice(mon, m['username'], int(m['timestamp']))
                biz_money_check(mon, m)
                item = watch_match(wl, mon['account'], m.get('username', ''))
                if not item:
                    continue
                conv_user = m['username']
                eff_item = dict(item)
                eff_item['username'] = conv_user
                route_immediate = (not m.get('is_group')) or self.is_at_me(mon, m)
                if route_immediate:
                    self.immediate.put((mon, eff_item, m))
                else:
                    key = (mon['account'], conv_user)
                    with self.lock:
                        buf = self.buffers.get(key)
                        if not buf:
                            buf = {'msgs': [], 'deadline': time.time() + int(CFG.get('quiet_seconds', 120)),
                                   'item': eff_item}
                            self.buffers[key] = buf
                        buf['msgs'].append(m)
                        buf['deadline'] = time.time() + int(CFG.get('quiet_seconds', 120))
        save_seen(seen)

    def tick_flush(self):
        now = time.time()
        due = []
        with self.lock:
            for key, buf in list(self.buffers.items()):
                if now >= buf['deadline']:
                    due.append((key, buf))
                    del self.buffers[key]
        for (account, username), buf in due:
            mon = M_BY_ACCOUNT.get(account)
            if mon:
                self.tasks.put(('auto', mon, buf['item'], buf['msgs']))

    def worker(self):
        while not self.stop.is_set():
            # 即时队列：聚 20 秒窗口同对象消息，避免连发刷屏逐条分析
            try:
                mon, item, m = self.immediate.get(timeout=2)
            except queue.Empty:
                mon = None
            if mon:
                batch = [m]
                deadline = time.time() + 20
                while time.time() < deadline and len(batch) < 30:
                    try:
                        mon2, item2, m2 = self.immediate.get(timeout=max(0.5, deadline - time.time()))
                    except queue.Empty:
                        break
                    if mon2['account'] == mon['account'] and item2['username'] == item['username']:
                        batch.append(m2)
                    else:
                        self.immediate.put((mon2, item2, m2))
                        break
                self._run_auto(mon, item, batch)
            try:
                task = self.tasks.get(timeout=0.5)
            except queue.Empty:
                continue
            kind = task[0]
            try:
                if kind == 'auto':
                    _, mon, item, msgs = task
                    self._run_auto(mon, item, msgs)
                elif kind == 'manual':
                    _, mon, item, hours, cb = task
                    try:
                        cb(None, analyze_manual(mon, item, hours))
                    except Exception as e:
                        cb(e, None)
                elif kind == 'upload':
                    _, path, name, match, cb = task
                    try:
                        cb(None, analyze_upload(path, name, **(match or {})))
                    except Exception as e:
                        cb(e, None)
                elif kind == 'recording':
                    _, note_id, cb = task
                    try:
                        cb(None, analyze_recording_task(note_id))
                    except Exception as e:
                        cb(e, None)
            except Exception as e:
                log(f'任务异常: {e}')

    def _run_auto(self, mon, item, msgs):
        r = analyze_conversation(mon, item, msgs)
        if r:
            log(f'[分析] {r["level"]} {focus_label(mon, item["username"], item)}: '
                f'{r["summary"][:50]} / 备选{len(r["suggestions"])}条')

    def loop(self):
        log('引擎 v2 启动')
        last_daily = last_nw = last_clean = time.time()
        while not self.stop.is_set():
            try:
                self.tick_poll()
                self.tick_flush()
                now = time.time()
                if now - last_daily > 60:
                    last_daily = now
                    daily_report()
                if now - last_nw > 600:
                    last_nw = now
                    nonwatch_scan()
                if now - last_daily > 60:
                    mp_scan.maybe_scan(CFG.get('accounts', {}), MONITORS, RT, DATA, CFG,
                                       llm_client, add_alert, log)
                if now - last_clean > 86400:
                    last_clean = now
                    cleanup_recordings()
            except Exception as e:
                log(f'引擎异常: {e}')
            self.stop.wait(int(CFG.get('poll_seconds', 15)))

def cleanup_recordings():
    keep = int(SETTINGS.get('recording_keep_days', 30)) * 86400
    now = time.time()
    n = 0
    for fn in os.listdir(os.path.join(DATA, 'recordings')):
        p = os.path.join(DATA, 'recordings', fn)
        try:
            if now - os.path.getmtime(p) > keep:
                os.remove(p)
                n += 1
        except OSError:
            pass
    if n:
        log(f'清理录音文件 {n} 个（保留 {SETTINGS.get("recording_keep_days", 30)} 天）')

ENGINE = Engine()

# ---------------- Flask ----------------

app = Flask(__name__, static_folder='static')

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/api/state')
def api_state():
    return jsonify({
        'port': PORT,
        'llm': llm_client.ready(CFG),
        'monitors': [{'account': m['account'], 'label': m['label'], 'port': m['port'], 'ok': mon_ok(m), 'status': monitor_status(m)} for m in MONITORS],
        'settings': {k: SETTINGS.get(k) for k in ('daily_on', 'daily_time', 'notify_high', 'recording_keep_days')},
        'watchlist': len(get_watchlist()),
        'alerts_total': len(ALERTS),
        'time': time.strftime('%H:%M:%S'),    })

@app.route('/api/debug_names')
def api_debug_names():
    """临时诊断：看名称缓存里某会话映射到什么、名单 item 是什么。"""
    account = request.args.get('account', '')
    username = request.args.get('username', '')
    mon = M_BY_ACCOUNT.get(account)
    wl = get_watchlist()
    item = watch_match(wl, account, username) if (wl and account) else None
    return jsonify({
        'account': account,
        'mon_cache_dir': mon['cache'] if mon else None,
        'contact_names_hit': (contact_names(mon).get(username) if mon else None),
        'watch_item': item,
        'focus_label_result': (focus_label(mon, username, item) if mon else None),
        'focus_st_label': (focus_get(account, username).get('label') if account and username else None),
    })

_RECONNECT_LOCK = {'busy': False}

def _wm_module():
    """加载上游 wechat_monitor 模块（手动重连用，主自愈仍归 watchdog daemon）。"""
    import importlib.util
    wm_path = os.path.expanduser(r'~\.claude\tools\wechat-decrypt-clean\wechat_monitor.py')
    spec = importlib.util.spec_from_file_location('wechat_monitor_reconnect', wm_path)
    wm = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wm)
    return wm

@app.route('/api/monitors_reconnect', methods=['POST'])
def api_monitors_reconnect():
    """离线账号手动重连：extract_keys → 失败则用 .bak 兜底 → start_monitor。
    仅限离线账号；在线的跳过。watchdog 30s 自愈仍是主链路，本接口是应急通道。"""
    if _RECONNECT_LOCK['busy']:
        return jsonify({'ok': False, 'msg': '已有重连在进行中，请稍候'}), 409
    _RECONNECT_LOCK['busy'] = True
    try:
        wm = _wm_module()
        accounts = wm.scan_accounts()
        results = {}
        for mon in MONITORS:
            label = mon['label']
            if mon_ok(mon):
                results[label] = 'already_ok'
                continue
            acct = accounts.get(mon['account'])
            if not acct:
                results[label] = 'no_db_dir'
                continue
            keys = wm.extract_keys(acct)
            src = 'extract'
            if not keys:
                # extract 失败（如多开实例扫不到 Config.Cipher）→ .bak 兜底
                kf = os.path.join(wm.KEYS_DIR, f'keys_{mon["account"]}.json')
                bak = kf + '.bak'
                if os.path.exists(bak) and os.path.getsize(bak) > 500:
                    import shutil as _sh
                    _sh.copy2(bak, kf)
                    keys, src = kf, 'bak_fallback'
            if keys:
                wm.start_monitor(acct, keys)
                time.sleep(3)
                results[label] = 'started_' + src
            else:
                results[label] = 'extract_failed'
        return jsonify({'ok': True, 'results': results,
                        'monitors': [{'account': m['account'], 'label': m['label'], 'ok': mon_ok(m)} for m in MONITORS]})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)}), 500
    finally:
        _RECONNECT_LOCK['busy'] = False

@app.route('/api/stream')
def api_stream():
    """实时瀑布：双账号合并 + 语音带实时转写文本。"""
    try:
        since_ts = int(request.args.get('since', '0'))
    except ValueError:
        since_ts = 0
    out = []
    wl = get_watchlist()
    for mon in MONITORS:
        try:
            msgs = mon_api(mon, f'/api/history?since={since_ts}&limit=500')
        except Exception:
            continue
        kept = []
        for m in msgs:
            if is_ignored(mon['account'], m.get('username', ''), m.get('sender') or ''):
                continue
            m['account'] = mon['label']
            m['account_dir'] = mon['account']
            if m.get('is_group'):
                m['chat_label'] = contact_names(mon).get(m.get('username', ''), '') or m.get('username', '')
            m['sender'] = _resolve_sender(mon, m)
            if is_voice_msg(m) and m.get('timestamp'):
                vt = voice_text(mon, m['username'], int(m['timestamp']))
                if vt:
                    m['voice_text'] = vt
                else:
                    ensure_voice(mon, m['username'], int(m['timestamp']))
                m['content'] = ''  # 4.1.x 语音 content 是 voicemsg XML，一律清掉防泄漏
            if m.get('content'):
                m['content'] = _appmsg_text(m['content'])
            w = watch_match(wl, mon['account'], m.get('username', ''))
            if w:
                m['watched'] = True
                m['watch_label'] = focus_label(mon, m['username'], w)
            kept.append(m)
        out.extend(dedup_msgs(kept))
    out.sort(key=lambda m: m.get('timestamp', 0), reverse=True)
    since_id = int(request.args.get('since_id', '0') or 0)
    dismissed = _dismissed_pairs()
    return jsonify({'messages': out,
                    'alerts': [a for a in ALERTS if a['id'] > since_id and (a['id'], a['ts']) not in dismissed][-50:],
                    'monitors': [{'account': m['label'], 'ok': mon_ok(m)} for m in MONITORS]})

# ---- focus 页 ----

def _strip_wxid_prefix(text):
    """剥掉群消息正文里残留的 '发送者wxid: ' 前缀（monitor 已给 sender 字段）。
    兼容 't601522965: xxx' / 'wxid_xxx: xxx' / 'yanqingchun: xxx' 等任意 wxid 形态。"""
    return re.sub(r'^\S{5,40}[:：]\s+', '', text or '').strip()

def _appmsg_text(c):
    """富媒体/系统模板消息 → 一行可读文本。appmsg 取 <title>；sysmsg 模板提取邀请文本。
    普通消息原样返回。"""
    t = (c or '').strip()
    if t.startswith('<sysmsg'):
        tm = re.search(r'<template>\s*<!\[CDATA\[(.*?)\]\]>\s*</template>', t, re.S)
        nicks = re.findall(r'<nickname>\s*<!\[CDATA\[(.*?)\]\]>', t)
        body = (tm.group(1) if tm else '').strip().strip('"')
        if nicks:
            body = body.replace('$username$', nicks[0])
            if len(nicks) > 1:
                body = body.replace('$names$', '、'.join(nicks[1:]))
        body = body.replace('"', '')
        return '[系统消息] ' + (body or '系统通知')
    if '<appmsg' in t or t.startswith('<?xml'):
        m = re.search(r'<title>([^<]{1,80})</title>', t)
        return '[应用消息] ' + m.group(1) if m and m.group(1).strip() else '[应用消息]'
    return c or ''

def dedup_msgs(msgs):
    """monitor 对同一消息常返回多条（裸文+带 wxid 前缀 / is_self 翻转 / 同 ts 交错重复），
    统一按 (ts, sender, 剥前缀正文) 全量去重，保留首条；is_self=True 优先（自己消息气泡侧向正确）。
    注：只适用于单账号/单会话内部去重（调用方均已按 monitor 或 username 分组）。"""
    seen = set()
    out = []
    ordered = sorted(msgs, key=lambda x: (x.get('timestamp', 0), not x.get('is_self')))
    for m in ordered:
        c = (m.get('content') or '').strip()
        if m.get('type') == '文本':
            c = _strip_wxid_prefix(c)
        key = (m.get('timestamp', 0), (m.get('sender') or '').strip(), c)
        if key in seen:
            continue
        seen.add(key)
        m['content'] = c
        out.append(m)
    return out

@app.route('/api/focus')
def api_focus():
    """左列对象卡 + 非关注重点横条。"""
    wl = get_watchlist()
    cards = []
    seen_keys = set()
    pins = get_pins()
    for item in wl:
        mon = M_BY_ACCOUNT.get(item.get('account'))
        if not mon:
            continue
        if item.get('username') == '*':
            for k, st in FOCUS.items():
                acc, _, uname = k.partition('|')
                if acc != item['account'] or uname == '*':
                    continue
                if is_ignored(acc, uname):
                    continue    # 已「不再关注/监控」的对象不出卡
                seen_keys.add(fkey(acc, uname))
                cards.append(_focus_card(mon, uname, st, item, pins))
            # 通配账号下还没产生 focus 记录的会话不出卡（来消息即出现）
        else:
            if is_ignored(item['account'], item['username']):
                continue        # 已「不再关注/监控」的对象不出卡
            seen_keys.add(fkey(item['account'], item['username']))
            cards.append(_focus_card(mon, item['username'], focus_get(item['account'], item['username']), item, pins))
    cards.sort(key=lambda c: (not c['pinned'], -(c['last_msg_ts'] or 0)))
    return jsonify({'objects': cards, 'nonwatch': NONWATCH.get('items', [])[:10],
                    'nonwatch_last_scan': NONWATCH.get('last_scan_ts', 0)})

def _focus_card(mon, username, st, item, pins=None):
    unread = 0
    last_seen = st.get('last_seen_ts', 0)
    last_msg = st.get('last_msg_ts', 0)
    if last_msg and last_msg > last_seen:
        if not last_seen:
            unread = 1          # 尚未点开过：标未读，由用户点开置已读
        else:
            try:
                msgs = mon_api(mon, f"/api/history?chat={urllib.request.quote(username)}&since={int(last_seen)}&limit=200")
                unread = len([m for m in msgs if m.get('username') == username])
            except Exception:
                unread = 0
    return {'username': username, 'account': mon['account'], 'account_label': mon['label'],
            'label': focus_label(mon, username, item),
            'wildcard': item.get('username') == '*',
            'pinned': fkey(mon['account'], username) in (pins if pins is not None else get_pins()),
            'preview': st.get('msg_preview', ''),
            'unread': unread, 'last_msg_ts': last_msg or 0,
            'level': st.get('last_level'), 'summary': st.get('last_summary', ''),
            'note': item.get('note', '')}

@app.route('/api/focus/detail')
def api_focus_detail():
    account = request.args.get('account', '')
    username = request.args.get('username', '')
    mon = M_BY_ACCOUNT.get(account)
    if not mon:
        return jsonify({'error': '账号不存在'}), 400
    limit = min(int(request.args.get('limit', '100') or 100), 300)
    try:
        msgs = mon_api(mon, f'/api/history?chat={urllib.request.quote(username)}&limit={limit}')
    except Exception:
        msgs = []
    msgs = [m for m in msgs if m.get('username') == username]
    # 关注对象：monitor 近期窗口消息少（如益然恩类低频群）时，用解密 DB 层回填近 7 天历史，打开即有分析上下文
    if username != '*' and len(msgs) < 60:
        start7 = int(time.time()) - 7 * 86400
        have = {(int(m.get('timestamp', 0)), (m.get('content') or '').strip()) for m in msgs}
        try:
            for r in history_rows(mon, username, start_ts=start7, limit=200):
                key = (int(r['ts']), (r['text'] or '').strip())
                if not r['text'] or key in have:
                    continue
                have.add(key)
                msgs.append({'timestamp': r['ts'], 'username': username,
                             'sender': '' if r['is_self'] else r['who'],
                             'content': r['text'],
                             'type': '文本' if r['kind'] == 'text' else f"[{r['kind']}]",
                             'is_self': r['is_self'],
                             'is_group': username.endswith('@chatroom')})
        except Exception as e:
            log(f'focus 详情 7 天回填失败 {username}: {e}')
    msgs = dedup_msgs(msgs)
    msgs.sort(key=lambda m: m.get('timestamp', 0))
    wl = get_watchlist()
    item = watch_match(wl, account, username) or {'username': username, 'account': account, 'label': username}
    for m in msgs:
        m['account'] = mon['label']
        if not m.get('is_group'):
            m['sender'] = '' if m.get('is_self') else _resolve_sender(mon, m)
        if is_voice_msg(m) and m.get('timestamp'):
            vt = voice_text(mon, username, int(m['timestamp']))
            if vt:
                m['voice_text'] = vt
            # 4.1.x 语音 content 是 voicemsg XML（非空），一律清掉，前端走 [语音] 渲染
            m['content'] = ''
        if m.get('content'):
            m['content'] = _appmsg_text(m['content'])
    st = focus_get(account, username)
    history = [a for a in list_archives() if a.get('username') == username and a.get('account') == account][:10]
    return jsonify({'messages': msgs, 'state': st, 'archives': history,
                    'label': focus_label(mon, username, item)})

@app.route('/api/focus/read', methods=['POST'])
def api_focus_read():
    body = request.get_json(force=True)
    st = focus_update(body.get('account', ''), body.get('username', ''), last_seen_ts=time.time())
    return jsonify({'ok': True})

@app.route('/api/focus/pin', methods=['POST'])
def api_focus_pin():
    """置顶/取消置顶关注对象：{account, username, pinned}。"""
    body = request.get_json(force=True)
    key = fkey(body.get('account', ''), body.get('username', ''))
    pins = get_pins()
    if body.get('pinned'):
        pins.add(key)
    else:
        pins.discard(key)
    save_pins(pins)
    return jsonify({'ok': True, 'pinned': key in pins})

@app.route('/api/suggest', methods=['POST'])
def api_suggest():
    """重新生成备选回复（基于最近上下文）。"""
    body = request.get_json(force=True)
    account, username = body.get('account', ''), body.get('username', '')
    mon = M_BY_ACCOUNT.get(account)
    if not mon:
        return jsonify({'error': '账号不存在'}), 400
    try:
        msgs = mon_api(mon, f'/api/history?chat={urllib.request.quote(username)}&limit=40')
    except Exception:
        msgs = []
    msgs = [m for m in msgs if m.get('username') == username]
    item = {'username': username, 'account': account}
    lines = _lines_from_monitor(mon, item, msgs)
    if not lines:
        return jsonify({'error': '没有可用上下文'}), 400
    label = focus_label(mon, username, item)
    user = (f"会话「{label}」最近消息：\n" + '\n'.join(lines)
            + '\n\n重新为庞恩浩生成备选回复，输出 JSON（suggestions 四条）。')
    try:
        r = llm_client.chat_json(CFG, ANALYZE_SYSTEM, user, timeout=180)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    suggestions = [{'style': s.get('style', '正常'), 'text': str(s.get('text', '')).strip()}
                   for s in (r.get('suggestions') or [])[:4] if s.get('text')]
    st = focus_update(account, username, suggestions=suggestions,
                      last_advice=r.get('advice', ''), last_analyze_ts=time.time())
    return jsonify({'suggestions': suggestions, 'advice': r.get('advice', ''), 'summary': r.get('summary', '')})

# ---- watchlist 管理 ----

@app.route('/api/watchlist', methods=['GET', 'POST'])
def api_watchlist():
    if request.method == 'GET':
        return jsonify(get_watchlist())
    item = request.get_json(force=True)
    username = (item.get('username') or '').strip()
    account_dir = item.get('account_dir') or (MONITORS[0]['account'] if MONITORS else '')
    if not username or not account_dir:
        return jsonify({'error': 'username/account_dir 必填'}), 400
    wl = get_watchlist()
    if any(w['username'] == username and w['account'] == account_dir for w in wl):
        return jsonify({'error': '已在关注名单'}), 400
    mon = M_BY_ACCOUNT.get(account_dir)
    label = (item.get('label') or '').strip()
    if not label or label in GENERIC_LABELS:
        # '对方'/'我' 等占位名一律回退联系人库（备注优先、其次昵称）
        label = (contact_names(mon).get(username) if mon else '') or label or username
    wl.append({'username': username, 'label': label, 'account': account_dir,
               'note': item.get('note', ''), 'created': time.strftime('%Y-%m-%d %H:%M')})
    save_watchlist(wl)
    return jsonify({'ok': True, 'item': wl[-1]})

@app.route('/api/watchlist/<path:spec>', methods=['DELETE'])
def api_watchlist_del(spec):
    account, _, username = spec.partition('|')
    wl = [w for w in get_watchlist() if not (w['username'] == username and w['account'] == account)]
    save_watchlist(wl)
    return jsonify({'ok': True})

@app.route('/api/contacts')
def api_contacts():
    q = request.args.get('q', '').strip().lower()
    if not q:
        return jsonify([])
    out = []
    for mon in MONITORS:
        for wxid, display in contact_names(mon).items():
            if q in wxid.lower() or q in display.lower():
                out.append({'account': mon['label'], 'account_dir': mon['account'],
                            'username': wxid, 'display': display})
            if len(out) >= 30:
                break
    return jsonify(out)

# ---- 历史检索（独立页） ----

@app.route('/api/history')
def api_history():
    account = request.args.get('account', '')
    username = request.args.get('username', '')
    mon = M_BY_ACCOUNT.get(account) or next((m for m in MONITORS if m['label'] == account), None)
    if not mon or not username:
        return jsonify({'error': '参数缺失'}), 400
    try:
        start = int(request.args.get('start', '0') or 0)
        end = int(request.args.get('end', '4102444800') or 4102444800)
        limit = min(int(request.args.get('limit', '200') or 200), 1000)
    except ValueError:
        return jsonify({'error': '参数格式错误'}), 400
    rows = history_rows(mon, username, start, end, limit, request.args.get('keyword', ''))
    return jsonify(list(reversed(rows)))

@app.route('/api/stats/today')
def api_stats_today():
    today = time.strftime('%Y-%m-%d')
    start = int(time.mktime(time.strptime(today, '%Y-%m-%d')))
    alerts_today = [a for a in ALERTS if a['ts'] >= start]
    counts = {}
    for a in alerts_today:
        counts[a['level']] = counts.get(a['level'], 0) + 1
    return jsonify({'alerts_today': len(alerts_today), 'by_level': counts,
                    'watch_count': len(get_watchlist()),
                    'focus_objects': len(FOCUS)})

# ---- 存档/上传/日报/设置 ----

@app.route('/api/archives')
def api_archives():
    return jsonify(list_archives(request.args.get('month'), request.args.get('username'),
                                 request.args.get('kind'), request.args.get('level')))

@app.route('/api/archive')
def api_archive():
    rel = request.args.get('p', '')
    if not rel.endswith(('.md', '.json')):
        return jsonify({'error': '非法路径'}), 403
    text = archive_read(rel)
    if text is None:
        return jsonify({'error': '文件不存在'}), 404
    return jsonify({'path': rel, 'text': text})

@app.route('/api/analyze', methods=['POST'])
def api_analyze():
    body = request.get_json(force=True)
    username = body.get('username', '')
    account = body.get('account', '')
    mon = M_BY_ACCOUNT.get(account) or (MONITORS[0] if MONITORS else None)
    if not mon:
        return jsonify({'error': '无可用账号监控'}), 400
    item = {'username': username, 'account': mon['account']}
    hours = float(body.get('hours', 24))
    done = threading.Event()
    box = {}
    def cb(err, res):
        box['err'], box['res'] = err, res
        done.set()
    ENGINE.tasks.put(('manual', mon, item, hours, cb))
    if not done.wait(300):
        return jsonify({'error': '分析超时（5分钟）'}), 504
    if box.get('err'):
        return jsonify({'error': str(box['err'])}), 500
    return jsonify(box['res'])

@app.route('/api/upload', methods=['POST'])
def api_upload():
    f = request.files.get('file')
    if not f:
        return jsonify({'error': '未收到文件'}), 400
    orig = f.filename or 'recording'
    safe = re.sub(r'[^\w.\-]+', '_', orig)[:80]
    saved = os.path.join(DATA, 'recordings', f"{time.strftime('%Y%m%d_%H%M%S')}_{safe}")
    f.save(saved)
    match = {}
    if request.form.get('match_username') and request.form.get('match_account'):
        match = {'match_username': request.form['match_username'], 'match_account': request.form['match_account']}
    done = threading.Event()
    box = {}
    def cb(err, res):
        box['err'], box['res'] = err, res
        done.set()
    ENGINE.tasks.put(('upload', saved, orig, match, cb))
    if not done.wait(300):
        return jsonify({'error': '分析超时'}), 504
    if box.get('err'):
        return jsonify({'error': str(box['err'])}), 500
    return jsonify(box['res'])

@app.route('/api/daily/force', methods=['POST'])
def api_daily_force():
    p = daily_report(force=True)
    return jsonify({'ok': True, 'path': p})

@app.route('/api/nonwatch/scan', methods=['POST'])
def api_nonwatch_scan():
    n = nonwatch_scan(force=True)
    return jsonify({'ok': True, 'new': n, 'items': NONWATCH.get('items', [])[:10]})

@app.route('/api/settings', methods=['GET', 'POST'])
def api_settings():
    if request.method == 'POST':
        body = request.get_json(force=True)
        for k in ('daily_on', 'daily_time', 'notify_high', 'recording_keep_days'):
            if k in body:
                SETTINGS[k] = body[k]
        _json_save('settings.json', SETTINGS)
        return jsonify({'ok': True, 'settings': SETTINGS})
    return jsonify(SETTINGS)

@app.route('/api/llm_test', methods=['POST'])
def api_llm_test():
    try:
        t = llm_client.chat(CFG, '', '用一句话确认你能正常工作。', timeout=60)
        return jsonify({'ok': True, 'reply': t[:200]})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/cleanup_recordings', methods=['POST'])
def api_cleanup():
    cleanup_recordings()
    return jsonify({'ok': True})

# ---------------- Get笔记 录音任务与端点 ----------------

def analyze_recording_task(note_id):
    d = biji_detail(note_id)
    return analyze_recording(note_id, d['title'], d['content'], d['original'], get_watchlist())

@app.route('/api/recordings')
def api_recordings():
    try:
        keyword = request.args.get('q', '').strip()
        days = float(request.args.get('days', '7') or 7)
        since = int(time.time()) - int(days * 86400) if days > 0 else 0
        notes = biji_list(keyword, since_ts=since, limit=50)
        return jsonify(notes)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/recording/analyze', methods=['POST'])
def api_recording_analyze():
    body = request.get_json(force=True)
    note_id = str(body.get('id', '')).strip()
    if not note_id:
        return jsonify({'error': '缺 id'}), 400
    done = threading.Event()
    box = {}
    def cb(err, res):
        box['err'], box['res'] = err, res
        done.set()
    ENGINE.tasks.put(('recording', note_id, cb))
    if not done.wait(300):
        return jsonify({'error': '分析超时（5分钟）'}), 504
    if box.get('err'):
        return jsonify({'error': str(box['err'])}), 500
    return jsonify(box['res'])

@app.route('/api/ignore', methods=['GET', 'POST'])
def api_ignore():
    ig = get_ignore()
    if request.method == 'GET':
        return jsonify(ig.get('items', []))
    body = request.get_json(force=True)
    account, username = body.get('account', ''), body.get('username', '')
    sender = (body.get('sender') or '').strip()
    label = body.get('label', '') or sender or username
    chat_label = body.get('chat_label', '')
    items = ig.setdefault('items', [])
    def _same(i):
        if sender:
            return i.get('sender') == sender
        return not i.get('sender') and i.get('username') == username
    if not any(_same(i) for i in items):
        items.append({'account': account, 'username': username, 'sender': sender,
                      'label': label, 'chat_label': chat_label,
                      'added': time.strftime('%Y-%m-%d %H:%M')})
        save_ignore(ig)
    return jsonify({'ok': True, 'count': len(items)})

@app.route('/api/ignore/<path:spec>', methods=['DELETE'])
def api_ignore_del(spec):
    parts = spec.split('|')
    account = parts[0]
    username = parts[1] if len(parts) > 1 else ''
    sender = parts[2] if len(parts) > 2 else ''
    ig = get_ignore()
    ig['items'] = [i for i in ig.get('items', [])
                   if not (i.get('account') == account and i.get('username', '') == username
                           and (i.get('sender') or '') == sender)]
    save_ignore(ig)
    return jsonify({'ok': True})

@app.route('/api/alerts')
def api_alerts():
    since_id = int(request.args.get('since_id', '0') or 0)
    days = min(int(request.args.get('days', '7') or 7), 30)
    since_ts = int(time.time()) - days * 86400
    dismissed = _dismissed_pairs()
    return jsonify([a for a in ALERTS if a['id'] > since_id and a['ts'] >= since_ts
                    and (a['id'], a['ts']) not in dismissed][-100:])

@app.route('/api/alerts/dismiss', methods=['POST'])
def api_alerts_dismiss():
    """关掉单条异常：(id,ts) 指纹记入 dismissed_alerts.json，alerts.jsonl 原文保留。"""
    body = request.get_json(silent=True) or {}
    aid, ats = int(body.get('id', 0) or 0), int(body.get('ts', 0) or 0)
    if not aid or not ats:
        return jsonify({'ok': False, 'error': '缺少 id/ts'}), 400
    items = DISMISSED_ALERTS.setdefault('items', [])
    if not any(i.get('id') == aid and i.get('ts') == ats for i in items):
        items.append({'id': aid, 'ts': ats})
        _json_save('dismissed_alerts.json', DISMISSED_ALERTS)
    return jsonify({'ok': True})

@app.route('/api/mp_scan')
def api_mp_scan():
    """公众号 AI 扫描状态 + 手动触发（POST）。"""
    if request.method == 'POST':
        r = mp_scan.run_scan(CFG.get('accounts', {}), MONITORS, RT, DATA, CFG,
                             llm_client, add_alert, log)
        return jsonify({'triggered': True, 'result': r, 'status': mp_scan.status()})
    return jsonify(mp_scan.status())

# ---------------- 开机自启 ----------------

def _lnk_path():
    return os.path.join(os.path.expanduser('~'), 'AppData', 'Roaming', 'Microsoft',
                        'Windows', 'Start Menu', 'Programs', 'Startup', 'OpsCenter.lnk')

def cmd_install():
    """Startup .lnk 直指 pythonw.exe（lnk Unicode 存储，绕开 vbs 中文路径坑）。"""
    exe = sys.executable
    if exe.lower().endswith('python.exe'):
        exe = exe.replace('python.exe', 'pythonw.exe')
    try:
        wsh = __import__('win32com.client', fromlist=['Dispatch']).Dispatch('WScript.Shell')
        s = wsh.CreateShortcut(_lnk_path())
        s.TargetPath = exe
        s.Arguments = f'"{os.path.join(BASE, "ops_center.py")}" daemon'
        s.WorkingDirectory = BASE
        s.WindowStyle = 7
        s.Description = '实时微信与录音监控中台'
        s.Save()
        print(f'已注册开机自启: {_lnk_path()}')
        print('重启后中台将自动运行：http://127.0.0.1:5690')
    except Exception as e:
        print(f'注册失败（缺 pywin32?）: {e}')

def cmd_uninstall():
    try:
        os.remove(_lnk_path())
        print(f'已移除: {_lnk_path()}')
    except OSError:
        pass

def _pid_alive(pid):
    try:
        import ctypes
        h = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
        if not h:
            return False
        code = ctypes.c_ulong()
        ok = ctypes.windll.kernel32.GetExitCodeProcess(h, ctypes.byref(code))
        ctypes.windll.kernel32.CloseHandle(h)
        return bool(ok) and code.value == 259
    except Exception:
        return False

def cmd_status():
    try:
        pid = int(open(os.path.join(DATA, 'ops_center.pid')).read().strip())
    except (OSError, ValueError):
        pid = None
    import socket
    s = socket.socket(); s.settimeout(1)
    try:
        s.connect(('127.0.0.1', PORT)); print(f'中台端口 {PORT}: HTTP 可访问')
    except OSError:
        print(f'中台端口 {PORT}: 不可访问')
    finally:
        s.close()
    if pid:
        print(f'pid 文件: {pid} ({"存活" if _pid_alive(pid) else "已退出"})')
    for m in MONITORS:
        print(f'监控 {m["label"]} :{m["port"]} {"OK" if mon_ok(m) else "不可达"}')

def cmd_daemon():
    # 先清旧实例：Windows SO_REUSEADDR 会让新旧进程同时绑 5690 都"成功"，
    # 请求随机落到旧代码（2026-10-16 实战踩坑），必须显式杀旧再绑。
    # 用 os.kill(SIGTERM) 而非 taskkill：native 运行下 //PID 参数会失效（守卫会静默放过），
    # 而 MSYS 下 /PID 又会被路径转换吃掉——os.kill 两条路都稳。
    try:
        old = int(open(os.path.join(DATA, 'ops_center.pid')).read().strip())
        if old != os.getpid() and _pid_alive(old):
            try:
                os.kill(old, signal.SIGTERM)
            except OSError:
                pass
            time.sleep(1)
    except (OSError, ValueError):
        pass
    with open(os.path.join(DATA, 'ops_center.pid'), 'w') as f:
        f.write(str(os.getpid()))
    log(f'中台 daemon 启动 pid={os.getpid()}')
    run()

def run():
    load_alerts()
    threading.Thread(target=ENGINE.loop, daemon=True).start()
    threading.Thread(target=ENGINE.worker, daemon=True).start()
    threading.Thread(target=voice_worker, daemon=True).start()
    log(f'中台 v2 监听 http://127.0.0.1:{PORT}')
    app.run(host='127.0.0.1', port=PORT, threaded=True, debug=False, use_reloader=False)

if __name__ == '__main__':
    if hasattr(sys.stdout, 'buffer'):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'run'
    {'run': run, 'daemon': cmd_daemon, 'install': cmd_install,
     'uninstall': cmd_uninstall, 'status': cmd_status}.get(cmd, lambda: print(__doc__))()
