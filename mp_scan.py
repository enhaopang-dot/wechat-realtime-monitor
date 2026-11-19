# -*- coding: utf-8 -*-
"""公众号 AI 价值扫描（2026-10-13 用户拍板：每天扫两次）。

链路：解密 biz_message_0.db（公众号消息独立分库，message_N.db 里没有）
→ 提 type=49 推文（local_type 需 &0xFFFFFFFF）→ zstd 解压 → 抽标题/链接/摘要
→ LLM 判 AI 相关度 → ≥7 分写入 alerts（前端警报列表统一可见）。

调度：ENGINE.loop 每 60s 调一次 maybe_scan()，距上次扫描 ≥10h 触发
（≈每天两次，抗睡眠/重启，无需固定时刻）。

踩坑固化：
- 公众号消息不在 message_message_*.db，在 message\\biz_message_0.db
- local_type 带高位标志（实测 21474836529），必须 &0xFFFFFFFF==49
- message_content 是 zstd 压缩（魔数 28 b5 2f fd），须 zstandard 解压
- 真推文特征：URL 含 mp.weixin.qq.com（type=5 模板通知也有 49，需 URL 过滤）
- keys 键名用反斜杠 'message\\biz_message_0.db'
"""
import hashlib
import json
import os
import re
import sqlite3
import threading
import time

from Crypto.Cipher import AES

PAGE_SZ = 4096
SALT_SZ = 16
RESERVE_SZ = 80
SQLITE_HDR = b'SQLite format 3\x00'

MP_MIN_SCORE = 7          # AI 相关度 ≥7 才提醒
MP_MAX_BATCH = 30         # 单次扫描最多送 LLM 的篇数（防爆量）
MP_FIRST_WINDOW = 86400   # 首次扫描窗口：过去 24h
MP_INTERVAL = 36000       # 扫描间隔 ≥10h ≈ 每天两次

_lock = threading.Lock()
_scanning = {'running': False, 'last_ts': 0, 'last_result': ''}

def _decrypt_page(enc_key, page_data, pgno):
    iv = page_data[PAGE_SZ - RESERVE_SZ: PAGE_SZ - RESERVE_SZ + 16]
    if pgno == 1:
        encrypted = page_data[SALT_SZ: PAGE_SZ - RESERVE_SZ]
        return bytearray(SQLITE_HDR + AES.new(enc_key, AES.MODE_CBC, iv).decrypt(encrypted) + b'\x00' * RESERVE_SZ)
    encrypted = page_data[: PAGE_SZ - RESERVE_SZ]
    return bytearray(AES.new(enc_key, AES.MODE_CBC, iv).decrypt(encrypted) + b'\x00' * RESERVE_SZ)

def _full_decrypt(db_path, out_path, enc_key):
    with open(db_path, 'rb') as fin, open(out_path, 'wb') as fout:
        pgno = 0
        while True:
            page = fin.read(PAGE_SZ)
            if len(page) < PAGE_SZ:
                break
            pgno += 1
            fout.write(_decrypt_page(enc_key, page, pgno))

def _zstd_maybe(raw):
    if raw[:4] == b'\x28\xb5\x2f\xfd':
        try:
            import zstandard as zstd
            return zstd.ZstdDecompressor().decompress(raw, max_output_size=8 * 1024 * 1024)
        except Exception:
            return None
    return raw

def _cdata(txt, tag):
    m = re.search(r'<%s><!\[CDATA\[(.*?)\]\]></%s>' % (tag, tag), txt, re.S)
    if m:
        return m.group(1).strip()
    m = re.search(r'<%s>(.*?)</%s>' % (tag, tag), txt, re.S)
    return m.group(1).strip() if m else ''

def _plain(s, limit):
    """去 HTML 标签 + 合并空白 + 截断。公众号'文字消息'类推文会把全文塞进 title/des。"""
    s = re.sub(r'<[^>]+>', '', s or '')
    s = re.sub(r'\s+', ' ', s).strip()
    return s[:limit]

def _load_mp_names(cache_dir):
    """gh_xxx → 公众号显示名，来自 contact_contact.db。"""
    out = {}
    p = os.path.join(cache_dir, 'contact_contact.db')
    if not os.path.exists(p):
        return out
    try:
        con = sqlite3.connect('file:%s?mode=ro' % p, uri=True, timeout=8)
        for uname, nick in con.execute(
                "select username, nick_name from contact where username like 'gh_%'"):
            if nick:
                out[uname] = nick
        con.close()
    except sqlite3.Error:
        pass
    return out

def collect_articles(mon, rt, since_ts):
    """扫一个账号的 biz_message_0.db，返回 [{key, gh, mp, title, url, digest, ts}]。"""
    wxid = mon['account']
    keys_p = os.path.join(rt, 'keys', 'keys_%s.json' % wxid)
    if not os.path.exists(keys_p):
        return []
    try:
        keys = json.load(open(keys_p, encoding='utf-8'))
        ki = keys.get('message\\biz_message_0.db')
        if not ki:
            return []
        enc_key = bytes.fromhex(ki['enc_key'])
    except (OSError, ValueError, KeyError):
        return []

    # 定位源库（与 wechat_monitor 同规则的 DATA_ROOTS）
    src = None
    for root in (r"D:\RJ\Weixinjilu\xwechat_files",
                 os.path.expanduser(r'~\Documents\xwechat_files')):
        p = os.path.join(root, wxid, 'db_storage', 'message', 'biz_message_0.db')
        if os.path.exists(p):
            src = p
            break
    if not src:
        return []

    tmp_dir = os.path.join(mon['runtime'], 'ops_center_tmp')
    os.makedirs(tmp_dir, exist_ok=True)
    dec = os.path.join(tmp_dir, 'biz_message_0_dec.db')
    try:
        _full_decrypt(src, dec, enc_key)
    except OSError:
        return []

    mp_names = _load_mp_names(mon['cache'])
    arts = []
    try:
        con = sqlite3.connect('file:%s?mode=ro' % dec, uri=True, timeout=10)
        # 本库自带 Name2Id（rowid→user_name），优先用它反查发送者（坑①：分库映射独立）
        n2i = {}
        try:
            n2i = {int(r[0]): r[1] for r in con.execute('select rowid, user_name from Name2Id').fetchall()}
        except sqlite3.Error:
            pass
        # 兜底：md5(gh_uname) 反查表名（覆盖 Name2Id 缺失场景）
        hash2name = {}
        for uname in mp_names:
            hash2name[hashlib.md5(uname.encode('utf-8')).hexdigest()] = uname
            hash2name[hashlib.md5((uname + '@app').encode('utf-8')).hexdigest()] = uname
        tabs = [r[0] for r in con.execute(
            "select name from sqlite_master where type='table' and name like 'Msg_%'")]
        for t in tabs:
            try:
                rows = con.execute(
                    'select local_id, create_time, real_sender_id, message_content from "%s" '
                    'where (local_type & 4294967295) = 49 and create_time > ? '
                    'order by create_time' % t, (since_ts,)).fetchall()
            except sqlite3.Error:
                continue
            for lid, cts, rid, raw in rows:
                if not raw:
                    continue
                if isinstance(raw, str):
                    raw = raw.encode('utf-8', errors='replace')
                raw = _zstd_maybe(raw)
                if raw is None:
                    continue
                txt = raw.decode('utf-8', errors='replace')
                url = _cdata(txt, 'url')
                if 'mp.weixin.qq.com' not in url:
                    continue  # 过滤模板通知（还款/支付等）
                title = _plain(_cdata(txt, 'title'), 80)
                if not title:
                    continue
                digest = _plain(_cdata(txt, 'des'), 100)
                try:
                    rid = int(rid)
                except (TypeError, ValueError):
                    rid = 0
                gh = n2i.get(rid) or hash2name.get(t[4:]) or t[4:]
                if gh in ('brandprivatemsg@hardcode', 'notifymessage', 'mphelper',
                          'weixinguanhaozhushou') or not gh.startswith('gh'):
                    continue  # 系统通道不算公众号推文
                arts.append({
                    'key': hashlib.md5(('%s|%s|%d' % (wxid, url, lid)).encode()).hexdigest()[:16],
                    'gh': gh, 'mp': mp_names.get(gh, gh), 'title': title, 'url': url,
                    'digest': digest, 'ts': int(cts),
                })
        con.close()
    except sqlite3.Error:
        pass
    return arts

def judge_ai_value(cfg, llm_client, items):
    """LLM 批量判 AI 相关度。返回 {key: {score, reason}}；失败抛 RuntimeError。"""
    lines = []
    for i, a in enumerate(items):
        lines.append('%d. [%s]《%s》 %s' % (i, a['mp'], a['title'],
                                           ('—— ' + a['digest']) if a['digest'] else ''))
    system = ('你是信息筛选助手。用户庞恩浩是 AI 创业者（AI Agent 落地/电商/命理自媒体方向）。'
              '判断每条公众号推文对他是否有价值：AI 技术/AI 工具/AI 商业落地/Agent/自动化的打高分；'
              '纯健康养生/时政/金融还款/营销软文打低分。'
              '只输出 JSON：{"items":[{"i":序号,"score":0-10,"reason":"一句话"}]}')
    user = '推文列表：\n' + '\n'.join(lines)
    data = llm_client.chat_json(cfg, system, user, timeout=120)
    out = {}
    for it in (data.get('items') or []):
        try:
            out[items[int(it['i'])]['key']] = {
                'score': max(0, min(10, int(it.get('score', 0)))),
                'reason': str(it.get('reason', ''))[:120],
            }
        except (ValueError, IndexError, TypeError):
            continue
    return out

def run_scan(accounts, mons, rt, data_dir, cfg, llm_client, add_alert, log):
    """全量扫一轮：收集→LLM 判分→高分写 alerts。accounts 是配置的 {wxid: label}。"""
    with _lock:
        if _scanning['running']:
            return 'busy'
        _scanning['running'] = True
    try:
        state_p = os.path.join(data_dir, 'mp_scan.json')
        try:
            state = json.load(open(state_p, encoding='utf-8'))
        except (OSError, ValueError):
            state = {'seen': {}, 'last_ts': 0}
        now = int(time.time())
        since = state.get('last_ts') or (now - MP_FIRST_WINDOW)
        # seen 只保留 7 天，防无限膨胀
        seen = {k: v for k, v in state.get('seen', {}).items() if v > now - 7 * 86400}

        all_arts = []
        for mon in mons:
            try:
                arts = collect_articles(mon, rt, since - 300)
                all_arts.extend(arts)
                log('[公众号扫描] %s 窗口内推文 %d 篇' % (mon['label'], len(arts)))
            except Exception as e:
                log('[公众号扫描] %s 失败: %s' % (mon['label'], e))

        fresh = [a for a in all_arts if a['key'] not in seen]
        fresh.sort(key=lambda a: -a['ts'])
        fresh = fresh[:MP_MAX_BATCH]
        if not fresh:
            state.update({'seen': seen, 'last_ts': now})
            json.dump(state, open(state_p, 'w', encoding='utf-8'), ensure_ascii=False)
            _scanning.update({'last_ts': now, 'last_result': '无新推文'})
            return 'empty'

        try:
            judged = judge_ai_value(cfg, llm_client, fresh)
        except Exception as e:
            log('[公众号扫描] LLM 判分失败（下轮重试）: %s' % e)
            return 'llm_fail'

        hit = 0
        for a in fresh:
            seen[a['key']] = now
            j = judged.get(a['key'])
            if not j or j['score'] < MP_MIN_SCORE:
                continue
            hit += 1
            summary = _plain('【AI相关度%d/10】%s —— %s' % (
                j['score'], a['title'],
                j['reason'] or a['digest'] or '点击查看原文'), 160)
            add_alert(
                'info', a['mp'], a.get('gh') or '', a['gh'],
                summary, ['公众号', 'AI', a['mp']], a['url'])
        state.update({'seen': seen, 'last_ts': now})
        json.dump(state, open(state_p, 'w', encoding='utf-8'), ensure_ascii=False)
        _scanning.update({'last_ts': now,
                          'last_result': '新推文%d 篇，AI 价值提醒 %d 条' % (len(fresh), hit)})
        log('[公众号扫描] 新推文 %d 篇，AI 价值提醒 %d 条' % (len(fresh), hit))
        return 'ok'
    finally:
        with _lock:
            _scanning['running'] = False

def maybe_scan(accounts, mons, rt, data_dir, cfg, llm_client, add_alert, log):
    """ENGINE.loop 每 60s 调用：距上次 ≥10h 才真扫。"""
    if time.time() - _scanning['last_ts'] < MP_INTERVAL:
        return
    run_scan(accounts, mons, rt, data_dir, cfg, llm_client, add_alert, log)

def status():
    return dict(_scanning)
