# -*- coding: utf-8 -*-
"""周期报告：按 天/周/月 拉全部会话（不限关注名单），生成 Markdown。

日报由 ops_center 21:00 自动出；周报/月报用这个脚本手动或定时跑。

    python report_period.py --days 7
    python report_period.py --days 30 --llm
    python report_period.py --from 2026-10-01 --to 2026-10-23

不加 --llm 只出原始时间线（零 token）；加 --llm 让模型写汇总（概览/要点/风险与待办）。
"""
from __future__ import annotations

import argparse
import os
import time
from datetime import datetime

import ops_center as o

MAX_CHATS = 25          # 单份报告最多纳入多少个会话
MAX_ROWS_PER_CHAT = 150


def period_report(start: int, end: int, use_llm: bool = False) -> str:
    parts, stats = [], []
    for mon in o.MONITORS:
        try:
            cands = o.active_chats(mon, start, end, min_rows=3, limit=60)
        except Exception as e:  # noqa: BLE001
            o.log(f'周期报告 active_chats 失败 {mon["label"]}: {e}')
            continue
        for c in cands:
            if len(parts) >= MAX_CHATS:
                break
            if o.is_ignored(mon['account'], c['username']):
                continue
            if any(k in c['label'] for k in o.NOISE_LABELS):
                continue
            try:
                rows = o.history_rows(mon, c['username'], start, end, MAX_ROWS_PER_CHAT)
            except Exception:  # noqa: BLE001
                continue
            if not rows:
                continue
            lines = [f"{datetime.fromtimestamp(r['ts']).strftime('%m-%d %H:%M')} {r['who']}: {r['text'][:120]}"
                     for r in rows]
            parts.append(f"### {c['label']}（{mon['label']}·{c['rows']}条）\n" + '\n'.join(lines))
            stats.append((mon['label'], c['label'], c['rows']))

    head = (f"# 周期报告 {datetime.fromtimestamp(start):%Y-%m-%d} ~ {datetime.fromtimestamp(end):%Y-%m-%d}\n\n"
            f"会话 {len(stats)} 个，消息 {sum(s[2] for s in stats)} 条\n\n---\n\n")
    if not parts:
        return head + "本周期无消息。\n"

    body = None
    if use_llm and parts:
        try:
            body = o.llm_client.chat(
                o.CFG, o.ANALYZE_SYSTEM,
                '本周期关注会话汇总：\n\n' + '\n\n'.join(parts[:15])
                + '\n\n输出周期汇总（概览/要点/风险与待办/下期关注），suggestions 为空数组。',
                timeout=420)
        except Exception as e:  # noqa: BLE001
            o.log(f'周期报告 LLM 失败: {e}')
            body = None
    if not body:
        body = '\n\n'.join(parts)
    return head + body


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=7)
    ap.add_argument('--from', dest='d0')
    ap.add_argument('--to', dest='d1')
    ap.add_argument('--llm', action='store_true')
    ap.add_argument('-o', '--out')
    a = ap.parse_args()

    if a.d0:
        start = int(time.mktime(time.strptime(a.d0, '%Y-%m-%d')))
        end = int(time.mktime(time.strptime(a.d1, '%Y-%m-%d')) + 86399) if a.d1 else int(time.time())
    else:
        end = int(time.time())
        start = end - a.days * 86400

    md = period_report(start, end, a.llm)
    out = a.out or os.path.join(o.DATA, 'reports',
                                f"{datetime.fromtimestamp(start):%Y%m%d}-{datetime.fromtimestamp(end):%Y%m%d}.md")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w', encoding='utf-8') as f:
        f.write(md)
    print(out)


if __name__ == '__main__':
    main()
