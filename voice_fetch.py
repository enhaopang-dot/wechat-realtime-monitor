# -*- coding: utf-8 -*-
"""子进程：从微信 media_0.db 提取单条语音 → WAV。

用法: python voice_fetch.py <account_runtime_dir> <username> <create_time> <out_wav>
输出: JSON {ok:true, wav:...} 或 {ok:false, error:...}
设计要点：独立子进程隔离 mcp_server 的模块级配置（每账号一套 keys/路径），
父进程只负责 sherpa 转写（模型常驻内存）。
"""
import io, json, os, sys, wave

WECHAT_CLEAN = r"<你的本地路径>"

def main():
    runtime_dir, username, create_time, out_wav = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
    os.environ['WECHAT_DECRYPT_APP_DIR'] = runtime_dir
    sys.path.insert(0, WECHAT_CLEAN)
    try:
        import pysilk
        from key_utils import get_key_info, strip_key_metadata
        import mcp_server  # 复用其 full_decrypt / decrypt_wal（模块级配置指向本账号 runtime）
    except Exception as e:
        print(json.dumps({'ok': False, 'error': f'import 失败: {e}'}, ensure_ascii=False))
        return 1

    try:
        cfg = json.load(open(os.path.join(runtime_dir, 'config.json'), encoding='utf-8'))
        keys = strip_key_metadata(json.load(open(cfg['keys_file'], encoding='utf-8')))
        rel = os.path.join('message', 'media_0.db')
        info = get_key_info(keys, rel)
        if not info:
            print(json.dumps({'ok': False, 'error': 'keys 文件无 media_0.db 密钥'}, ensure_ascii=False))
            return 1
        src = os.path.join(cfg['db_dir'], rel)
        if not os.path.exists(src):
            print(json.dumps({'ok': False, 'error': f'源库不存在: {src}'}, ensure_ascii=False))
            return 1
        tmp_dir = os.path.join(runtime_dir, 'ops_center_tmp')
        os.makedirs(tmp_dir, exist_ok=True)
        tmp = os.path.join(tmp_dir, 'media_0_dec.db')
        enc_key = bytes.fromhex(info['enc_key'])
        if (not os.path.exists(tmp)) or os.path.getmtime(tmp) < os.path.getmtime(src):
            mcp_server.full_decrypt(src, tmp, enc_key)
            wal = src + '-wal'
            if os.path.exists(wal):
                mcp_server.decrypt_wal(wal, tmp, enc_key)

        import sqlite3
        conn = sqlite3.connect(tmp)
        row = conn.execute("SELECT rowid FROM Name2Id WHERE user_name = ?", (username,)).fetchone()
        if not row:
            conn.close()
            print(json.dumps({'ok': False, 'error': f'Name2Id 无该会话: {username}'}, ensure_ascii=False))
            return 1
        vr = conn.execute(
            "SELECT voice_data FROM VoiceInfo WHERE chat_name_id = ? AND create_time = ? ORDER BY local_id LIMIT 1",
            (row[0], create_time)).fetchone()
        conn.close()
        if not vr or not vr[0]:
            print(json.dumps({'ok': False, 'error': f'VoiceInfo 无该语音 t={create_time}'}, ensure_ascii=False))
            return 1
        data = bytes(vr[0])
        silk = data[1:] if data[:1] == b'\x02' else data
        out = io.BytesIO()
        pysilk.decode(io.BytesIO(silk), out, 24000)
        pcm = out.getvalue()
        os.makedirs(os.path.dirname(out_wav), exist_ok=True)
        with wave.open(out_wav, 'wb') as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(24000)
            wf.writeframes(pcm)
        print(json.dumps({'ok': True, 'wav': out_wav, 'ms': len(pcm) // 48}))
        return 0
    except Exception as e:
        print(json.dumps({'ok': False, 'error': f'{type(e).__name__}: {e}'}, ensure_ascii=False))
        return 1

if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.exit(main())
