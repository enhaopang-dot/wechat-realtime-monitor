# -*- coding: utf-8 -*-
"""语音转写（父进程侧）：sherpa SenseVoice 常驻 + 微信语音子进程取数 + 上传音频转写。

2026-10-07 验证链路：VoiceInfo.voice_data 去首字节 \x02 = SILK_V3 → pysilk@24000
→ 裸 s16le PCM。上传音频统一 ffmpeg 重采样 16k WAV 后送 SenseVoice。
"""
import json, os, subprocess, sys, tempfile, threading
import array

MODEL_DIR = os.path.expanduser(r"~\.claude\tools\wechat_extraction\models\sensevoice")
PROJECT = os.path.dirname(os.path.abspath(__file__))
FETCHER = os.path.join(PROJECT, 'voice_fetch.py')

_lock = threading.Lock()
_recognizer = None
_cache = None
_cache_path = None

def _load_cache(path):
    global _cache, _cache_path
    _cache_path = path
    try:
        _cache = json.load(open(path, encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        _cache = {}

def _save_cache():
    try:
        with open(_cache_path, 'w', encoding='utf-8') as f:
            json.dump(_cache, f, ensure_ascii=False)
    except OSError:
        pass

def _get_recognizer():
    global _recognizer
    with _lock:
        if _recognizer is None:
            import sherpa_onnx
            _recognizer = sherpa_onnx.OfflineRecognizer.from_sense_voice(
                model=os.path.join(MODEL_DIR, 'model.int8.onnx'),
                tokens=os.path.join(MODEL_DIR, 'tokens.txt'),
                use_itn=True)
        return _recognizer

def _wav_to_text(wav_path, sample_rate):
    import wave
    with wave.open(wav_path, 'rb') as wf:
        sr = wf.getframerate()
        raw = wf.readframes(wf.getnframes())
    samples = array.array('h', raw)  # 本机小端，与 WAV 存储同序，勿 byteswap
    rec = _get_recognizer()
    stream = rec.create_stream()
    stream.accept_waveform(sr, samples)
    rec.decode_stream(stream)
    return stream.result.text.strip()

def _ensure_wav16k(src, dst):
    r = subprocess.run(['ffmpeg', '-y', '-i', src, '-ac', '1', '-ar', '16000', dst],
                       capture_output=True, timeout=180)
    if not os.path.exists(dst) or os.path.getsize(dst) == 0:
        raise RuntimeError('ffmpeg 转码失败: ' + (r.stderr.decode('utf-8', 'replace') or '')[-200:])

def init(cache_path):
    _load_cache(cache_path)

def transcribe_upload(src_path):
    """上传的录音文件 → 文本。支持任意 ffmpeg 可解码格式（m4a/mp3/wav/amr…）。"""
    with tempfile.TemporaryDirectory() as td:
        wav = os.path.join(td, 'up.wav')
        _ensure_wav16k(src_path, wav)
        return _wav_to_text(wav, 16000)

def transcribe_wechat(runtime_dir, username, create_time):
    """微信语音消息 → 文本（子进程取 SILK → WAV，父进程转写）。"""
    key = f'{os.path.basename(runtime_dir)}|{username}|{create_time}'
    if key in _cache:
        return _cache[key]
    with tempfile.TemporaryDirectory() as td:
        wav = os.path.join(td, 'v.wav')
        r = subprocess.run([sys.executable, FETCHER, runtime_dir, username, str(create_time), wav],
                           capture_output=True, timeout=180)
        try:
            info = json.loads((r.stdout or b'').decode('utf-8', 'replace').strip().splitlines()[-1])
        except (ValueError, IndexError):
            raise RuntimeError(f'voice_fetch 异常: {(r.stderr or b"").decode("utf-8", "replace")[-200:]}')
        if not info.get('ok'):
            raise RuntimeError(info.get('error', 'voice_fetch 失败'))
        text = _wav_to_text(info['wav'], 24000)
    if text:
        _cache[key] = text
        _save_cache()
    return text
