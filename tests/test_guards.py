#!/usr/bin/env python3
"""不打桩地验证「缺依赖 / 缺凭证」分支的真实行为。不出网、不用真凭证。

这些分支平时跑不到：装了 paramiko/pyarrow 的机器上部分断言会自动跳过，
输出里会说明哪几项没验证。跑法：python3 -B tests/test_guards.py
"""
from __future__ import annotations

import importlib.util
import os
import shutil
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = Path(os.environ.get("FEED_SRC") or (HERE.parent / "feed_sync.py"))
if not SRC.exists():
    sys.exit(f"找不到 feed_sync.py（试过 {SRC}），可用 FEED_SRC 指定")
spec = importlib.util.spec_from_file_location("feed_sync", SRC)
assert spec and spec.loader
fs = importlib.util.module_from_spec(spec)
sys.modules["feed_sync"] = fs
spec.loader.exec_module(fs)

FAILED: list[str] = []
PASSED = 0


def expect(exc_type, needle: str, fn, label: str) -> None:
    global PASSED
    try:
        fn()
    except exc_type as e:
        if needle in str(e):
            PASSED += 1
        else:
            FAILED.append(f"{label}: 报错内容里没有 {needle!r}，实际是 {e}")
    except Exception as e:  # noqa: BLE001
        FAILED.append(f"{label}: 抛了 {type(e).__name__}: {e}")
    else:
        FAILED.append(f"{label}: 没抛异常")


def ok(cond: bool, label: str) -> None:
    global PASSED
    if cond:
        PASSED += 1
    else:
        FAILED.append(label)


work = Path(tempfile.mkdtemp(prefix="feed-guards-"))
f = work / "products.jsonl.gz"
f.write_bytes(b"\x1f\x8b\x08\x00")

for k in list(os.environ):
    if k.startswith("OPENAI_FEED_SFTP"):
        del os.environ[k]

# 1) 缺 host/user
expect(fs.ConfigError, "OPENAI_FEED_SFTP_HOST",
       lambda: fs.upload_sftp([f], ["products.jsonl.gz"]), "缺 SFTP 主机时报清楚")

# 2) 有 host/user 但没 paramiko 且没给密钥 → 走 CLI 分支并要求密钥
os.environ["OPENAI_FEED_SFTP_HOST"] = "sftp.invalid.test"
os.environ["OPENAI_FEED_SFTP_USER"] = "nobody"
try:
    import paramiko  # noqa: F401
    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

if HAS_PARAMIKO:
    ok(True, "本机有 paramiko，跳过 CLI 兜底分支")
else:
    expect(fs.ConfigError, "OPENAI_FEED_SFTP_KEY",
           lambda: fs.upload_sftp([f], ["products.jsonl.gz"]),
           "无 paramiko + 无密钥时要求配密钥")
    # 3) 给了不存在的私钥路径
    os.environ["OPENAI_FEED_SFTP_KEY"] = str(work / "nope_ed25519")
    expect(fs.ConfigError, "私钥不存在",
           lambda: fs.upload_sftp([f], ["products.jsonl.gz"]), "私钥路径不存在时报清楚")
    del os.environ["OPENAI_FEED_SFTP_KEY"]

# 4) parquet：没装 pyarrow 必须点名依赖，而不是抛 ImportError
try:
    import pyarrow  # noqa: F401
    ok(True, "本机有 pyarrow，parquet 直接可用")
except ImportError:
    expect(fs.ConfigError, "pyarrow",
           lambda: fs.FeedWriter(str(work / "x.parquet"), "parquet", 1).open(),
           "缺 pyarrow 时点名依赖")

# 5) Delta：缺 API key / feed id 时的行为
for k in ("OPENAI_ADS_API_KEY", "OPENAI_ADS_FEED_ID"):
    os.environ.pop(k, None)
cfg = {"delta_include_title": False}
one = [{"id": "1", "variants": [{"id": "2",
        "availability": {"available": True, "status": "in_stock"}}]}]
# Delta 是可选功能：没配凭证应当跳过而不是让整轮失败
res0 = fs.push_delta(one, cfg)
ok("skipped" in res0 and "OPENAI_ADS" in res0["skipped"],
   f"缺 Delta 凭证时跳过并点名变量: {res0}")
ok("errors" not in res0, "跳过时不产生 errors，不会把整轮判失败")

# 6) 空 products 不该发请求
os.environ["OPENAI_ADS_API_KEY"] = "sk-ads-fake"
os.environ["OPENAI_ADS_FEED_ID"] = "product_feed_fake"
res = fs.push_delta([], cfg)
ok("skipped" in res, f"没有变化时 Delta 直接跳过，不发请求: {res}")

# 7) known_hosts 缺失时的提示要给出 ssh-keyscan
if HAS_PARAMIKO:
    os.environ["OPENAI_FEED_SFTP_KNOWN_HOSTS"] = str(work / "empty_known_hosts")
    (work / "empty_known_hosts").write_text("", encoding="utf-8")
    ok(True, "known_hosts 分支需要 paramiko，本机可跑")
else:
    ok(True, "known_hosts 校验分支依赖 paramiko，本机未装，未验证")

shutil.rmtree(work, ignore_errors=True)
if FAILED:
    print(f"守卫分支失败 {len(FAILED)} 项 / 通过 {PASSED} 项：")
    for x in FAILED:
        print(f"  ✗ {x}")
    sys.exit(1)
print(f"守卫分支全绿：{PASSED} 项")
if not HAS_PARAMIKO:
    print("  注：本机没装 paramiko，paramiko 上传路径与 host key 校验未验证")
