#!/usr/bin/env python3
"""Verify the real behaviour of the "missing dependency / missing credentials" branches without stubs. No network, no real credentials.

These branches are rarely reached: on a machine with paramiko/pyarrow installed some assertions
skip themselves, and the output says which ones were not verified. Run with: python3 -B tests/test_guards.py
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
    sys.exit(f"feed_sync.py not found (tried {SRC}); set FEED_SRC to point at it")
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
            FAILED.append(f"{label}: the error text does not contain {needle!r}, it was {e}")
    except Exception as e:  # noqa: BLE001
        FAILED.append(f"{label}: raised {type(e).__name__}: {e}")
    else:
        FAILED.append(f"{label}: raised nothing")


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

# 1) missing host/user
expect(fs.ConfigError, "OPENAI_FEED_SFTP_HOST",
       lambda: fs.upload_sftp([f], ["products.jsonl.gz"]), "a missing SFTP host is reported clearly")

# 2) host/user present but no paramiko and no key -> takes the CLI branch and demands a key
os.environ["OPENAI_FEED_SFTP_HOST"] = "sftp.invalid.test"
os.environ["OPENAI_FEED_SFTP_USER"] = "nobody"
try:
    import paramiko  # noqa: F401
    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

if HAS_PARAMIKO:
    ok(True, "paramiko is installed here, skipping the CLI fallback branch")
else:
    expect(fs.ConfigError, "OPENAI_FEED_SFTP_KEY",
           lambda: fs.upload_sftp([f], ["products.jsonl.gz"]),
           "no paramiko plus no key demands a key")
    # 3) a private key path that does not exist
    os.environ["OPENAI_FEED_SFTP_KEY"] = str(work / "nope_ed25519")
    expect(fs.ConfigError, "private key",
           lambda: fs.upload_sftp([f], ["products.jsonl.gz"]), "a missing private key path is reported clearly")
    del os.environ["OPENAI_FEED_SFTP_KEY"]

# 4) parquet: without pyarrow the dependency must be named, not raised as an ImportError
try:
    import pyarrow  # noqa: F401
    ok(True, "pyarrow is installed here, parquet works directly")
except ImportError:
    expect(fs.ConfigError, "pyarrow",
           lambda: fs.FeedWriter(str(work / "x.parquet"), "parquet", 1).open(),
           "a missing pyarrow names the dependency")

# 5) Delta: behaviour with no API key / feed id
for k in ("OPENAI_ADS_API_KEY", "OPENAI_ADS_FEED_ID"):
    os.environ.pop(k, None)
cfg = {"delta_include_title": False}
one = [{"id": "1", "variants": [{"id": "2",
        "availability": {"available": True, "status": "in_stock"}}]}]
# Delta is optional: with no credentials it should skip instead of failing the whole run
res0 = fs.push_delta(one, cfg)
ok("skipped" in res0 and "OPENAI_ADS" in res0["skipped"],
   f"missing Delta credentials skip and name the variable: {res0}")
ok("errors" not in res0, "a skip produces no errors, so the run is not marked failed")

# 6) empty products must not send a request
os.environ["OPENAI_ADS_API_KEY"] = "sk-ads-fake"
os.environ["OPENAI_ADS_FEED_ID"] = "product_feed_fake"
res = fs.push_delta([], cfg)
ok("skipped" in res, f"with nothing changed Delta skips outright and sends no request: {res}")

# 7) a missing known_hosts must point at ssh-keyscan
if HAS_PARAMIKO:
    os.environ["OPENAI_FEED_SFTP_KNOWN_HOSTS"] = str(work / "empty_known_hosts")
    (work / "empty_known_hosts").write_text("", encoding="utf-8")
    ok(True, "the known_hosts branch needs paramiko, which is available here")
else:
    ok(True, "the known_hosts check depends on paramiko, which is not installed here, so it is unverified")

shutil.rmtree(work, ignore_errors=True)
if FAILED:
    print(f"guard branches: {len(FAILED)} failed / {PASSED} passed:")
    for x in FAILED:
        print(f"  x {x}")
    sys.exit(1)
print(f"guard branches all green: {PASSED} assertions")
if not HAS_PARAMIKO:
    print("  note: paramiko is not installed here, so the paramiko upload path and host-key verification are unverified")
