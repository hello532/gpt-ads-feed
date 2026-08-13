#!/usr/bin/env python3
"""端到端：把 Shopify 网络层换成假数据，真跑 run()/_publish()/上传/Delta 编排。

跟 feed_sync.py --self-test 的分工：自检查单个函数，这里查整条链路
（配置加载 → 抓取 → 映射 → 校验 → 分片落盘 → 状态对比 → 上传/Delta → 报告）。

不碰真凭证、不出网、产物只写临时目录。跑法：
    python3 -B tests/test_e2e.py
退出码非 0 即失败。
"""
from __future__ import annotations

import gzip
import importlib.util
import io
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = Path(os.environ.get("FEED_SRC") or (HERE.parent / "feed_sync.py"))
if not SRC.exists():
    sys.exit(f"找不到 feed_sync.py（试过 {SRC}），可用 FEED_SRC 指定")
TMP_ROOT = Path(tempfile.mkdtemp(prefix="feed-e2e-"))

spec = importlib.util.spec_from_file_location("feed_sync", SRC)
assert spec and spec.loader
fs = importlib.util.module_from_spec(spec)
sys.modules["feed_sync"] = fs
spec.loader.exec_module(fs)

FAILED: list[str] = []
PASSED = 0


def ok(cond: bool, label: str) -> None:
    global PASSED
    if cond:
        PASSED += 1
    else:
        FAILED.append(label)


def eq(got, want, label: str) -> None:
    ok(got == want, f"{label}: got {got!r}, want {want!r}")


# ---------- 假店铺数据 ----------

def variant(i: int, *, price="19.90", avail=True, title=None, pid=None, qty=5):
    pid = pid if pid is not None else 900 + i
    return {
        "id": f"gid://shopify/ProductVariant/{1000 + i}",
        "title": title or ("Blue / M" if i % 2 else "Red / L"),
        "sku": f"SKU-{i:03d}",
        "barcode": "0012345678905",
        "price": price,
        "compareAtPrice": "29.99",
        "availableForSale": avail,
        "inventoryQuantity": qty,
        "inventoryPolicy": "DENY",
        "selectedOptions": [{"name": "Color", "value": "Blue"}, {"name": "Size", "value": "M"}],
        "image": {"url": f"https://cdn.example.com/v{i}.jpg"},
        "product": {
            "id": f"gid://shopify/Product/{pid}",
            "handle": f"tee-{pid}",
            "title": f"Cotton Tee {pid}",
            "descriptionHtml": f"<p>Soft <b>cotton</b> tee {pid}.</p>",
            "vendor": "ExampleBrand",
            "productType": "Shirts",
            "status": "ACTIVE",
            "onlineStoreUrl": f"https://shop.example.com/products/tee-{pid}",
            "tags": ["summer"],
            "variantsCount": {"count": 2},
            "featuredMedia": {"preview": {"image": {"url": f"https://cdn.example.com/p{pid}.jpg"}}},
            "media": {"nodes": [
                {"preview": {"image": {"url": f"https://cdn.example.com/p{pid}-2.jpg"}}},
            ]},
        },
    }


def make_rows(n: int, **over):
    return [variant(i, **over) for i in range(n)]


class Calls:
    def __init__(self) -> None:
        self.uploads: list[tuple[list[str], list[str]]] = []
        self.deltas: list[list[dict]] = []
        self.notices: list[tuple[str, bool, str]] = []


def patch(rows, calls: Calls, *, delta_result=None):
    fs.upload_sftp = lambda paths, names: (   # type: ignore[assignment]
        calls.uploads.append(([p.name for p in paths], list(names))),
        {"ok": True, "host": "sf**tp.example.com", "dir": "/upload", "files": len(paths)},
    )[1]
    fs.push_delta = lambda products, cfg: (   # type: ignore[assignment]
        calls.deltas.append(products),
        delta_result if delta_result is not None else {"ok": True, "sent": len(products), "errors": []},
    )[1]
    fs.notify_discord = lambda title, fields, ok=True, note="": calls.notices.append(  # type: ignore[assignment]
        (title, ok, note, fields))
    fs.Shopify.shop_info = lambda self: {"domain": "shop.example.com", "currency": "USD"}  # type: ignore[assignment]
    fs.Shopify.iter_variants = lambda self, cfg: iter(rows)   # type: ignore[assignment]
    fs.Shopify.iter_variants_bulk = lambda self, cfg: iter(rows)   # type: ignore[assignment]


def write_cfg(work: Path, **over) -> Path:
    cfg = {
        "shop_domain": "shop.example.com",
        "seller_name": "Example Store",
        "seller_url": "https://shop.example.com",
        "return_policy": "https://shop.example.com/policies/refund-policy",
        "api_version": fs.DEFAULT_SHOPIFY_API_VERSION,
        "output_path": str(work / "feed.jsonl.gz"),
        "output_format": "jsonl.gz",
        "state_path": str(work / "state.json"),
        "rejects_path": str(work / "rejects.csv"),
        "remote_filename": "products.jsonl.gz",
        "shard_count": 1,
        "is_eligible_checkout": False,
        "is_ads_eligible": True,
        "min_rows": 1,
    }
    cfg.update(over)
    p = work / "config.json"
    p.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    return p


def args(cfg: Path, **over):
    ns = fs.build_parser().parse_args(["--config", str(cfg)])
    for k, v in over.items():
        setattr(ns, k, v)
    return ns


def read_jsonl_gz(p: Path) -> list[dict]:
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def capture(fn):
    """吃掉 log 输出（走 stdout），只留返回值，便于摘要干净。"""
    buf = io.StringIO()
    old, sys.stdout = sys.stdout, buf
    try:
        return fn(), buf.getvalue()
    finally:
        sys.stdout = old
def case_dry_run(work: Path) -> None:
    """dry-run：出文件、不上传、不写状态。"""
    calls = Calls()
    patch(make_rows(6), calls)
    cfg = write_cfg(work)
    rc, out = capture(lambda: fs.run(args(cfg, dry_run=True)))
    eq(rc, fs.EXIT_OK, "dry-run 退出码 0")
    ok(not calls.uploads, "dry-run 没上传")
    ok(not (work / "state.json").exists(), "dry-run 没写状态")
    rows = read_jsonl_gz(work / "feed.jsonl.gz")
    eq(len(rows), 6, "6 个变体出 6 行")
    ok("dry-run" in out, "日志里说明了 dry-run")

    r = rows[0]
    # compareAtPrice=29.99 > price=19.90 → 规范口径是 price 放原价、sale_price 放现价
    eq(r["price"], "29.99 USD", "打折时 price 放原价并带币种")
    eq(r["sale_price"], "19.90 USD", "打折时 sale_price 放现价")
    eq(r["is_eligible_search"], "true", "默认可被搜索")
    eq(r["is_eligible_checkout"], "false", "未开 checkout")
    eq(r["is_ads_eligible"], "true", "开了 ads")
    ok("is_ads_enabled" not in r, "不写非法字段 is_ads_enabled")
    ok("inventory_quantity" not in r, "不写规范外字段 inventory_quantity")
    ok(r["item_id"] and r["group_id"], "item_id/group_id 都在")
    ok(r["additional_image_urls"], "附图字段用复数形式")
    ok("<" not in r["description"], "描述已去 HTML")
    unknown = set(r) - set(fs.FEED_COLUMNS)
    eq(unknown, set(), "产出列都在规范内")


def case_full_cycle(work: Path) -> None:
    """首轮全量上传 + 写状态；次轮改库存后走 Delta。"""
    calls = Calls()
    rows = make_rows(5)
    patch(rows, calls)
    cfg = write_cfg(work)

    rc, _ = capture(lambda: fs.run(args(cfg, delta=True)))
    eq(rc, fs.EXIT_OK, "首轮退出码 0")
    eq(len(calls.uploads), 1, "首轮上传一次")
    eq(calls.uploads[0][1], ["products.jsonl.gz"], "单分片用裸文件名")
    ok(not calls.deltas, "首轮没基线，不推 Delta")
    st = json.loads((work / "state.json").read_text(encoding="utf-8"))
    eq(len(st["items"]), 5, "状态记了 5 条")
    eq(st["version"], fs.STATE_VERSION, "状态带版本号")
    first_sha = st["content_sha256"]

    # 第 2 轮：1 条断货、1 条改价，其余不变
    rows2 = make_rows(5)
    rows2[0]["availableForSale"] = False
    rows2[0]["inventoryQuantity"] = 0
    rows2[1]["price"] = "17.50"
    calls2 = Calls()
    patch(rows2, calls2)
    rc, out = capture(lambda: fs.run(args(cfg, delta=True)))
    eq(rc, fs.EXIT_OK, "次轮退出码 0")
    eq(len(calls2.uploads), 1, "内容变了照常全量上传")
    eq(len(calls2.deltas), 1, "推了一次 Delta")
    prods = calls2.deltas[0]
    eq(len(prods), 1, "只有断货那条进 Delta")
    v = prods[0]["variants"][0]
    eq(v["availability"], {"available": False, "status": "out_of_stock"},
       "availability 是对象，且 available/status 一致")
    ok("price" not in v, "Delta 不带价格字段")
    ok("改了价格" in out and "不支持价格" in out, "日志提醒改价只能靠全量")
    st2 = json.loads((work / "state.json").read_text(encoding="utf-8"))
    ok(st2["content_sha256"] != first_sha, "内容指纹随数据变化")
def case_unchanged(work: Path) -> None:
    """字节完全一致时：默认照传，配置打开才跳过。"""
    calls = Calls()
    rows = make_rows(4)
    patch(rows, calls)
    cfg = write_cfg(work)
    capture(lambda: fs.run(args(cfg)))
    sha1 = json.loads((work / "state.json").read_text(encoding="utf-8"))["content_sha256"]

    calls2 = Calls()
    patch(make_rows(4), calls2)
    rc, out = capture(lambda: fs.run(args(cfg)))
    eq(rc, fs.EXIT_OK, "重跑退出码 0")
    eq(len(calls2.uploads), 1, "默认「至少每天一次」照常上传")
    ok("仍按" in out or "照常投递" in out, "日志说明了为什么还传")
    sha2 = json.loads((work / "state.json").read_text(encoding="utf-8"))["content_sha256"]
    eq(sha2, sha1, "同样输入产出同样指纹（gzip 确定性）")

    calls3 = Calls()
    patch(make_rows(4), calls3)
    cfg2 = write_cfg(work, skip_upload_when_unchanged=True)
    rc, out = capture(lambda: fs.run(args(cfg2)))
    eq(rc, fs.EXIT_OK, "跳传模式退出码 0")
    ok(not calls3.uploads, "打开开关后内容未变就不传")


def case_guards(work: Path) -> None:
    """空快照/行数过少/坏行过多 都必须拦停，且不顶掉上一版文件。"""
    calls = Calls()
    patch(make_rows(3), calls)
    cfg = write_cfg(work, min_rows=3)
    capture(lambda: fs.run(args(cfg)))
    good = (work / "feed.jsonl.gz").read_bytes()

    calls2 = Calls()
    patch([], calls2)
    try:
        capture(lambda: fs.run(args(cfg)))
        FAILED.append("空快照应该抛 DataGuardError")
    except fs.DataGuardError:
        PASS_GUARD = True
        ok(True, "空快照被拦停")
    eq((work / "feed.jsonl.gz").read_bytes(), good, "拦停后上一版文件没被顶掉")
    ok(not calls2.uploads, "拦停后没上传")

    calls3 = Calls()
    patch(make_rows(1), calls3)
    try:
        capture(lambda: fs.run(args(cfg)))
        FAILED.append("行数低于 min_rows 应该抛 DataGuardError")
    except fs.DataGuardError:
        ok(True, "行数暴跌被拦停")

    # 坏行：product.onlineStoreUrl 缺失 → 整行被拒
    bad = make_rows(4)
    for r in bad[:3]:
        r["product"]["onlineStoreUrl"] = None
    calls4 = Calls()
    patch(bad, calls4)
    try:
        capture(lambda: fs.run(args(write_cfg(work, min_rows=1, max_reject_ratio=0.2))))
        FAILED.append("坏行比例超阈值应该抛 DataGuardError")
    except fs.DataGuardError:
        ok(True, "坏行比例超阈值被拦停")


def case_shards(work: Path) -> None:
    """多分片：文件名带序号，remote 名一一对应，指纹覆盖全部分片。"""
    calls = Calls()
    patch(make_rows(9), calls)
    cfg = write_cfg(work, shard_count=3)
    rc, _ = capture(lambda: fs.run(args(cfg)))
    eq(rc, fs.EXIT_OK, "分片模式退出码 0")
    names = sorted(calls.uploads[0][1])
    # 规范只要求分片集合跨次稳定，没规定命名；这里用固定编号保证稳定
    eq(names, ["products-0000.jsonl.gz",
               "products-0001.jsonl.gz",
               "products-0002.jsonl.gz"], "分片远端名带固定编号")
    eq(sorted(calls.uploads[0][0]), ["feed-0000.jsonl.gz", "feed-0001.jsonl.gz",
                                     "feed-0002.jsonl.gz"], "本地分片名同规则")
    def placement() -> dict[str, str]:
        out: dict[str, str] = {}
        for p in sorted(work.glob("feed-*.jsonl.gz")):
            for r in read_jsonl_gz(p):
                out[r["item_id"]] = p.name
        return out

    first = placement()
    eq(len(first), 9, "9 行分布在各分片里，不丢不重")
    ok(len({v for v in first.values()}) > 1, "行确实散到了多个分片")

    # 同一 item_id 必须每次落到同一分片：用 sha1 路由，不受 PYTHONHASHSEED 影响
    calls2 = Calls()
    patch(make_rows(9)[::-1], calls2)   # 打乱顺序也不能改变落点
    capture(lambda: fs.run(args(cfg)))
    eq(placement(), first, "换个抓取顺序重跑，每个 item_id 仍落在同一分片")


def case_cli(work: Path) -> None:
    """CLI 层：缺 token 退 2，护栏退 3，且都发了告警。"""
    cfg = write_cfg(work)
    calls = Calls()
    patch(make_rows(3), calls)
    saved = os.environ.pop("SHOPIFY_ADMIN_TOKEN", None)
    rc, _ = capture(lambda: fs.main(["--config", str(cfg)]))
    eq(rc, 2, "缺凭证退出码 2")
    if saved:
        os.environ["SHOPIFY_ADMIN_TOKEN"] = saved

    os.environ["SHOPIFY_ADMIN_TOKEN"] = "shpat_" + "0" * 32
    calls2 = Calls()
    patch([], calls2)
    rc, _ = capture(lambda: fs.main(["--config", str(cfg)]))
    eq(rc, 3, "数据护栏退出码 3")
    ok(bool(calls2.notices) and not calls2.notices[-1][1], "护栏拦停时发了失败告警")

    calls3 = Calls()
    patch(make_rows(3), calls3)
    rc, _ = capture(lambda: fs.main(["--config", str(cfg), "--limit", "2"]))
    eq(rc, 0, "--limit 正常退出")
    ok(calls3.notices and calls3.notices[-1][1], "成功时发了成功报告")
    txt = json.dumps(calls3.notices, ensure_ascii=False)
    ok("shop.example.com" not in txt, "报告里域名做了脱敏")
    ok("shpat_" not in txt, "报告里没有 token")
def case_formats(work: Path) -> None:
    """格式分支：csv.gz 带表头且列齐；缺 pyarrow 时 parquet 必须报清楚。"""
    calls = Calls()
    patch(make_rows(3), calls)
    cfg = write_cfg(work, output_path=str(work / "feed.csv.gz"), output_format="csv.gz",
                    remote_filename="products.csv.gz")
    rc, _ = capture(lambda: fs.run(args(cfg, dry_run=True)))
    eq(rc, fs.EXIT_OK, "csv.gz 退出码 0")
    with gzip.open(work / "feed.csv.gz", "rt", encoding="utf-8") as fh:
        lines = [ln.rstrip("\n") for ln in fh if ln.strip()]
    eq(len(lines), 4, "csv 是 1 表头 + 3 行")
    header = lines[0].split(",")
    eq(header, list(fs.FEED_COLUMNS), "csv 表头就是规范列顺序")

    cfg2 = write_cfg(work, output_path=str(work / "feed.parquet"), output_format="parquet",
                     remote_filename="products.parquet")
    try:
        capture(lambda: fs.run(args(cfg2, dry_run=True)))
        ok(True, "本机装了 pyarrow，parquet 直接跑通")
    except fs.ConfigError as exc:
        ok("pyarrow" in str(exc), f"缺 pyarrow 时报错点名依赖: {exc}")
    except Exception as exc:  # noqa: BLE001
        FAILED.append(f"parquet 分支抛了意外异常: {type(exc).__name__}: {exc}")


def case_sale_and_skips(work: Path) -> None:
    """打折窗口与跳过规则在真实链路里的表现。"""
    rows = make_rows(4)
    rows[0]["price"] = "19.90"
    rows[0]["compareAtPrice"] = "29.99"           # 打折
    rows[1]["compareAtPrice"] = None              # 无原价
    rows[2]["product"]["status"] = "DRAFT"        # 草稿应跳过
    rows[3]["price"] = None                       # 无价格应跳过
    calls = Calls()
    patch(rows, calls)
    cfg = write_cfg(work, min_rows=1, max_reject_ratio=1.0)
    rc, _ = capture(lambda: fs.run(args(cfg, dry_run=True)))
    eq(rc, fs.EXIT_OK, "混合数据退出码 0")
    out = read_jsonl_gz(work / "feed.jsonl.gz")
    eq(len(out), 2, "草稿与无价变体都被跳过")
    by_id = {r["item_id"]: r for r in out}
    disc = by_id[fs.gid_num(rows[0]["id"])]
    eq(disc["price"], "29.99 USD", "打折时 price 放原价")
    eq(disc["sale_price"], "19.90 USD", "打折时 sale_price 放现价")
    plain = by_id[fs.gid_num(rows[1]["id"])]
    eq(plain.get("sale_price"), None, "没原价就不写 sale_price")
    for r in out:
        eq(fs.validate(r), [], f"产出行自身通过校验: {r['item_id']}")


def case_delta_failure(work: Path) -> None:
    """Delta 被拒时不能报成功；权限没开则算跳过、不算失败。"""
    calls = Calls()
    patch(make_rows(4), calls)
    cfg = write_cfg(work)
    capture(lambda: fs.run(args(cfg)))

    rows2 = make_rows(4)
    rows2[0]["availableForSale"] = False
    calls2 = Calls()
    patch(rows2, calls2, delta_result={"ok": False, "sent": 0,
                                       "errors": ["HTTP 400 unknown field"]})
    rc, _ = capture(lambda: fs.run(args(cfg, delta=True)))
    eq(rc, fs.EXIT_RUNTIME, "Delta 报错时退出码非 0")

    rows3 = make_rows(4)
    rows3[1]["availableForSale"] = False
    calls3 = Calls()
    patch(rows3, calls3, delta_result={"skipped": "Delta API 未开通", "errors": []})
    rc, _ = capture(lambda: fs.run(args(cfg, delta=True)))
    eq(rc, fs.EXIT_OK, "权限未开通算跳过，不算失败")


def main() -> int:
    os.environ.setdefault("SHOPIFY_ADMIN_TOKEN", "shpat_" + "0" * 32)
    os.environ.pop("DISCORD_WEBHOOK_URL", None)
    cases = [case_dry_run, case_full_cycle, case_unchanged, case_guards,
             case_shards, case_cli, case_formats, case_sale_and_skips,
             case_delta_failure]
    for fn in cases:
        work = TMP_ROOT / fn.__name__
        work.mkdir(parents=True, exist_ok=True)
        try:
            fn(work)
        except Exception as exc:  # noqa: BLE001
            import traceback
            FAILED.append(f"{fn.__name__} 抛异常: {type(exc).__name__}: {exc}\n"
                          + "".join(traceback.format_tb(exc.__traceback__)[-3:]))
    shutil.rmtree(TMP_ROOT, ignore_errors=True)
    if FAILED:
        print(f"E2E 失败 {len(FAILED)} 项 / 通过 {PASSED} 项：")
        for f in FAILED:
            print(f"  ✗ {f}")
        return 1
    print(f"E2E 全绿：{PASSED} 项断言通过，{len(cases)} 个场景")
    return 0


if __name__ == "__main__":
    sys.exit(main())
