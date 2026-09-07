#!/usr/bin/env python3
"""End to end: swap the Shopify network layer for fake data and really run run()/_publish()/upload/Delta.

Division of labour with feed_sync.py --self-test: the self-test checks individual functions,
this checks the whole chain (config load -> fetch -> map -> validate -> shard to disk ->
state diff -> upload/Delta -> report).

No real credentials, no network, and output only goes to a temp directory. Run with:
    python3 -B tests/test_e2e.py
A non-zero exit code means failure.
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
    sys.exit(f"feed_sync.py not found (tried {SRC}); set FEED_SRC to point at it")
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


# ---------- Fake shop data ----------

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
    """Swallow log output (it goes to stdout) and keep only the return value, so the summary stays clean."""
    buf = io.StringIO()
    old, sys.stdout = sys.stdout, buf
    try:
        return fn(), buf.getvalue()
    finally:
        sys.stdout = old
def case_dry_run(work: Path) -> None:
    """dry-run: produce the file, do not upload, do not write state."""
    calls = Calls()
    patch(make_rows(6), calls)
    cfg = write_cfg(work)
    rc, out = capture(lambda: fs.run(args(cfg, dry_run=True)))
    eq(rc, fs.EXIT_OK, "dry-run exit code 0")
    ok(not calls.uploads, "dry-run did not upload")
    ok(not (work / "state.json").exists(), "dry-run did not write state")
    rows = read_jsonl_gz(work / "feed.jsonl.gz")
    eq(len(rows), 6, "6 variants produce 6 rows")
    ok("dry-run" in out, "the log says it was a dry-run")

    r = rows[0]
    # compareAtPrice=29.99 > price=19.90 -> per the spec price holds the list price and sale_price the current one
    eq(r["price"], "29.99 USD", "on a discount price holds the list price and carries a currency")
    eq(r["sale_price"], "19.90 USD", "on a discount sale_price holds the current price")
    eq(r["is_eligible_search"], "true", "searchable by default")
    eq(r["is_eligible_checkout"], "false", "checkout is off")
    eq(r["is_ads_eligible"], "true", "ads is on")
    ok("is_ads_enabled" not in r, "the illegal field is_ads_enabled is not written")
    ok("inventory_quantity" not in r, "the out-of-spec field inventory_quantity is not written")
    ok(r["item_id"] and r["group_id"], "item_id and group_id are both present")
    ok(r["additional_image_urls"], "the extra-image field uses the plural form")
    ok("<" not in r["description"], "the description has had its HTML stripped")
    unknown = set(r) - set(fs.FEED_COLUMNS)
    eq(unknown, set(), "every produced column is within the spec")


def case_full_cycle(work: Path) -> None:
    """First round uploads the full snapshot and writes state; the second round changes stock and goes through Delta."""
    calls = Calls()
    rows = make_rows(5)
    patch(rows, calls)
    cfg = write_cfg(work)

    rc, _ = capture(lambda: fs.run(args(cfg, delta=True)))
    eq(rc, fs.EXIT_OK, "first round exit code 0")
    eq(len(calls.uploads), 1, "the first round uploads once")
    eq(calls.uploads[0][1], ["products.jsonl.gz"], "a single shard uses the bare filename")
    ok(not calls.deltas, "with no baseline the first round pushes no Delta")
    st = json.loads((work / "state.json").read_text(encoding="utf-8"))
    eq(len(st["items"]), 5, "state recorded 5 rows")
    eq(st["version"], fs.STATE_VERSION, "state carries a version number")
    first_sha = st["content_sha256"]

    # Round 2: one row out of stock, one row repriced, the rest unchanged
    rows2 = make_rows(5)
    rows2[0]["availableForSale"] = False
    rows2[0]["inventoryQuantity"] = 0
    rows2[1]["price"] = "17.50"
    calls2 = Calls()
    patch(rows2, calls2)
    rc, out = capture(lambda: fs.run(args(cfg, delta=True)))
    eq(rc, fs.EXIT_OK, "second round exit code 0")
    eq(len(calls2.uploads), 1, "changed content still uploads the full snapshot")
    eq(len(calls2.deltas), 1, "Delta was pushed once")
    prods = calls2.deltas[0]
    eq(len(prods), 1, "only the out-of-stock row goes into Delta")
    v = prods[0]["variants"][0]
    eq(v["availability"], {"available": False, "status": "out_of_stock"},
       "availability is an object, and available/status agree")
    ok("price" not in v, "Delta carries no price field")
    ok("changed price" in out and "does not support price" in out,
       "the log points out that a price change only takes effect through the full snapshot")
    st2 = json.loads((work / "state.json").read_text(encoding="utf-8"))
    ok(st2["content_sha256"] != first_sha, "the content fingerprint follows the data")
def case_unchanged(work: Path) -> None:
    """When the bytes are identical: deliver anyway by default, skip only when configured to."""
    calls = Calls()
    rows = make_rows(4)
    patch(rows, calls)
    cfg = write_cfg(work)
    capture(lambda: fs.run(args(cfg)))
    sha1 = json.loads((work / "state.json").read_text(encoding="utf-8"))["content_sha256"]

    calls2 = Calls()
    patch(make_rows(4), calls2)
    rc, out = capture(lambda: fs.run(args(cfg)))
    eq(rc, fs.EXIT_OK, "rerun exit code 0")
    eq(len(calls2.uploads), 1, 'by default "at least once a day" uploads regardless')
    ok("still delivering" in out or "at least once a day" in out,
       "the log explains why it delivered anyway")
    sha2 = json.loads((work / "state.json").read_text(encoding="utf-8"))["content_sha256"]
    eq(sha2, sha1, "the same input produces the same fingerprint (gzip is deterministic)")

    calls3 = Calls()
    patch(make_rows(4), calls3)
    cfg2 = write_cfg(work, skip_upload_when_unchanged=True)
    rc, out = capture(lambda: fs.run(args(cfg2)))
    eq(rc, fs.EXIT_OK, "skip-upload mode exit code 0")
    ok(not calls3.uploads, "with the switch on, unchanged content is not uploaded")


def case_guards(work: Path) -> None:
    """An empty snapshot, too few rows, or too many bad rows must all stop the run without clobbering the previous file."""
    calls = Calls()
    patch(make_rows(3), calls)
    cfg = write_cfg(work, min_rows=3)
    capture(lambda: fs.run(args(cfg)))
    good = (work / "feed.jsonl.gz").read_bytes()

    calls2 = Calls()
    patch([], calls2)
    try:
        capture(lambda: fs.run(args(cfg)))
        FAILED.append("an empty snapshot should raise DataGuardError")
    except fs.DataGuardError:
        PASS_GUARD = True
        ok(True, "the empty snapshot was stopped")
    eq((work / "feed.jsonl.gz").read_bytes(), good, "after the stop the previous file was not clobbered")
    ok(not calls2.uploads, "nothing was uploaded after the stop")

    calls3 = Calls()
    patch(make_rows(1), calls3)
    try:
        capture(lambda: fs.run(args(cfg)))
        FAILED.append("a row count below min_rows should raise DataGuardError")
    except fs.DataGuardError:
        ok(True, "the row-count collapse was stopped")

    # Bad rows: a missing product.onlineStoreUrl rejects the whole row
    bad = make_rows(4)
    for r in bad[:3]:
        r["product"]["onlineStoreUrl"] = None
    calls4 = Calls()
    patch(bad, calls4)
    try:
        capture(lambda: fs.run(args(write_cfg(work, min_rows=1, max_reject_ratio=0.2))))
        FAILED.append("a bad-row ratio over the threshold should raise DataGuardError")
    except fs.DataGuardError:
        ok(True, "the bad-row ratio over the threshold was stopped")


def case_shards(work: Path) -> None:
    """Multiple shards: filenames carry an index, remote names correspond one to one, and the fingerprint covers every shard."""
    calls = Calls()
    patch(make_rows(9), calls)
    cfg = write_cfg(work, shard_count=3)
    rc, _ = capture(lambda: fs.run(args(cfg)))
    eq(rc, fs.EXIT_OK, "shard mode exit code 0")
    names = sorted(calls.uploads[0][1])
    # The spec only requires the shard set to stay stable across runs, not a naming scheme; fixed indices keep it stable
    eq(names, ["products-0000.jsonl.gz",
               "products-0001.jsonl.gz",
               "products-0002.jsonl.gz"], "remote shard names carry a fixed index")
    eq(sorted(calls.uploads[0][0]), ["feed-0000.jsonl.gz", "feed-0001.jsonl.gz",
                                     "feed-0002.jsonl.gz"], "local shard names follow the same rule")
    def placement() -> dict[str, str]:
        out: dict[str, str] = {}
        for p in sorted(work.glob("feed-*.jsonl.gz")):
            for r in read_jsonl_gz(p):
                out[r["item_id"]] = p.name
        return out

    first = placement()
    eq(len(first), 9, "9 rows spread across the shards, none lost or duplicated")
    ok(len({v for v in first.values()}) > 1, "the rows really did land in more than one shard")

    # The same item_id must land in the same shard every time: sha1 routing, unaffected by PYTHONHASHSEED
    calls2 = Calls()
    patch(make_rows(9)[::-1], calls2)   # a shuffled order must not change placement
    capture(lambda: fs.run(args(cfg)))
    eq(placement(), first, "rerun with a different fetch order and every item_id still lands in the same shard")


def case_cli(work: Path) -> None:
    """CLI layer: a missing token exits 2, a guard exits 3, and both send a notification."""
    cfg = write_cfg(work)
    calls = Calls()
    patch(make_rows(3), calls)
    saved = os.environ.pop("SHOPIFY_ADMIN_TOKEN", None)
    rc, _ = capture(lambda: fs.main(["--config", str(cfg)]))
    eq(rc, 2, "missing credentials exit code 2")
    if saved:
        os.environ["SHOPIFY_ADMIN_TOKEN"] = saved

    os.environ["SHOPIFY_ADMIN_TOKEN"] = "shpat_" + "0" * 32
    calls2 = Calls()
    patch([], calls2)
    rc, _ = capture(lambda: fs.main(["--config", str(cfg)]))
    eq(rc, 3, "data guard exit code 3")
    ok(bool(calls2.notices) and not calls2.notices[-1][1], "a guard stop sent a failure notification")

    calls3 = Calls()
    patch(make_rows(3), calls3)
    rc, _ = capture(lambda: fs.main(["--config", str(cfg), "--limit", "2"]))
    eq(rc, 0, "--limit exits normally")
    ok(calls3.notices and calls3.notices[-1][1], "a success sent a success report")
    txt = json.dumps(calls3.notices, ensure_ascii=False)
    ok("shop.example.com" not in txt, "the domain is redacted in the report")
    ok("shpat_" not in txt, "there is no token in the report")
def case_formats(work: Path) -> None:
    """Format branches: csv.gz has a header with all columns; without pyarrow, parquet must say so clearly."""
    calls = Calls()
    patch(make_rows(3), calls)
    cfg = write_cfg(work, output_path=str(work / "feed.csv.gz"), output_format="csv.gz",
                    remote_filename="products.csv.gz")
    rc, _ = capture(lambda: fs.run(args(cfg, dry_run=True)))
    eq(rc, fs.EXIT_OK, "csv.gz exit code 0")
    with gzip.open(work / "feed.csv.gz", "rt", encoding="utf-8") as fh:
        lines = [ln.rstrip("\n") for ln in fh if ln.strip()]
    eq(len(lines), 4, "the csv is 1 header plus 3 rows")
    header = lines[0].split(",")
    eq(header, list(fs.FEED_COLUMNS), "the csv header is exactly the spec column order")

    cfg2 = write_cfg(work, output_path=str(work / "feed.parquet"), output_format="parquet",
                     remote_filename="products.parquet")
    try:
        capture(lambda: fs.run(args(cfg2, dry_run=True)))
        ok(True, "pyarrow is installed here, so parquet runs straight through")
    except fs.ConfigError as exc:
        ok("pyarrow" in str(exc), f"without pyarrow the error names the dependency: {exc}")
    except Exception as exc:  # noqa: BLE001
        FAILED.append(f"the parquet branch raised an unexpected exception: {type(exc).__name__}: {exc}")


def case_sale_and_skips(work: Path) -> None:
    """How the discount window and the skip rules behave in the real chain."""
    rows = make_rows(4)
    rows[0]["price"] = "19.90"
    rows[0]["compareAtPrice"] = "29.99"           # on discount
    rows[1]["compareAtPrice"] = None              # no list price
    rows[2]["product"]["status"] = "DRAFT"        # a draft should be skipped
    rows[3]["price"] = None                       # no price should be skipped
    calls = Calls()
    patch(rows, calls)
    cfg = write_cfg(work, min_rows=1, max_reject_ratio=1.0)
    rc, _ = capture(lambda: fs.run(args(cfg, dry_run=True)))
    eq(rc, fs.EXIT_OK, "mixed data exit code 0")
    out = read_jsonl_gz(work / "feed.jsonl.gz")
    eq(len(out), 2, "both the draft and the priceless variant were skipped")
    by_id = {r["item_id"]: r for r in out}
    disc = by_id[fs.gid_num(rows[0]["id"])]
    eq(disc["price"], "29.99 USD", "on a discount price holds the list price")
    eq(disc["sale_price"], "19.90 USD", "on a discount sale_price holds the current price")
    plain = by_id[fs.gid_num(rows[1]["id"])]
    eq(plain.get("sale_price"), None, "with no list price sale_price is not written")
    for r in out:
        eq(fs.validate(r), [], f"the produced row passes validation on its own: {r['item_id']}")


def case_delta_failure(work: Path) -> None:
    """A rejected Delta must not report success; a feature that is not enabled counts as skipped, not failed."""
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
    eq(rc, fs.EXIT_RUNTIME, "a Delta error means a non-zero exit code")

    rows3 = make_rows(4)
    rows3[1]["availableForSale"] = False
    calls3 = Calls()
    patch(rows3, calls3, delta_result={"skipped": "the Delta API is not enabled", "errors": []})
    rc, _ = capture(lambda: fs.run(args(cfg, delta=True)))
    eq(rc, fs.EXIT_OK, "a feature that is not enabled counts as skipped, not failed")


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
            FAILED.append(f"{fn.__name__} raised: {type(exc).__name__}: {exc}\n"
                          + "".join(traceback.format_tb(exc.__traceback__)[-3:]))
    shutil.rmtree(TMP_ROOT, ignore_errors=True)
    if FAILED:
        print(f"E2E: {len(FAILED)} failed / {PASSED} passed:")
        for f in FAILED:
            print(f"  x {f}")
        return 1
    print(f"E2E all green: {PASSED} assertions passed across {len(cases)} scenarios")
    return 0


if __name__ == "__main__":
    sys.exit(main())
