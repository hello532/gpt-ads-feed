#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shopify → OpenAI 产品 Feed 同步器（全量快照 + 盘中增量）。

跑法：
    python3 feed_sync.py --config config.json                  # 全量快照 + SFTP 覆盖 + 通知
    python3 feed_sync.py --config config.json --dry-run        # 只落盘，不上传
    python3 feed_sync.py --config config.json --limit 100      # 首次上线用的小样本
    python3 feed_sync.py --config config.json --bulk           # 大目录走 Bulk Operations
    python3 feed_sync.py --config config.json --delta          # 只推真实变化的 availability
    python3 feed_sync.py --self-test                           # 离线自检，不需要凭证

交付约束（全部来自官方规范，不是推测）：
    - Feed 是「全量快照」语义：同一路径同一文件名原地覆盖，至少每天一次。
    - 首选 parquet(zstd)；jsonl.gz / csv.gz / tsv.gz 同样受支持。
    - 单 shard ≤ 50 万条，目标 < ~500MB；分片集合必须跨次稳定。
    - 删除商品 = 下次快照里不出现，或 is_eligible_search=false。
    - Ads 通道的 feed 连接与 SFTP 凭证只能在 Ads Manager 手工开通；
      公开 API 只有 PATCH /feeds/{id}/products，且只能改 title 与 availability。

硬规矩：
    - 凭证只从环境变量读，全程不落盘；日志与通知一律过脱敏。
    - 金额一律 Decimal + ROUND_HALF_UP，禁止 float（差一分钱就是差一分钱）。
    - 空快照、拒收率超阈值、上传后字节数不符 —— 都拒绝发布。
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html import unescape
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

EXIT_OK = 0
EXIT_RUNTIME = 1
EXIT_CONFIG = 2
EXIT_DATA_GUARD = 3

# ── 官方规范常量（developers.openai.com/commerce/specs/file-upload/products）──
REQUIRED_FIELDS: tuple[str, ...] = (
    "is_eligible_search", "is_eligible_checkout", "item_id", "title",
    "description", "url", "brand", "image_url", "price", "availability",
    "seller_name", "seller_url", "return_policy", "target_countries",
    "store_country",
)
AVAILABILITY_ENUM = ("in_stock", "out_of_stock", "pre_order", "backorder", "unknown")
AGE_GROUP_ENUM = ("newborn", "infant", "toddler", "kids", "adult")
CONDITION_ENUM = ("new", "refurbished", "used")

# 规范给出的长度上限，超限会被逐行拒收
MAXLEN: dict[str, int] = {
    "item_id": 100, "title": 150, "description": 5000, "brand": 70,
    "mpn": 70, "material": 100, "color": 40, "size": 20,
    "item_group_title": 150, "seller_name": 70, "pricing_trend": 80,
}

# CSV/TSV 固定表头。流式写盘不能先扫全量再求列并集，所以列集必须预先固定。
FEED_COLUMNS: tuple[str, ...] = (
    "is_eligible_search", "is_eligible_checkout", "is_ads_eligible",
    "item_id", "gtin", "mpn", "title", "description", "url",
    "brand", "condition", "product_category", "material",
    "weight", "item_weight_unit", "age_group", "gender",
    "image_url", "additional_image_urls",
    "price", "sale_price",
    "availability", "availability_date",
    "group_id", "listing_has_variations", "variant_dict",
    "item_group_title", "color", "size", "size_system",
    "is_digital",
    "seller_name", "seller_url", "seller_privacy_policy", "seller_tos",
    "accepts_returns", "return_deadline_in_days", "return_policy",
    "review_count", "star_rating",
    "age_restriction", "warning",
    "target_countries", "store_country",
)

ADS_API_BASE = "https://api.ads.openai.com/v1"
USER_AGENT = "gpt-ads-feed/2.0"
DEFAULT_SHOPIFY_API_VERSION = "2026-04"
MAX_ITEMS_PER_SHARD = 500_000
GTIN_VALID_LENGTHS = (8, 12, 13, 14)
CENT = Decimal("0.01")
DISCORD_DESC_MAX = 4096
DISCORD_FIELD_MAX = 1024
DISCORD_FIELDS_MAX = 25

# ── 脱敏 ─────────────────────────────────────────────────────────────────
# 异常消息、URL、GraphQL 报错都可能带 token；所有出口（日志/Discord）过一遍。
SECRET_ENV_KEYS = (
    "SHOPIFY_ADMIN_TOKEN", "OPENAI_ADS_API_KEY", "OPENAI_FEED_SFTP_PASSWORD",
    "DISCORD_WEBHOOK_URL",
)
_SECRET_PATTERNS = (
    re.compile(r"shp(at|ca|pa|ss)_[A-Za-z0-9]{8,}"),
    re.compile(r"sk-[A-Za-z0-9_\-]{16,}"),
    re.compile(r"(?i)(bearer|token|api[_-]?key|password)\s*[:=]\s*\S+"),
    re.compile(r"(?i)https://discord(app)?\.com/api/webhooks/\S+"),
    re.compile(r"://[^/\s:@]+:[^/\s@]+@"),  # URL 内嵌 user:pass
)


def redact(text: Any) -> str:
    """把任何可能的凭证替换成 ***。出口唯一入口，不要绕过它打印。"""
    out = str(text)
    for key in SECRET_ENV_KEYS:
        val = os.environ.get(key)
        if val and len(val) >= 8:
            out = out.replace(val, "***")
    for pat in _SECRET_PATTERNS:
        out = pat.sub(lambda m: "***", out)
    return out


def log(msg: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{stamp}] {redact(msg)}", flush=True)


class ConfigError(RuntimeError):
    """配置或凭证问题 —— 退出码 2，重试没用。"""


class DataGuardError(RuntimeError):
    """数据质量闸门拦下 —— 退出码 3，宁可不发也不发坏快照。"""


def env(name: str, required: bool = True) -> str:
    val = os.environ.get(name, "").strip()
    if required and not val:
        raise ConfigError(f"缺少环境变量 {name}")
    return val

# ── 基础工具 ─────────────────────────────────────────────────────────────
def to_decimal(raw: Any) -> Decimal | None:
    """Shopify 金额是十进制字符串，用 Decimal 接住。float 会丢分，禁用。"""
    if raw is None or raw == "":
        return None
    try:
        val = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError):
        return None
    return val if val.is_finite() else None


def money(amount: Decimal, currency: str) -> str:
    """规范要求：金额 + 空格 + ISO 4217。ROUND_HALF_UP 对齐店铺前台显示。"""
    return f"{amount.quantize(CENT, rounding=ROUND_HALF_UP)} {currency.upper()}"


def bool_str(val: bool) -> str:
    """规范明确写「Lower-case string」，不是 JSON 布尔。"""
    return "true" if val else "false"


_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_BLOCK_RE = re.compile(r"(?i)</(p|div|li|h[1-6]|tr)>|<br\s*/?>")


def strip_html(html: str) -> str:
    """规范要求 description 是 plain text；块级标签换成空格避免单词粘连。"""
    if not html:
        return ""
    text = _BLOCK_RE.sub(" ", html)
    text = _TAG_RE.sub(" ", text)
    return _WS_RE.sub(" ", unescape(text)).strip()


def clamp(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


_GID_RE = re.compile(r"/(\d+)(?:[?#].*)?$")


def gid_num(gid: str) -> str:
    """gid://shopify/ProductVariant/123?ns=x → 123（必须剥掉 query）。"""
    if not gid:
        return ""
    m = _GID_RE.search(gid)
    if m:
        return m.group(1)
    # 兜底只接受纯数字（历史 REST id）。返回非数字会让 item_id 变成垃圾，
    # 而 map_variant 靠空串判断「解析失败」。
    tail = gid.rsplit("/", 1)[-1]
    return tail if tail.isdigit() else ""

def gtin_check_digit_ok(digits: str) -> bool:
    """GS1 mod-10：从右往左权重 3,1,3,1…（末位是校验位）。"""
    body, check = digits[:-1], int(digits[-1])
    total = 0
    for i, ch in enumerate(reversed(body)):
        total += int(ch) * (3 if i % 2 == 0 else 1)
    return (10 - total % 10) % 10 == check


def gtin_of(barcode: str | None) -> str:
    """barcode 是店主手填字段，脏数据重灾区：长度不对或校验位错就当没有。"""
    if not barcode:
        return ""
    digits = re.sub(r"[\s\-]", "", str(barcode))
    if not digits.isdigit() or len(digits) not in GTIN_VALID_LENGTHS:
        return ""
    return digits if gtin_check_digit_ok(digits) else ""


_ISO2_RE = re.compile(r"^[A-Z]{2}$")
_CRED_URL_RE = re.compile(r"^[a-z][a-z0-9+.\-]*://[^/\s:@]+:[^/\s@]+@")


def url_problem(value: str, field: str) -> str | None:
    """规范：http/https 都合法，但内嵌用户名密码会整行拒收。"""
    if not value:
        return None
    low = value.strip().lower()
    if not (low.startswith("https://") or low.startswith("http://")):
        return f"{field} 不是 http(s) URL"
    if _CRED_URL_RE.match(low):
        return f"{field} 内嵌了用户名/密码，会被整行拒收"
    return None


# Shopify WeightUnit 枚举 → 规范要求的单位缩写
WEIGHT_UNIT_MAP = {
    "GRAMS": "g", "KILOGRAMS": "kg", "OUNCES": "oz", "POUNDS": "lb",
}


def add_variant_url(base: str, variant_id: str) -> str:
    """onlineStoreUrl 可能已带 query 或 fragment，拼 ?variant= 要看清楚。"""
    if not variant_id:
        return base
    head, sep, frag = base.partition("#")
    joiner = "&" if "?" in head else "?"
    return f"{head}{joiner}variant={variant_id}{sep}{frag}"

# ── 配置 ─────────────────────────────────────────────────────────────────
FORMAT_SUFFIX = {
    "jsonl.gz": ".jsonl.gz", "csv.gz": ".csv.gz",
    "tsv.gz": ".tsv.gz", "parquet": ".parquet",
}
_MF_KEY_RE = re.compile(r"^[a-zA-Z0-9_\-.]{1,64}$")

CONFIG_DEFAULTS: dict[str, Any] = {
    "api_version": DEFAULT_SHOPIFY_API_VERSION,
    "output_format": "jsonl.gz",
    "output_path": "out/products.jsonl.gz",
    "remote_filename": "products.jsonl.gz",
    "state_path": "out/.state.json",
    "rejects_path": "out/rejects.csv",
    "page_size": 100,
    "is_eligible_search": True,
    "is_eligible_checkout": False,
    "is_ads_eligible": True,
    "min_price": "0",
    "exclude_tags": [],
    "default_brand": "",
    # 只在 is_eligible_checkout=true 时必填，但给空默认值，避免别处取键炸掉
    "seller_privacy_policy": "",
    "seller_tos": "",
    "target_countries": ["US"],
    "store_country": "US",
    "condition": "new",
    "accepts_returns": True,
    "return_deadline_in_days": 30,
    "oversell_availability": "backorder",
    "backorder_restock_days": None,
    "preorder_lead_days": None,
    "prefer_seo_description": True,
    "emit_additional_images": True,
    "max_additional_images": 10,
    "age_group": "",
    "gender": "",
    "age_restriction": None,
    "warning": "",
    "metafields": {},
    "min_rows": 1,
    "max_reject_ratio": 0.35,
    "shard_count": 1,
    "delta_include_title": False,
    "currency_override": "",
    # 默认即使内容一模一样也照传：规范建议「至少每天」投递一次全量快照，
    # 省掉这次上传就要赌摄取端不看文件时间，不值得。
    "skip_upload_when_unchanged": False,
}

def load_config(path: str) -> dict[str, Any]:
    """业务配置从 json 读，凭证一律不进这个文件。"""
    p = Path(path).expanduser()
    if not p.exists():
        raise ConfigError(f"配置文件不存在: {p}")
    try:
        cfg: dict[str, Any] = json.loads(p.read_text("utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"配置不是合法 JSON: {exc}") from exc

    for field in ("shop_domain", "seller_name", "seller_url", "return_policy"):
        if not str(cfg.get(field, "")).strip():
            raise ConfigError(f"配置缺少必填项 {field}")
    for key, default in CONFIG_DEFAULTS.items():
        cfg.setdefault(key, default)

    fmt = cfg["output_format"]
    if fmt not in FORMAT_SUFFIX:
        raise ConfigError(f"不支持的 output_format: {fmt}（可选 {list(FORMAT_SUFFIX)}）")
    # 远端文件名后缀必须跟格式一致，否则 OpenAI 侧按后缀选解析器会直接失败
    if not str(cfg["remote_filename"]).endswith(FORMAT_SUFFIX[fmt]):
        raise ConfigError(
            f"remote_filename 必须以 {FORMAT_SUFFIX[fmt]} 结尾（当前 {cfg['remote_filename']}）"
        )

    if not cfg["is_eligible_search"] and cfg["is_eligible_checkout"]:
        raise ConfigError("规范要求：is_eligible_checkout=true 时 is_eligible_search 必须为 true")
    if cfg["is_eligible_checkout"]:
        for field in ("seller_privacy_policy", "seller_tos"):
            if not str(cfg.get(field, "")).strip():
                raise ConfigError(f"开了 checkout 就必须提供 {field}")

    for field in ("seller_url", "return_policy", "seller_privacy_policy", "seller_tos"):
        problem = url_problem(str(cfg.get(field, "")), field)
        if problem:
            raise ConfigError(problem)

    if cfg["oversell_availability"] not in ("in_stock", "backorder", "pre_order"):
        raise ConfigError("oversell_availability 只能是 in_stock / backorder / pre_order")
    if cfg["condition"] and cfg["condition"] not in CONDITION_ENUM:
        raise ConfigError(f"condition 只能是 {CONDITION_ENUM}")
    if cfg["age_group"] and cfg["age_group"] not in AGE_GROUP_ENUM:
        raise ConfigError(f"age_group 只能是 {AGE_GROUP_ENUM}")
    return _validate_numeric_config(cfg)

def _validate_numeric_config(cfg: dict[str, Any]) -> dict[str, Any]:
    min_price = to_decimal(cfg["min_price"])
    if min_price is None or min_price < 0:
        raise ConfigError("min_price 必须是 >= 0 的数字")
    cfg["_min_price"] = min_price

    size = int(cfg["page_size"])
    if not 1 <= size <= 250:
        raise ConfigError("page_size 必须在 1..250（Shopify GraphQL 上限）")
    cfg["page_size"] = size

    shards = int(cfg["shard_count"])
    if not 1 <= shards <= 64:
        raise ConfigError("shard_count 必须在 1..64")
    cfg["shard_count"] = shards

    ratio = float(cfg["max_reject_ratio"])
    if not 0.0 <= ratio <= 1.0:
        raise ConfigError("max_reject_ratio 必须在 0..1")
    cfg["max_reject_ratio"] = ratio

    countries = cfg["target_countries"]
    if isinstance(countries, str):
        countries = [countries]
    countries = [str(c).strip().upper() for c in countries if str(c).strip()]
    if not countries or not all(_ISO2_RE.match(c) for c in countries):
        raise ConfigError("target_countries 必须是 ISO 3166-1 alpha-2，例如 [\"US\"]")
    cfg["target_countries"] = countries

    store_country = str(cfg["store_country"]).strip().upper()
    if not _ISO2_RE.match(store_country):
        raise ConfigError("store_country 必须是 ISO 3166-1 alpha-2，例如 \"US\"")
    cfg["store_country"] = store_country

    mf = cfg["metafields"]
    if not isinstance(mf, dict):
        raise ConfigError("metafields 必须是 {feed字段: \"namespace.key\"} 的映射")
    parsed: dict[str, tuple[str, str]] = {}
    for feed_field, ref in mf.items():
        if feed_field not in FEED_COLUMNS:
            raise ConfigError(f"metafields 里的 {feed_field} 不是规范字段")
        ns, _, key = str(ref).partition(".")
        if not _MF_KEY_RE.match(ns) or not _MF_KEY_RE.match(key):
            raise ConfigError(f"metafield 引用格式应为 namespace.key（当前 {ref}）")
        parsed[feed_field] = (ns, key)
    cfg["_metafields"] = parsed
    cfg["_exclude_tags"] = {str(t).strip().lower() for t in cfg["exclude_tags"] if str(t).strip()}
    return cfg

# ── GraphQL ──────────────────────────────────────────────────────────────
# 只要 read_products 就能拿到的字段。这一组不允许降级，缺了就没法建 feed。
CORE_VARIANT_FIELDS = """
      id
      title
      sku
      barcode
      price
      compareAtPrice
      availableForSale
      selectedOptions { name value }
"""
CORE_PRODUCT_FIELDS = """
        id
        handle
        title
        descriptionHtml
        vendor
        productType
        status
        onlineStoreUrl
        tags
"""

# 可降级字段组：权限不足或该 API 版本没这个字段时，整条 query 会报错。
# 逐组摘掉重试，而不是让整个脚本挂掉 —— 这是 v1 最致命的坑。
OPTIONAL_GROUPS: dict[str, tuple[str, str]] = {
    "variant_image": ("variant", "image { url altText }"),
    "variants_count": ("product", "variantsCount { count }"),
    "product_category": ("product", "category { fullName }"),
    "seo": ("product", "seo { description }"),
    "featured_media": ("product", "featuredMedia { preview { image { url } } }"),
    "media_gallery": (
        "product",
        "media(first: 12, query: \"media_type:IMAGE\") { nodes { preview { image { url } } } }",
    ),
    # 下面两组需要 read_inventory：只授 read_products 时会 ACCESS_DENIED
    "inventory": ("variant", "inventoryQuantity inventoryPolicy"),
    "inventory_item": (
        "variant",
        "inventoryItem { requiresShipping measurement { weight { unit value } } }",
    ),
}

def metafield_selection(metafields: dict[str, tuple[str, str]]) -> str:
    """用别名一次拉多个 metafield（评论数、星级这类 DTC 必备数据在这里）。"""
    parts = []
    for idx, (ns, key) in enumerate(metafields.values()):
        parts.append(f'mf{idx}: metafield(namespace: "{ns}", key: "{key}") {{ value }}')
    return "\n        ".join(parts)


def build_variants_query(
    groups: Iterable[str],
    metafields: dict[str, tuple[str, str]],
    use_status_filter: bool,
) -> str:
    active = set(groups)
    v_extra = "\n      ".join(
        sel for name, (scope, sel) in OPTIONAL_GROUPS.items()
        if scope == "variant" and name in active
    )
    p_extra = "\n        ".join(
        sel for name, (scope, sel) in OPTIONAL_GROUPS.items()
        if scope == "product" and name in active
    )
    mf = metafield_selection(metafields)
    # product_status:active 让 Shopify 侧就过滤掉草稿/归档，省掉大量无效额度
    query_arg = ', query: "product_status:active"' if use_status_filter else ""
    return f"""
query Variants($size: Int!, $cursor: String) {{
  productVariants(first: $size, after: $cursor{query_arg}) {{
    pageInfo {{ hasNextPage endCursor }}
    nodes {{
{CORE_VARIANT_FIELDS}
      {v_extra}
      product {{
{CORE_PRODUCT_FIELDS}
        {p_extra}
        {mf}
      }}
    }}
  }}
}}
"""


SHOP_QUERY = """
{ shop { name currencyCode myshopifyDomain primaryDomain { url } } }
"""

RETRY_STATUS = {429, 500, 502, 503, 504}
_FIELD_ERR_RE = re.compile(r"[Ff]ield '([A-Za-z_][A-Za-z0-9_]*)'")


class Shopify:
    """Admin GraphQL 客户端。REST products 端点已弃用，一律走 GraphQL。"""

    def __init__(self, domain: str, token: str, api_version: str) -> None:
        self.endpoint = f"https://{domain}/admin/api/{api_version}/graphql.json"
        self.token = token
        self.groups: set[str] = set(OPTIONAL_GROUPS)
        self.use_status_filter = True
        self.available_points: float | None = None
        self.restore_rate: float = 50.0

    def call(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        body = json.dumps({"query": query, "variables": variables or {}}).encode()
        last: Exception | None = None
        for attempt in range(5):
            # Request 不能跨重试复用：data 已被消费过
            req = urllib.request.Request(
                self.endpoint, data=body, method="POST",
                headers={
                    "Content-Type": "application/json",
                    "X-Shopify-Access-Token": self.token,
                    "Accept": "application/json",
                },
            )
            try:
                with urllib.request.urlopen(req, timeout=90) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                break
            except urllib.error.HTTPError as exc:
                if exc.code not in RETRY_STATUS or attempt == 4:
                    detail = redact(exc.read().decode("utf-8", "replace")[:400])
                    raise RuntimeError(f"Shopify HTTP {exc.code}: {detail}") from exc
                last = exc
            except urllib.error.URLError as exc:
                if attempt == 4:
                    raise RuntimeError(f"Shopify 网络错误: {redact(exc)}") from exc
                last = exc
            time.sleep(2 ** attempt)
        else:  # pragma: no cover - 循环必然 break 或 raise
            raise RuntimeError(f"Shopify 请求失败: {redact(last)}")

        self._absorb_cost(payload.get("extensions"))
        return payload

    def _absorb_cost(self, extensions: Any) -> None:
        """读 throttleStatus 做主动配速，比等 429 再退避高效得多。"""
        try:
            status = extensions["cost"]["throttleStatus"]
            self.available_points = float(status["currentlyAvailable"])
            self.restore_rate = max(float(status["restoreRate"]), 1.0)
        except (KeyError, TypeError, ValueError):
            return

    def pace(self, next_cost: float = 120.0) -> None:
        """漏桶余量不够下一页就先睡够恢复时间，别去撞限流。"""
        if self.available_points is None or self.available_points >= next_cost:
            return
        need = (next_cost - self.available_points) / self.restore_rate
        wait = min(max(need, 0.2), 10.0)
        log(f"额度余量 {self.available_points:.0f}，配速等待 {wait:.1f}s")
        time.sleep(wait)

    def query_with_degrade(self, build: Callable[[], str], variables: dict[str, Any]) -> dict[str, Any]:
        """字段不存在/无权限时，摘掉对应字段组重试，而不是整体失败。"""
        for _ in range(len(OPTIONAL_GROUPS) + 2):
            payload = self.call(build(), variables)
            errors = payload.get("errors")
            if not errors:
                return payload["data"]
            msgs = json.dumps(errors, ensure_ascii=False)
            if "THROTTLED" in msgs.upper():
                self.available_points = 0.0
                self.pace()
                continue
            dropped = self._drop_group_for(msgs)
            if dropped:
                log(f"Shopify 拒了字段组 {dropped}（权限或 API 版本），已摘掉重试")
                continue
            raise RuntimeError(f"Shopify GraphQL 错误: {redact(msgs)}")
        raise RuntimeError("Shopify GraphQL 反复失败，已放弃降级重试")

    def _drop_group_for(self, msgs: str) -> str | None:
        """按报错里的字段名定位该摘哪一组。"""
        named = {m.lower() for m in _FIELD_ERR_RE.findall(msgs)}
        for name in list(self.groups):
            _, selection = OPTIONAL_GROUPS[name]
            head = selection.strip().split(" ", 1)[0].split("(", 1)[0].lower()
            if head in named or head in msgs.lower():
                self.groups.discard(name)
                return name
        # 报错没点名字段：先怀疑 query 过滤器，再按序摘组兜底
        if self.use_status_filter and ("query" in msgs.lower() or "argument" in msgs.lower()):
            self.use_status_filter = False
            return "product_status 过滤器"
        if self.groups:
            name = sorted(self.groups)[0]
            self.groups.discard(name)
            return name
        return None

    def shop_info(self) -> dict[str, Any]:
        payload = self.call(SHOP_QUERY)
        if payload.get("errors"):
            raise RuntimeError(f"读取店铺信息失败: {redact(json.dumps(payload['errors']))}")
        return payload["data"]["shop"]

    def iter_variants(self, cfg: dict[str, Any]) -> Iterator[dict[str, Any]]:
        cursor: str | None = None
        page = 0
        while True:
            self.pace()
            data = self.query_with_degrade(
                lambda: build_variants_query(self.groups, cfg["_metafields"], self.use_status_filter),
                {"size": cfg["page_size"], "cursor": cursor},
            )
            block = data["productVariants"]
            page += 1
            yield from block["nodes"]
            if not block["pageInfo"]["hasNextPage"]:
                log(f"分页拉取完成，共 {page} 页")
                return
            cursor = block["pageInfo"]["endCursor"]

    # ── Bulk Operations：大目录唯一正解 ──────────────────────────────────
    # 一次导出整个 productVariants 到 JSONL，不吃分页额度。
    # 注意 bulk 里连接字段不能带分页参数，所以 media(first:12) 必须摘掉。
    BULK_START = """
mutation BulkRun($q: String!) {
  bulkOperationRunQuery(query: $q) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
"""
    BULK_POLL = """
{ currentBulkOperation(type: QUERY) {
    id status errorCode objectCount fileSize url } }
"""

    def build_bulk_query(self, cfg: dict[str, Any]) -> str:
        groups = self.groups - {"media_gallery"}
        v_extra = "\n      ".join(
            sel for name, (scope, sel) in OPTIONAL_GROUPS.items()
            if scope == "variant" and name in groups
        )
        p_extra = "\n        ".join(
            sel for name, (scope, sel) in OPTIONAL_GROUPS.items()
            if scope == "product" and name in groups
        )
        return f"""
{{
  productVariants {{
    edges {{
      node {{
{CORE_VARIANT_FIELDS}
        {v_extra}
        product {{
{CORE_PRODUCT_FIELDS}
          {p_extra}
          {metafield_selection(cfg["_metafields"])}
        }}
      }}
    }}
  }}
}}
"""

    def iter_variants_bulk(self, cfg: dict[str, Any], poll_seconds: float = 5.0) -> Iterator[dict[str, Any]]:
        payload = self.call(self.BULK_START, {"q": self.build_bulk_query(cfg)})
        if payload.get("errors"):
            raise RuntimeError(f"启动 bulk 失败: {redact(json.dumps(payload['errors']))}")
        result = payload["data"]["bulkOperationRunQuery"]
        if result.get("userErrors"):
            raise RuntimeError(f"bulk userErrors: {redact(json.dumps(result['userErrors']))}")
        op_id = result["bulkOperation"]["id"]
        log(f"bulk 已启动 {op_id}，轮询中")

        url = None
        for _ in range(720):  # 最多等 1 小时
            time.sleep(poll_seconds)
            data = self.call(self.BULK_POLL)["data"]["currentBulkOperation"]
            status = (data or {}).get("status")
            if status == "COMPLETED":
                url = data.get("url")
                log(f"bulk 完成：{data.get('objectCount')} 个对象，{data.get('fileSize')} 字节")
                break
            if status in ("FAILED", "CANCELED", "EXPIRED"):
                raise RuntimeError(f"bulk 结束于 {status}，errorCode={data.get('errorCode')}")
        else:
            raise RuntimeError("bulk 超过 1 小时未完成，放弃")

        if not url:
            log("bulk 完成但没有结果文件（目录为空）")
            return
        req = urllib.request.Request(url, headers={"Accept": "application/jsonl"})
        with urllib.request.urlopen(req, timeout=300) as resp:
            for raw in io.TextIOWrapper(resp, encoding="utf-8"):
                line = raw.strip()
                if not line:
                    continue
                node = json.loads(line)
                # 只要 ProductVariant 行；嵌套连接会另起行，这里不需要
                if "ProductVariant/" in str(node.get("id", "")):
                    yield node

# ── 映射 ─────────────────────────────────────────────────────────────────
class SkipRow(Exception):
    """这一行不该进 feed，附上原因用于 rejects 报表。"""


def availability_of(variant: dict[str, Any], cfg: dict[str, Any]) -> tuple[str, str | None]:
    """返回 (availability, availability_date)。

    availableForSale 是权威口径：没开库存跟踪时 inventoryQuantity 可能是 0 但仍可售。
    只有在拿到 inventoryPolicy 时才能识别「超卖中」，也就是真正的 backorder。
    """
    if not variant.get("availableForSale"):
        return "out_of_stock", None

    policy = variant.get("inventoryPolicy")
    qty = variant.get("inventoryQuantity")
    oversold = policy == "CONTINUE" and isinstance(qty, int) and qty <= 0
    if not oversold:
        return "in_stock", None

    status = cfg["oversell_availability"]
    if status == "in_stock":
        return "in_stock", None
    days = cfg["backorder_restock_days"] if status == "backorder" else cfg["preorder_lead_days"]
    when = None
    if isinstance(days, int) and days > 0:
        when = (datetime.now(timezone.utc).date() + timedelta(days=days)).isoformat()
    if status == "pre_order" and not when:
        # 规范：pre_order 必须带 availability_date，给不出来就别用这个状态
        return "backorder", None
    return status, when


def _metafield_values(product: dict[str, Any], cfg: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for idx, field in enumerate(cfg["_metafields"]):
        node = product.get(f"mf{idx}")
        value = (node or {}).get("value") if isinstance(node, dict) else None
        if value not in (None, ""):
            out[field] = str(value).strip()
    return out

def _images_of(variant: dict[str, Any], product: dict[str, Any], cfg: dict[str, Any]) -> tuple[str, list[str]]:
    """主图优先用变体图，回落到商品主图；附图去重且不含主图。"""
    main = ((variant.get("image") or {}).get("url") or "").strip()
    featured = (((product.get("featuredMedia") or {}).get("preview") or {}).get("image") or {})
    featured_url = (featured.get("url") or "").strip()
    if not main:
        main = featured_url

    extras: list[str] = []
    if cfg["emit_additional_images"]:
        gallery = ((product.get("media") or {}).get("nodes") or [])
        for node in gallery:
            url = (((node or {}).get("preview") or {}).get("image") or {}).get("url") or ""
            url = url.strip()
            if url and url != main and url not in extras:
                extras.append(url)
        if featured_url and featured_url != main and featured_url not in extras:
            extras.insert(0, featured_url)
        extras = [u for u in extras if not url_problem(u, "additional_image_urls")]
        extras = extras[: max(int(cfg["max_additional_images"]), 0)]
    return main, extras


def _title_of(variant: dict[str, Any], product: dict[str, Any]) -> str:
    """多变体才把变体名拼进标题；单变体拼上去只会污染标题。"""
    base = (product.get("title") or "").strip()
    vtitle = (variant.get("title") or "").strip()
    count = ((product.get("variantsCount") or {}).get("count"))
    is_default = vtitle.lower() in ("", "default title")
    multi = count > 1 if isinstance(count, int) else not is_default
    if multi and not is_default:
        return f"{base} - {vtitle}"
    return base


def _description_of(product: dict[str, Any], cfg: dict[str, Any], fallback: str) -> str:
    """SEO 描述通常已是干净的 plain text，比 descriptionHtml 剥标签更可靠。"""
    if cfg["prefer_seo_description"]:
        seo = ((product.get("seo") or {}).get("description") or "").strip()
        if seo:
            return seo
    body = strip_html(product.get("descriptionHtml") or "")
    return body or fallback

def map_variant(variant: dict[str, Any], cfg: dict[str, Any], currency: str) -> dict[str, Any]:
    """一个 Shopify 变体 → 一条 feed 记录。不合规就抛 SkipRow。"""
    product = variant.get("product") or {}

    if (product.get("status") or "").upper() != "ACTIVE":
        raise SkipRow(f"商品非 ACTIVE（{product.get('status')}）")
    online_url = (product.get("onlineStoreUrl") or "").strip()
    if not online_url:
        raise SkipRow("未发布到在线商店，没有可用 URL")
    problem = url_problem(online_url, "url")
    if problem:
        raise SkipRow(problem)

    tags = {str(t).strip().lower() for t in (product.get("tags") or [])}
    hit = tags & cfg["_exclude_tags"]
    if hit:
        raise SkipRow(f"命中排除标签 {sorted(hit)}")

    price_amt = to_decimal(variant.get("price"))
    if price_amt is None or price_amt <= 0:
        raise SkipRow("价格缺失或非正数（规范要求正数）")
    if price_amt < cfg["_min_price"]:
        raise SkipRow(f"低于 min_price {cfg['_min_price']}")

    # compareAtPrice 才是原价，price 是现价；规范要求 sale_price <= price
    compare_amt = to_decimal(variant.get("compareAtPrice"))
    list_amt, sale_amt = price_amt, None
    if compare_amt is not None and compare_amt > price_amt:
        list_amt, sale_amt = compare_amt, price_amt

    image_url, extra_images = _images_of(variant, product, cfg)
    if not image_url:
        raise SkipRow("没有任何图片")
    problem = url_problem(image_url, "image_url")
    if problem:
        raise SkipRow(problem)

    brand = (product.get("vendor") or "").strip() or str(cfg["default_brand"]).strip()
    if not brand:
        raise SkipRow("brand 为空且未配置 default_brand（规范里 brand 必填）")

    title = _title_of(variant, product)
    if not title:
        raise SkipRow("标题为空")

    variant_id = gid_num(variant.get("id") or "")
    product_id = gid_num(product.get("id") or "")
    if not variant_id:
        raise SkipRow("变体 id 解析失败")
    return _assemble_row(
        variant, product, cfg, currency, variant_id, product_id, title,
        brand, image_url, extra_images, list_amt, sale_amt,
    )

def _assemble_row(
    variant: dict[str, Any], product: dict[str, Any], cfg: dict[str, Any],
    currency: str, variant_id: str, product_id: str, title: str, brand: str,
    image_url: str, extra_images: list[str], list_amt: Decimal, sale_amt: Decimal | None,
) -> dict[str, Any]:
    availability, avail_date = availability_of(variant, cfg)
    row: dict[str, Any] = {
        "is_eligible_search": bool_str(bool(cfg["is_eligible_search"])),
        "is_eligible_checkout": bool_str(bool(cfg["is_eligible_checkout"])),
        "is_ads_eligible": bool_str(bool(cfg["is_ads_eligible"])),
        "item_id": clamp(variant_id, MAXLEN["item_id"]),
        "title": clamp(title, MAXLEN["title"]),
        "description": clamp(_description_of(product, cfg, title), MAXLEN["description"]),
        "url": add_variant_url(product["onlineStoreUrl"].strip(), variant_id),
        "brand": clamp(brand, MAXLEN["brand"]),
        "image_url": image_url,
        "price": money(list_amt, currency),
        "availability": availability,
        "seller_name": clamp(str(cfg["seller_name"]), MAXLEN["seller_name"]),
        "seller_url": str(cfg["seller_url"]),
        "return_policy": str(cfg["return_policy"]),
        "accepts_returns": bool_str(bool(cfg["accepts_returns"])),
        "target_countries": list(cfg["target_countries"]),
        "store_country": cfg["store_country"],
        "group_id": product_id or variant_id,
    }
    if sale_amt is not None:
        row["sale_price"] = money(sale_amt, currency)
    if avail_date:
        row["availability_date"] = avail_date
    if extra_images:
        row["additional_image_urls"] = ",".join(extra_images)
    if cfg["condition"]:
        row["condition"] = cfg["condition"]
    if isinstance(cfg["return_deadline_in_days"], int) and cfg["return_deadline_in_days"] > 0:
        row["return_deadline_in_days"] = cfg["return_deadline_in_days"]

    category = ((product.get("category") or {}).get("fullName") or "").strip()
    if not category:
        category = (product.get("productType") or "").strip()
    if category:
        row["product_category"] = category

    gtin = gtin_of(variant.get("barcode"))
    if gtin:
        row["gtin"] = gtin
    else:
        sku = (variant.get("sku") or "").strip()
        if sku:
            row["mpn"] = clamp(sku, MAXLEN["mpn"])
    return _decorate_row(row, variant, product, cfg)

_OPTION_ALIASES = {
    "color": "color", "colour": "color", "颜色": "color",
    "size": "size", "尺码": "size", "尺寸": "size",
    "material": "material", "材质": "material",
}


def _decorate_row(
    row: dict[str, Any], variant: dict[str, Any],
    product: dict[str, Any], cfg: dict[str, Any],
) -> dict[str, Any]:
    """变体维度 + 重量 + 可选常量 + metafield 覆盖。"""
    options = {
        str(o.get("name", "")).strip(): str(o.get("value", "")).strip()
        for o in (variant.get("selectedOptions") or [])
        if str(o.get("value", "")).strip()
    }
    options.pop("Title", None)
    if options:
        row["variant_dict"] = options
        for name, value in options.items():
            field = _OPTION_ALIASES.get(name.strip().lower())
            if field and field not in row:
                row[field] = clamp(value, MAXLEN[field])
        if "size" in row:
            row["size_system"] = cfg["store_country"]

    group_title = (product.get("title") or "").strip()
    if row.get("variant_dict") and group_title and group_title != row["title"]:
        row["item_group_title"] = clamp(group_title, MAXLEN["item_group_title"])

    # 以 variantsCount 为准：只有一个变体的商品即使有 Color/Size 选项，
    # 也不是「多变体列表」。拿不到该字段（组被降级）时才退回看选项。
    count = ((product.get("variantsCount") or {}).get("count"))
    if isinstance(count, int):
        row["listing_has_variations"] = bool_str(count > 1)
    else:
        row["listing_has_variations"] = bool_str(bool(row.get("variant_dict")))

    inv_item = variant.get("inventoryItem") or {}
    weight = ((inv_item.get("measurement") or {}).get("weight") or {})
    w_val, w_unit = weight.get("value"), WEIGHT_UNIT_MAP.get(str(weight.get("unit") or ""))
    if isinstance(w_val, (int, float)) and w_val > 0 and w_unit:
        row["weight"] = f"{w_val:g}"
        row["item_weight_unit"] = w_unit
    if "requiresShipping" in inv_item:
        row["is_digital"] = bool_str(not inv_item["requiresShipping"])

    for field in ("age_group", "gender", "warning"):
        if str(cfg[field]).strip():
            row[field] = str(cfg[field]).strip()
    if isinstance(cfg["age_restriction"], int) and cfg["age_restriction"] > 0:
        row["age_restriction"] = cfg["age_restriction"]
    if cfg["is_eligible_checkout"]:
        row["seller_privacy_policy"] = str(cfg["seller_privacy_policy"])
        row["seller_tos"] = str(cfg["seller_tos"])

    for field, value in _metafield_values(product, cfg).items():
        row[field] = clamp(value, MAXLEN[field]) if field in MAXLEN else value
    return row

# ── 校验 ─────────────────────────────────────────────────────────────────
_PRICE_RE = re.compile(r"^\d+\.\d{2} [A-Z]{3}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _split_money(value: str) -> tuple[Decimal | None, str]:
    parts = str(value).split(" ")
    if len(parts) != 2:
        return None, ""
    return to_decimal(parts[0]), parts[1]


def validate(row: dict[str, Any]) -> list[str]:
    """本地先把 OpenAI 侧会拒的行拦下来，省一轮 Upload History 排查。"""
    problems: list[str] = []
    for field in REQUIRED_FIELDS:
        value = row.get(field)
        if value in (None, "", []):
            problems.append(f"缺少必填字段 {field}")

    if row.get("availability") not in AVAILABILITY_ENUM:
        problems.append(f"availability 非法: {row.get('availability')}")
    if row.get("availability") == "pre_order" and not row.get("availability_date"):
        problems.append("availability=pre_order 必须带 availability_date")
    for field in ("availability_date", "sale_price_start_date", "sale_price_end_date"):
        if row.get(field) and not _DATE_RE.match(str(row[field])):
            problems.append(f"{field} 不是 ISO 8601 日期")

    price = str(row.get("price", ""))
    if not _PRICE_RE.match(price):
        problems.append(f"price 必须是「金额 ISO4217」两位小数: {price!r}")
    if row.get("sale_price"):
        sale_amt, sale_cur = _split_money(row["sale_price"])
        list_amt, list_cur = _split_money(price)
        if sale_amt is None or not _PRICE_RE.match(str(row["sale_price"])):
            problems.append("sale_price 格式非法")
        elif list_amt is not None and sale_amt > list_amt:
            problems.append("sale_price 必须 <= price")
        elif sale_cur != list_cur:
            problems.append("sale_price 币种必须与 price 一致")
        elif sale_amt <= 0:
            problems.append("sale_price 必须为正数")

    problems.extend(_validate_shape(row))
    return problems

def _validate_shape(row: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    if row.get("is_eligible_checkout") == "true":
        if row.get("is_eligible_search") != "true":
            problems.append("is_eligible_checkout=true 要求 is_eligible_search=true")
        for field in ("seller_privacy_policy", "seller_tos"):
            if not row.get(field):
                problems.append(f"checkout 开启但缺少 {field}")

    for field in ("url", "image_url", "seller_url", "return_policy",
                  "seller_privacy_policy", "seller_tos", "video_url", "model_3d_url"):
        problem = url_problem(str(row.get(field, "")), field)
        if problem:
            problems.append(problem)
    for url in str(row.get("additional_image_urls", "")).split(","):
        problem = url_problem(url.strip(), "additional_image_urls")
        if problem:
            problems.append(problem)

    for field, limit in MAXLEN.items():
        value = row.get(field)
        if isinstance(value, str) and len(value) > limit:
            problems.append(f"{field} 超长 {len(value)}>{limit}")

    gtin = str(row.get("gtin", ""))
    if gtin and (len(gtin) not in GTIN_VALID_LENGTHS or not gtin.isdigit()
                 or not gtin_check_digit_ok(gtin)):
        problems.append(f"gtin 校验位或长度不合法: {gtin}")
    if row.get("age_group") and row["age_group"] not in AGE_GROUP_ENUM:
        problems.append(f"age_group 非法: {row['age_group']}")
    if row.get("condition") and row["condition"] not in CONDITION_ENUM:
        problems.append(f"condition 非法: {row['condition']}")

    unknown = sorted(set(row) - set(FEED_COLUMNS))
    if unknown:
        problems.append(f"含规范外字段（会被丢弃或整行拒收）: {unknown}")
    return problems

# ── 落盘 ─────────────────────────────────────────────────────────────────
def flatten_value(value: Any) -> str:
    """表格类格式（csv/tsv/parquet）需要标量；list 用逗号，dict 用 JSON。"""
    if value is None:
        return ""
    if isinstance(value, bool):
        return bool_str(value)
    if isinstance(value, (list, tuple)):
        return ",".join(str(v) for v in value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


class ShardWriter:
    """一个分片文件。流式写，不在内存里攒全量。"""

    def __init__(self, path: Path, fmt: str) -> None:
        self.path = path
        self.fmt = fmt
        self.tmp = path.with_name(path.name + ".partial")
        self.count = 0
        self._raw = None
        self._gz = None
        self._text = None
        self._csv = None
        self._parquet = None
        self._batch: list[dict[str, str]] = []
        path.parent.mkdir(parents=True, exist_ok=True)
        self._open()

    def _open(self) -> None:
        if self.fmt == "parquet":
            self._open_parquet()
            return
        self._raw = self.tmp.open("wb")
        # mtime=0 + 固定压缩级别：内容不变则文件字节不变，指纹才能用来判断「今天没变化」
        self._gz = gzip.GzipFile(filename="", mode="wb", fileobj=self._raw, mtime=0, compresslevel=6)
        self._text = io.TextIOWrapper(self._gz, encoding="utf-8", newline="")
        if self.fmt in ("csv.gz", "tsv.gz"):
            delim = "\t" if self.fmt == "tsv.gz" else ","
            self._csv = csv.DictWriter(
                self._text, fieldnames=list(FEED_COLUMNS),
                delimiter=delim, extrasaction="ignore", lineterminator="\n",
            )
            self._csv.writeheader()

    def _open_parquet(self) -> None:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:  # 规范首选 parquet，但那是可选依赖
            raise ConfigError(
                "output_format=parquet 需要 pyarrow：pip install pyarrow；"
                "或改用 jsonl.gz / csv.gz / tsv.gz"
            ) from exc
        self._pa, self._pq = pa, pq
        # 全列 string：feed 里 price 本来就是「79.99 USD」这种带币种的字符串
        self._schema = pa.schema([(c, pa.string()) for c in FEED_COLUMNS])
        self._parquet = pq.ParquetWriter(str(self.tmp), self._schema, compression="zstd")

    def write(self, row: dict[str, Any]) -> None:
        self.count += 1
        if self.fmt == "jsonl.gz":
            self._text.write(json.dumps(row, ensure_ascii=False) + "\n")
            return
        flat = {col: flatten_value(row.get(col)) for col in FEED_COLUMNS}
        if self.fmt == "parquet":
            self._batch.append(flat)
            if len(self._batch) >= 5000:
                self._flush_parquet()
            return
        self._csv.writerow(flat)

    def _flush_parquet(self) -> None:
        if not self._batch:
            return
        cols = {c: [r[c] for r in self._batch] for c in FEED_COLUMNS}
        table = self._pa.Table.from_pydict(cols, schema=self._schema)
        self._parquet.write_table(table)
        self._batch.clear()

    def close(self) -> Path:
        """按 text → gzip → raw 的顺序关。v1 没关 raw，句柄一直泄漏。"""
        if self.fmt == "parquet":
            self._flush_parquet()
            self._parquet.close()
        else:
            try:
                self._text.flush()
                self._text.close()   # 会连带 close gzip
            finally:
                self._raw.close()    # 底层文件必须显式关
        os.replace(self.tmp, self.path)  # 同目录 rename，本地也要原子
        return self.path

def shard_name(base: str, index: int, total: int) -> str:
    """分片集合必须跨次稳定，所以用固定编号而不是「按需切分」。"""
    if total <= 1:
        return base
    for suffix in sorted(FORMAT_SUFFIX.values(), key=len, reverse=True):
        if base.endswith(suffix):
            return f"{base[: -len(suffix)]}-{index:04d}{suffix}"
    return f"{base}-{index:04d}"


class FeedWriter:
    """按 item_id 哈希把行路由到固定分片，写完统一原子落位。"""

    def __init__(self, out_path: str, fmt: str, shard_count: int) -> None:
        self.base = Path(out_path).expanduser()
        self.fmt = fmt
        self.shard_count = shard_count
        self.shards: list[ShardWriter] = [
            ShardWriter(self.base.with_name(shard_name(self.base.name, i, shard_count)), fmt)
            for i in range(shard_count)
        ]

    def write(self, row: dict[str, Any]) -> None:
        if self.shard_count == 1:
            self.shards[0].write(row)
            return
        digest = hashlib.sha1(str(row["item_id"]).encode()).digest()
        self.shards[digest[0] % self.shard_count].write(row)

    def close(self) -> list[Path]:
        paths = [s.close() for s in self.shards]
        for shard in self.shards:
            if shard.count > MAX_ITEMS_PER_SHARD:
                log(f"警告：{shard.path.name} 有 {shard.count} 条，超过单分片 50 万条建议值，"
                    f"请调大 shard_count")
        return paths

    def abort(self) -> None:
        """异常路径：清掉 .partial，绝不让半截文件顶掉上一版。"""
        for shard in self.shards:
            for handle in (shard._text, shard._raw):
                try:
                    if handle is not None:
                        handle.close()
                except Exception:
                    pass
            shard.tmp.unlink(missing_ok=True)


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()

# ── 状态与差异 ───────────────────────────────────────────────────────────
STATE_VERSION = 2


def load_state(path: str) -> dict[str, Any]:
    p = Path(path).expanduser()
    empty = {"version": STATE_VERSION, "items": {}, "content_sha256": "", "generated_at": ""}
    if not p.exists():
        return empty
    try:
        data = json.loads(p.read_text("utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        log(f"状态文件读不了，按首次运行处理: {redact(exc)}")
        return empty
    if data.get("version") != STATE_VERSION or not isinstance(data.get("items"), dict):
        log("状态文件版本不匹配，按首次运行处理")
        return empty
    return data


def save_state(path: str, items: dict[str, str], content_sha: str) -> None:
    p = Path(path).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": STATE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "content_sha256": content_sha,
        "items": items,
    }
    tmp = p.with_name(p.name + ".partial")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), "utf-8")
    os.replace(tmp, p)


def fingerprint(row: dict[str, Any]) -> str:
    """判断一行「变了没有」只需要会影响买家决策的字段。

    末位存 group_id：Delta API 要求 products[].id 是父商品 ID，
    而删除的行只在旧状态里存在，不重新抓一遍就拿不到它的 group_id。
    分隔符用 \\x1f，商品标题里不会出现。
    """
    return "\x1f".join((
        str(row.get("availability", "")),
        str(row.get("price", "")),
        str(row.get("sale_price", "")),
        str(row.get("title", "")),
        str(row.get("group_id", "")),
    ))


def _unpack(fp: str) -> tuple[str, str, str, str, str]:
    parts = fp.split("\x1f") if "\x1f" in fp else fp.split("|")
    parts += [""] * (5 - len(parts))
    return parts[0], parts[1], parts[2], parts[3], parts[4]


def diff_state(old: dict[str, str], new: dict[str, str]) -> dict[str, Any]:
    """算出「今天真正变了什么」。Delta 只推这一部分，也用来写日报。"""
    old_keys, new_keys = set(old), set(new)
    added = sorted(new_keys - old_keys)
    removed = sorted(old_keys - new_keys)
    price_changed: list[str] = []
    stock_changed: list[str] = []
    title_changed: list[str] = []
    for item_id in sorted(new_keys & old_keys):
        if new[item_id] == old[item_id]:
            continue
        o_avail, o_price, o_sale, o_title, _ = _unpack(old[item_id])
        n_avail, n_price, n_sale, n_title, _ = _unpack(new[item_id])
        if (o_price, o_sale) != (n_price, n_sale):
            price_changed.append(item_id)
        if o_avail != n_avail:
            stock_changed.append(item_id)
        if o_title != n_title:
            title_changed.append(item_id)
    changed = sorted(set(price_changed) | set(stock_changed) | set(title_changed))
    return {
        "added": added,
        "removed": removed,
        "price_changed": price_changed,
        "stock_changed": stock_changed,
        "title_changed": title_changed,
        "changed": changed,
        "first_run": not old,
    }

# ── 上传 ─────────────────────────────────────────────────────────────────
def sftp_settings() -> dict[str, Any]:
    """凭证只从环境变量取。缺 host 就当没配 SFTP，让调用方决定要不要报错。"""
    return {
        "host": env("OPENAI_FEED_SFTP_HOST", required=False),
        "port": int(env("OPENAI_FEED_SFTP_PORT", required=False) or "22"),
        "user": env("OPENAI_FEED_SFTP_USER", required=False),
        "dir": env("OPENAI_FEED_SFTP_DIR", required=False) or ".",
        "key": env("OPENAI_FEED_SFTP_KEY", required=False),
        "password": env("OPENAI_FEED_SFTP_PASSWORD", required=False),
        "known_hosts": env("OPENAI_FEED_SFTP_KNOWN_HOSTS", required=False),
    }


def _remote_join(directory: str, name: str) -> str:
    d = (directory or ".").rstrip("/")
    return name if d in ("", ".") else f"{d}/{name}"


def _load_host_keys(client: Any, s: dict[str, Any]) -> None:
    """明确拒绝未知 host key。AutoAddPolicy 等于关掉中间人防护，不用。"""
    import paramiko

    loaded = False
    if s["known_hosts"]:
        path = Path(s["known_hosts"]).expanduser()
        if not path.exists():
            raise ConfigError(f"OPENAI_FEED_SFTP_KNOWN_HOSTS 指向的文件不存在: {path}")
        client.load_host_keys(str(path))
        loaded = True
    else:
        default = Path("~/.ssh/known_hosts").expanduser()
        if default.exists():
            client.load_host_keys(str(default))
            loaded = True
    if not loaded:
        raise ConfigError(
            "找不到 known_hosts。先固定主机指纹再上传：\n"
            f"  ssh-keyscan -p {s['port']} {s['host']} >> ~/.ssh/known_hosts\n"
            "或用 OPENAI_FEED_SFTP_KNOWN_HOSTS 指定文件路径"
        )
    client.set_missing_host_key_policy(paramiko.RejectPolicy())

def _upload_one(sftp: Any, local: Path, remote_dir: str, remote_name: str) -> None:
    """先传 .tmp，核对字节数，再 rename 覆盖。摄取端永远看不到半截文件。"""
    target = _remote_join(remote_dir, remote_name)
    staging = f"{target}.tmp"
    size = local.stat().st_size
    sftp.put(str(local), staging)
    uploaded = sftp.stat(staging).st_size
    if uploaded != size:
        try:
            sftp.remove(staging)
        except Exception:
            pass
        raise RuntimeError(f"{remote_name} 上传字节不符：本地 {size}，远端 {uploaded}")
    try:
        sftp.posix_rename(staging, target)   # 同目录原子覆盖
    except (AttributeError, IOError):
        try:
            sftp.remove(target)              # 老服务器没有 posix_rename 扩展
        except IOError:
            pass
        sftp.rename(staging, target)
    final = sftp.stat(target).st_size
    if final != size:
        raise RuntimeError(f"{remote_name} 落位后字节不符：期望 {size}，远端 {final}")
    log(f"已上传 {remote_name}（{size} bytes）")


def upload_sftp(paths: list[Path], remote_names: list[str]) -> dict[str, Any]:
    s = sftp_settings()
    if not s["host"] or not s["user"]:
        raise ConfigError("缺少 OPENAI_FEED_SFTP_HOST / OPENAI_FEED_SFTP_USER")
    try:
        import paramiko  # noqa: F401
    except ImportError:
        log("没装 paramiko，改用系统 sftp 命令")
        return _upload_sftp_cli(paths, remote_names, s)
    return _upload_sftp_paramiko(paths, remote_names, s)

def _upload_sftp_paramiko(paths: list[Path], names: list[str], s: dict[str, Any]) -> dict[str, Any]:
    import paramiko

    last: Exception | None = None
    for attempt in range(1, 4):
        client = paramiko.SSHClient()
        try:
            _load_host_keys(client, s)
            kwargs: dict[str, Any] = {
                "hostname": s["host"], "port": s["port"], "username": s["user"],
                "timeout": 30, "banner_timeout": 30, "auth_timeout": 30,
                "allow_agent": False, "look_for_keys": False,
            }
            if s["key"]:
                key_path = Path(s["key"]).expanduser()
                if not key_path.exists():
                    raise ConfigError(f"OPENAI_FEED_SFTP_KEY 指向的私钥不存在: {key_path}")
                kwargs["key_filename"] = str(key_path)
            elif s["password"]:
                kwargs["password"] = s["password"]
            else:
                raise ConfigError("SFTP 既没有 KEY 也没有 PASSWORD")
            client.connect(**kwargs)
            sftp = client.open_sftp()
            try:
                for local, name in zip(paths, names):
                    _upload_one(sftp, local, s["dir"], name)
            finally:
                sftp.close()
            return {"ok": True, "transport": "paramiko", "files": names}
        except (ConfigError, paramiko.SSHException) as exc:
            if isinstance(exc, paramiko.BadHostKeyException):
                raise ConfigError(
                    f"主机指纹和 known_hosts 不一致（可能是服务器换了密钥，也可能是中间人）。"
                    f"确认无误后再更新：ssh-keyscan -p {s['port']} {s['host']}"
                ) from exc
            if isinstance(exc, ConfigError):
                raise
            last = exc
        except OSError as exc:
            last = exc
        finally:
            client.close()
        if attempt < 3:
            wait = 2 ** attempt
            log(f"SFTP 第 {attempt} 次失败，{wait}s 后重试: {redact(last)}")
            time.sleep(wait)
    raise RuntimeError(f"SFTP 上传失败（3 次）: {redact(last)}")

def _upload_sftp_cli(paths: list[Path], names: list[str], s: dict[str, Any]) -> dict[str, Any]:
    """没有 paramiko 时的兜底。只支持密钥登录，密码留给 paramiko。"""
    exe = shutil.which("sftp")
    if not exe:
        raise ConfigError("既没有 paramiko 也没有 sftp 命令：pip install paramiko")
    if not s["key"]:
        raise ConfigError("sftp 命令模式必须用 OPENAI_FEED_SFTP_KEY（密码模式请装 paramiko）")
    key_path = Path(s["key"]).expanduser()
    if not key_path.exists():
        raise ConfigError(f"OPENAI_FEED_SFTP_KEY 指向的私钥不存在: {key_path}")

    lines: list[str] = []
    for local, name in zip(paths, names):
        target = _remote_join(s["dir"], name)
        # 远端路径一律 shlex.quote，防止文件名里的空格或引号被 sftp 解析成额外参数
        lines.append(f"put {shlex.quote(str(local))} {shlex.quote(target + '.tmp')}")
        lines.append(f"-rm {shlex.quote(target)}")
        lines.append(f"rename {shlex.quote(target + '.tmp')} {shlex.quote(target)}")
        lines.append(f"ls -l {shlex.quote(target)}")
    script = "\n".join(lines) + "\nbye\n"

    cmd = [
        exe, "-b", "-", "-i", str(key_path), "-P", str(s["port"]),
        "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
        "-o", "ConnectTimeout=30",
    ]
    if s["known_hosts"]:
        cmd += ["-o", f"UserKnownHostsFile={Path(s['known_hosts']).expanduser()}"]
    cmd.append(f"{s['user']}@{s['host']}")

    proc = subprocess.run(cmd, input=script, capture_output=True, text=True, timeout=1800)
    if proc.returncode != 0:
        raise RuntimeError(f"sftp 命令失败（exit {proc.returncode}）: {redact(proc.stderr.strip())}")
    log(f"已通过 sftp 命令上传 {len(names)} 个文件")
    return {"ok": True, "transport": "sftp-cli", "files": names}

# ── HTTP ─────────────────────────────────────────────────────────────────
def post_json(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str] | None = None,
    method: str = "POST",
    attempts: int = 4,
) -> tuple[int, str]:
    """带退避的 JSON 请求。返回 (状态码, 响应体)，4xx 不重试。"""
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    base_headers = {"Content-Type": "application/json", "User-Agent": USER_AGENT}
    base_headers.update(headers or {})
    last = ""
    for attempt in range(1, attempts + 1):
        req = urllib.request.Request(url, data=body, headers=base_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.status, resp.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            text = exc.read().decode("utf-8", "replace")
            if exc.code not in RETRY_STATUS or attempt == attempts:
                return exc.code, text
            last = f"HTTP {exc.code}"
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt == attempts:
                raise RuntimeError(f"请求 {url.split('?')[0]} 失败: {redact(exc)}") from exc
            last = str(exc)
        wait = min(2 ** attempt, 16)
        log(f"请求第 {attempt} 次失败（{redact(last)}），{wait}s 后重试")
        time.sleep(wait)
    return 0, ""

# ── Delta Feeds ──────────────────────────────────────────────────────────
DELTA_CHUNK = 500
# 文档只给了 in_stock / out_of_stock 两个显式例子，但 status 是「显式可用性」，
# 与 flat-file 的 availability 同一套枚举，所以直接透传当前值。
_DELTA_IN_STOCK = {"in_stock", "pre_order", "backorder"}


def build_delta_products(
    item_ids: list[str],
    new_state: dict[str, str],
    include_title: bool,
) -> list[dict[str, Any]]:
    """按父商品聚合变体。同一变体不允许出现两次，所以先按 item_id 去重。"""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item_id in dict.fromkeys(item_ids):
        fp = new_state.get(item_id)
        if not fp:
            continue
        availability, _price, _sale, title, group_id = _unpack(fp)
        if not group_id or availability not in AVAILABILITY_ENUM or availability == "unknown":
            continue  # unknown 推过去没有意义，父 ID 缺失则无法定位
        variant: dict[str, Any] = {
            "id": item_id,
            # available 与 status 同时给：文档说两者都在时 status 优先，
            # 这样即使某侧被忽略语义也一致。
            "availability": {
                "available": availability in _DELTA_IN_STOCK,
                "status": availability,
            },
        }
        if include_title and title:
            variant["title"] = clamp(title, MAXLEN["title"])
        grouped.setdefault(group_id, []).append(variant)
    return [{"id": gid, "variants": v} for gid, v in grouped.items() if v]

def _delta_error_code(text: str) -> str:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return ""
    err = data.get("error")
    if isinstance(err, dict):
        return str(err.get("code") or err.get("type") or "")
    return str(data.get("code") or "")


def push_delta(products: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    """只推库存/标题变化。价格变化无法走 delta，必须靠全量快照。"""
    feed_id = env("OPENAI_ADS_FEED_ID", required=False)
    api_key = env("OPENAI_ADS_API_KEY", required=False)
    if not feed_id or not api_key:
        return {"skipped": "缺少 OPENAI_ADS_FEED_ID / OPENAI_ADS_API_KEY"}
    if not products:
        return {"skipped": "没有可推送的库存或标题变化"}

    url = f"{ADS_API_BASE}/feeds/{urllib.parse.quote(feed_id, safe='')}/products"
    headers = {"Authorization": f"Bearer {api_key}"}
    sent_variants = accepted_chunks = 0
    errors: list[str] = []

    for start in range(0, len(products), DELTA_CHUNK):
        chunk = products[start : start + DELTA_CHUNK]
        payload = {"products": chunk}
        canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        headers["Idempotency-Key"] = hashlib.sha256(canonical.encode()).hexdigest()[:32]
        status, text = post_json(url, payload, headers=headers, method="PATCH")
        n_variants = sum(len(p["variants"]) for p in chunk)

        if status == 200:
            accepted = False
            try:
                accepted = bool(json.loads(text).get("accepted"))
            except json.JSONDecodeError:
                errors.append("200 但响应不是合法 JSON")
            # accepted=false 表示 feed 处理侧没收下，不能当成功
            if accepted:
                accepted_chunks += 1
                sent_variants += n_variants
            else:
                errors.append(f"accepted=false: {clamp(redact(text), 200)}")
            continue

        code = _delta_error_code(text)
        if status == 403 and code in ("product_feed_api_disabled", "product_feed_delta_api_disabled"):
            # 明确文档化：未开通就不要反复重试
            return {"skipped": f"账户未开通 Delta Feeds API（{code}），找 OpenAI 客户团队开权限"}
        errors.append(f"HTTP {status} {code or ''}: {clamp(redact(text), 200)}".strip())
        break

    return {
        "ok": not errors,
        "chunks": accepted_chunks,
        "variants": sent_variants,
        "errors": errors,
    }

# ── 通知 ─────────────────────────────────────────────────────────────────
def mask_domain(domain: str) -> str:
    """日报里店铺名脱敏，只留首尾各 2 个字符。"""
    head, _, tail = domain.partition(".")
    if len(head) <= 4:
        return f"{head[:1]}***.{tail}" if tail else f"{head[:1]}***"
    return f"{head[:2]}***{head[-2:]}.{tail}" if tail else f"{head[:2]}***{head[-2:]}"


def notify_discord(title: str, fields: list[tuple[str, str]], ok: bool, note: str = "") -> None:
    webhook = env("DISCORD_WEBHOOK_URL", required=False)
    if not webhook:
        return
    embed: dict[str, Any] = {
        "title": clamp(title, 256),
        "color": 0x2ECC71 if ok else 0xE74C3C,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fields": [
            {"name": clamp(redact(n), 256), "value": clamp(redact(v) or "-", DISCORD_FIELD_MAX), "inline": True}
            for n, v in fields[:DISCORD_FIELDS_MAX]
        ],
    }
    if note:
        embed["description"] = clamp(redact(note), DISCORD_DESC_MAX)
    status, text = post_json(webhook, {"embeds": [embed]}, attempts=3)
    if status not in (200, 204):
        log(f"Discord 通知失败 HTTP {status}: {clamp(redact(text), 200)}")


def write_rejects(path: str, rejects: list[tuple[str, str, str]]) -> Path | None:
    """被拒的行单独落一个 CSV，方便回店里改数据。"""
    if not rejects:
        return None
    p = Path(path).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".partial")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(("item_id", "title", "reason"))
        for item_id, row_title, reason in rejects:
            writer.writerow((item_id, clamp(row_title, 120), reason))
    os.replace(tmp, p)
    return p

# ── 采集 ─────────────────────────────────────────────────────────────────
REJECT_SAMPLE_MAX = 200


def collect(
    variants: Iterable[dict[str, Any]],
    cfg: dict[str, Any],
    currency: str,
    writer: FeedWriter | None,
    limit: int = 0,
) -> dict[str, Any]:
    """边抓边写。内存里只留指纹和计数，不攒全量行。"""
    state: dict[str, str] = {}
    reasons: dict[str, int] = {}
    samples: list[tuple[str, str, str]] = []
    seen_products: set[str] = set()
    written = skipped = invalid = dupes = 0

    def note(item_id: str, row_title: str, reason: str) -> None:
        reasons[reason] = reasons.get(reason, 0) + 1
        if len(samples) < REJECT_SAMPLE_MAX:
            samples.append((item_id, row_title, reason))

    for variant in variants:
        try:
            row = map_variant(variant, cfg, currency)
        except SkipRow as exc:
            skipped += 1
            note(gid_num(str(variant.get("id") or "")), str(variant.get("title") or ""), str(exc))
            continue

        problems = validate(row) + _validate_shape(row)
        if problems:
            invalid += 1
            note(row["item_id"], row["title"], "; ".join(problems[:3]))
            continue

        item_id = row["item_id"]
        if item_id in state:
            dupes += 1
            note(item_id, row["title"], "item_id 重复，已丢弃后一条")
            continue

        if writer is not None:
            writer.write(row)
        state[item_id] = fingerprint(row)
        seen_products.add(row.get("group_id") or item_id)
        written += 1
        if limit and written >= limit:
            log(f"已达 --limit {limit}，停止抓取")
            break

    return {
        "state": state, "written": written, "skipped": skipped, "invalid": invalid,
        "dupes": dupes, "products": len(seen_products),
        "reasons": dict(sorted(reasons.items(), key=lambda kv: -kv[1])), "samples": samples,
    }

# ── 主流程 ───────────────────────────────────────────────────────────────
def _guard_snapshot(res: dict[str, Any], cfg: dict[str, Any]) -> None:
    """全量快照语义下，一次写错就等于全店下架。这里是最后一道闸。"""
    written = res["written"]
    if written == 0:
        raise DataGuardError(
            "快照为空。全量投递里空文件等于「本店无商品」，会下架整个目录，已拒绝发布。"
            f" 跳过 {res['skipped']} 条，校验失败 {res['invalid']} 条"
        )
    if written < int(cfg["min_rows"]):
        raise DataGuardError(
            f"只产出 {written} 条，低于 min_rows={cfg['min_rows']}，已拒绝发布"
        )
    total = written + res["skipped"] + res["invalid"] + res["dupes"]
    ratio = (total - written) / total if total else 0.0
    if ratio > cfg["max_reject_ratio"]:
        top = list(res["reasons"].items())[:3]
        raise DataGuardError(
            f"拒收率 {ratio:.1%} 超过 max_reject_ratio={cfg['max_reject_ratio']:.0%}，"
            f"已拒绝发布。主要原因: {top}"
        )


def _fetch_variants(client: Shopify, cfg: dict[str, Any], use_bulk: bool) -> Iterable[dict[str, Any]]:
    if use_bulk:
        log("使用 Bulk Operations 抓取")
        return client.iter_variants_bulk(cfg)
    return client.iter_variants(cfg)

def run(args: argparse.Namespace) -> int:
    started = time.time()
    cfg = load_config(args.config)
    token = env("SHOPIFY_ADMIN_TOKEN")
    domain = str(cfg["shop_domain"]).strip().lower().removeprefix("https://").rstrip("/")
    client = Shopify(domain, token, str(cfg["api_version"]))

    currency = str(cfg["currency_override"]).strip().upper()
    if currency:
        log(f"币种用配置里的 {currency}")
    else:
        shop = client.shop_info()
        currency = shop["currency"]
        log(f"店铺 {mask_domain(shop['domain'])}，币种 {currency}")
    if not _ISO2_RE.match(currency[:2]) or len(currency) != 3:
        raise ConfigError(f"币种不是合法 ISO 4217: {currency}")

    prev = load_state(str(cfg["state_path"]))
    writer = FeedWriter(str(cfg["output_path"]), str(cfg["output_format"]), int(cfg["shard_count"]))
    try:
        res = collect(_fetch_variants(client, cfg, args.bulk), cfg, currency, writer, args.limit)
        _guard_snapshot(res, cfg)
        paths = writer.close()
    except BaseException:
        writer.abort()   # 半截文件绝不允许顶掉上一版
        raise

    sizes = {p.name: p.stat().st_size for p in paths}
    content_sha = hashlib.sha256(
        "".join(sha256_of(p) for p in sorted(paths)).encode()
    ).hexdigest()
    unchanged = bool(prev.get("content_sha256")) and prev["content_sha256"] == content_sha
    log(f"产出 {res['written']} 条 / {res['products']} 个商品，"
        f"{sum(sizes.values())} 字节，sha256={content_sha[:12]}")

    diff = diff_state(prev.get("items") or {}, res["state"])
    rejects_file = write_rejects(str(cfg["rejects_path"]), res["samples"])
    return _publish(args, cfg, res, diff, paths, sizes, content_sha,
                    unchanged, rejects_file, started)

def _publish(
    args: argparse.Namespace, cfg: dict[str, Any], res: dict[str, Any],
    diff: dict[str, Any], paths: list[Path], sizes: dict[str, int],
    content_sha: str, unchanged: bool, rejects_file: Path | None, started: float,
) -> int:
    remote_base = str(cfg["remote_filename"])
    shard_count = int(cfg["shard_count"])
    remote_names = [shard_name(remote_base, i, shard_count) for i in range(shard_count)]

    upload: dict[str, Any] = {"skipped": "--dry-run"}
    delta: dict[str, Any] = {"skipped": "未开启 --delta"}
    if args.dry_run:
        log("dry-run：本地文件已生成，不上传、不写状态")
    elif unchanged and cfg["skip_upload_when_unchanged"]:
        upload = {"skipped": "内容与上次完全一致"}
        log("内容未变化，按配置跳过上传")
    else:
        if unchanged:
            log("内容与上次一致，仍按「至少每天一次」照常投递")
        upload = upload_sftp(paths, remote_names)

    # Delta 只能改已存在变体的库存/标题：首次运行没有基线，新增和删除也推不了
    if args.delta and not args.dry_run:
        if diff["first_run"]:
            delta = {"skipped": "首次运行，没有基线可比"}
        else:
            targets = sorted(set(diff["stock_changed"]) | (
                set(diff["title_changed"]) if cfg["delta_include_title"] else set()
            ))
            products = build_delta_products(targets, res["state"], bool(cfg["delta_include_title"]))
            delta = push_delta(products, cfg)
            if diff["price_changed"]:
                log(f"{len(diff['price_changed'])} 条改了价格，Delta API 不支持价格，"
                    f"这部分靠上面的全量快照生效")

    ok = ("skipped" in upload or upload.get("ok")) and not delta.get("errors")
    if not args.dry_run and not args.limit and (upload.get("ok") or upload.get("skipped")):
        save_state(str(cfg["state_path"]), res["state"], content_sha)
    elif args.limit:
        log("--limit 模式不写状态，避免把子集当成完整基线")

    _report(cfg, res, diff, sizes, content_sha, upload, delta, rejects_file, ok, started)
    return EXIT_OK if ok else EXIT_RUNTIME

def _status_text(result: dict[str, Any]) -> str:
    if "skipped" in result:
        return f"skipped（{result['skipped']}）"
    if result.get("errors"):
        return f"failed: {'; '.join(result['errors'][:2])}"
    return "success"


def _report(
    cfg: dict[str, Any], res: dict[str, Any], diff: dict[str, Any],
    sizes: dict[str, int], content_sha: str, upload: dict[str, Any],
    delta: dict[str, Any], rejects_file: Path | None, ok: bool, started: float,
) -> None:
    total_in = res["written"] + res["skipped"] + res["invalid"] + res["dupes"]
    fields: list[tuple[str, str]] = [
        ("店铺", mask_domain(str(cfg["shop_domain"]))),
        ("Shopify 商品", str(res["products"])),
        ("Feed 记录", f"{res['written']}/{total_in}"),
        ("跳过/错误", f"{res['skipped']}/{res['invalid'] + res['dupes']}"),
        ("文件大小", f"{sum(sizes.values())} bytes" + (f" × {len(sizes)} 分片" if len(sizes) > 1 else "")),
        ("内容指纹", content_sha[:12]),
        ("OpenAI 状态", _status_text(upload)),
        ("Delta 状态", _status_text(delta) if "skipped" not in delta
            else f"skipped（{delta['skipped']}）"),
        ("耗时", f"{time.time() - started:.1f}s"),
    ]
    if not diff["first_run"]:
        fields.append((
            "变化",
            f"新增 {len(diff['added'])} / 下架 {len(diff['removed'])} / "
            f"价格 {len(diff['price_changed'])} / 库存 {len(diff['stock_changed'])}",
        ))
    if delta.get("variants"):
        fields.append(("Delta 推送", f"{delta['variants']} 个变体 / {delta['chunks']} 批"))

    note = ""
    if res["reasons"]:
        top = list(res["reasons"].items())[:5]
        note = "拒收原因 Top:\n" + "\n".join(f"· {r} × {n}" for r, n in top)
        if rejects_file:
            note += f"\n明细: {rejects_file}"

    for name, value in fields:
        log(f"  {name}: {value}")
    notify_discord(
        "GPT Ads Feed 每日更新成功" if ok else "GPT Ads Feed 更新失败",
        fields, ok, note,
    )

# ── 自检 ─────────────────────────────────────────────────────────────────
SELF_TEST_CFG_RAW: dict[str, Any] = {
    "shop_domain": "example.myshopify.com",
    "seller_name": "Example Store",
    "seller_url": "https://example.com",
    "return_policy": "https://example.com/policies/refund-policy",
    "default_brand": "ExampleBrand",
    "output_path": "out/products.jsonl.gz",
    "remote_filename": "products.jsonl.gz",
}


def _cfg(**over: Any) -> dict[str, Any]:
    """自检用配置。走真正的 load_config，顺便覆盖校验逻辑。"""
    raw = dict(SELF_TEST_CFG_RAW)
    raw.update(over)
    tmp = Path(os.environ.get("TMPDIR", "/tmp")) / f"feedcfg_{os.getpid()}.json"
    tmp.write_text(json.dumps(raw), "utf-8")
    try:
        return load_config(str(tmp))
    finally:
        tmp.unlink(missing_ok=True)


def _variant(**over: Any) -> dict[str, Any]:
    product = {
        "id": "gid://shopify/Product/900",
        "handle": "tee",
        "title": "Cotton Tee",
        "descriptionHtml": "<p>Soft <b>cotton</b> tee.</p><p>Made in PT.</p>",
        "vendor": "ExampleBrand",
        "productType": "Shirts",
        "status": "ACTIVE",
        "onlineStoreUrl": "https://example.com/products/tee",
        "tags": ["summer"],
        "variantsCount": {"count": 2},
        "featuredMedia": {"preview": {"image": {"url": "https://cdn.example.com/a.jpg"}}},
    }
    product.update(over.pop("product", {}))
    variant = {
        "id": "gid://shopify/ProductVariant/123",
        "title": "Blue / M",
        "sku": "TEE-BL-M",
        "barcode": "0012345678905",
        "price": "19.9",
        "compareAtPrice": "29.99",
        "availableForSale": True,
        "selectedOptions": [{"name": "Color", "value": "Blue"}, {"name": "Size", "value": "M"}],
        "product": product,
    }
    variant.update(over)
    return variant

class _Check:
    def __init__(self) -> None:
        self.passed = 0
        self.failed: list[str] = []

    def ok(self, cond: bool, label: str) -> None:
        if cond:
            self.passed += 1
        else:
            self.failed.append(label)

    def eq(self, got: Any, want: Any, label: str) -> None:
        self.ok(got == want, f"{label}: got {got!r}, want {want!r}")

    def raises(self, exc: type[BaseException], fn: Callable[[], Any], label: str) -> None:
        try:
            fn()
        except exc:
            self.passed += 1
            return
        except Exception as other:  # noqa: BLE001
            self.failed.append(f"{label}: 抛了 {type(other).__name__} 而不是 {exc.__name__}")
            return
        self.failed.append(f"{label}: 应该抛 {exc.__name__} 但没抛")


def _t_money(c: _Check) -> None:
    c.eq(money(Decimal("19.9"), "usd"), "19.90 USD", "money 补两位并大写币种")
    c.eq(money(Decimal("0.005"), "USD"), "0.01 USD", "money 用 ROUND_HALF_UP 而不是银行家舍入")
    c.eq(money(Decimal("2.675"), "USD"), "2.68 USD", "money 0.675 进位")
    c.eq(to_decimal("abc"), None, "to_decimal 拒绝非数字")
    c.eq(to_decimal(None), None, "to_decimal 拒绝 None")
    c.eq(to_decimal("NaN"), None, "to_decimal 拒绝 NaN")
    c.eq(to_decimal("Infinity"), None, "to_decimal 拒绝 Infinity")
    c.eq(to_decimal("19.90"), Decimal("19.90"), "to_decimal 正常解析")
    c.eq(str(to_decimal(0.1)), "0.1", "to_decimal 先转 str 再进 Decimal，避免浮点尾巴")


def _t_strings(c: _Check) -> None:
    c.eq(strip_html("<p>a</p><p>b</p>"), "a b", "strip_html 段落间补空格")
    c.eq(strip_html("a &amp; b"), "a & b", "strip_html 解实体")
    c.eq(strip_html("<br>x"), "x", "strip_html 处理 br")
    c.eq(clamp("abcdef", 4), "abc…", "clamp 截断并留省略号")
    c.ok(len(clamp("x" * 300, 150)) == 150, "clamp 后长度不超限")
    c.eq(clamp("ab", 4), "ab", "clamp 不动短串")
    c.eq(gid_num("gid://shopify/ProductVariant/42"), "42", "gid_num 取尾号")
    c.eq(gid_num("gid://shopify/ProductVariant/42?x=1"), "42", "gid_num 忽略 query")
    c.eq(gid_num("nonsense"), "", "gid_num 无数字返回空")
    c.eq(gid_num("456"), "456", "gid_num 接受裸数字 ID")
    c.eq(gid_num(""), "", "gid_num 容忍空串")

def _t_gtin_url(c: _Check) -> None:
    c.ok(gtin_check_digit_ok("0012345678905"), "GTIN-13 合法校验位")
    c.ok(not gtin_check_digit_ok("0012345678904"), "GTIN 错校验位被拒")
    c.eq(gtin_of("0012345678905"), "0012345678905", "gtin_of 通过")
    c.eq(gtin_of("00-1234 5678905"), "0012345678905", "gtin_of 去空格连字符")
    c.eq(gtin_of("12345"), "", "gtin_of 拒绝非法长度")
    c.eq(gtin_of("abcdefgh"), "", "gtin_of 拒绝非数字")
    c.eq(gtin_of(None), "", "gtin_of 容忍 None")
    c.eq(url_problem("https://a.com/x", "url"), None, "https 通过")
    c.ok(url_problem("ftp://a.com", "url") is not None, "非 http(s) 被拒")
    c.ok(url_problem("https://u:p@a.com/x", "url") is not None, "带账密的 URL 被拒")
    # 空串交给必填校验处理：选填 URL（如未开 checkout 时的隐私政策）本来就允许空
    c.eq(url_problem("", "url"), None, "空 URL 不在这里判，留给必填校验")
    c.eq(url_problem("http://a.com/x", "url"), None, "http 也合法")
    c.eq(add_variant_url("https://a.com/p/t", "9"), "https://a.com/p/t?variant=9", "变体参数用 ?")
    c.eq(add_variant_url("https://a.com/p/t?x=1", "9"), "https://a.com/p/t?x=1&variant=9",
         "已有 query 时用 &")
    c.eq(add_variant_url("https://a.com/p/t#frag", "9"), "https://a.com/p/t?variant=9#frag",
         "锚点保留在最后")


def _t_redaction(c: _Check) -> None:
    c.ok("shpat_" not in redact("token=shpat_0123456789abcdef0123456789abcdef"),
         "shpat_ 令牌被脱敏")
    c.ok("sk-" not in redact("key sk-proj-abcdefghijklmnopqrstuvwxyz1234"), "sk- 密钥被脱敏")
    c.ok("hunter2" not in redact("password: hunter2sekrit"), "password 赋值被脱敏")
    c.ok("hunter2" not in redact("https://u:hunter2@host/p"), "URL 内嵌账密被脱敏")
    c.ok("123456" not in redact("https://discord.com/api/webhooks/123456/abcdefg"),
         "Discord webhook 被脱敏")
    c.eq(redact("正常日志"), "正常日志", "普通文本不动")
    c.eq(mask_domain("verylongshop.myshopify.com"), "ve***op.myshopify.com", "店铺名脱敏")
    c.eq(mask_domain("abc.myshopify.com"), "a***.myshopify.com", "短店铺名脱敏")

def _t_availability(c: _Check) -> None:
    cfg = _cfg()
    c.eq(availability_of(_variant(), cfg)[0], "in_stock", "可售即 in_stock")
    c.eq(availability_of(_variant(availableForSale=False), cfg)[0], "out_of_stock",
         "availableForSale=false 即 out_of_stock")
    # 允许超卖 + 库存 <= 0 才是真的 backorder
    oversold = _variant(availableForSale=True, inventoryPolicy="CONTINUE", inventoryQuantity=0)
    c.eq(availability_of(oversold, cfg)[0], "backorder", "超卖中默认 backorder")
    stocked = _variant(availableForSale=True, inventoryPolicy="CONTINUE", inventoryQuantity=5)
    c.eq(availability_of(stocked, cfg)[0], "in_stock", "有库存不算 backorder")
    denied = _variant(availableForSale=True, inventoryPolicy="DENY", inventoryQuantity=0)
    c.eq(availability_of(denied, cfg)[0], "in_stock",
         "不允许超卖时以 availableForSale 为准（可能没开库存跟踪）")

    # pre_order 规范要求同时给 availability_date，给不出来就降级
    cfg_pre = _cfg(oversell_availability="pre_order")
    avail, date_str = availability_of(oversold, cfg_pre)
    c.eq(avail, "backorder", "pre_order 缺日期时降级为 backorder")
    c.eq(date_str, None, "降级后不带日期")
    cfg_pre_days = _cfg(oversell_availability="pre_order", preorder_lead_days=7)
    avail2, date2 = availability_of(oversold, cfg_pre_days)
    c.eq(avail2, "pre_order", "配了 lead_days 才允许 pre_order")
    c.ok(bool(date2 and _DATE_RE.match(date2)), "pre_order 日期是 YYYY-MM-DD")


def _t_mapping(c: _Check) -> None:
    cfg = _cfg()
    row = map_variant(_variant(), cfg, "USD")
    c.eq(row["item_id"], "123", "item_id 用变体数字 ID")
    c.eq(row["group_id"], "900", "group_id 用商品数字 ID")
    c.eq(row["price"], "29.99 USD", "compareAtPrice 更高时它才是 price")
    c.eq(row["sale_price"], "19.90 USD", "现价进 sale_price")
    c.eq(row["gtin"], "0012345678905", "barcode 合法则写 gtin")
    c.eq(row["url"], "https://example.com/products/tee?variant=123", "url 带 variant 参数")
    c.eq(row["title"], "Cotton Tee - Blue / M", "多变体时标题拼后缀")
    c.eq(row["color"], "Blue", "从 selectedOptions 取颜色")
    c.eq(row["size"], "M", "从 selectedOptions 取尺码")
    c.eq(row["listing_has_variations"], "true", "variantsCount>1 则有变体")
    c.eq(row["brand"], "ExampleBrand", "brand 取 vendor")
    c.eq(row["is_eligible_search"], "true", "布尔是小写字符串")
    c.eq(row["availability"], "in_stock", "availability 正常")
    c.ok("cotton tee" in row["description"].lower(), "description 从 HTML 剥出")
    c.eq(validate(row) + _validate_shape(row), [], "标准行校验无问题")

def _t_skips(c: _Check) -> None:
    cfg = _cfg()
    cases = [
        ("非 ACTIVE", _variant(product={"status": "DRAFT"})),
        ("无在线 URL", _variant(product={"onlineStoreUrl": ""})),
        ("价格为 0", _variant(price="0")),
        ("价格非法", _variant(price="abc")),
        ("没有图片", _variant(product={"featuredMedia": None})),
    ]
    for label, variant in cases:
        c.raises(SkipRow, lambda v=variant: map_variant(v, cfg, "USD"), f"应跳过：{label}")

    tagged = _variant(product={"tags": ["clearance"]})
    c.raises(SkipRow, lambda: map_variant(tagged, _cfg(exclude_tags=["Clearance"]), "USD"),
             "排除标签大小写不敏感")
    c.raises(SkipRow, lambda: map_variant(_variant(price="5"), _cfg(min_price="10"), "USD"),
             "低于 min_price 被跳过")
    no_brand = _variant(product={"vendor": ""})
    c.raises(SkipRow, lambda: map_variant(no_brand, _cfg(default_brand=""), "USD"),
             "brand 为空且无兜底则跳过")
    c.eq(map_variant(no_brand, _cfg(default_brand="Fallback"), "USD")["brand"], "Fallback",
         "default_brand 兜底生效")

    # compareAtPrice 不高于 price 时不该产生 sale_price
    no_sale = map_variant(_variant(compareAtPrice="10.00", price="19.90"), cfg, "USD")
    c.eq(no_sale["price"], "19.90 USD", "compareAtPrice 更低时忽略它")
    c.eq(no_sale.get("sale_price", ""), "", "没打折就不写 sale_price")
    single = map_variant(_variant(title="Default Title", product={"variantsCount": {"count": 1}}), cfg, "USD")
    c.eq(single["title"], "Cotton Tee", "单变体不拼标题后缀")
    c.eq(single["listing_has_variations"], "false", "单变体 listing_has_variations=false")


def _t_validate(c: _Check) -> None:
    base = map_variant(_variant(), _cfg(), "USD")
    c.ok(any("availability" in p for p in validate({**base, "availability": "sold_out"})),
         "非法 availability 被抓")
    c.ok(any("price" in p for p in validate({**base, "price": "19.9 USD"})),
         "price 少一位小数被抓")
    c.ok(any("price" in p for p in validate({**base, "price": "19.90"})),
         "price 缺币种被抓")
    c.ok(validate({**base, "sale_price": "29.99 USD", "price": "29.99 USD"}) == [],
         "sale_price 等于 price 是合法的（规范是 <=）")
    c.ok(any("sale_price" in p for p in validate({**base, "sale_price": "39.99 USD"})),
         "sale_price 高于 price 被抓")
    c.ok(any("sale_price" in p for p in validate({**base, "sale_price": "9.99 EUR"})),
         "sale_price 币种不一致被抓")
    c.ok(any("item_id" in p for p in validate({**base, "item_id": ""})), "必填字段为空被抓")
    c.ok(any("规范外字段" in p for p in _validate_shape({**base, "bogus_col": "x"})),
         "未知列被抓")
    c.ok(any("gtin" in p for p in _validate_shape({**base, "gtin": "0012345678904"})),
         "gtin 校验位错被抓")
    c.ok(any("title" in p for p in _validate_shape({**base, "title": "x" * 200})),
         "title 超长被抓")
    c.ok(any("age_group" in p for p in _validate_shape({**base, "age_group": "teen"})),
         "非法 age_group 被抓")
    pre = {**base, "availability": "pre_order"}
    c.ok(any("availability_date" in p for p in validate(pre)), "pre_order 缺日期被抓")

def _t_config(c: _Check) -> None:
    c.raises(ConfigError, lambda: _cfg(output_format="xml"), "非法 output_format")
    c.raises(ConfigError, lambda: _cfg(remote_filename="products.csv.gz"),
             "远端后缀与格式不一致")
    c.raises(ConfigError, lambda: _cfg(is_eligible_search=False, is_eligible_checkout=True),
             "checkout 依赖 search")
    c.raises(ConfigError, lambda: _cfg(is_eligible_checkout=True),
             "checkout 缺隐私政策与 TOS")
    c.raises(ConfigError, lambda: _cfg(seller_url="notaurl"), "seller_url 非法")
    c.raises(ConfigError, lambda: _cfg(target_countries=["USA"]), "国家码必须两位")
    c.raises(ConfigError, lambda: _cfg(store_country="usa"), "store_country 必须两位")
    c.raises(ConfigError, lambda: _cfg(page_size=500), "page_size 上限 250")
    c.raises(ConfigError, lambda: _cfg(shard_count=0), "shard_count 下限 1")
    c.raises(ConfigError, lambda: _cfg(max_reject_ratio=1.5), "拒收率阈值范围")
    c.raises(ConfigError, lambda: _cfg(min_price="-1"), "min_price 不能为负")
    c.raises(ConfigError, lambda: _cfg(condition="brand-new"), "condition 枚举")
    c.raises(ConfigError, lambda: _cfg(metafields={"nope_field": "ns.key"}),
             "metafield 目标必须是规范字段")
    c.raises(ConfigError, lambda: _cfg(metafields={"color": "bad key!"}),
             "metafield 引用格式")
    cfg = _cfg(target_countries="us", metafields={"color": "custom.shade"})
    c.eq(cfg["target_countries"], ["US"], "单字符串国家码归一成列表")
    c.eq(cfg["_metafields"]["color"], ("custom", "shade"), "metafield 引用解析")


def _t_state(c: _Check) -> None:
    row = map_variant(_variant(), _cfg(), "USD")
    fp = fingerprint(row)
    c.ok(fp.count("\x1f") == 4, "指纹 5 段")
    c.eq(_unpack(fp)[4], "900", "指纹末位是 group_id")
    changed = fingerprint({**row, "availability": "out_of_stock"})
    c.ok(fp != changed, "库存变化改变指纹")

    old = {"a": "in_stock\x1f10.00 USD\x1f\x1fA\x1fp1",
           "b": "in_stock\x1f10.00 USD\x1f\x1fB\x1fp1"}
    new = {"a": "out_of_stock\x1f10.00 USD\x1f\x1fA\x1fp1",
           "c": "in_stock\x1f5.00 USD\x1f\x1fC\x1fp2"}
    d = diff_state(old, new)
    c.eq(d["added"], ["c"], "diff 认出新增")
    c.eq(d["removed"], ["b"], "diff 认出下架")
    c.eq(d["stock_changed"], ["a"], "diff 认出库存变化")
    c.eq(d["price_changed"], [], "价格没变就不报价格变化")
    c.ok(not d["first_run"], "有基线时 first_run=false")
    d2 = diff_state({}, new)
    c.ok(d2["first_run"], "空基线即首次运行")

    price_new = {"a": "in_stock\x1f12.00 USD\x1f\x1fA\x1fp1"}
    d3 = diff_state({"a": old["a"]}, price_new)
    c.eq(d3["price_changed"], ["a"], "diff 认出价格变化")
    c.eq(d3["stock_changed"], [], "价格变化不算库存变化")

def _t_delta(c: _Check) -> None:
    state = {
        "v1": "out_of_stock\x1f10.00 USD\x1f\x1fShirt A\x1fp1",
        "v2": "in_stock\x1f10.00 USD\x1f\x1fShirt B\x1fp1",
        "v3": "backorder\x1f10.00 USD\x1f\x1fPants\x1fp2",
        "v4": "unknown\x1f10.00 USD\x1f\x1fHat\x1fp3",
        "v5": "in_stock\x1f10.00 USD\x1f\x1fNoParent\x1f",
    }
    products = build_delta_products(["v1", "v2", "v3", "v4", "v5", "v1"], state, False)
    by_id = {p["id"]: p for p in products}
    c.eq(sorted(by_id), ["p1", "p2"], "按父商品聚合，unknown 与无父 ID 被剔除")
    c.eq(len(by_id["p1"]["variants"]), 2, "同父商品的变体合并，重复 id 去掉")
    v1 = next(v for v in by_id["p1"]["variants"] if v["id"] == "v1")
    c.eq(v1["availability"], {"available": False, "status": "out_of_stock"},
         "availability 是对象且 available 与 status 一致")
    v3 = by_id["p2"]["variants"][0]
    c.eq(v3["availability"]["available"], True, "backorder 视为可售")
    c.eq(v3["availability"]["status"], "backorder", "status 透传原枚举")
    c.ok("title" not in v1, "默认不推标题")
    with_title = build_delta_products(["v1"], state, True)
    c.eq(with_title[0]["variants"][0]["title"], "Shirt A", "开了开关才推标题")
    c.eq(build_delta_products([], state, False), [], "空输入返回空")
    c.eq(_delta_error_code('{"error":{"code":"product_feed_delta_api_disabled"}}'),
         "product_feed_delta_api_disabled", "解析错误码")
    c.eq(_delta_error_code("not json"), "", "错误码解析容错")


def _t_writer(c: _Check) -> None:
    base = Path(os.environ.get("TMPDIR", "/tmp")) / f"feedtest_{os.getpid()}"
    shutil.rmtree(base, ignore_errors=True)
    rows = [map_variant(_variant(), _cfg(), "USD"),
            map_variant(_variant(id="gid://shopify/ProductVariant/124"), _cfg(), "USD")]
    try:
        out = base / "products.jsonl.gz"
        w = FeedWriter(str(out), "jsonl.gz", 1)
        for r in rows:
            w.write(r)
        paths = w.close()
        c.eq(len(paths), 1, "单分片只产一个文件")
        c.ok(out.exists(), "输出文件已落位")
        c.ok(not out.with_name(out.name + ".partial").exists(), ".partial 已被 rename 掉")
        with gzip.open(out, "rt", encoding="utf-8") as fh:
            lines = [json.loads(x) for x in fh if x.strip()]
        c.eq(len(lines), 2, "jsonl 行数正确")
        c.eq(lines[0]["item_id"], "123", "jsonl 内容正确")
        c.ok("bogus" not in lines[0], "只写规范字段")

        # mtime=0 + 固定压缩级别 ⇒ 同内容必须同字节，否则「内容未变」判断失效
        out2 = base / "again.jsonl.gz"
        w2 = FeedWriter(str(out2), "jsonl.gz", 1)
        for r in rows:
            w2.write(r)
        w2.close()
        c.eq(sha256_of(out), sha256_of(out2), "同内容产出同字节（gzip 可复现）")
    finally:
        shutil.rmtree(base, ignore_errors=True)

def _t_writer_more(c: _Check) -> None:
    base = Path(os.environ.get("TMPDIR", "/tmp")) / f"feedtest2_{os.getpid()}"
    shutil.rmtree(base, ignore_errors=True)
    cfg = _cfg()
    try:
        # CSV 表头必须是完整的 44 列，顺序固定
        out = base / "p.csv.gz"
        w = FeedWriter(str(out), "csv.gz", 1)
        w.write(map_variant(_variant(), cfg, "USD"))
        w.close()
        with gzip.open(out, "rt", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader)
            data = next(reader)
        c.eq(tuple(header), FEED_COLUMNS, "CSV 表头与列定义一致")
        c.eq(len(data), len(FEED_COLUMNS), "CSV 数据列数对齐")
        c.eq(data[header.index("additional_image_urls")].count(","), 0,
             "单张附图不产生多余逗号")

        # 分片：同一 item_id 必须稳定落在同一分片（跨进程也一样）
        shards = base / "s.jsonl.gz"
        w3 = FeedWriter(str(shards), "jsonl.gz", 4)
        c.eq(len(w3.shards), 4, "按 shard_count 开够分片")
        names = [s.path.name for s in w3.shards]
        c.eq(names[0], "s-0000.jsonl.gz", "分片命名插在后缀前")
        idx = hashlib.sha1(b"123").digest()[0] % 4
        w3.write(map_variant(_variant(), cfg, "USD"))
        c.eq(w3.shards[idx].count, 1, "路由到 sha1 决定的分片")
        paths = w3.close()
        c.eq(len(paths), 4, "空分片也要落地（分片集合必须稳定）")
        c.ok(all(p.exists() for p in paths), "所有分片文件都存在")

        # abort 必须清掉 .partial，绝不让半截文件顶掉上一版
        w4 = FeedWriter(str(base / "a.jsonl.gz"), "jsonl.gz", 1)
        w4.write(map_variant(_variant(), cfg, "USD"))
        partial = w4.shards[0].tmp
        c.ok(partial.exists(), "写入中存在 .partial")
        w4.abort()
        c.ok(not partial.exists(), "abort 清掉 .partial")
        c.ok(not (base / "a.jsonl.gz").exists(), "abort 不产出正式文件")

        c.eq(shard_name("p.jsonl.gz", 0, 1), "p.jsonl.gz", "单分片不改名")
        c.eq(shard_name("p.parquet", 2, 3), "p-0002.parquet", "parquet 分片命名")
        c.eq(flatten_value(["a", "b"]), "a,b", "列表拼逗号")
        c.eq(flatten_value(True), "true", "布尔转小写字符串")
        c.eq(flatten_value(None), "", "None 转空串")
    finally:
        shutil.rmtree(base, ignore_errors=True)

def _t_collect(c: _Check) -> None:
    cfg = _cfg()
    variants = [
        _variant(),
        _variant(id="gid://shopify/ProductVariant/124"),
        _variant(id="gid://shopify/ProductVariant/125", product={"status": "DRAFT"}),
        _variant(id="gid://shopify/ProductVariant/126", price="0"),
        _variant(),  # 重复 item_id
    ]
    res = collect(variants, cfg, "USD", None)
    c.eq(res["written"], 2, "只写通过的行")
    c.eq(res["skipped"], 2, "跳过 DRAFT 与 0 价")
    c.eq(res["dupes"], 1, "重复 item_id 计入 dupes")
    c.eq(res["products"], 1, "两个变体同属一个商品")
    c.eq(len(res["state"]), 2, "状态里只有写出去的行")
    c.ok(res["reasons"], "拒收原因有记录")
    c.ok(all(len(s) == 3 for s in res["samples"]), "样本是三元组")

    limited = collect([_variant(), _variant(id="gid://shopify/ProductVariant/124")],
                      cfg, "USD", None, limit=1)
    c.eq(limited["written"], 1, "--limit 生效")

    # 数据闸门
    c.raises(DataGuardError, lambda: _guard_snapshot(
        {"written": 0, "skipped": 3, "invalid": 0, "dupes": 0, "reasons": {}}, cfg),
        "空快照被拦住")
    c.raises(DataGuardError, lambda: _guard_snapshot(
        {"written": 5, "skipped": 0, "invalid": 0, "dupes": 0, "reasons": {}},
        _cfg(min_rows=10)), "低于 min_rows 被拦住")
    c.raises(DataGuardError, lambda: _guard_snapshot(
        {"written": 10, "skipped": 90, "invalid": 0, "dupes": 0, "reasons": {"x": 90}}, cfg),
        "拒收率过高被拦住")
    _guard_snapshot({"written": 100, "skipped": 5, "invalid": 0, "dupes": 0, "reasons": {}}, cfg)
    c.passed += 1  # 正常快照不抛


def _t_query(c: _Check) -> None:
    cfg = _cfg(metafields={"color": "custom.shade"})
    q = build_variants_query(set(OPTIONAL_GROUPS), cfg["_metafields"], True)
    c.ok("productVariants(" in q, "查的是 productVariants")
    c.ok("product_status:active" in q, "带上 active 过滤")
    c.ok("inventoryQuantity" in q, "含库存字段组")
    c.ok('mf0: metafield(namespace: "custom", key: "shade")' in q, "metafield 别名注入")
    c.ok("pageInfo" in q and "endCursor" in q, "含游标分页")
    q2 = build_variants_query(set(), cfg["_metafields"], False)
    c.ok("inventoryQuantity" not in q2, "降级后不再请求库存")
    c.ok("product_status:active" not in q2, "关掉状态过滤后 query 参数消失")
    c.ok("id" in q2 and "price" in q2, "核心字段永远保留")
    # bulk 不允许嵌套连接带分页参数，media 组必须摘掉
    bq = Shopify("x.myshopify.com", "t", DEFAULT_SHOPIFY_API_VERSION).build_bulk_query(cfg)
    c.ok("media(first:" not in bq, "bulk 查询不带 media 分页参数")
    c.ok("pageInfo" not in bq, "bulk 查询不需要 pageInfo")

def _t_degrade(c: _Check) -> None:
    def errs(*messages: str) -> str:
        # 真实调用方传的是 json.dumps(payload["errors"])，这里保持同一形状
        return json.dumps([{"message": m} for m in messages], ensure_ascii=False)

    client = Shopify("x.myshopify.com", "t", DEFAULT_SHOPIFY_API_VERSION)
    # 只授 read_products 时，库存字段会让整条 query 失败
    dropped = client._drop_group_for(
        errs("Field 'inventoryQuantity' doesn't exist on type 'ProductVariant'"))
    c.eq(dropped, "inventory", "按报错字段名定位到 inventory 组")
    c.ok("inventory" not in client.groups, "该组已从后续 query 摘掉")
    c.ok("variant_image" in client.groups, "只摘中招的那一组")
    again = client._drop_group_for(errs("Field 'inventoryQuantity' doesn't exist"))
    c.ok(again != "inventory", "同一组不会被摘两次")

    c2 = Shopify("x.myshopify.com", "t", DEFAULT_SHOPIFY_API_VERSION)
    c2._drop_group_for(errs("Invalid argument in query: product_status"))
    c.ok(not c2.use_status_filter, "认不出字段名时先退回关掉状态过滤")
    c3b = Shopify("x.myshopify.com", "t", DEFAULT_SHOPIFY_API_VERSION)
    c.eq(c3b._drop_group_for(errs("Field 'seo' doesn't exist")), "seo", "定位到 seo 组")

    # 限流：剩余点数不足时应该算出等待时间
    c3 = Shopify("x.myshopify.com", "t", DEFAULT_SHOPIFY_API_VERSION)
    c3._absorb_cost({"cost": {"throttleStatus": {"currentlyAvailable": 20, "restoreRate": 50}}})
    c.eq(c3.available_points, 20.0, "读到剩余点数")
    c.eq(c3.restore_rate, 50.0, "读到恢复速率")
    start = time.time()
    c3.pace(next_cost=30.0)
    c.ok(time.time() - start >= 0.15, "点数不够时确实睡了一下")
    c4 = Shopify("x.myshopify.com", "t", DEFAULT_SHOPIFY_API_VERSION)
    c4._absorb_cost({"cost": {"throttleStatus": {"currentlyAvailable": 900, "restoreRate": 50}}})
    t2 = time.time()
    c4.pace(next_cost=30.0)
    c.ok(time.time() - t2 < 0.1, "点数充足时不睡")


def _t_misc(c: _Check) -> None:
    c.eq(_remote_join("/in", "p.jsonl.gz"), "/in/p.jsonl.gz", "远端路径拼接")
    c.eq(_remote_join(".", "p.jsonl.gz"), "p.jsonl.gz", "当前目录不加前缀")
    c.eq(_remote_join("", "p.jsonl.gz"), "p.jsonl.gz", "空目录不加前缀")
    c.eq(_remote_join("/in/", "p.jsonl.gz"), "/in/p.jsonl.gz", "去掉重复斜杠")
    c.eq(_status_text({"ok": True}), "success", "状态文案 success")
    c.eq(_status_text({"skipped": "x"}), "skipped（x）", "状态文案 skipped")
    c.ok(_status_text({"errors": ["boom"]}).startswith("failed"), "状态文案 failed")
    c.eq(len(REQUIRED_FIELDS), 15, "必填字段 15 个")
    c.eq(len(set(FEED_COLUMNS)), len(FEED_COLUMNS), "列定义无重复")
    c.ok(all(f in FEED_COLUMNS for f in REQUIRED_FIELDS), "必填字段都在列定义里")
    c.ok("inventory_quantity" not in FEED_COLUMNS, "规范里没有 inventory_quantity 这个字段")
    c.ok("is_ads_enabled" not in FEED_COLUMNS, "is_ads_enabled 不是合法字段名")
    c.ok("additional_image_urls" in FEED_COLUMNS, "附图字段是复数形式")
    c.ok("is_ads_eligible" in FEED_COLUMNS, "广告资格字段名正确")
    state_file = Path(os.environ.get("TMPDIR", "/tmp")) / f"st_{os.getpid()}.json"
    try:
        save_state(str(state_file), {"a": "x"}, "deadbeef")
        loaded = load_state(str(state_file))
        c.eq(loaded["items"], {"a": "x"}, "状态可写可读")
        c.eq(loaded["content_sha256"], "deadbeef", "指纹落盘")
        state_file.write_text("{ broken", "utf-8")
        c.eq(load_state(str(state_file))["items"], {}, "坏状态文件按首次运行处理")
        c.eq(load_state(str(state_file / "nope"))["items"], {}, "状态文件不存在也不报错")
    finally:
        state_file.unlink(missing_ok=True)

SELF_TESTS: tuple[Callable[[_Check], None], ...] = (
    _t_money, _t_strings, _t_gtin_url, _t_redaction, _t_availability, _t_mapping,
    _t_skips, _t_validate, _t_config, _t_state, _t_delta, _t_writer,
    _t_writer_more, _t_collect, _t_query, _t_degrade, _t_misc,
)


def self_test() -> int:
    c = _Check()
    for fn in SELF_TESTS:
        try:
            fn(c)
        except Exception as exc:  # noqa: BLE001
            c.failed.append(f"{fn.__name__} 抛异常: {type(exc).__name__}: {redact(exc)}")
    if c.failed:
        log(f"自检失败 {len(c.failed)} 项 / 通过 {c.passed} 项：")
        for item in c.failed:
            log(f"  ✗ {item}")
        return EXIT_RUNTIME
    log(f"自检全绿：{c.passed} 项断言通过")
    return EXIT_OK

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="feed_sync.py",
        description="把 Shopify 目录转成 OpenAI 商品 feed 并投递（全量快照 + 可选 Delta）",
    )
    p.add_argument("--config", default="config.json", help="业务配置路径（凭证走环境变量）")
    p.add_argument("--dry-run", action="store_true", help="只在本地产出文件，不上传、不写状态")
    p.add_argument("--limit", type=int, default=0, help="只处理前 N 条，用于试跑（不写状态）")
    p.add_argument("--bulk", action="store_true", help="用 Bulk Operations 抓取（大目录更稳）")
    p.add_argument("--delta", action="store_true", help="额外用 Delta API 推库存变化")
    p.add_argument("--self-test", action="store_true", help="跑内置断言，不联网")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.self_test:
        return self_test()
    if args.limit < 0:
        log("--limit 不能为负")
        return EXIT_CONFIG
    try:
        return run(args)
    except ConfigError as exc:
        log(f"配置错误：{redact(exc)}")
        return EXIT_CONFIG
    except DataGuardError as exc:
        log(f"数据闸门拦下本次发布：{redact(exc)}")
        notify_discord("GPT Ads Feed 已拦停（数据异常）", [("原因", str(exc)[:1000])], ok=False)
        return EXIT_DATA_GUARD
    except KeyboardInterrupt:
        log("已中断")
        return EXIT_RUNTIME
    except Exception as exc:  # noqa: BLE001
        log(f"运行失败：{type(exc).__name__}: {redact(exc)}")
        notify_discord("GPT Ads Feed 更新失败",
                       [("异常", f"{type(exc).__name__}: {redact(exc)}"[:1000])], ok=False)
        return EXIT_RUNTIME


if __name__ == "__main__":
    sys.exit(main())
