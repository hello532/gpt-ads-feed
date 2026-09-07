#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shopify -> OpenAI product feed sync (full snapshot plus intraday delta).

Usage:
    python3 feed_sync.py --config config.json                  # full snapshot + SFTP overwrite + notification
    python3 feed_sync.py --config config.json --dry-run        # write files only, no upload
    python3 feed_sync.py --config config.json --limit 100      # small sample for the first go-live
    python3 feed_sync.py --config config.json --bulk           # Bulk Operations for large catalogs
    python3 feed_sync.py --config config.json --delta          # push only genuinely changed availability
    python3 feed_sync.py --self-test                           # offline self-test, no credentials needed

Delivery constraints (all from the official spec, not guesswork):
    - The feed has full-snapshot semantics: overwrite the same path and filename in place, at least once a day.
    - parquet(zstd) is preferred; jsonl.gz / csv.gz / tsv.gz are equally supported.
    - At most 500,000 rows per shard, target < ~500MB; the shard set must stay stable across runs.
    - Deleting a product means it is absent from the next snapshot, or is_eligible_search=false.
    - The Ads-channel feed connection and SFTP credentials can only be provisioned by hand in Ads Manager;
      the public API offers only PATCH /feeds/{id}/products, and only title and availability can change.

Hard rules:
    - Credentials are read from the environment only and never written to disk; logs and notifications are always redacted.
    - Money is always Decimal + ROUND_HALF_UP, never float (a cent off is a cent off).
    - An empty snapshot, a reject ratio over the threshold, or a byte-count mismatch after upload all refuse to publish.
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

# ── Official spec constants (developers.openai.com/commerce/specs/file-upload/products) ──
REQUIRED_FIELDS: tuple[str, ...] = (
    "is_eligible_search", "is_eligible_checkout", "item_id", "title",
    "description", "url", "brand", "image_url", "price", "availability",
    "seller_name", "seller_url", "return_policy", "target_countries",
    "store_country",
)
AVAILABILITY_ENUM = ("in_stock", "out_of_stock", "pre_order", "backorder", "unknown")
AGE_GROUP_ENUM = ("newborn", "infant", "toddler", "kids", "adult")
CONDITION_ENUM = ("new", "refurbished", "used")

# Length limits from the spec; anything over is rejected row by row
MAXLEN: dict[str, int] = {
    "item_id": 100, "title": 150, "description": 5000, "brand": 70,
    "mpn": 70, "material": 100, "color": 40, "size": 20,
    "item_group_title": 150, "seller_name": 70, "pricing_trend": 80,
}

# Fixed CSV/TSV header. Streaming to disk cannot scan everything first to union the columns, so the column set is fixed up front.
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

# ── Redaction ────────────────────────────────────────────────────────────
# Exception messages, URLs and GraphQL errors can all carry a token; every exit (logs/Discord) goes through this.
SECRET_ENV_KEYS = (
    "SHOPIFY_ADMIN_TOKEN", "OPENAI_ADS_API_KEY", "OPENAI_FEED_SFTP_PASSWORD",
    "DISCORD_WEBHOOK_URL",
)
_SECRET_PATTERNS = (
    re.compile(r"shp(at|ca|pa|ss)_[A-Za-z0-9]{8,}"),
    re.compile(r"sk-[A-Za-z0-9_\-]{16,}"),
    re.compile(r"(?i)(bearer|token|api[_-]?key|password)\s*[:=]\s*\S+"),
    re.compile(r"(?i)https://discord(app)?\.com/api/webhooks/\S+"),
    re.compile(r"://[^/\s:@]+:[^/\s@]+@"),  # user:pass embedded in a URL
)


def redact(text: Any) -> str:
    """Replace anything that might be a credential with ***. This is the only exit; do not print around it."""
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
    """A config or credential problem -- exit code 2, retrying will not help."""


class DataGuardError(RuntimeError):
    """The data-quality gate blocked this run -- exit code 3; better to publish nothing than a bad snapshot."""


def env(name: str, required: bool = True) -> str:
    val = os.environ.get(name, "").strip()
    if required and not val:
        raise ConfigError(f"environment variable {name} is missing")
    return val

# ── Basic helpers ────────────────────────────────────────────────────────
def to_decimal(raw: Any) -> Decimal | None:
    """Shopify money is a decimal string, so hold it in a Decimal. float loses cents and is banned."""
    if raw is None or raw == "":
        return None
    try:
        val = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError):
        return None
    return val if val.is_finite() else None


def money(amount: Decimal, currency: str) -> str:
    """The spec requires amount + space + ISO 4217. ROUND_HALF_UP matches the storefront display."""
    return f"{amount.quantize(CENT, rounding=ROUND_HALF_UP)} {currency.upper()}"


def bool_str(val: bool) -> str:
    """The spec explicitly says "Lower-case string", not a JSON boolean."""
    return "true" if val else "false"


_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_BLOCK_RE = re.compile(r"(?i)</(p|div|li|h[1-6]|tr)>|<br\s*/?>")


def strip_html(html: str) -> str:
    """The spec requires description to be plain text; block tags become spaces so words do not run together."""
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
    """gid://shopify/ProductVariant/123?ns=x -> 123 (the query must be stripped)."""
    if not gid:
        return ""
    m = _GID_RE.search(gid)
    if m:
        return m.group(1)
    # The fallback only accepts pure digits (a legacy REST id). Returning a non-number would make
    # item_id garbage, and map_variant relies on the empty string to mean "could not parse".
    tail = gid.rsplit("/", 1)[-1]
    return tail if tail.isdigit() else ""

def gtin_check_digit_ok(digits: str) -> bool:
    """GS1 mod-10: weights 3,1,3,1... from the right (the last digit is the check digit)."""
    body, check = digits[:-1], int(digits[-1])
    total = 0
    for i, ch in enumerate(reversed(body)):
        total += int(ch) * (3 if i % 2 == 0 else 1)
    return (10 - total % 10) % 10 == check


def gtin_of(barcode: str | None) -> str:
    """barcode is typed in by the shop owner and is a dirty-data hotspot: a wrong length or check digit counts as absent."""
    if not barcode:
        return ""
    digits = re.sub(r"[\s\-]", "", str(barcode))
    if not digits.isdigit() or len(digits) not in GTIN_VALID_LENGTHS:
        return ""
    return digits if gtin_check_digit_ok(digits) else ""


_ISO2_RE = re.compile(r"^[A-Z]{2}$")
_CRED_URL_RE = re.compile(r"^[a-z][a-z0-9+.\-]*://[^/\s:@]+:[^/\s@]+@")


def url_problem(value: str, field: str) -> str | None:
    """Spec: http and https are both legal, but embedded credentials reject the whole row."""
    if not value:
        return None
    low = value.strip().lower()
    if not (low.startswith("https://") or low.startswith("http://")):
        return f"{field} is not an http(s) URL"
    if _CRED_URL_RE.match(low):
        return f"{field} embeds a username/password, which rejects the whole row"
    return None


# Shopify WeightUnit enum -> the unit abbreviations the spec requires
WEIGHT_UNIT_MAP = {
    "GRAMS": "g", "KILOGRAMS": "kg", "OUNCES": "oz", "POUNDS": "lb",
}


def add_variant_url(base: str, variant_id: str) -> str:
    """onlineStoreUrl may already carry a query or fragment, so appending ?variant= needs care."""
    if not variant_id:
        return base
    head, sep, frag = base.partition("#")
    joiner = "&" if "?" in head else "?"
    return f"{head}{joiner}variant={variant_id}{sep}{frag}"

# ── Config ───────────────────────────────────────────────────────────────
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
    # Only required when is_eligible_checkout=true, but defaulted to empty so key lookups elsewhere do not blow up
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
    # By default deliver even when the content is byte-identical: the spec recommends a full
    # snapshot at least once a day, and skipping the upload means betting that the ingest side
    # ignores file timestamps. Not worth it.
    "skip_upload_when_unchanged": False,
}

def load_config(path: str) -> dict[str, Any]:
    """Business config comes from json; credentials never go in this file."""
    p = Path(path).expanduser()
    if not p.exists():
        raise ConfigError(f"config file does not exist: {p}")
    try:
        cfg: dict[str, Any] = json.loads(p.read_text("utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"config is not valid JSON: {exc}") from exc

    for field in ("shop_domain", "seller_name", "seller_url", "return_policy"):
        if not str(cfg.get(field, "")).strip():
            raise ConfigError(f"config is missing the required field {field}")
    for key, default in CONFIG_DEFAULTS.items():
        cfg.setdefault(key, default)

    fmt = cfg["output_format"]
    if fmt not in FORMAT_SUFFIX:
        raise ConfigError(f"unsupported output_format: {fmt} (choose from {list(FORMAT_SUFFIX)})")
    # The remote filename suffix must match the format, or the OpenAI side picks a parser by suffix and fails outright
    if not str(cfg["remote_filename"]).endswith(FORMAT_SUFFIX[fmt]):
        raise ConfigError(
            f"remote_filename must end in {FORMAT_SUFFIX[fmt]} (currently {cfg['remote_filename']})"
        )

    if not cfg["is_eligible_search"] and cfg["is_eligible_checkout"]:
        raise ConfigError("the spec requires is_eligible_search=true whenever is_eligible_checkout=true")
    if cfg["is_eligible_checkout"]:
        for field in ("seller_privacy_policy", "seller_tos"):
            if not str(cfg.get(field, "")).strip():
                raise ConfigError(f"{field} is required once checkout is on")

    for field in ("seller_url", "return_policy", "seller_privacy_policy", "seller_tos"):
        problem = url_problem(str(cfg.get(field, "")), field)
        if problem:
            raise ConfigError(problem)

    if cfg["oversell_availability"] not in ("in_stock", "backorder", "pre_order"):
        raise ConfigError("oversell_availability must be in_stock / backorder / pre_order")
    if cfg["condition"] and cfg["condition"] not in CONDITION_ENUM:
        raise ConfigError(f"condition must be one of {CONDITION_ENUM}")
    if cfg["age_group"] and cfg["age_group"] not in AGE_GROUP_ENUM:
        raise ConfigError(f"age_group must be one of {AGE_GROUP_ENUM}")
    return _validate_numeric_config(cfg)

def _validate_numeric_config(cfg: dict[str, Any]) -> dict[str, Any]:
    min_price = to_decimal(cfg["min_price"])
    if min_price is None or min_price < 0:
        raise ConfigError("min_price must be a number >= 0")
    cfg["_min_price"] = min_price

    size = int(cfg["page_size"])
    if not 1 <= size <= 250:
        raise ConfigError("page_size must be within 1..250 (the Shopify GraphQL limit)")
    cfg["page_size"] = size

    shards = int(cfg["shard_count"])
    if not 1 <= shards <= 64:
        raise ConfigError("shard_count must be within 1..64")
    cfg["shard_count"] = shards

    ratio = float(cfg["max_reject_ratio"])
    if not 0.0 <= ratio <= 1.0:
        raise ConfigError("max_reject_ratio must be within 0..1")
    cfg["max_reject_ratio"] = ratio

    countries = cfg["target_countries"]
    if isinstance(countries, str):
        countries = [countries]
    countries = [str(c).strip().upper() for c in countries if str(c).strip()]
    if not countries or not all(_ISO2_RE.match(c) for c in countries):
        raise ConfigError("target_countries must be ISO 3166-1 alpha-2, for example [\"US\"]")
    cfg["target_countries"] = countries

    store_country = str(cfg["store_country"]).strip().upper()
    if not _ISO2_RE.match(store_country):
        raise ConfigError("store_country must be ISO 3166-1 alpha-2, for example \"US\"")
    cfg["store_country"] = store_country

    mf = cfg["metafields"]
    if not isinstance(mf, dict):
        raise ConfigError("metafields must be a mapping of {feed field: \"namespace.key\"}")
    parsed: dict[str, tuple[str, str]] = {}
    for feed_field, ref in mf.items():
        if feed_field not in FEED_COLUMNS:
            raise ConfigError(f"{feed_field} in metafields is not a spec field")
        ns, _, key = str(ref).partition(".")
        if not _MF_KEY_RE.match(ns) or not _MF_KEY_RE.match(key):
            raise ConfigError(f"a metafield reference must look like namespace.key (currently {ref})")
        parsed[feed_field] = (ns, key)
    cfg["_metafields"] = parsed
    cfg["_exclude_tags"] = {str(t).strip().lower() for t in cfg["exclude_tags"] if str(t).strip()}
    return cfg

# ── GraphQL ──────────────────────────────────────────────────────────────
# Fields available with read_products alone. This group may not be degraded; without it there is no feed.
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

# Degradable field groups: with insufficient scopes, or on an API version without the field,
# the whole query errors out. Drop one group and retry instead of killing the run --
# this was v1's most damaging bug.
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
    # The next two need read_inventory: with read_products alone they return ACCESS_DENIED
    "inventory": ("variant", "inventoryQuantity inventoryPolicy"),
    "inventory_item": (
        "variant",
        "inventoryItem { requiresShipping measurement { weight { unit value } } }",
    ),
}

def metafield_selection(metafields: dict[str, tuple[str, str]]) -> str:
    """Pull several metafields at once via aliases (review count, star rating, the DTC essentials)."""
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
    # product_status:active makes Shopify filter out drafts/archived, saving a lot of wasted quota
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
    """Admin GraphQL client. The REST products endpoint is deprecated, so everything goes through GraphQL."""

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
            # A Request cannot be reused across retries: its data has already been consumed
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
                    raise RuntimeError(f"Shopify network error: {redact(exc)}") from exc
                last = exc
            time.sleep(2 ** attempt)
        else:  # pragma: no cover - the loop always breaks or raises
            raise RuntimeError(f"Shopify request failed: {redact(last)}")

        self._absorb_cost(payload.get("extensions"))
        return payload

    def _absorb_cost(self, extensions: Any) -> None:
        """Read throttleStatus to pace proactively; far better than waiting for a 429 and backing off."""
        try:
            status = extensions["cost"]["throttleStatus"]
            self.available_points = float(status["currentlyAvailable"])
            self.restore_rate = max(float(status["restoreRate"]), 1.0)
        except (KeyError, TypeError, ValueError):
            return

    def pace(self, next_cost: float = 120.0) -> None:
        """When the leaky bucket cannot cover the next page, sleep off the recovery time instead of hitting the limit."""
        if self.available_points is None or self.available_points >= next_cost:
            return
        need = (next_cost - self.available_points) / self.restore_rate
        wait = min(max(need, 0.2), 10.0)
        log(f"{self.available_points:.0f} quota points left, pacing for {wait:.1f}s")
        time.sleep(wait)

    def query_with_degrade(self, build: Callable[[], str], variables: dict[str, Any]) -> dict[str, Any]:
        """When a field is missing or not permitted, drop that field group and retry instead of failing outright."""
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
                log(f"Shopify rejected field group {dropped} (scopes or API version), dropped it and retrying")
                continue
            raise RuntimeError(f"Shopify GraphQL error: {redact(msgs)}")
        raise RuntimeError("Shopify GraphQL kept failing, giving up on degrade retries")

    def _drop_group_for(self, msgs: str) -> str | None:
        """Use the field name in the error to work out which group to drop."""
        named = {m.lower() for m in _FIELD_ERR_RE.findall(msgs)}
        for name in list(self.groups):
            _, selection = OPTIONAL_GROUPS[name]
            head = selection.strip().split(" ", 1)[0].split("(", 1)[0].lower()
            if head in named or head in msgs.lower():
                self.groups.discard(name)
                return name
        # The error named no field: suspect the query filter first, then drop groups in order
        if self.use_status_filter and ("query" in msgs.lower() or "argument" in msgs.lower()):
            self.use_status_filter = False
            return "product_status filter"
        if self.groups:
            name = sorted(self.groups)[0]
            self.groups.discard(name)
            return name
        return None

    def shop_info(self) -> dict[str, Any]:
        payload = self.call(SHOP_QUERY)
        if payload.get("errors"):
            raise RuntimeError(f"failed to read shop info: {redact(json.dumps(payload['errors']))}")
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
                log(f"paged fetch complete, {page} pages")
                return
            cursor = block["pageInfo"]["endCursor"]

    # ── Bulk Operations: the only real answer for large catalogs ────────
    # Export all of productVariants to JSONL in one go, without spending pagination quota.
    # Note that bulk forbids pagination arguments on connections, so media(first:12) must go.
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
            raise RuntimeError(f"failed to start bulk: {redact(json.dumps(payload['errors']))}")
        result = payload["data"]["bulkOperationRunQuery"]
        if result.get("userErrors"):
            raise RuntimeError(f"bulk userErrors: {redact(json.dumps(result['userErrors']))}")
        op_id = result["bulkOperation"]["id"]
        log(f"bulk started {op_id}, polling")

        url = None
        for _ in range(720):  # wait at most 1 hour
            time.sleep(poll_seconds)
            data = self.call(self.BULK_POLL)["data"]["currentBulkOperation"]
            status = (data or {}).get("status")
            if status == "COMPLETED":
                url = data.get("url")
                log(f"bulk complete: {data.get('objectCount')} objects, {data.get('fileSize')} bytes")
                break
            if status in ("FAILED", "CANCELED", "EXPIRED"):
                raise RuntimeError(f"bulk ended in {status}, errorCode={data.get('errorCode')}")
        else:
            raise RuntimeError("bulk did not finish within 1 hour, giving up")

        if not url:
            log("bulk finished with no result file (the catalog is empty)")
            return
        req = urllib.request.Request(url, headers={"Accept": "application/jsonl"})
        with urllib.request.urlopen(req, timeout=300) as resp:
            for raw in io.TextIOWrapper(resp, encoding="utf-8"):
                line = raw.strip()
                if not line:
                    continue
                node = json.loads(line)
                # Only ProductVariant rows; nested connections come on their own lines and are not needed
                if "ProductVariant/" in str(node.get("id", "")):
                    yield node

# ── Mapping ──────────────────────────────────────────────────────────────
class SkipRow(Exception):
    """This row does not belong in the feed; the reason is attached for the rejects report."""


def availability_of(variant: dict[str, Any], cfg: dict[str, Any]) -> tuple[str, str | None]:
    """Returns (availability, availability_date).

    availableForSale is authoritative: without inventory tracking, inventoryQuantity may be 0
    while the variant is still sellable. Only inventoryPolicy reveals an active oversell,
    which is the real backorder.
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
        # Spec: pre_order requires availability_date, so do not use that status without one
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
    """The main image prefers the variant image and falls back to the product's; extras are deduped and exclude the main image."""
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
    """Only a multi-variant product gets the variant name in the title; on a single variant it just pollutes it."""
    base = (product.get("title") or "").strip()
    vtitle = (variant.get("title") or "").strip()
    count = ((product.get("variantsCount") or {}).get("count"))
    is_default = vtitle.lower() in ("", "default title")
    multi = count > 1 if isinstance(count, int) else not is_default
    if multi and not is_default:
        return f"{base} - {vtitle}"
    return base


def _description_of(product: dict[str, Any], cfg: dict[str, Any], fallback: str) -> str:
    """The SEO description is usually already clean plain text, more reliable than stripping descriptionHtml."""
    if cfg["prefer_seo_description"]:
        seo = ((product.get("seo") or {}).get("description") or "").strip()
        if seo:
            return seo
    body = strip_html(product.get("descriptionHtml") or "")
    return body or fallback

def map_variant(variant: dict[str, Any], cfg: dict[str, Any], currency: str) -> dict[str, Any]:
    """One Shopify variant -> one feed record. Anything non-compliant raises SkipRow."""
    product = variant.get("product") or {}

    if (product.get("status") or "").upper() != "ACTIVE":
        raise SkipRow(f"product is not ACTIVE ({product.get('status')})")
    online_url = (product.get("onlineStoreUrl") or "").strip()
    if not online_url:
        raise SkipRow("not published to the online store, no usable URL")
    problem = url_problem(online_url, "url")
    if problem:
        raise SkipRow(problem)

    tags = {str(t).strip().lower() for t in (product.get("tags") or [])}
    hit = tags & cfg["_exclude_tags"]
    if hit:
        raise SkipRow(f"matched an excluded tag {sorted(hit)}")

    price_amt = to_decimal(variant.get("price"))
    if price_amt is None or price_amt <= 0:
        raise SkipRow("price is missing or not positive (the spec requires a positive number)")
    if price_amt < cfg["_min_price"]:
        raise SkipRow(f"below min_price {cfg['_min_price']}")

    # compareAtPrice is the list price, price is the current one; the spec requires sale_price <= price
    compare_amt = to_decimal(variant.get("compareAtPrice"))
    list_amt, sale_amt = price_amt, None
    if compare_amt is not None and compare_amt > price_amt:
        list_amt, sale_amt = compare_amt, price_amt

    image_url, extra_images = _images_of(variant, product, cfg)
    if not image_url:
        raise SkipRow("no image at all")
    problem = url_problem(image_url, "image_url")
    if problem:
        raise SkipRow(problem)

    brand = (product.get("vendor") or "").strip() or str(cfg["default_brand"]).strip()
    if not brand:
        raise SkipRow("brand is empty and default_brand is not configured (the spec requires brand)")

    title = _title_of(variant, product)
    if not title:
        raise SkipRow("title is empty")

    variant_id = gid_num(variant.get("id") or "")
    product_id = gid_num(product.get("id") or "")
    if not variant_id:
        raise SkipRow("could not parse the variant id")
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

# The Chinese keys are Shopify option names as they appear in Chinese-language stores.
# They are input data, never printed, so they stay as they are.
_OPTION_ALIASES = {
    "color": "color", "colour": "color", "颜色": "color",
    "size": "size", "尺码": "size", "尺寸": "size",
    "material": "material", "材质": "material",
}


def _decorate_row(
    row: dict[str, Any], variant: dict[str, Any],
    product: dict[str, Any], cfg: dict[str, Any],
) -> dict[str, Any]:
    """Variant dimensions + weight + optional constants + metafield overrides."""
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

    # variantsCount is authoritative: a product with a single variant is not a
    # "multi-variant listing" even if it has Color/Size options. Only fall back to the
    # options when that field is unavailable (its group was degraded away).
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

# ── Validation ───────────────────────────────────────────────────────────
_PRICE_RE = re.compile(r"^\d+\.\d{2} [A-Z]{3}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _split_money(value: str) -> tuple[Decimal | None, str]:
    parts = str(value).split(" ")
    if len(parts) != 2:
        return None, ""
    return to_decimal(parts[0]), parts[1]


def validate(row: dict[str, Any]) -> list[str]:
    """Catch rows the OpenAI side would reject before they leave, saving a round of Upload History digging."""
    problems: list[str] = []
    for field in REQUIRED_FIELDS:
        value = row.get(field)
        if value in (None, "", []):
            problems.append(f"required field {field} is missing")

    if row.get("availability") not in AVAILABILITY_ENUM:
        problems.append(f"availability is illegal: {row.get('availability')}")
    if row.get("availability") == "pre_order" and not row.get("availability_date"):
        problems.append("availability=pre_order requires availability_date")
    for field in ("availability_date", "sale_price_start_date", "sale_price_end_date"):
        if row.get(field) and not _DATE_RE.match(str(row[field])):
            problems.append(f"{field} is not an ISO 8601 date")

    price = str(row.get("price", ""))
    if not _PRICE_RE.match(price):
        problems.append(f"price must be \"amount ISO4217\" with two decimals: {price!r}")
    if row.get("sale_price"):
        sale_amt, sale_cur = _split_money(row["sale_price"])
        list_amt, list_cur = _split_money(price)
        if sale_amt is None or not _PRICE_RE.match(str(row["sale_price"])):
            problems.append("sale_price format is illegal")
        elif list_amt is not None and sale_amt > list_amt:
            problems.append("sale_price must be <= price")
        elif sale_cur != list_cur:
            problems.append("sale_price currency must match price")
        elif sale_amt <= 0:
            problems.append("sale_price must be positive")

    problems.extend(_validate_shape(row))
    return problems

def _validate_shape(row: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    if row.get("is_eligible_checkout") == "true":
        if row.get("is_eligible_search") != "true":
            problems.append("is_eligible_checkout=true requires is_eligible_search=true")
        for field in ("seller_privacy_policy", "seller_tos"):
            if not row.get(field):
                problems.append(f"checkout is on but {field} is missing")

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
            problems.append(f"{field} is too long, {len(value)}>{limit}")

    gtin = str(row.get("gtin", ""))
    if gtin and (len(gtin) not in GTIN_VALID_LENGTHS or not gtin.isdigit()
                 or not gtin_check_digit_ok(gtin)):
        problems.append(f"gtin check digit or length is invalid: {gtin}")
    if row.get("age_group") and row["age_group"] not in AGE_GROUP_ENUM:
        problems.append(f"age_group is illegal: {row['age_group']}")
    if row.get("condition") and row["condition"] not in CONDITION_ENUM:
        problems.append(f"condition is illegal: {row['condition']}")

    unknown = sorted(set(row) - set(FEED_COLUMNS))
    if unknown:
        problems.append(f"fields outside the spec (dropped, or the whole row rejected): {unknown}")
    return problems

# ── Output files ─────────────────────────────────────────────────────────
def flatten_value(value: Any) -> str:
    """Tabular formats (csv/tsv/parquet) need scalars; a list joins with commas, a dict becomes JSON."""
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
    """One shard file. Streamed, never buffering all rows in memory."""

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
        # mtime=0 plus a fixed compression level: identical content means identical bytes, which is
        # what lets the fingerprint answer "nothing changed today"
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
        except ImportError as exc:  # the spec prefers parquet, but that is an optional dependency
            raise ConfigError(
                "output_format=parquet needs pyarrow: pip install pyarrow; "
                "or switch to jsonl.gz / csv.gz / tsv.gz"
            ) from exc
        self._pa, self._pq = pa, pq
        # Every column is a string: in the feed, price is already a string like "79.99 USD"
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
        """Close in text -> gzip -> raw order. v1 never closed raw and leaked handles."""
        if self.fmt == "parquet":
            self._flush_parquet()
            self._parquet.close()
        else:
            try:
                self._text.flush()
                self._text.close()   # this closes gzip too
            finally:
                self._raw.close()    # the underlying file must be closed explicitly
        os.replace(self.tmp, self.path)  # same-directory rename, atomic locally too
        return self.path

def shard_name(base: str, index: int, total: int) -> str:
    """The shard set must stay stable across runs, so numbering is fixed instead of split on demand."""
    if total <= 1:
        return base
    for suffix in sorted(FORMAT_SUFFIX.values(), key=len, reverse=True):
        if base.endswith(suffix):
            return f"{base[: -len(suffix)]}-{index:04d}{suffix}"
    return f"{base}-{index:04d}"


class FeedWriter:
    """Route rows to a fixed shard by item_id hash, then land them all atomically."""

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
                log(f"warning: {shard.path.name} holds {shard.count} rows, above the suggested "
                    f"500,000 per shard; raise shard_count")
        return paths

    def abort(self) -> None:
        """Error path: clear .partial, so a half-written file never replaces the previous one."""
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

# ── State and diff ───────────────────────────────────────────────────────
STATE_VERSION = 2


def load_state(path: str) -> dict[str, Any]:
    p = Path(path).expanduser()
    empty = {"version": STATE_VERSION, "items": {}, "content_sha256": "", "generated_at": ""}
    if not p.exists():
        return empty
    try:
        data = json.loads(p.read_text("utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        log(f"the state file could not be read, treating this as a first run: {redact(exc)}")
        return empty
    if data.get("version") != STATE_VERSION or not isinstance(data.get("items"), dict):
        log("state file version mismatch, treating this as a first run")
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
    """Deciding whether a row changed only needs the fields that affect a buyer's decision.

    group_id is stored last: the Delta API requires products[].id to be the parent product ID,
    and a removed row exists only in the old state, so without a refetch its group_id is gone.
    The separator is \\x1f, which never appears in a product title.
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
    """Work out what actually changed today. Delta pushes only this, and the report uses it too."""
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

# ── Upload ───────────────────────────────────────────────────────────────
def sftp_settings() -> dict[str, Any]:
    """Credentials come from the environment only. No host means SFTP is unconfigured; the caller decides whether that is an error."""
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
    """Unknown host keys are refused outright. AutoAddPolicy turns off MITM protection, so it is not used."""
    import paramiko

    loaded = False
    if s["known_hosts"]:
        path = Path(s["known_hosts"]).expanduser()
        if not path.exists():
            raise ConfigError(f"the file OPENAI_FEED_SFTP_KNOWN_HOSTS points at does not exist: {path}")
        client.load_host_keys(str(path))
        loaded = True
    else:
        default = Path("~/.ssh/known_hosts").expanduser()
        if default.exists():
            client.load_host_keys(str(default))
            loaded = True
    if not loaded:
        raise ConfigError(
            "No known_hosts found. Pin the host fingerprint before uploading:\n"
            f"  ssh-keyscan -p {s['port']} {s['host']} >> ~/.ssh/known_hosts\n"
            "or point OPENAI_FEED_SFTP_KNOWN_HOSTS at a file"
        )
    client.set_missing_host_key_policy(paramiko.RejectPolicy())

def _upload_one(sftp: Any, local: Path, remote_dir: str, remote_name: str) -> None:
    """Upload to .tmp, check the byte count, then rename over the target. The ingest side never sees a half file."""
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
        raise RuntimeError(f"{remote_name} byte count mismatch on upload: local {size}, remote {uploaded}")
    try:
        sftp.posix_rename(staging, target)   # atomic overwrite within the same directory
    except (AttributeError, IOError):
        try:
            sftp.remove(target)              # older servers have no posix_rename extension
        except IOError:
            pass
        sftp.rename(staging, target)
    final = sftp.stat(target).st_size
    if final != size:
        raise RuntimeError(f"{remote_name} byte count mismatch after landing: expected {size}, remote {final}")
    log(f"uploaded {remote_name} ({size} bytes)")


def upload_sftp(paths: list[Path], remote_names: list[str]) -> dict[str, Any]:
    s = sftp_settings()
    if not s["host"] or not s["user"]:
        raise ConfigError("OPENAI_FEED_SFTP_HOST / OPENAI_FEED_SFTP_USER are missing")
    try:
        import paramiko  # noqa: F401
    except ImportError:
        log("paramiko is not installed, falling back to the system sftp command")
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
                    raise ConfigError(f"the private key OPENAI_FEED_SFTP_KEY points at does not exist: {key_path}")
                kwargs["key_filename"] = str(key_path)
            elif s["password"]:
                kwargs["password"] = s["password"]
            else:
                raise ConfigError("SFTP has neither KEY nor PASSWORD")
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
                    f"The host fingerprint does not match known_hosts (the server may have rotated its key, "
                    f"or this may be a man in the middle). Verify first, then update: ssh-keyscan -p {s['port']} {s['host']}"
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
            log(f"SFTP attempt {attempt} failed, retrying in {wait}s: {redact(last)}")
            time.sleep(wait)
    raise RuntimeError(f"SFTP upload failed after 3 attempts: {redact(last)}")

def _upload_sftp_cli(paths: list[Path], names: list[str], s: dict[str, Any]) -> dict[str, Any]:
    """Fallback when paramiko is absent. Key auth only; passwords are left to paramiko."""
    exe = shutil.which("sftp")
    if not exe:
        raise ConfigError("neither paramiko nor an sftp command is available: pip install paramiko")
    if not s["key"]:
        raise ConfigError("the sftp command mode requires OPENAI_FEED_SFTP_KEY (install paramiko for password auth)")
    key_path = Path(s["key"]).expanduser()
    if not key_path.exists():
        raise ConfigError(f"the private key OPENAI_FEED_SFTP_KEY points at does not exist: {key_path}")

    lines: list[str] = []
    for local, name in zip(paths, names):
        target = _remote_join(s["dir"], name)
        # Always shlex.quote the remote path, so spaces or quotes in a filename are not parsed as extra sftp arguments
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
        raise RuntimeError(f"the sftp command failed (exit {proc.returncode}): {redact(proc.stderr.strip())}")
    log(f"uploaded {len(names)} files through the sftp command")
    return {"ok": True, "transport": "sftp-cli", "files": names}

# ── HTTP ─────────────────────────────────────────────────────────────────
def post_json(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str] | None = None,
    method: str = "POST",
    attempts: int = 4,
) -> tuple[int, str]:
    """JSON request with backoff. Returns (status code, body); 4xx is not retried."""
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
                raise RuntimeError(f"request to {url.split('?')[0]} failed: {redact(exc)}") from exc
            last = str(exc)
        wait = min(2 ** attempt, 16)
        log(f"request attempt {attempt} failed ({redact(last)}), retrying in {wait}s")
        time.sleep(wait)
    return 0, ""

# ── Delta Feeds ──────────────────────────────────────────────────────────
DELTA_CHUNK = 500
# The docs only spell out in_stock / out_of_stock, but status is the "explicit availability"
# and shares the enum with the flat-file availability, so the current value is passed through.
_DELTA_IN_STOCK = {"in_stock", "pre_order", "backorder"}


def build_delta_products(
    item_ids: list[str],
    new_state: dict[str, str],
    include_title: bool,
) -> list[dict[str, Any]]:
    """Group variants by parent product. A variant may not appear twice, so dedupe by item_id first."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item_id in dict.fromkeys(item_ids):
        fp = new_state.get(item_id)
        if not fp:
            continue
        availability, _price, _sale, title, group_id = _unpack(fp)
        if not group_id or availability not in AVAILABILITY_ENUM or availability == "unknown":
            continue  # unknown is pointless to push, and without a parent ID there is nothing to target
        variant: dict[str, Any] = {
            "id": item_id,
            # available and status are both supplied: the docs say status wins when both are
            # present, so the semantics stay the same even if one side is ignored.
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
    """Only stock/title changes are pushed. Price changes cannot go through delta, only the full snapshot."""
    feed_id = env("OPENAI_ADS_FEED_ID", required=False)
    api_key = env("OPENAI_ADS_API_KEY", required=False)
    if not feed_id or not api_key:
        return {"skipped": "OPENAI_ADS_FEED_ID / OPENAI_ADS_API_KEY are missing"}
    if not products:
        return {"skipped": "no stock or title changes to push"}

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
                errors.append("HTTP 200 but the response is not valid JSON")
            # accepted=false means the feed side did not take it, so this is not a success
            if accepted:
                accepted_chunks += 1
                sent_variants += n_variants
            else:
                errors.append(f"accepted=false: {clamp(redact(text), 200)}")
            continue

        code = _delta_error_code(text)
        if status == 403 and code in ("product_feed_api_disabled", "product_feed_delta_api_disabled"):
            # Explicitly documented: do not keep retrying when it is not enabled
            return {"skipped": f"the account does not have the Delta Feeds API enabled ({code}), ask your OpenAI account team"}
        errors.append(f"HTTP {status} {code or ''}: {clamp(redact(text), 200)}".strip())
        break

    return {
        "ok": not errors,
        "chunks": accepted_chunks,
        "variants": sent_variants,
        "errors": errors,
    }

# ── Notifications ────────────────────────────────────────────────────────
def mask_domain(domain: str) -> str:
    """Mask the shop name in the report, keeping only 2 characters at each end."""
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
        log(f"Discord notification failed, HTTP {status}: {clamp(redact(text), 200)}")


def write_rejects(path: str, rejects: list[tuple[str, str, str]]) -> Path | None:
    """Rejected rows go into their own CSV, so the data can be fixed back in the store."""
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

# ── Collection ───────────────────────────────────────────────────────────
REJECT_SAMPLE_MAX = 200


def collect(
    variants: Iterable[dict[str, Any]],
    cfg: dict[str, Any],
    currency: str,
    writer: FeedWriter | None,
    limit: int = 0,
) -> dict[str, Any]:
    """Write while fetching. Only fingerprints and counters stay in memory, never all rows."""
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
            note(item_id, row["title"], "duplicate item_id, the later row was dropped")
            continue

        if writer is not None:
            writer.write(row)
        state[item_id] = fingerprint(row)
        seen_products.add(row.get("group_id") or item_id)
        written += 1
        if limit and written >= limit:
            log(f"reached --limit {limit}, stopping the fetch")
            break

    return {
        "state": state, "written": written, "skipped": skipped, "invalid": invalid,
        "dupes": dupes, "products": len(seen_products),
        "reasons": dict(sorted(reasons.items(), key=lambda kv: -kv[1])), "samples": samples,
    }

# ── Main flow ────────────────────────────────────────────────────────────
def _guard_snapshot(res: dict[str, Any], cfg: dict[str, Any]) -> None:
    """With full-snapshot semantics one bad write delists the whole store. This is the last gate."""
    written = res["written"]
    if written == 0:
        raise DataGuardError(
            "The snapshot is empty. In a full delivery an empty file means \"this store has no products\""
            " and would delist the entire catalog, so publishing was refused."
            f" Skipped {res['skipped']} rows, {res['invalid']} failed validation"
        )
    if written < int(cfg["min_rows"]):
        raise DataGuardError(
            f"Only {written} rows were produced, below min_rows={cfg['min_rows']}, publishing refused"
        )
    total = written + res["skipped"] + res["invalid"] + res["dupes"]
    ratio = (total - written) / total if total else 0.0
    if ratio > cfg["max_reject_ratio"]:
        top = list(res["reasons"].items())[:3]
        raise DataGuardError(
            f"Reject ratio {ratio:.1%} exceeds max_reject_ratio={cfg['max_reject_ratio']:.0%}, "
            f"publishing refused. Main reasons: {top}"
        )


def _fetch_variants(client: Shopify, cfg: dict[str, Any], use_bulk: bool) -> Iterable[dict[str, Any]]:
    if use_bulk:
        log("fetching with Bulk Operations")
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
        log(f"using the currency from the config: {currency}")
    else:
        shop = client.shop_info()
        currency = shop["currency"]
        log(f"shop {mask_domain(shop['domain'])}, currency {currency}")
    if not _ISO2_RE.match(currency[:2]) or len(currency) != 3:
        raise ConfigError(f"currency is not a valid ISO 4217 code: {currency}")

    prev = load_state(str(cfg["state_path"]))
    writer = FeedWriter(str(cfg["output_path"]), str(cfg["output_format"]), int(cfg["shard_count"]))
    try:
        res = collect(_fetch_variants(client, cfg, args.bulk), cfg, currency, writer, args.limit)
        _guard_snapshot(res, cfg)
        paths = writer.close()
    except BaseException:
        writer.abort()   # a half-written file must never replace the previous one
        raise

    sizes = {p.name: p.stat().st_size for p in paths}
    content_sha = hashlib.sha256(
        "".join(sha256_of(p) for p in sorted(paths)).encode()
    ).hexdigest()
    unchanged = bool(prev.get("content_sha256")) and prev["content_sha256"] == content_sha
    log(f"produced {res['written']} rows / {res['products']} products, "
        f"{sum(sizes.values())} bytes, sha256={content_sha[:12]}")

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
    delta: dict[str, Any] = {"skipped": "--delta not enabled"}
    if args.dry_run:
        log("dry-run: local files written, no upload and no state")
    elif unchanged and cfg["skip_upload_when_unchanged"]:
        upload = {"skipped": "content is byte-identical to last time"}
        log("content unchanged, skipping the upload as configured")
    else:
        if unchanged:
            log('content is identical to last time, still delivering to honour "at least once a day"')
        upload = upload_sftp(paths, remote_names)

    # Delta can only change stock/title on variants that already exist: a first run has no
    # baseline, and additions and removals cannot be pushed either
    if args.delta and not args.dry_run:
        if diff["first_run"]:
            delta = {"skipped": "first run, no baseline to compare against"}
        else:
            targets = sorted(set(diff["stock_changed"]) | (
                set(diff["title_changed"]) if cfg["delta_include_title"] else set()
            ))
            products = build_delta_products(targets, res["state"], bool(cfg["delta_include_title"]))
            delta = push_delta(products, cfg)
            if diff["price_changed"]:
                log(f"{len(diff['price_changed'])} rows changed price; the Delta API does not support price, "
                    f"so those take effect through the full snapshot above")

    ok = ("skipped" in upload or upload.get("ok")) and not delta.get("errors")
    if not args.dry_run and not args.limit and (upload.get("ok") or upload.get("skipped")):
        save_state(str(cfg["state_path"]), res["state"], content_sha)
    elif args.limit:
        log("--limit mode does not write state, so a subset is never mistaken for a full baseline")

    _report(cfg, res, diff, sizes, content_sha, upload, delta, rejects_file, ok, started)
    return EXIT_OK if ok else EXIT_RUNTIME

def _status_text(result: dict[str, Any]) -> str:
    if "skipped" in result:
        return f"skipped ({result['skipped']})"
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
        ("Shop", mask_domain(str(cfg["shop_domain"]))),
        ("Shopify products", str(res["products"])),
        ("Feed records", f"{res['written']}/{total_in}"),
        ("Skipped/errors", f"{res['skipped']}/{res['invalid'] + res['dupes']}"),
        ("File size", f"{sum(sizes.values())} bytes" + (f" x {len(sizes)} shards" if len(sizes) > 1 else "")),
        ("Content fingerprint", content_sha[:12]),
        ("OpenAI status", _status_text(upload)),
        ("Delta status", _status_text(delta) if "skipped" not in delta
            else f"skipped ({delta['skipped']})"),
        ("Duration", f"{time.time() - started:.1f}s"),
    ]
    if not diff["first_run"]:
        fields.append((
            "Changes",
            f"added {len(diff['added'])} / removed {len(diff['removed'])} / "
            f"price {len(diff['price_changed'])} / stock {len(diff['stock_changed'])}",
        ))
    if delta.get("variants"):
        fields.append(("Delta push", f"{delta['variants']} variants / {delta['chunks']} batches"))

    note = ""
    if res["reasons"]:
        top = list(res["reasons"].items())[:5]
        note = "Top reject reasons:\n" + "\n".join(f"- {r} x {n}" for r, n in top)
        if rejects_file:
            note += f"\nDetails: {rejects_file}"

    for name, value in fields:
        log(f"  {name}: {value}")
    notify_discord(
        "GPT Ads Feed daily update succeeded" if ok else "GPT Ads Feed update failed",
        fields, ok, note,
    )

# ── Self-test ────────────────────────────────────────────────────────────
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
    """Self-test config. Goes through the real load_config, covering validation too."""
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
            self.failed.append(f"{label}: raised {type(other).__name__} instead of {exc.__name__}")
            return
        self.failed.append(f"{label}: expected {exc.__name__}, nothing was raised")


def _t_money(c: _Check) -> None:
    c.eq(money(Decimal("19.9"), "usd"), "19.90 USD", "money pads to two decimals and upper-cases the currency")
    c.eq(money(Decimal("0.005"), "USD"), "0.01 USD", "money uses ROUND_HALF_UP, not banker's rounding")
    c.eq(money(Decimal("2.675"), "USD"), "2.68 USD", "money rounds 2.675 up")
    c.eq(to_decimal("abc"), None, "to_decimal rejects non-numbers")
    c.eq(to_decimal(None), None, "to_decimal rejects None")
    c.eq(to_decimal("NaN"), None, "to_decimal rejects NaN")
    c.eq(to_decimal("Infinity"), None, "to_decimal rejects Infinity")
    c.eq(to_decimal("19.90"), Decimal("19.90"), "to_decimal parses a normal amount")
    c.eq(str(to_decimal(0.1)), "0.1", "to_decimal goes through str first, avoiding float tails")


def _t_strings(c: _Check) -> None:
    c.eq(strip_html("<p>a</p><p>b</p>"), "a b", "strip_html puts a space between paragraphs")
    c.eq(strip_html("a &amp; b"), "a & b", "strip_html unescapes entities")
    c.eq(strip_html("<br>x"), "x", "strip_html handles br")
    c.eq(clamp("abcdef", 4), "abc…", "clamp truncates and keeps an ellipsis")
    c.ok(len(clamp("x" * 300, 150)) == 150, "clamp output stays within the limit")
    c.eq(clamp("ab", 4), "ab", "clamp leaves short strings alone")
    c.eq(gid_num("gid://shopify/ProductVariant/42"), "42", "gid_num takes the trailing number")
    c.eq(gid_num("gid://shopify/ProductVariant/42?x=1"), "42", "gid_num ignores the query string")
    c.eq(gid_num("nonsense"), "", "gid_num returns empty when there is no number")
    c.eq(gid_num("456"), "456", "gid_num accepts a bare numeric ID")
    c.eq(gid_num(""), "", "gid_num tolerates an empty string")

def _t_gtin_url(c: _Check) -> None:
    c.ok(gtin_check_digit_ok("0012345678905"), "GTIN-13 with a valid check digit")
    c.ok(not gtin_check_digit_ok("0012345678904"), "GTIN with a wrong check digit is rejected")
    c.eq(gtin_of("0012345678905"), "0012345678905", "gtin_of accepts a valid barcode")
    c.eq(gtin_of("00-1234 5678905"), "0012345678905", "gtin_of strips spaces and hyphens")
    c.eq(gtin_of("12345"), "", "gtin_of rejects an invalid length")
    c.eq(gtin_of("abcdefgh"), "", "gtin_of rejects non-digits")
    c.eq(gtin_of(None), "", "gtin_of tolerates None")
    c.eq(url_problem("https://a.com/x", "url"), None, "https passes")
    c.ok(url_problem("ftp://a.com", "url") is not None, "a non-http(s) URL is rejected")
    c.ok(url_problem("https://u:p@a.com/x", "url") is not None, "a URL with credentials is rejected")
    # An empty string is left to the required-field check: optional URLs (the privacy
    # policy while checkout is off, say) are allowed to be empty.
    c.eq(url_problem("", "url"), None, "an empty URL is not judged here, the required-field check owns it")
    c.eq(url_problem("http://a.com/x", "url"), None, "http is legal too")
    c.eq(add_variant_url("https://a.com/p/t", "9"), "https://a.com/p/t?variant=9", "the variant param uses ?")
    c.eq(add_variant_url("https://a.com/p/t?x=1", "9"), "https://a.com/p/t?x=1&variant=9",
         "an existing query means &")
    c.eq(add_variant_url("https://a.com/p/t#frag", "9"), "https://a.com/p/t?variant=9#frag",
         "the fragment stays at the end")


def _t_redaction(c: _Check) -> None:
    c.ok("shpat_" not in redact("token=shpat_0123456789abcdef0123456789abcdef"),
         "an shpat_ token is redacted")
    c.ok("sk-" not in redact("key sk-proj-abcdefghijklmnopqrstuvwxyz1234"), "an sk- key is redacted")
    c.ok("hunter2" not in redact("password: hunter2sekrit"), "a password assignment is redacted")
    c.ok("hunter2" not in redact("https://u:hunter2@host/p"), "credentials inside a URL are redacted")
    c.ok("123456" not in redact("https://discord.com/api/webhooks/123456/abcdefg"),
         "a Discord webhook is redacted")
    c.eq(redact("ordinary log line"), "ordinary log line", "plain text is left alone")
    c.eq(mask_domain("verylongshop.myshopify.com"), "ve***op.myshopify.com", "the shop domain is masked")
    c.eq(mask_domain("abc.myshopify.com"), "a***.myshopify.com", "a short shop domain is masked")

def _t_availability(c: _Check) -> None:
    cfg = _cfg()
    c.eq(availability_of(_variant(), cfg)[0], "in_stock", "sellable means in_stock")
    c.eq(availability_of(_variant(availableForSale=False), cfg)[0], "out_of_stock",
         "availableForSale=false means out_of_stock")
    # Overselling allowed plus inventory <= 0 is the only real backorder
    oversold = _variant(availableForSale=True, inventoryPolicy="CONTINUE", inventoryQuantity=0)
    c.eq(availability_of(oversold, cfg)[0], "backorder", "an active oversell defaults to backorder")
    stocked = _variant(availableForSale=True, inventoryPolicy="CONTINUE", inventoryQuantity=5)
    c.eq(availability_of(stocked, cfg)[0], "in_stock", "stock on hand is not a backorder")
    denied = _variant(availableForSale=True, inventoryPolicy="DENY", inventoryQuantity=0)
    c.eq(availability_of(denied, cfg)[0], "in_stock",
         "without overselling, availableForSale wins (inventory tracking may be off)")

    # The spec requires availability_date with pre_order, so downgrade when we cannot supply one
    cfg_pre = _cfg(oversell_availability="pre_order")
    avail, date_str = availability_of(oversold, cfg_pre)
    c.eq(avail, "backorder", "pre_order without a date downgrades to backorder")
    c.eq(date_str, None, "no date after the downgrade")
    cfg_pre_days = _cfg(oversell_availability="pre_order", preorder_lead_days=7)
    avail2, date2 = availability_of(oversold, cfg_pre_days)
    c.eq(avail2, "pre_order", "pre_order is only allowed once lead_days is set")
    c.ok(bool(date2 and _DATE_RE.match(date2)), "the pre_order date is YYYY-MM-DD")


def _t_mapping(c: _Check) -> None:
    cfg = _cfg()
    row = map_variant(_variant(), cfg, "USD")
    c.eq(row["item_id"], "123", "item_id is the numeric variant ID")
    c.eq(row["group_id"], "900", "group_id is the numeric product ID")
    c.eq(row["price"], "29.99 USD", "the higher compareAtPrice becomes price")
    c.eq(row["sale_price"], "19.90 USD", "the current price becomes sale_price")
    c.eq(row["gtin"], "0012345678905", "a valid barcode is written as gtin")
    c.eq(row["url"], "https://example.com/products/tee?variant=123", "url carries the variant param")
    c.eq(row["title"], "Cotton Tee - Blue / M", "a multi-variant title gets the variant suffix")
    c.eq(row["color"], "Blue", "color comes from selectedOptions")
    c.eq(row["size"], "M", "size comes from selectedOptions")
    c.eq(row["listing_has_variations"], "true", "variantsCount>1 means the listing has variations")
    c.eq(row["brand"], "ExampleBrand", "brand comes from vendor")
    c.eq(row["is_eligible_search"], "true", "booleans are lower-case strings")
    c.eq(row["availability"], "in_stock", "availability is normal")
    c.ok("cotton tee" in row["description"].lower(), "description is stripped out of the HTML")
    c.eq(validate(row) + _validate_shape(row), [], "a standard row validates cleanly")

def _t_skips(c: _Check) -> None:
    cfg = _cfg()
    cases = [
        ("not ACTIVE", _variant(product={"status": "DRAFT"})),
        ("no online URL", _variant(product={"onlineStoreUrl": ""})),
        ("price is 0", _variant(price="0")),
        ("price is not a number", _variant(price="abc")),
        ("no image", _variant(product={"featuredMedia": None})),
    ]
    for label, variant in cases:
        c.raises(SkipRow, lambda v=variant: map_variant(v, cfg, "USD"), f"should skip: {label}")

    tagged = _variant(product={"tags": ["clearance"]})
    c.raises(SkipRow, lambda: map_variant(tagged, _cfg(exclude_tags=["Clearance"]), "USD"),
             "excluded tags are case-insensitive")
    c.raises(SkipRow, lambda: map_variant(_variant(price="5"), _cfg(min_price="10"), "USD"),
             "below min_price is skipped")
    no_brand = _variant(product={"vendor": ""})
    c.raises(SkipRow, lambda: map_variant(no_brand, _cfg(default_brand=""), "USD"),
             "an empty brand with no fallback is skipped")
    c.eq(map_variant(no_brand, _cfg(default_brand="Fallback"), "USD")["brand"], "Fallback",
         "default_brand fills in")

    # compareAtPrice at or below price must not produce a sale_price
    no_sale = map_variant(_variant(compareAtPrice="10.00", price="19.90"), cfg, "USD")
    c.eq(no_sale["price"], "19.90 USD", "a lower compareAtPrice is ignored")
    c.eq(no_sale.get("sale_price", ""), "", "no discount means no sale_price")
    single = map_variant(_variant(title="Default Title", product={"variantsCount": {"count": 1}}), cfg, "USD")
    c.eq(single["title"], "Cotton Tee", "a single variant gets no title suffix")
    c.eq(single["listing_has_variations"], "false", "a single variant means listing_has_variations=false")


def _t_validate(c: _Check) -> None:
    base = map_variant(_variant(), _cfg(), "USD")
    c.ok(any("availability" in p for p in validate({**base, "availability": "sold_out"})),
         "an illegal availability is caught")
    c.ok(any("price" in p for p in validate({**base, "price": "19.9 USD"})),
         "a price with one decimal is caught")
    c.ok(any("price" in p for p in validate({**base, "price": "19.90"})),
         "a price without a currency is caught")
    c.ok(validate({**base, "sale_price": "29.99 USD", "price": "29.99 USD"}) == [],
         "sale_price equal to price is legal (the spec says <=)")
    c.ok(any("sale_price" in p for p in validate({**base, "sale_price": "39.99 USD"})),
         "a sale_price above price is caught")
    c.ok(any("sale_price" in p for p in validate({**base, "sale_price": "9.99 EUR"})),
         "a sale_price in another currency is caught")
    c.ok(any("item_id" in p for p in validate({**base, "item_id": ""})), "an empty required field is caught")
    c.ok(any("outside the spec" in p for p in _validate_shape({**base, "bogus_col": "x"})),
         "an unknown column is caught")
    c.ok(any("gtin" in p for p in _validate_shape({**base, "gtin": "0012345678904"})),
         "a wrong gtin check digit is caught")
    c.ok(any("title" in p for p in _validate_shape({**base, "title": "x" * 200})),
         "an over-long title is caught")
    c.ok(any("age_group" in p for p in _validate_shape({**base, "age_group": "teen"})),
         "an illegal age_group is caught")
    pre = {**base, "availability": "pre_order"}
    c.ok(any("availability_date" in p for p in validate(pre)), "pre_order without a date is caught")

def _t_config(c: _Check) -> None:
    c.raises(ConfigError, lambda: _cfg(output_format="xml"), "an illegal output_format")
    c.raises(ConfigError, lambda: _cfg(remote_filename="products.csv.gz"),
             "a remote suffix that does not match the format")
    c.raises(ConfigError, lambda: _cfg(is_eligible_search=False, is_eligible_checkout=True),
             "checkout depends on search")
    c.raises(ConfigError, lambda: _cfg(is_eligible_checkout=True),
             "checkout without a privacy policy and TOS")
    c.raises(ConfigError, lambda: _cfg(seller_url="notaurl"), "an illegal seller_url")
    c.raises(ConfigError, lambda: _cfg(target_countries=["USA"]), "country codes must be two letters")
    c.raises(ConfigError, lambda: _cfg(store_country="usa"), "store_country must be two letters")
    c.raises(ConfigError, lambda: _cfg(page_size=500), "page_size caps at 250")
    c.raises(ConfigError, lambda: _cfg(shard_count=0), "shard_count starts at 1")
    c.raises(ConfigError, lambda: _cfg(max_reject_ratio=1.5), "the reject ratio threshold range")
    c.raises(ConfigError, lambda: _cfg(min_price="-1"), "min_price cannot be negative")
    c.raises(ConfigError, lambda: _cfg(condition="brand-new"), "the condition enum")
    c.raises(ConfigError, lambda: _cfg(metafields={"nope_field": "ns.key"}),
             "a metafield target must be a spec field")
    c.raises(ConfigError, lambda: _cfg(metafields={"color": "bad key!"}),
             "the metafield reference format")
    cfg = _cfg(target_countries="us", metafields={"color": "custom.shade"})
    c.eq(cfg["target_countries"], ["US"], "a single country string becomes a list")
    c.eq(cfg["_metafields"]["color"], ("custom", "shade"), "the metafield reference is parsed")


def _t_state(c: _Check) -> None:
    row = map_variant(_variant(), _cfg(), "USD")
    fp = fingerprint(row)
    c.ok(fp.count("\x1f") == 4, "a fingerprint has 5 segments")
    c.eq(_unpack(fp)[4], "900", "the last fingerprint segment is group_id")
    changed = fingerprint({**row, "availability": "out_of_stock"})
    c.ok(fp != changed, "a stock change changes the fingerprint")

    old = {"a": "in_stock\x1f10.00 USD\x1f\x1fA\x1fp1",
           "b": "in_stock\x1f10.00 USD\x1f\x1fB\x1fp1"}
    new = {"a": "out_of_stock\x1f10.00 USD\x1f\x1fA\x1fp1",
           "c": "in_stock\x1f5.00 USD\x1f\x1fC\x1fp2"}
    d = diff_state(old, new)
    c.eq(d["added"], ["c"], "diff spots an addition")
    c.eq(d["removed"], ["b"], "diff spots a removal")
    c.eq(d["stock_changed"], ["a"], "diff spots a stock change")
    c.eq(d["price_changed"], [], "an unchanged price is not reported as a price change")
    c.ok(not d["first_run"], "with a baseline, first_run=false")
    d2 = diff_state({}, new)
    c.ok(d2["first_run"], "an empty baseline is a first run")

    price_new = {"a": "in_stock\x1f12.00 USD\x1f\x1fA\x1fp1"}
    d3 = diff_state({"a": old["a"]}, price_new)
    c.eq(d3["price_changed"], ["a"], "diff spots a price change")
    c.eq(d3["stock_changed"], [], "a price change is not a stock change")

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
    c.eq(sorted(by_id), ["p1", "p2"], "grouped by parent product, unknown and parentless rows dropped")
    c.eq(len(by_id["p1"]["variants"]), 2, "variants of one parent merge and duplicate ids drop out")
    v1 = next(v for v in by_id["p1"]["variants"] if v["id"] == "v1")
    c.eq(v1["availability"], {"available": False, "status": "out_of_stock"},
         "availability is an object whose available matches status")
    v3 = by_id["p2"]["variants"][0]
    c.eq(v3["availability"]["available"], True, "backorder counts as available")
    c.eq(v3["availability"]["status"], "backorder", "status passes the original enum through")
    c.ok("title" not in v1, "the title is not pushed by default")
    with_title = build_delta_products(["v1"], state, True)
    c.eq(with_title[0]["variants"][0]["title"], "Shirt A", "the title is pushed only with the switch on")
    c.eq(build_delta_products([], state, False), [], "empty input returns empty")
    c.eq(_delta_error_code('{"error":{"code":"product_feed_delta_api_disabled"}}'),
         "product_feed_delta_api_disabled", "the error code is parsed")
    c.eq(_delta_error_code("not json"), "", "error code parsing tolerates non-JSON")


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
        c.eq(len(paths), 1, "a single shard produces one file")
        c.ok(out.exists(), "the output file is in place")
        c.ok(not out.with_name(out.name + ".partial").exists(), ".partial has been renamed away")
        with gzip.open(out, "rt", encoding="utf-8") as fh:
            lines = [json.loads(x) for x in fh if x.strip()]
        c.eq(len(lines), 2, "the jsonl line count is right")
        c.eq(lines[0]["item_id"], "123", "the jsonl content is right")
        c.ok("bogus" not in lines[0], "only spec fields are written")

        # mtime=0 plus a fixed compression level means identical content gives identical
        # bytes; without that the "content unchanged" check would be useless
        out2 = base / "again.jsonl.gz"
        w2 = FeedWriter(str(out2), "jsonl.gz", 1)
        for r in rows:
            w2.write(r)
        w2.close()
        c.eq(sha256_of(out), sha256_of(out2), "identical content produces identical bytes (gzip is reproducible)")
    finally:
        shutil.rmtree(base, ignore_errors=True)

def _t_writer_more(c: _Check) -> None:
    base = Path(os.environ.get("TMPDIR", "/tmp")) / f"feedtest2_{os.getpid()}"
    shutil.rmtree(base, ignore_errors=True)
    cfg = _cfg()
    try:
        # The CSV header must carry every column, in a fixed order
        out = base / "p.csv.gz"
        w = FeedWriter(str(out), "csv.gz", 1)
        w.write(map_variant(_variant(), cfg, "USD"))
        w.close()
        with gzip.open(out, "rt", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader)
            data = next(reader)
        c.eq(tuple(header), FEED_COLUMNS, "the CSV header matches the column definition")
        c.eq(len(data), len(FEED_COLUMNS), "the CSV data has the same column count")
        c.eq(data[header.index("additional_image_urls")].count(","), 0,
             "a single extra image produces no stray comma")

        # Sharding: one item_id must always land in the same shard, across processes too
        shards = base / "s.jsonl.gz"
        w3 = FeedWriter(str(shards), "jsonl.gz", 4)
        c.eq(len(w3.shards), 4, "opens as many shards as shard_count")
        names = [s.path.name for s in w3.shards]
        c.eq(names[0], "s-0000.jsonl.gz", "shard numbering goes before the suffix")
        idx = hashlib.sha1(b"123").digest()[0] % 4
        w3.write(map_variant(_variant(), cfg, "USD"))
        c.eq(w3.shards[idx].count, 1, "routed to the shard sha1 picks")
        paths = w3.close()
        c.eq(len(paths), 4, "empty shards are written too (the shard set must stay stable)")
        c.ok(all(p.exists() for p in paths), "every shard file exists")

        # abort must clear .partial; a half-written file must never replace the previous one
        w4 = FeedWriter(str(base / "a.jsonl.gz"), "jsonl.gz", 1)
        w4.write(map_variant(_variant(), cfg, "USD"))
        partial = w4.shards[0].tmp
        c.ok(partial.exists(), ".partial exists while writing")
        w4.abort()
        c.ok(not partial.exists(), "abort clears .partial")
        c.ok(not (base / "a.jsonl.gz").exists(), "abort produces no final file")

        c.eq(shard_name("p.jsonl.gz", 0, 1), "p.jsonl.gz", "a single shard keeps the base name")
        c.eq(shard_name("p.parquet", 2, 3), "p-0002.parquet", "parquet shard naming")
        c.eq(flatten_value(["a", "b"]), "a,b", "a list joins with commas")
        c.eq(flatten_value(True), "true", "a bool becomes a lower-case string")
        c.eq(flatten_value(None), "", "None becomes an empty string")
    finally:
        shutil.rmtree(base, ignore_errors=True)

def _t_collect(c: _Check) -> None:
    cfg = _cfg()
    variants = [
        _variant(),
        _variant(id="gid://shopify/ProductVariant/124"),
        _variant(id="gid://shopify/ProductVariant/125", product={"status": "DRAFT"}),
        _variant(id="gid://shopify/ProductVariant/126", price="0"),
        _variant(),  # duplicate item_id
    ]
    res = collect(variants, cfg, "USD", None)
    c.eq(res["written"], 2, "only passing rows are written")
    c.eq(res["skipped"], 2, "the DRAFT and the zero-price row are skipped")
    c.eq(res["dupes"], 1, "a duplicate item_id counts as a dupe")
    c.eq(res["products"], 1, "both variants belong to one product")
    c.eq(len(res["state"]), 2, "state holds only the rows written")
    c.ok(res["reasons"], "reject reasons are recorded")
    c.ok(all(len(s) == 3 for s in res["samples"]), "samples are 3-tuples")

    limited = collect([_variant(), _variant(id="gid://shopify/ProductVariant/124")],
                      cfg, "USD", None, limit=1)
    c.eq(limited["written"], 1, "--limit takes effect")

    # Data guards
    c.raises(DataGuardError, lambda: _guard_snapshot(
        {"written": 0, "skipped": 3, "invalid": 0, "dupes": 0, "reasons": {}}, cfg),
        "an empty snapshot is blocked")
    c.raises(DataGuardError, lambda: _guard_snapshot(
        {"written": 5, "skipped": 0, "invalid": 0, "dupes": 0, "reasons": {}},
        _cfg(min_rows=10)), "below min_rows is blocked")
    c.raises(DataGuardError, lambda: _guard_snapshot(
        {"written": 10, "skipped": 90, "invalid": 0, "dupes": 0, "reasons": {"x": 90}}, cfg),
        "too high a reject ratio is blocked")
    _guard_snapshot({"written": 100, "skipped": 5, "invalid": 0, "dupes": 0, "reasons": {}}, cfg)
    c.passed += 1  # a healthy snapshot raises nothing


def _t_query(c: _Check) -> None:
    cfg = _cfg(metafields={"color": "custom.shade"})
    q = build_variants_query(set(OPTIONAL_GROUPS), cfg["_metafields"], True)
    c.ok("productVariants(" in q, "it queries productVariants")
    c.ok("product_status:active" in q, "it carries the active filter")
    c.ok("inventoryQuantity" in q, "it includes the inventory field group")
    c.ok('mf0: metafield(namespace: "custom", key: "shade")' in q, "the metafield alias is injected")
    c.ok("pageInfo" in q and "endCursor" in q, "it includes cursor pagination")
    q2 = build_variants_query(set(), cfg["_metafields"], False)
    c.ok("inventoryQuantity" not in q2, "after degrading, inventory is no longer requested")
    c.ok("product_status:active" not in q2, "with the status filter off the query argument disappears")
    c.ok("id" in q2 and "price" in q2, "core fields are always kept")
    # bulk forbids pagination arguments on nested connections, so the media group must go
    bq = Shopify("x.myshopify.com", "t", DEFAULT_SHOPIFY_API_VERSION).build_bulk_query(cfg)
    c.ok("media(first:" not in bq, "the bulk query carries no media pagination argument")
    c.ok("pageInfo" not in bq, "the bulk query needs no pageInfo")

def _t_degrade(c: _Check) -> None:
    def errs(*messages: str) -> str:
        # The real caller passes json.dumps(payload["errors"]); keep the same shape here
        return json.dumps([{"message": m} for m in messages], ensure_ascii=False)

    client = Shopify("x.myshopify.com", "t", DEFAULT_SHOPIFY_API_VERSION)
    # With only read_products granted, the inventory fields fail the whole query
    dropped = client._drop_group_for(
        errs("Field 'inventoryQuantity' doesn't exist on type 'ProductVariant'"))
    c.eq(dropped, "inventory", "the field name in the error locates the inventory group")
    c.ok("inventory" not in client.groups, "that group is dropped from later queries")
    c.ok("variant_image" in client.groups, "only the offending group is dropped")
    again = client._drop_group_for(errs("Field 'inventoryQuantity' doesn't exist"))
    c.ok(again != "inventory", "the same group is not dropped twice")

    c2 = Shopify("x.myshopify.com", "t", DEFAULT_SHOPIFY_API_VERSION)
    c2._drop_group_for(errs("Invalid argument in query: product_status"))
    c.ok(not c2.use_status_filter, "with no field name to go on, the status filter is dropped first")
    c3b = Shopify("x.myshopify.com", "t", DEFAULT_SHOPIFY_API_VERSION)
    c.eq(c3b._drop_group_for(errs("Field 'seo' doesn't exist")), "seo", "it locates the seo group")

    # Throttling: too few points left must produce a wait
    c3 = Shopify("x.myshopify.com", "t", DEFAULT_SHOPIFY_API_VERSION)
    c3._absorb_cost({"cost": {"throttleStatus": {"currentlyAvailable": 20, "restoreRate": 50}}})
    c.eq(c3.available_points, 20.0, "it reads the points left")
    c.eq(c3.restore_rate, 50.0, "it reads the restore rate")
    start = time.time()
    c3.pace(next_cost=30.0)
    c.ok(time.time() - start >= 0.15, "too few points really does sleep")
    c4 = Shopify("x.myshopify.com", "t", DEFAULT_SHOPIFY_API_VERSION)
    c4._absorb_cost({"cost": {"throttleStatus": {"currentlyAvailable": 900, "restoreRate": 50}}})
    t2 = time.time()
    c4.pace(next_cost=30.0)
    c.ok(time.time() - t2 < 0.1, "plenty of points means no sleep")


def _t_misc(c: _Check) -> None:
    c.eq(_remote_join("/in", "p.jsonl.gz"), "/in/p.jsonl.gz", "remote path join")
    c.eq(_remote_join(".", "p.jsonl.gz"), "p.jsonl.gz", "the current directory adds no prefix")
    c.eq(_remote_join("", "p.jsonl.gz"), "p.jsonl.gz", "an empty directory adds no prefix")
    c.eq(_remote_join("/in/", "p.jsonl.gz"), "/in/p.jsonl.gz", "a duplicate slash is removed")
    c.eq(_status_text({"ok": True}), "success", "status text success")
    c.eq(_status_text({"skipped": "x"}), "skipped (x)", "status text skipped")
    c.ok(_status_text({"errors": ["boom"]}).startswith("failed"), "status text failed")
    c.eq(len(REQUIRED_FIELDS), 15, "15 required fields")
    c.eq(len(set(FEED_COLUMNS)), len(FEED_COLUMNS), "no duplicate columns")
    c.ok(all(f in FEED_COLUMNS for f in REQUIRED_FIELDS), "every required field is a defined column")
    c.ok("inventory_quantity" not in FEED_COLUMNS, "the spec has no inventory_quantity field")
    c.ok("is_ads_enabled" not in FEED_COLUMNS, "is_ads_enabled is not a legal field name")
    c.ok("additional_image_urls" in FEED_COLUMNS, "the extra-image field is plural")
    c.ok("is_ads_eligible" in FEED_COLUMNS, "the ads-eligibility field name is right")
    state_file = Path(os.environ.get("TMPDIR", "/tmp")) / f"st_{os.getpid()}.json"
    try:
        save_state(str(state_file), {"a": "x"}, "deadbeef")
        loaded = load_state(str(state_file))
        c.eq(loaded["items"], {"a": "x"}, "state round-trips")
        c.eq(loaded["content_sha256"], "deadbeef", "the fingerprint is persisted")
        state_file.write_text("{ broken", "utf-8")
        c.eq(load_state(str(state_file))["items"], {}, "a broken state file is treated as a first run")
        c.eq(load_state(str(state_file / "nope"))["items"], {}, "a missing state file does not raise")
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
            c.failed.append(f"{fn.__name__} raised: {type(exc).__name__}: {redact(exc)}")
    if c.failed:
        log(f"self-test failed {len(c.failed)} / passed {c.passed}:")
        for item in c.failed:
            log(f"  ✗ {item}")
        return EXIT_RUNTIME
    log(f"self-test all green: {c.passed} assertions passed")
    return EXIT_OK

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="feed_sync.py",
        description="Turn a Shopify catalog into an OpenAI product feed and deliver it (full snapshot plus optional Delta)",
    )
    p.add_argument("--config", default="config.json", help="path to the business config (credentials come from the environment)")
    p.add_argument("--dry-run", action="store_true", help="write files locally only: no upload, no state")
    p.add_argument("--limit", type=int, default=0, help="process the first N rows only, for a trial run (state is not written)")
    p.add_argument("--bulk", action="store_true", help="fetch with Bulk Operations (steadier on large catalogs)")
    p.add_argument("--delta", action="store_true", help="also push availability changes through the Delta API")
    p.add_argument("--self-test", action="store_true", help="run the built-in assertions, no network")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.self_test:
        return self_test()
    if args.limit < 0:
        log("--limit cannot be negative")
        return EXIT_CONFIG
    try:
        return run(args)
    except ConfigError as exc:
        log(f"config error: {redact(exc)}")
        return EXIT_CONFIG
    except DataGuardError as exc:
        log(f"data guard blocked this publish: {redact(exc)}")
        notify_discord("GPT Ads Feed halted (data guard)", [("Reason", str(exc)[:1000])], ok=False)
        return EXIT_DATA_GUARD
    except KeyboardInterrupt:
        log("interrupted")
        return EXIT_RUNTIME
    except Exception as exc:  # noqa: BLE001
        log(f"run failed: {type(exc).__name__}: {redact(exc)}")
        notify_discord("GPT Ads Feed update failed",
                       [("Exception", f"{type(exc).__name__}: {redact(exc)}"[:1000])], ok=False)
        return EXIT_RUNTIME


if __name__ == "__main__":
    sys.exit(main())
