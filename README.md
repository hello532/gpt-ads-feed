# gpt-ads-feed

Turns Shopify products and inventory into an OpenAI product feed, once a day, as a
full snapshot — and **refuses to upload a bad one**.

Single file, `feed_sync.py`, **standard library only** (2,400+ lines). Optional
dependencies are needed only for the features that use them.

[中文说明 / Chinese](README.zh.md)

---

## What it does

- Reads Shopify Admin **GraphQL** (the REST `products` endpoint is deprecated), cursor
  paginated, and backs off from `extensions.cost.throttleStatus` leaky-bucket credit
  before Shopify throttles it
- Maps fields to the OpenAI flat-file spec, emitting `jsonl.gz` / `csv.gz` / `tsv.gz` / `parquet`
- Uploads to SFTP under a **fixed filename, overwriting in place** — full-snapshot
  semantics, not incremental append
- **Three data guards** catch a bad snapshot: volume drop, required-field miss rate,
  empty set. Any one of them exits `3` and **uploads nothing**
- Optional Discord notification; optional Delta Feeds API patch

## What it does not do

- **It cannot create the feed connection for you.** SFTP credentials and feed binding
  are configured by hand in Ads Manager
- **The Delta API cannot change price.** It only changes `availability` and `title` on
  variants that already exist — it cannot create a feed and cannot add products
- **It does not improve your organic ranking in AI search.** Feed frequency governs how
  *fresh* your product data is, so ChatGPT does not quote a sold-out item or a stale
  price. Retrieval ranking is a separate problem. Decide with that in mind

## Two channels that do not talk to each other

OpenAI has two separate paths for product data. Do not mix them:

| | Commerce / ACP | Ads |
|---|---|---|
| Spec | `developers.openai.com/commerce` | `developers.openai.com/ads` |
| Purpose | Appear in ChatGPT shopping results, checkout | Ad delivery |
| Delivery | File upload (this project's main path) | File upload + Delta API |
| Feed creation | Manual | Manual (Ads Manager) |

The main path here is Commerce file upload: full snapshot, fixed filename, overwrite.
Delta is an optional supplement.

---

## Quick start

```bash
# 1. Configure: copy the templates, then fill in real values (both are gitignored)
cp config.example.json config.json
cp env.example ~/.config/gpt-ads-feed/env   # credentials live only here, never in the repo

# 2. Dry run first: 20 products, writes nothing, uploads nothing
python3 feed_sync.py --config config.json --dry-run --limit 20

# 3. Once the dry run looks right, write files locally (still no upload)
python3 feed_sync.py --config config.json --limit 20 --no-upload

# 4. Real run
python3 feed_sync.py --config config.json
```

Required config: `shop_domain`, `seller_name`, `seller_url`, `return_policy`.
Enabling `is_eligible_checkout` also requires `seller_privacy_policy` and `seller_tos`.

### Optional dependencies

| Package | Needed for | If missing |
|---|---|---|
| `paramiko` | SFTP upload | Falls back to the `sftp` CLI (needs an existing key + pinned host key) |
| `pyarrow` | `output_format: parquet` | Fails at startup, telling you to change format or install it |
| `zstandard` | zstd compression for parquet | Falls back to the default codec |

```bash
pip3 install paramiko pyarrow zstandard
```

Pin the host key before the first upload, or `StrictHostKeyChecking=yes` refuses to connect:

```bash
ssh-keyscan -p 22 <sftp-host> >> ~/.ssh/known_hosts
```

---

## Things that bit us, already handled in the code

**A missing `read_inventory` scope kills the entire query.** Those fields do not come
back empty — the whole GraphQL request returns `ACCESS_DENIED`. So fields are organised
into groups: recognise the offending field name in the error text, drop *only that
group*, retry. One missing scope costs you a group of fields, not the run.

**Shard routing uses `hashlib.sha1`, not the builtin `hash()`.** The builtin is
randomised by `PYTHONHASHSEED`, so after a restart the same SKU would jump to a
different shard — which breaks the delivery requirement that the shard set stay stable.

**gzip is written with `mtime=0`.** Unchanged content produces byte-identical output, so
a diff can tell you whether anything really changed.

**Writes are atomic.** Local `.partial` + `os.replace`, remote `.tmp` + `posix_rename`,
so the other end never reads half a file.

**Delta's `availability` is an object, not a string** (`{"available": bool}` or
`{"status": "in_stock"}`; when both are present `status` wins). And **200 OK does not
mean success** — you have to parse `accepted: true`. A `product_feed_api_disabled`
response means the permission was never granted; do not retry it.

**Field names were checked against the spec one at a time**: `additional_image_urls` is
plural; `is_eligible_search` / `is_eligible_checkout` / `is_ads_eligible` (`is_eligible_ads`
is a legacy alias, `is_ads_enabled` is invalid); the spec wants `sale_price <= price`
where Google wants `<`; booleans are lowercase string literals; a URL carrying
`user:pass@` gets the whole row rejected; money uses `Decimal` with `ROUND_HALF_UP`,
never float.

## Scheduling (macOS launchd)

```bash
sed -e "s|__PROJECT_DIR__|$(pwd)|g" -e "s|__HOME__|$HOME|g" \
    com.user.gpt-ads-feed.plist > ~/Library/LaunchAgents/com.user.gpt-ads-feed.plist
launchctl load ~/Library/LaunchAgents/com.user.gpt-ads-feed.plist
```

Defaults to 07:20 daily with `RunAtLoad` false. The documented delivery cadence is "at
least once a day".

---

## Tests

```bash
bash tests/run_all.sh
```

Three layers, 277 assertions:

| Layer | Assertions | Covers |
|---|---|---|
| `feed_sync.py --self-test` | 202 | Field mapping, GTIN mod-10 check digit, money rounding, field-group degradation |
| `tests/test_e2e.py` | 67 | 9 scenarios: dry run, full run, no-change, guard trip, sharding, CLI, three formats, discount & skip, Delta failure |
| `tests/test_guards.py` | 8 | Missing-dependency and missing-credential branches |

E2E stubs out the network calls and cleans up its temp directory. One assertion exists
purely to check that no token and no shop domain appears in notification text.

## Verified / not verified

An honest line, so nothing untested is presented as fact:

**Verified (green on this machine)** — all 277 assertions; the actual bytes emitted for
`jsonl.gz` / `csv.gz` / `tsv.gz`; all three guards; the CLI switches; shard stability
(re-run with the input order shuffled, the same SKU lands in the same shard).

**Not verified** — `parquet` byte output (no `pyarrow` installed here, only the guard
branch was exercised); a real SFTP upload (no `paramiko`); a live Delta API call (needs
OpenAI to grant the permission plus Ads Manager credentials). Those three have been
through their guard branches only — no real bytes, no real connection.

## Security

- Credentials are read from environment variables only. `config.json` and the env file
  are both gitignored; the repo carries `.example` templates
- Tokens and shop domains are redacted in logs and notifications, with a test covering it
- SFTP forces `StrictHostKeyChecking=yes` and `BatchMode=yes`
- When a guard trips: exit `3`, upload nothing. Better a day without an update than a
  bad snapshot overwriting live data

## Spec references

- Field definitions: `developers.openai.com/commerce/specs/file-upload/products`
- Delivery constraints: `developers.openai.com/commerce/specs/file-upload/overview`
- Delta API: `developers.openai.com/ads/delta-feeds`

Shards are suggested to stay under 500k rows and 500MB each. The spec does **not**
mandate a shard filename format — it only requires the shard set to stay stable, with
the same batch of files overwritten each time.

## If you want this running against your own store

This feed is the worked example, not a product line. I build the same shape — a
scheduled export against the Shopify Admin GraphQL API, with guards that refuse to
publish rather than overwrite live data with a bad snapshot, and a test suite you can
run before trusting any of it — as a fixed-price job:
[hello532.github.io/services.html](https://hello532.github.io/services.html), or
coolun.337@gmail.com. The "Verified / not verified" section above is the same standard
you would get in writing for your own job. Issues and PRs here are welcome either way;
nothing in this repository needs paying for.

## Licence

MIT

