# gpt-ads-feed

把 Shopify 商品与库存每天转成 OpenAI 能吃的商品 feed，全量快照上传，失败不覆盖线上数据。

单文件 `feed_sync.py`，**纯标准库**（2400+ 行）。可选依赖只在用到对应功能时才需要。

---

## 它做什么

- 走 Shopify Admin **GraphQL**（REST products 已废弃）拉商品 + 变体，游标分页，读 `extensions.cost.throttleStatus` 漏桶余量主动退避
- 按 OpenAI 官方 flat-file 规范映射字段，输出 `jsonl.gz` / `csv.gz` / `tsv.gz` / `parquet`
- 固定文件名**原地覆盖**上传到 SFTP（全量快照语义，不是增量追加）
- 三条数据守卫拦异常快照：掉量、必填字段缺失率、空集 —— 任一触发就退出码 3 且**不上传**
- 可选 Discord 通知；可选 Delta Feeds API 补丁

## 它不做什么

- **不能替你开通 feed 连接**。SFTP 凭证与 feed 绑定只能在 Ads Manager 手动配置
- **Delta API 改不了价格**。它只能改已存在变体的 `availability` 和 `title`，不能建 feed、不能加商品
- **不提升 AI 搜索的自然排名**。feed 频率决定的是商品数据的新鲜度（别让 ChatGPT 报已售罄的货或旧价），跟自然检索排名是两条独立的东西。想清楚这点再决定投入

## 两条独立通道

OpenAI 的商品数据有两条**互不相通**的通道，别混：

| | Commerce / ACP | Ads |
|---|---|---|
| 规范 | `developers.openai.com/commerce` | `developers.openai.com/ads` |
| 用途 | 进 ChatGPT 购物结果、结账 | 广告投放 |
| 投递 | 文件上传（本项目主路径） | 文件上传 + Delta API |
| 建 feed | 手动 | 手动（Ads Manager） |

本项目主路径走 Commerce 的文件上传：全量快照 + 固定文件名覆盖。Delta 是可选补充。

---

## 快速开始

```bash
# 1. 配置：复制模板后填真实值（两个文件都在 .gitignore 里）
cp config.example.json config.json
cp env.example ~/.config/gpt-ads-feed/env   # 凭证只放这里，不进仓

# 2. 先干跑，只取 20 条，不写不传
python3 feed_sync.py --config config.json --dry-run --limit 20

# 3. 干跑没问题再落地文件（仍不上传）
python3 feed_sync.py --config config.json --limit 20 --no-upload

# 4. 正式跑
python3 feed_sync.py --config config.json
```

必填配置项：`shop_domain`、`seller_name`、`seller_url`、`return_policy`。
开 `is_eligible_checkout` 时还要 `seller_privacy_policy` 和 `seller_tos`。

### 可选依赖

| 依赖 | 什么时候需要 | 缺了会怎样 |
|---|---|---|
| `paramiko` | SFTP 上传 | 退回 `sftp` CLI（要求已有密钥 + 已固定 host key） |
| `pyarrow` | `output_format: parquet` | 启动即报错，让你换格式或装依赖 |
| `zstandard` | parquet 的 zstd 压缩 | 退回默认压缩 |

```bash
pip3 install paramiko pyarrow zstandard
```

上传前必须先固定 host key，否则 `StrictHostKeyChecking=yes` 会拒连：

```bash
ssh-keyscan -p 22 <sftp-host> >> ~/.ssh/known_hosts
```

---

## 几个踩过的坑（代码里已处理）

**`read_inventory` scope 缺失会废掉整条查询。** 不是那几个字段返回空，是整个 GraphQL 请求 `ACCESS_DENIED`。所以字段按组织：从报错文本里认出中招的字段名 → 只摘掉那一组 → 重试。缺一个 scope 的代价是丢一组字段，不是丢整次运行。

**分片路由用 `hashlib.sha1`，不用内置 `hash()`。** 后者被 `PYTHONHASHSEED` 随机化，重启后同一个 SKU 会跳到别的分片，破坏"分片集合保持稳定"这条投递要求。

**gzip 固定 `mtime=0`。** 内容不变则字节完全一致，方便 diff 和判断是否真的变了。

**写入是原子的。** 本地 `.partial` + `os.replace`，远端 `.tmp` + `posix_rename`，避免对端读到半个文件。

**Delta 的 `availability` 是对象不是字符串**（`{"available": bool}` 或 `{"status": "in_stock"}`，同时给时 `status` 优先）。而且 **200 OK 不等于成功** —— 必须解析 `accepted: true`。返回 `product_feed_api_disabled` 说明权限没开，别重试。

**字段名对着规范逐个核过**：`additional_image_urls` 是复数；`is_eligible_search` / `is_eligible_checkout` / `is_ads_eligible`（`is_eligible_ads` 是旧别名，`is_ads_enabled` 非法）；规范要求 `sale_price <= price`（Google 是 `<`）；布尔是小写字符串字面量；URL 里带 `user:pass@` 整行会被拒；金额用 `Decimal` + `ROUND_HALF_UP`，不用浮点。

## 定时任务（macOS launchd）

```bash
sed -e "s|__PROJECT_DIR__|$(pwd)|g" -e "s|__HOME__|$HOME|g" \
    com.user.gpt-ads-feed.plist > ~/Library/LaunchAgents/com.user.gpt-ads-feed.plist
launchctl load ~/Library/LaunchAgents/com.user.gpt-ads-feed.plist
```

默认每天 07:20，`RunAtLoad` 为 false。官方建议的投递频率是「至少每天一次」。

---

## 测试

```bash
bash tests/run_all.sh
```

三层，共 277 项断言：

| 层 | 断言 | 覆盖 |
|---|---|---|
| `feed_sync.py --self-test` | 202 | 字段映射、GTIN mod-10 校验位、金额舍入、字段组降级 |
| `tests/test_e2e.py` | 67 | 9 个场景：干跑、全量、无变化、守卫触发、分片、CLI、三种格式、折扣与跳过、Delta 失败 |
| `tests/test_guards.py` | 8 | 缺依赖 / 缺凭证的分支 |

E2E 用 stub 顶掉网络调用，在临时目录跑完自动清理。其中一条断言专门检查通知内容里不出现 token 和店铺域名。

## 已验证 / 未验证

诚实划线，别把没跑通的当事实：

**已验证（本机跑绿）** —— 277 项断言全过；`jsonl.gz` / `csv.gz` / `tsv.gz` 的实际字节输出；三条守卫；CLI 开关；分片稳定性（打乱输入顺序重跑，同一 SKU 落在同一分片）。

**未验证** —— `parquet` 字节输出（本机没装 `pyarrow`，只验了守卫分支）；真实 SFTP 上传（没装 `paramiko`）；线上 Delta API 调用（需要 OpenAI 开权限 + Ads Manager 凭证）。这三块只走过 guard 分支，没验过真实字节和真实连接。

## 安全

- 凭证只从环境变量读。`config.json` 和 env 文件都在 `.gitignore` 里，仓库只放 `.example` 模板
- 日志和通知里的 token 与店铺域名都做脱敏，有测试覆盖
- SFTP 强制 `StrictHostKeyChecking=yes` + `BatchMode=yes`
- 守卫触发时退出码 3 且不上传 —— 宁可当天不更新，也不用坏快照覆盖线上

## 规范依据

- 字段定义：`developers.openai.com/commerce/specs/file-upload/products`
- 投递约束：`developers.openai.com/commerce/specs/file-upload/overview`
- Delta API：`developers.openai.com/ads/delta-feeds`

分片建议每片 50 万条以内、单文件 500MB 以内；规范**没有**规定分片文件名格式，只要求分片集合保持稳定、每次覆盖同一批文件。

## 许可

MIT

