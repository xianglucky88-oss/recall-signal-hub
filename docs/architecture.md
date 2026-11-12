# 模块契约与一致性

## 数据流

```mermaid
flowchart LR
    P[HTTP 发布方 / fixture] --> S[来源适配与传输]
    S --> N[字段标准化]
    N --> R[风险策略]
    N --> I[产品身份与多源归并]
    R --> I
    I --> D[事务证据与案例存储]
    D --> Q[告警 outbox]
    Q --> A[租约投递与接收方幂等]
```

## 发布方接入

`SourceAdapter.pull(cursor)` 返回一页 `SourceDocument` 和下一位置。HTTP 适配器支持两个真实不同的记录封装：

- `product-feed`：顶层 `records`；记录字段为 `id`、`sequence`、`published_at`、`product`。
- `recall-bulletin`：顶层 `bulletins`；记录字段为 `reference`、`position`、`issued_at`、`details`。

两个格式都用顶层 `next_cursor` 返回下一位置。位置必须单调递增且为正整数；适配器过滤已提交的重叠记录，拒绝倒退或无序页。每页最多 2000 条，HTTP 响应最多 2 MB，默认超时 10 秒。传输层只允许无内嵌账号密码的 HTTP/HTTPS 地址。

网络获取发生在事务外。获取成功后，存储层对旧游标做比较并交换检查；竞争失败返回 `superseded`，不会覆盖其他 worker 的进度。整页错误不推进位置。网络错误按来源持久化指数退避，2 秒起步、上限 300 秒。

测试入口：`tests/test_http.py`、`tests/test_collection.py`。

## 标准化与风险

标准化执行 Unicode NFKC、casefold、标点与空白归一，保留非拉丁文字。品牌、产品、市场、风险描述不能为空，时间必须包含时区。业务坏记录保存业务载荷和拒绝原因后与正常记录一起提交，因此不会永远堵住同一页。

`RiskPolicy` 有独立的规则、版本和 `RiskDecision` 接口。默认规则按词组边界匹配，显式发布方等级优先；无法识别的显式等级隔离处理。它是演示风险规则，不是监管或医学评级。当前案例保存合并等级与风险描述，`RiskDecision` 的解释接口可独立调用；案例中尚未持久化每条规则的解释。

测试入口：`tests/test_normalize.py`、`tests/test_risk.py`。

## 身份与归并

产品身份由标准化后的品牌、产品型号、市场三元组确定，使用结构化序列化避免分隔符碰撞。跨来源相同身份汇总最新来源声明，风险等级取最高，风险描述取有序并集，不随来源处理顺序而振荡。

当前不做模糊匹配，不猜测两个不同型号是否同物。发布方修正产品身份时，声明移动到新案例；原案例没有剩余声明时标为 inactive，历史保留。一个采集页先更新全部声明，再汇总受影响的案例，避免页内瞬时告警。

测试入口：`tests/test_normalize.py`、`tests/test_collection.py`。

## 事务边界与证据

`RecallStore.commit_batch` 在同一 SQLite 写事务中完成业务载荷留存、坏记录隔离、来源声明更新、案例修订、告警入队、游标推进。任何一步失败均回滚；重试不会出现游标已过但证据缺失的状态。

业务载荷是收到的来源 ID、来源记录 ID、发布时间和 payload 的规范 JSON，按 SHA-256 去重。它不等于完整 HTTP 报文存档。修订表示案例的合并业务状态变化；相同状态不制造新修订。原始业务证据和修订保存在 SQLite，不使用第二个文件存储来制造跨资源事务窗口。

SQLite 每个 worker 使用独立连接，WAL 与 `BEGIN IMMEDIATE` 保障写入串行化。基线使用单个 SQLite 文件，不声称支持分布式数据库或无限并发。

测试入口：`tests/test_collection.py` 的故障注入、竞争游标、重开数据库和完整性检查。

## 告警队列与投递

`Outbox.enqueue` 必须加入调用方现有事务，不能独立提交；队列按 `(case_key, revision)` 去重。`Outbox.claim` 原子领取到期任务，领取次数和唯一租约 ID 持久化。

状态流：`pending -> leased -> delivered`；失败回到 `pending` 并退避，租约到期可重新领取。确认与重试都检查租约 ID 和有效期，旧 worker 无法覆盖新 worker。派送器用实际发送完成时刻确认，避免慢请求误用领取时刻。

Webhook 始终携带同一案例修订的固定 `Idempotency-Key`。本地状态和远程接收无法形成原子事务，所以仅保证至少一次发送；远端幂等是安全重投的必要契约。进程内 `RecordingSink` 用于演示此契约。

测试入口：`tests/test_delivery.py`、`tests/test_http.py`。

## 模块演进约束

- 传输与发布方格式的变化通过 `SourceAdapter` 输出契约隔离。
- 产品身份与多来源归并保持在 `reconcile.py`，风险规则在 `risk.py`。
- 投递协议与队列租约分别位于 `alerts.py` 和 `outbox.py`，不放进采集管线。
- SQLite 的连接与事务辅助只有 `database.py`；它是共享基础设施，不是每个功能必须重写的业务模块。
- 根目录 CLI 只组装依赖，不承载业务判定。
