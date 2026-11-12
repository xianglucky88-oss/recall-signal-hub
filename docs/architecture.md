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
    Q --> T[订阅展开与按目标任务]
    T --> A[租约投递与接收方幂等]
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

## 按目标路由与独立记账

无路由配置时，投递保持上面的单目标 `outbox` 行为，完全不变。提供本地路由配置（`routing.py` 的 `RouteConfig`）后，风险规则是否告警的判定不变，路由只在 `needs_alert` 已判定需要通知之后，依据该条告警不可变的案例摘要（市场、风险等级）展开收件目标。

- 订阅声明稳定的 `recipient_id`、市场集合与最低风险等级；市场按标准化口径匹配。`RouteConfig` 的版本只由订阅内容做 SHA-256 内容寻址，相同配置得到相同版本，与加载时机和顺序无关。
- 展开与告警在同一写事务内持久化到 `outbox_alerts` 与 `outbox_targets`：目标集合在任何任务可被领取之前固定，记录所用 `route_version`、每个接收方当时的端点快照；`UNIQUE(alert_id, recipient_id)` 保证同一接收方只生成一份任务。无人匹配时 `target_count=0` 且没有任务行，是可解释的“无目标”终态，而不是仍在等待的空队列。
- 每个目标独立保存 `pending/leased/delivered/paused` 状态、租约、尝试次数、退避时间与最后错误。接收端幂等键为 `recall:<case_key>:<revision>:<recipient_id>`，同时区分案件修订与接收方；任一目标成功不会把其余目标一并完成，一个目标失败只退避它自己。
- 加载新的 `RouteConfig` 不会修改任何已排队任务：任务继续使用展开时冻结的接收方、端点与配置版本，新订阅不会回填旧告警，也不会把旧任务静默改派。`RouteAdministrator` 只报告漂移（接收方移除、端点改变）；变更任务必须走显式、带审计事件的决定——`pause_target`（暂停，保留原快照直到显式恢复）或 `repoint_target`（显式改派到新端点，接收方与幂等键不变）。

测试入口：`tests/test_routing.py`（含两个目标同时命中、一败一成、订阅变更、旧领取者迟到确认的端到端场景）、`tests/test_cli.py` 的路由 CLI 用例。所有验证只使用 `RecordingTargetSink` 或本地 HTTP 接收替身，不向真实接收方发消息。

## 模块演进约束

- 传输与发布方格式的变化通过 `SourceAdapter` 输出契约隔离。
- 产品身份与多来源归并保持在 `reconcile.py`，风险规则在 `risk.py`；路由（`routing.py`）只在风险规则已判定需要告警之后展开目标，不改变危险分级、案件身份或告警创建时机。
- 投递协议与队列租约分别位于 `alerts.py` 和 `outbox.py`，不放进采集管线；按目标的展开持久化与显式漂移处理位于 `outbox.py`、`route_admin.py`。
- SQLite 的连接与事务辅助只有 `database.py`；它是共享基础设施，不是每个功能必须重写的业务模块。
- 根目录 CLI 只组装依赖，不承载业务判定。
