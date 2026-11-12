# Recall Signal Hub

消费品召回信号的采集与汇聚服务。它从异构发布方获取产品风险记录，保存收到的原始业务载荷，跨来源归并同一产品的风险声明，通过事务 outbox 向接收端投递告警。

这是本地创建、已实现并通过测试的工程基线；没有连接真实监管站点或投入生产。HTTP 采集与 webhook 投递都有实际实现，通过本地 HTTP 服务验证，不依赖真实账号。

## 模块边界

| 责任 | 生产模块 | 可观察结果 |
| --- | --- | --- |
| 发布方接入 | `sources.py`、`transport.py` | 两种发布格式、增量页、响应大小限制、失败分类 |
| 记录标准化 | `normalize.py` | Unicode 兼容、字段校验、坏记录隔离 |
| 产品身份与跨源归并 | `reconcile.py` | 相同品牌/型号/市场归为同一案例，各来源风险声明合并 |
| 风险策略 | `risk.py` | 可独立配置的规则、策略版本、判定原因、告警触发规则 |
| 采集状态与证据 | `storage.py`、`database.py`、`pipeline.py` | 原始业务载荷、游标原子提交、失败退避、案例修订 |
| 告警可靠投递 | `outbox.py`、`alerts.py` | 队列租约、失效租约隔离、重投、接收方幂等键 |
| 按目标路由 | `routing.py`、`route_admin.py` | 稳定接收方标识、内容寻址配置版本、目标集合冻结、每目标独立记账、漂移显式处理 |

具体接口、状态和测试对应关系见 [模块契约](docs/architecture.md)。CLI 只是启动和检查入口，项目核心是数据采集与风险事件处理。

## 环境与依赖

- Python 3.11+；实际基线验证使用 Python 3.14.7。
- 运行依赖：Python 标准库，其中 SQLite 由 Python 提供。
- 测试依赖：pytest >=8.3,<9；实际验证使用 pytest 8.4.2。
- 打包依赖：setuptools >=69；验证使用 setuptools 84.2.1、build 1.6.1。

仓库不保存真实连接地址、账号、密钥或 token。HTTP 发布方和 webhook 接收方的地址只通过运行环境提供；错误输出不回显这些配置。

## 离线运行

在仓库根目录执行：

```bash
PYTHONPATH=src python -m recall_signal_hub.cli collect \
  --fixture fixtures/recalls.json --database /tmp/recall-signal-hub.sqlite3
PYTHONPATH=src python -m recall_signal_hub.cli inspect \
  --database /tmp/recall-signal-hub.sqlite3
PYTHONPATH=src python -m recall_signal_hub.cli dispatch \
  --database /tmp/recall-signal-hub.sqlite3
```

初次采集得到 2 个案例、3 条有效来源声明、1 条隔离记录和 2 条待投递告警。再次采集不会新增证据或告警。`dispatch` 默认使用进程内幂等接收器模拟成功投递；它是演示模式，不会通知真实接收人。

## HTTP 运行

由部署环境注入 `RECALL_SOURCE_URL` 后：

```bash
PYTHONPATH=src python -m recall_signal_hub.cli collect --http \
  --source consumer-agency --format product-feed \
  --database /tmp/recall-signal-hub.sqlite3
```

另一个支持的格式是 `recall-bulletin`。每次采集处理一页，通过 `after` 查询参数传入已提交位置；调度方可以重复调用直到无新记录。发布方必须提供单调递增的正整数位置。

由部署环境注入 `RECALL_WEBHOOK_URL` 后：

```bash
PYTHONPATH=src python -m recall_signal_hub.cli dispatch --webhook \
  --database /tmp/recall-signal-hub.sqlite3
```

真实 webhook 接收端需要支持 `Idempotency-Key`。投递语义是至少一次；远端接收成功但本地确认之前的中断会导致重投，相同告警的幂等键保持不变。

## 按目标路由

不提供路由配置时，系统保持单目标投递：告警进入 `outbox`，行为与基线完全一致。提供本地路由配置后，告警在风险规则判定需要通知之后展开为按接收方记账的独立任务：

```bash
PYTHONPATH=src python -m recall_signal_hub.cli collect \
  --fixture fixtures/recalls.json --routes fixtures/routes.example.json \
  --database /tmp/recall-signal-hub-routed.sqlite3
PYTHONPATH=src python -m recall_signal_hub.cli dispatch \
  --routes fixtures/routes.example.json \
  --database /tmp/recall-signal-hub-routed.sqlite3
```

路由配置声明稳定接收方标识、市场范围与最低风险等级；配置版本由内容哈希确定。目标集合在任务首次可领取前固定，并冻结每个接收方的端点与所用配置版本。每个目标独立保存领取、尝试、成功、退避与暂停状态，幂等键同时区分案件修订与接收方；一个接收方失败不会影响其他接收方。订阅之后被移除或端点改变时，已排队任务默认继续使用原版本与原端点；漂移只被报告，必须通过显式暂停或显式改派处理并留下审计事件，不会静默改派。无接收方匹配时记录为零目标的可解释终态。

`--webhook` 路由模式下每个任务投递到自身冻结的端点，无需单一环境变量端点；本地演示默认仍使用进程内接收替身，不联系真实接收人。

## 测试与构建

```bash
python -m pip install -e '.[test]'
PYTHONPATH=src python -m pytest -q
python -m pip wheel --no-deps .
```

测试包含本地 HTTP 采集和 webhook、Unicode、发布方格式错误、跨源风险冲突、坏记录隔离、事务故障回滚、游标竞争、退避持久化、租约过期、进程中断重投、按目标路由展开与独立记账、订阅漂移与 CLI 重启。没有外网测试。

## 后续扩展边界

接入层、产品归并、风险策略和告警投递拥有不同的输入、状态与验证入口，后续可分别演进。新增需求仍需以实际代码审查复杂度和主模块重叠；模块数量不直接等于合格题目数量。当前仓库不包含题面、预埋缺陷或题目答案。
