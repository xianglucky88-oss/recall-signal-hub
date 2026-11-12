# Recall Signal Hub

消费品召回信号的采集与汇聚服务。它从异构发布方获取产品风险记录，保存收到的原始业务载荷，跨来源归并同一产品的风险声明，通过事务 outbox 向接收端投递告警。

这是本地创建、已实现并通过测试的工程基线；没有连接真实监管站点或投入生产。HTTP 采集与 webhook 投递都有实际实现，通过本地 HTTP 服务验证，不依赖真实账号。

## 模块边界

| 责任 | 生产模块 | 可观察结果 |
| --- | --- | --- |
| 发布方接入 | `sources.py`、`transport.py` | 两种发布格式、增量页、响应大小限制、失败分类 |
| 记录标准化 | `normalize.py` | Unicode 兼容、字段校验、坏记录隔离、风险判定标识留存 |
| 产品身份与跨源归并 | `reconcile.py` | 相同品牌/型号/市场归为同一案例，各来源风险声明合并 |
| 风险策略 | `risk.py` | 可独立配置的规则、策略版本、判定原因、版本登记、告警触发规则 |
| 采集状态与证据 | `storage.py`、`database.py`、`pipeline.py` | 原始业务载荷、修订证据闭包、游标原子提交、失败退避、案例修订 |
| 证据包与离线核验 | `evidence.py` | 自包含证据包导出、字节/引用/派生逐项只读核验、篡改分类 |
| 告警可靠投递 | `outbox.py`、`alerts.py` | 队列租约、失效租约隔离、重投、接收方幂等键 |

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

## 证据包导出与离线核验

每个修订在生成时于同一事务冻结它实际依赖的最小证据闭包（来源声明快照、原始业务证据、当时风险规则快照），而不是在导出时用可能已被覆盖的当前 `source_signals` 行反推。导出固定在指定 `case_key` 与 `revision` 上：

```bash
PYTHONPATH=src python -m recall_signal_hub.cli export \
  --database /tmp/recall-signal-hub.sqlite3 \
  --case-key <case_key> --revision 1 --output /tmp/revision-1.zip
PYTHONPATH=src python -m recall_signal_hub.cli verify /tmp/revision-1.zip
```

`verify` 不接受数据库参数，也不联网：它重算原文字节摘要、检查引用闭包、从原文重放标准化与风险判定、重算修订摘要。核验只读并返回结构化报告，不回写案例、不发通知；包自洽只代表字节与关系一致，不认证发布者身份，系统不含签名服务。退出码：0 通过、2 核验失败、3 证据链不完整（旧修订无闭包或证据缺失，不产出包）、4 导出失败。完整篡改矩阵演示见 `demo/evidence_demo.py`（生成产物落在被 git 忽略的 `demo/workdir/`）。

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

## 测试与构建

```bash
python -m pip install -e '.[test]'
PYTHONPATH=src python -m pytest -q
python -m pip wheel --no-deps .
```

测试包含本地 HTTP 采集和 webhook、Unicode、发布方格式错误、跨源风险冲突、坏记录隔离、事务故障回滚、游标竞争、退避持久化、租约过期、进程中断重投与 CLI 重启，以及证据闭包冻结、早晚修订独立导出、缺件/篡改/替换/错误关联/合法重排的离线核验区分和失败导出无残留。没有外网测试。

## 后续扩展边界

接入层、产品归并、风险策略和告警投递拥有不同的输入、状态与验证入口，后续可分别演进。新增需求仍需以实际代码审查复杂度和主模块重叠；模块数量不直接等于合格题目数量。当前仓库不包含题面、预埋缺陷或题目答案。
