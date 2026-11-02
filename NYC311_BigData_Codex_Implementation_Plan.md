# NYC 311 Big Data 项目实施路线图（Codex 执行版）

> 项目：**NYC 311 Streaming Analytics, Resolution-Time Prediction & Capacity Planning**  
> 依据：上传的 `Big_Data_Proposal (1).pdf`（Qijun Feng & Zhuchang Gu，1 页）  
> 文档用途：把 Proposal 转成可执行的工程任务、验收条件和可直接交给 Codex 的总提示词。  
> 规划日期：2026-11-02。此文档是一份**实施计划**，不代表代码、实验或模型已经完成。

---

## 0. 先理解我们要交付什么

原始 Proposal 的目标是：持续接收 NYC 311 服务请求、预测请求的解决时间、找出季节性激增，并向市政机构提出容量分配建议；同时通过 1/2/4/8 个 Worker 和 10×/100× 数据量，论证项目的 Big Data 性质。

| Proposal 目标 | 对应交付物 | 如何证明完成 |
|---|---|---|
| 连续接收请求和更新 | Spark Structured Streaming + 可重放事件生产器 | 直播演示创建/关闭事件、乱序/重复、重启恢复 |
| 处理历史大数据 | Spark 清洗、分区 Parquet、数据质量报告 | 处理真实 NYC 311 数据而非仅玩具样本 |
| Kaplan–Meier | 按 Agency / Complaint Type / Borough 的生存曲线 | PNG 图、表、分组样本量、删失比例 |
| Cox 或梯度提升生存模型 | CoxPH 训练、推断、保存模型 | 基于创建时间的时间切分测试 + C-index |
| 请求到达时预测解决时间 | 流式微批创建事件触发预测 | 每张新工单写入预测值和模型版本 |
| 季节性和天气激增 | 每日趋势、异常检测、天气关联分析 | 冷天 Heat/Hot Water 案例及图表（若数据支持） |
| 机构容量建议 | 排名 + 显式假设的排队/吞吐量情景模拟 | +10%/+20%/+30% 方案及不确定性说明 |
| 横向扩展 | 1/2/4/8 Workers 的基准实验 | 原始记录、用时、吞吐量、加速比、曲线 |
| 10×–100× 数据 | 标注为**合成扩展**的压力测试数据 | 原始/10×/100× 的资源与时间结果 |

**完成标准：** 不要求最复杂的架构，要求每条 Proposal 主张都有可运行的程序、测试或图表证据。不能用 README 里的“预期结果”冒充真实执行结果。

### 重要现实限制（必须写进最终报告）

1. **纽约市官方 311 开放数据每日更新，并非提供逐条、实时 CDC 事件。** 本项目用真实历史数据构建 `created/closed` 事件重放流；这是**历史事件重构 + 模拟流式到达**，不可表述为官方实时 Kafka feed。必要时可以另做每日快照轮询，但它仍不等价于源系统 CDC。
2. NYC 已把 311 数据拆分为 `2020–Present` 和 `2010–2019` 两个数据集。新数据的 ID 仍是 **`erm2-nwe9`**；虽然网站将 **Complaint Type** 的显示名称改为 **Problem**，API 后端字段仍是 **`complaint_type`**。
3. **没有真实的工作人员人数、服务能力和干预数据**，就不能声称“增加 20% 人手一定使处理时间减少 X%”。我们能提供有明确假设的 **what-if scenario**，不能提供已验证的因果结论。
4. Spark 处理百万级记录不代表全模型必须用 Spark 训练。允许 Spark 分布式计算全量特征，Cox 在经过说明的有代表性样本上训练，并明确二者处理规模不同。

---

## 1. 建议技术架构

### 1.1 技术栈（控制复杂度）

| 组件 | 首选技术 | 作用 |
|---|---|---|
| 编程语言 | Python 3.11 | ETL、建模、实验、可视化 |
| 分布式数据处理 | PySpark + Spark Structured Streaming | 历史 ETL、流式微批和扩展实验 |
| 原始/中间数据 | JSONL、Parquet（Snappy 或 ZSTD） | 可重放、可压缩、可分区 |
| 持久化状态 | PostgreSQL | 存工单当前状态、去重事件、模型预测、仪表盘汇总 |
| 生存分析 | `lifelines` | Kaplan–Meier、CoxPH、删失处理 |
| 基线和编码 | scikit-learn | 训练/验证拆分、类别编码及简单基线 |
| 图表 | pandas + matplotlib / seaborn 可选 | 生成报告图表（尽量使用 matplotlib） |
| 展示 | Streamlit（最后再做） | 展示预测、积压、趋势和容量情景 |
| 集群实验 | Spark Standalone + Docker Compose | 可配置 1/2/4/8 Workers |
| 测试 | pytest | 数据、模型、流处理、恢复与 CLI 测试 |
| 外部天气 | Open-Meteo Historical Weather API | 日温度/降水与投诉趋势关联 |

**优先采用 Spark 的目录流源，不要第一天就引入 Kafka、Airflow、Kubernetes。** 文件源支持 Spark Structured Streaming、checkpoint 和故障恢复，更符合两人课程项目的时间限制。Kafka 作为有余力时的扩展。

版本策略：让 Codex 选择**互相兼容、确实能安装和运行的固定版本**，提交 lock/依赖文件，避免 `latest`。对 Java、Python、Spark 和 Docker 镜像的兼容性执行一次实际 smoke test。

### 1.2 数据流架构

```text
NYC Open Data (Socrata, erm2-nwe9)
               |
               v
     按日期分段下载原始数据
      data/raw/ (with manifest)
               |
               v
        PySpark Batch ETL
  标准化 -> 质量检查 -> 去重
               |
               v
     data/silver/*.parquet
          /             \
         /               \
        v                 v
历史分析与训练         事件重构 / Replay Producer
(KM/Cox/天气/容量)         (created/closed/duplicate/late)
        |                           |
        v                           v
  artifacts/models/          data/events/incoming/
  artifacts/figures/                  |
        |                             v
        |                   Spark Structured Streaming
        |                 validate -> dedup -> microbatch
        |                             |
        |                             v
        |                  PostgreSQL (upsert + checkpoint)
        |                  tickets / events / predictions
         \                            /
          \                          /
           v                        v
             Streamlit / 报告 / Demo

独立实验：同一套 Spark ETL / Stream 计算，在 1、2、4、8 Workers
和原始、10×、100× 数据规模下测量并保存性能指标。
```

**状态语义：**
- 事件文件与 Spark checkpoint 负责可重放的输入进度。
- PostgreSQL `ticket_current` 保存当前工单状态（open/closed），`processed_events` 和版本号确保重复写入不会污染结果。
- Spark 用 event-time window/水位线保存增量窗口统计状态；超过水位线的事件如何补算必须单独说明。
- 生存模型是离线定期训练，流式创建事件使用**已训练模型进行预测**，不要每条事件重新训练。

---

## 2. 建议的仓库目录

让 Codex 建立以下目录（可以调整名称，但不要把所有逻辑塞进一个 notebook）：

```text
nyc311-bigdata/
├── AGENTS.md                     # Codex 的项目规则
├── README.md                     # 先决条件、快速运行、演示和可复现性
├── pyproject.toml                # Python 依赖与工具配置
├── .env.example                  # 仅示例值，不能有真实密钥
├── .gitignore
├── Makefile                      # 一键运行入口
├── docker-compose.yml            # Spark + Postgres（UI 可选）
├── config/
│   └── default.yaml              # 日期、采样、路径、阈值、种子
├── src/nyc311/
│   ├── config.py
│   ├── ingest/
│   │   ├── download_311.py       # API 分页、重试、恢复
│   │   └── download_weather.py
│   ├── etl/
│   │   ├── schema.py
│   │   ├── clean.py
│   │   ├── quality.py
│   │   └── build_silver.py
│   ├── streaming/
│   │   ├── event_builder.py      # 快照 -> 创建/关闭事件
│   │   ├── producer.py           # 原子写入微批文件
│   │   ├── consumer.py           # Spark readStream
│   │   ├── sink.py               # 事务化/幂等落库
│   │   └── db_schema.sql
│   ├── analysis/
│   │   ├── survival.py
│   │   ├── train_cox.py
│   │   ├── predict.py
│   │   ├── seasonality.py
│   │   └── capacity.py
│   ├── benchmark/
│   │   ├── expand_data.py
│   │   ├── run_benchmark.py
│   │   └── plot_scaling.py
│   └── cli.py                    # 统一 CLI
├── dashboard/
│   └── app.py
├── tests/
│   ├── fixtures/                # 小而真实的边界案例 / 合成样例
│   ├── test_ingestion.py
│   ├── test_etl.py
│   ├── test_censoring.py
│   ├── test_streaming.py
│   ├── test_model.py
│   ├── test_capacity.py
│   └── test_benchmark.py
├── data/                         # 整个目录 gitignore
│   ├── raw/
│   ├── silver/
│   ├── events/incoming/
│   ├── events/staging/
│   ├── checkpoints/
│   └── weather/
├── artifacts/
│   ├── models/
│   ├── figures/
│   ├── metrics/
│   └── benchmark/
└── reports/
    ├── methodology.md
    ├── final_report.md           # 课程报告，建议英文
    └── demo_script.md
```

### 预期的一键命令（必须由 Codex 实现，不是已经存在的命令）

```bash
make setup         # 环境检查/安装步骤说明
make test          # 单元测试；不依赖外网
make smoke         # 极小 fixture 跑完关键端到端流程
make fetch-sample  # 下载有限时段的 NYC 311 样例
make etl           # 构建 silver Parquet
make analyze       # KM + Cox + 趋势 + 情景分析
make stream-demo   # 生产事件、启动流并验证结果
make bench         # 按环境能力运行真实性能实验
make report        # 汇总图表/指标，产出可交的报告文件
make dashboard     # 打开 Streamlit
```

如果某个目标需要额外 Docker 服务，Makefile 应打印明确的启动命令并做依赖检查，不应静默失败。

---

## 3. 阶段一：建立仓库与最小可运行环境

### Codex 要做什么

1. 初始化 Git、Python 项目、依赖锁、pytest、日志和 YAML 配置。
2. 写 `README.md`：Windows/macOS/Linux 环境要求、Docker 使用方法、命令、数据来源、资源需求。
3. 配置 `docker-compose.yml`：PostgreSQL、Spark master、可扩容 Spark worker、可选 dashboard。
4. 生成只有少量数据的**离线测试 fixture**，避免没有 API Token 就无法开发。
5. 配置 `.env.example`，例如 `SOCRATA_APP_TOKEN=`、`DATABASE_URL=`，真实密钥仅在本地 `.env`。
6. 用 `make smoke` 证明 Python、Spark 和 Postgres 能够互相通信。

### 这一阶段的验收

- `pytest` 可以运行。
- Spark 任务成功处理小型测试数据。
- `docker compose up -d` 启动基础服务，容器健康检查通过。
- README 记录实际通过的运行环境与版本。

---

## 4. 阶段二：下载并清洗真实 NYC 311 数据

### 4.1 官方数据位置

- 官方数据页：<https://data.cityofnewyork.us/d/erm2-nwe9>
- 数据集 ID：`erm2-nwe9`（2020 至今）
- 常用 SODA2 资源 URL：`https://data.cityofnewyork.us/resource/erm2-nwe9.json`
- SODA3 文档：<https://support.socrata.com/hc/en-us/articles/43491231777047-Using-the-SODA3-API>
- 源数据 schema 变动说明：<https://www.nyc.gov/opendata/news/all-news/311-Service-Requests-Updates>

建议首先做小规模验证，然后扩大。**不要第一次运行就下载全部 2300 万行左右的数据。**

| 运行模式 | 范围 | 用途 |
|---|---|---|
| `smoke` | 100–1000 条离线 fixture | 证明各模块能工作 |
| `pilot` | 例如 2025 年某 30–90 天，或限制最多 5 万条 | 检查 schema、清洗、可视化 |
| `full` | 例如 2025-01-01 至 2026-06-30 | 训练、测试、历史分析（量级可能为数百万行） |
| `benchmark` | 从真实样本复制并换唯一 ID 的**合成扩展** | 1×/10×/100× 压力测试，禁止混入真实性统计 |

这些日期是**推荐实验设置**，不是 Proposal 原文规定。下载实际条数和数据快照时间写进 manifest。

### 4.2 首先获取的字段

| API 字段 | 用途 | 注意事项 |
|---|---|---|
| `unique_key` | 工单唯一 ID | 按字符串处理，不能默认数值连续 |
| `created_date` | 创建时间、模型输入 | 必须存在且可解析 |
| `closed_date` | 生存分析结束事件 | 可以为空；不能把空值填成 0 |
| `agency` | 机构 | 可能缺失、变化 |
| `complaint_type` | 投诉类型 | 官网页面显示“Problem”，API 名不变 |
| `descriptor` | 投诉详情（可选） | 高基数，最初不必入模 |
| `borough` | 行政区 | 规范为五大区/UNKNOWN |
| `status` | 当前状态演示、质量校验 | **不能作为新工单预测特征** |
| `incident_zip` | 地理特征（可选） | 需清洗；不公开精确个人地址 |
| `latitude`, `longitude` | 地理分布（可选） | 仅做适当聚合 |

其他字段按需增加，不下载 `incident_address` 等无关的精确地址字段。

### 4.3 下载器的工程要求

- 支持 `$select`、`$where`（以 `created_date` 分日期窗口）、`$order`，通过分页或 keyset pagination 避免默认 1000 条限制。
- 使用 `X-App-Token` 作为可选配置；无 Token 时提供低频率、小规模下载功能。
- 对 HTTP 429、5xx 使用指数退避、有上限的重试及超时。
- 日期范围拆成天/周/月；记录每片下载状态，失败后可以只补失败片。
- 每片保存原始数据、行数、时间范围、请求参数（但不泄漏 Token）、文件校验和，以及下载时间。
- API 当前数据会变化；不能宣称重复下载必定得到逐字相同的历史快照。
- 进行 schema introspection/验证，不把 API 字段名写死到无法适应非关键列变化的程度。

### 4.4 使用 Spark 做 ETL

1. 读取原始 JSON/CSV，指定明确 schema。
2. 将日期按 NYC 本地时区解释并规范化到统一格式，写下时区和 DST 假设。
3. 对空值、无效行政区、不合法日期、重复 `unique_key` 做统计。
4. 如果 `closed_date < created_date`，放入异常记录，不可算成负解决时间。
5. 对同一个工单的多个快照，依据明确的快照版本/观测时间留最新版本；只有一个最终快照时不得杜撰历史修改过程。
6. 保存 `data/silver/` Parquet，推荐按 `created_year` / `created_month` 分区。
7. 输出 `data_quality.json`：输入、输出、缺失、重复、无效日期、open/closed 数量。

### 阶段验收

- 相同输入可幂等重复 ETL；`unique_key` 无重复。
- `input_rows = accepted + rejected + deduplicated`（严格定义计数口径）。
- 真实数据经过 Spark 写入 Parquet，能使用 Spark SQL 聚合出各 Borough/Agency/Complaint Type 的请求量。
- 全部指标通过程序计算并写入文件，不能靠手工填写。

---

## 5. 阶段三：把历史快照重构成流式事件

### 为什么需要这一步

Socrata 公开记录的 `created_date` 和 `closed_date` 可以重构教学用事件序列，但**无法从最终快照恢复真实的每一次状态修改和更新先后**。我们只重构有证据的创建/关闭事件；其他版本更新若要演示，需要明确标记为**合成更新测试事件**。

### 5.1 事件格式（示例）

```json
{
  "event_id": "hash(request_id,event_version)",
  "request_id": "12345678",
  "event_type": "CREATED",
  "event_time": "2025-02-01T14:30:00Z",
  "ingest_time": "2026-11-02T19:00:00Z",
  "event_version": 1,
  "agency": "HPD",
  "complaint_type": "HEAT/HOT WATER",
  "borough": "BRONX",
  "is_synthetic_update": false
}
```

对有合法关闭日期的工单额外生成 `CLOSED` 事件（示例 `event_version=2`）。对于无法从原始数据证实的中间状态，禁止标注为真实来源。

### 5.2 Replay Producer

1. 从 `silver` 读取样本，按照事件时间重构 `CREATED` 和 `CLOSED`。
2. 将事件分成固定大小的 JSONL 文件，先写 `.tmp`，完成后原子 rename 到 `data/events/incoming/`。
3. 支持 `--speed`、`--batch-size`、`--seed`、`--max-events`。
4. 可选择注入一部分**故意重复**和**故意乱序/延迟**的事件，并将注入比例记录在日志中。
5. Producer 可连续追加新文件，而 Consumer 一直保持 `readStream` 运行；同时支持有限事件的确定性测试模式。

### 5.3 Spark Consumer

1. 用固定 schema 的 `spark.readStream.json(...)` 读取增量目录。
2. 对事件 `event_id` 去重，并用事件版本保护当前工单状态。
3. 使用 checkpoint 保存流式进度；通过 `foreachBatch` 将微批写入 PostgreSQL。
4. **`foreachBatch` 默认是 at-least-once，不自动保证数据库恰好写一次。** 需要数据库唯一键、幂等 UPSERT 和批次提交记录。
5. 对同一个请求，在同一个微批内先合并/选择最高版本，再写 `ticket_current`，避免批内乱序。
6. 对先到达的 `CLOSED(version=2)`、后到达的 `CREATED(version=1)`，最终状态必须仍是 CLOSED。
7. PostgreSQL 至少包含：
   - `processed_events(event_id PRIMARY KEY, ...)`
   - `ticket_current(request_id PRIMARY KEY, latest_event_version, status, ...)`
   - `ticket_predictions(request_id, model_version, prediction_time, predicted_median_days, p_closed_30d, ...)`
   - `batch_commits(pipeline_id, batch_id, ...)`
8. 若使用 JDBC 中转到 staging table，则最终 UPSERT + `batch_commits` 标记必须处于同一个 PostgreSQL 事务中；任务失败后允许重试而不重复计数。
9. 维护各 Agency/Borough 的当前 open 数、最近 1 小时/1 天新增数和关闭数。窗口聚合的 watermark、迟到事件及补算限制要写明。
10. 新工单的预测由 `CREATED` 事件触发，加载已存在的 Cox 模型版本。微批预测需要设置上限以避免将大量数据 `collect()` 到 driver。

### 5.4 强制的故障测试

| 场景 | 期望结果 |
|---|---|
| 重复事件发 2 次 | 工单状态和汇总不重复 |
| 关闭事件先于创建事件到达 | 最终状态仍 CLOSED |
| 停止 consumer 并从同一 checkpoint 重启 | 不漏已投递数据、不重复累计 |
| 数据库成功提交后、checkpoint 完成前发生故障 | 重放不导致第二次业务写入 |
| 一条事件字段缺失/日期非法 | 隔离并记录，不让整条 pipeline 悄悄坏掉 |
| 非常迟到的事件 | 当前工单状态正确；窗口指标的补算或限制有文档解释 |

**最终 Demo：** 一边运行 producer，一边展示 Postgres 表或 Streamlit 面板，看 open 数、新增数、预测值变化，再演示停止/恢复 consumer。

---

## 6. 阶段四：解决时间分析（Kaplan–Meier）

这是 Proposal 最重要的统计设计之一：**未关闭的请求是右删失（right-censored），不是处理时间为零的请求**。

### 6.1 正确构造标签

以分析时点 `as_of` 为界，对 `created_date <= as_of` 的合法工单：

```text
event_observed = 1，当 closed_date 存在且 created_date <= closed_date <= as_of
                 0，其他有效情况（尚未观察到在 as_of 前关闭）
end_date       = closed_date（event_observed=1）
                 as_of（event_observed=0）
duration_days  = (end_date - created_date) / 86400
```

- 非法 `closed_date < created_date` 不进入训练，而应放到质量报告。
- 合法的零持续时间可以按模型要求设置很小正数（例如 1 分钟），并记录调整条数。
- `status` 与 `closed_date` 冲突时记录数量，不能悄悄根据 `status` 改写事件标签。
- 所有实验固定 `as_of`，防止重新运行时偷偷改变结果。

### 6.2 生存曲线

使用 `lifelines.KaplanMeierFitter`，分别按：

- Agency：展示样本量最大的若干机构。
- Complaint Type：展示最常见且具有足够样本的类型。
- Borough：五大行政区，并单独统计 UNKNOWN。

每张图包含图例、样本数、删失比例、95% CI（适当时）、时间轴单位。未达到 50% 关闭概率时，KM median 可能不存在或为无穷大，应显示 **Not reached**，而不是硬造数字。

### 输出

- `artifacts/figures/km_by_agency.png`
- `artifacts/figures/km_by_complaint.png`
- `artifacts/figures/km_by_borough.png`
- `artifacts/metrics/survival_summary.csv`
- `tests/test_censoring.py` 包含时间截断、零天、空关闭时间、负天数边界案例。

---

## 7. 阶段五：在工单创建时预测解决时间（Cox）

### 7.1 先有可比较的基线

Baseline：**按训练集 Agency × Complaint Type 或 Complaint Type 的 KM 中位数**预测。稀少分组退回更粗粒度的全局结果。这样 Cox 是否真的有效有参照，不会只展示一个孤立分数。

### 7.2 特征（只能包含创建时可知的信息）

**允许：**

- `agency`（前提是该字段在新工单提交时可知；无法确定时写入局限）
- `complaint_type`
- `borough`
- 创建时 `month`, `day_of_week`, `hour`
- 经过清洗/截断的 `incident_zip`（可选）

**禁止数据泄漏：** `closed_date`、当前 `status`、最终 resolution text、创建之后才更新的动作日期、最终处理时长，以及当时尚不可得的未来天气统计。

类别取最常见 N 类，其余归入 OTHER；保存编码器并保证训练和推断一致。先尝试 2–5 万条确定性样本，资源允许扩大到 10–20 万，记录采样方法和比例。全量 Spark ETL 与 Cox 样本训练的规模不能混淆。

### 7.3 固定时间切分（建议）

| 子集 | 工单创建日期 | 用来观察是否关闭的截止时点 |
|---|---|---|
| Train | 2025-01-01 ～ 2025-12-31 | 2025-12-31 23:59:59 |
| Test | 2026-01-01 ～ 2026-06-30 | 2026-09-30 23:59:59 |

> 这只是推荐默认实验窗口；如果 API 数据缺失，可在 config 中调整并在报告记录。训练必须在时间逻辑上模拟“2025 年末能够知道什么”，不能因为今天下载的数据已经关闭就把 2026 年的关闭事实写进 2025 年训练标签。最新下载快照中一些非时间字段可能曾被修改，这一限制同样需披露。

### 7.4 训练及评估

1. `lifelines.CoxPHFitter(penalizer=...)`；遇到共线性/类别过多先简化特征。
2. 保存 Cox 模型、编码器、模型元信息、训练时间、训练/测试的切分参数。
3. 对测试集报告 **concordance index (C-index)**，与基线比较；正确处理删失可比较样本对。
4. 输出预测中位数，以及 `P(resolved within 7/30/90 days)`；如果曲线未达到 50%，中位数要标记未达到，不应随意截断。
5. 可额外展示已关闭请求的 MAE，但必须注明其**仅对已关闭样本计算，会有选择偏差**，不能把它当主指标。
6. 检查 Cox proportional hazards 假设；无法满足时在报告中明确限制，不要假装模型完美。
7. 用一条从未见过的 CREATED 事件演示：输入工单的创建时特征，输出预测和 `model_version`。

### 输出

- `artifacts/models/cox_model.pkl` 或其他可靠序列化产物。
- `artifacts/metrics/model_evaluation.json`。
- `artifacts/figures/model_evaluation.png`。
- CLI 示例（由 Codex 实现）：`python -m nyc311.cli predict --agency HPD --complaint-type 'HEAT/HOT WATER' --borough BRONX --created-at '2026-02-01T12:00:00'`。

---

## 8. 阶段六：季节性与天气驱动的激增

### 8.1 季节性

基于真实工单的 `created_date`，按日期、投诉类型、行政区计算：

- 每天请求总数、7 天移动平均。
- 按月份、星期几的长期周期图。
- 以同类请求的历史参考水平检测突增（例如滚动基线 + z-score，阈值和最小计数写在配置中）。

避免把“周末比工作日低”之类规律直接误报为异常，基线最好按星期、季节分别估计。

### 8.2 天气

- 数据来源：<https://open-meteo.com/en/docs/historical-weather-api>。
- 先以 NYC 一个固定代表点的日最低温、日最高温、降水做演示，写明**这是全市天气近似**，不是每个 Borough 的精确气象观测。
- 重点展示真实数据中存在的 `HEAT/HOT WATER`（名称以实际 API 数据为准）等投诉类别，与低温日的关系。
- 可以比较低温日与正常温度日的日投诉数量，并控制月份/季节做简单分层，避免冬季本来就有更多供暖投诉造成虚假解释。
- 使用 Spearman 相关或简单回归展示**关联性**；不能据此断言天气因果效应。

### 输出

- `daily_volume.csv`, `surge_events.csv`, `weather_joined.csv`。
- `seasonal_volume.png`, `weather_heat_complaints.png`, `surge_examples.png`。
- 至少一段文字说明分析发现，以及是否存在明显支持/不支持 Proposal 举例的证据。

---

## 9. 阶段七：机构容量推荐（情景分析，不冒充因果）

### 9.1 先找出优先机构-行政区组合

对 `(agency, borough)` 汇总：

- 最近请求量 `arrivals`。
- 当前 backlog / 未关闭比例。
- KM 中位解决时间（或“尚未达到”）。
- 近 30–90 天的关闭量和到达量。
- 样本数阈值（例如不少于 500，阈值可配置）。

先筛出候选机构-行政区，再限制 Top 5 或 Top 10 输出。不同机构处理不同投诉，排行不代表“机构绩效优劣”，要同时展示投诉类型构成。

### 9.2 建立最小可解释的 what-if 模拟

按日构建某组合的到达请求数量 `A_t`、关闭请求数量 `C_t`，使用 FIFO 队列作为**简化假设**。建立一个历史基线模拟，然后情景化提升服务能力：

- `+10%` daily service capacity
- `+20%` daily service capacity
- `+30%` daily service capacity

模拟基线与历史真实解决时间的数量级、趋势、median 先进行对照。如果校准明显失败，**不能发布貌似精确的“新增容量节省 X 天”的结论**；应保留为仅供讨论的敏感性结果或调整模拟方法。

必须在报告中写出：

- 关闭事件数只是能力的**代理变量**，不等于真实工作人员数量或每人产能。
- FIFO、每日处理能力按比例变化、请求难度相同等都是假设。
- 情景中的预测改善不代表真实增员的因果效应。
- 结果可用于**优先调查哪些部门**，而非直接形成政府采购/招聘决定。

### 输出

`capacity_scenarios.csv`：至少包含 `agency`, `borough`, `sample_n`, `current_backlog`, `baseline_median_days`, `scenario_capacity_uplift`, `scenario_median_days`, `modelled_delta_days`, `assumptions`, `calibration_status`。

注意：模拟的 `baseline_median_days` 应和真实 KM median 分开列，不能混为一谈。若某一项在数据中无法估计就填 NA，并说明原因。

---

## 10. 阶段八：Big Data 核心证据（扩展性实验）

### 10.1 两组实验，缺一不可

**A. Worker 扩展实验：** 对同一个 Spark ETL/聚合任务，在 **1、2、4、8 Workers** 下分别运行，记录完整数据工作负载、资源、用时和吞吐量。

**B. 数据规模实验：** 对固定 Spark 配置，以**真实样本数据 1×、合成扩充 10×、100×**运行相同作业，验证资源使用和增长规律。

可额外测试 streaming 微批输入吞吐和端到端延迟，但要明确区分：ETL throughput、producer rate、consumer input rate、数据库写入瓶颈。不能把其中一个速度冒充全系统吞吐。

### 10.2 生成合成压力数据的正确方法

从真实样本中确定性重复记录，并将每个复制品的 ID 变为 `original_unique_key + replica_id` 或对应 hash，避免因 ID 冲突导致重复去重。保留与原样本近似相同的字段分布。

- `synthetic=true`、`replica_id`、`seed` 写入元数据。
- **10×/100× 只用于系统压力和性能，不参加真实 311 事件数、模型评估或政府建议统计。**
- 资源不足则让压测配置基数变小：例如原始 5 万 → 50 万 → 500 万；不能造出“100× 已通过”但没有运行的结论。

### 10.3 实验公平性

1. 确保 worker 是**实际 Spark worker/executor 进程**，而不是把 Python multiprocessing 进程叫作 Spark Worker。
2. 每种配置至少重复 3 次（如果资源允许），分别保存数据并取 median。
3. 控制同一输入、同一业务逻辑、固定分区数量或记录分区策略（策略必须说明）。
4. 记录 CPU 核数、内存、Spark 版本、Executor 配置、磁盘、是否本地单机部署。
5. 如果 8 个 Docker worker 都运行在**同一物理电脑**，这只能说明**单机容器集群中的 worker 扩展实验**，不能直接宣称跨机器水平扩展。
6. 如 Worker 增加但总 CPU 不增加，应解释这主要测的是调度/进程划分；如总资源同步增加，应标明投入资源差异。
7. 若某配置 OOM/失败，保存错误原因并如实列为 Failed，不伪造结果。

### 10.4 指标与图

```text
elapsed_seconds = end_time - start_time
throughput_rows_per_sec = processed_rows / elapsed_seconds
speedup(N) = runtime(1 worker) / runtime(N workers)
parallel_efficiency(N) = speedup(N) / N
```

保存原始 benchmark CSV/JSON（包括 run_id、timestamp、commit hash、worker_count、rows、data_multiplier、synthetic、elapsed、throughput、CPU/RAM、run_status）。

预期图形：

- `runtime_vs_workers.png`
- `throughput_vs_workers.png`
- `speedup_efficiency.png`
- `runtime_vs_data_size.png`

**不是所有图都必须上升/下降得漂亮。** 负加速也是合理研究结果，解释启动开销、shuffle、I/O、内存、Postgres 等瓶颈即可。

---

## 11. 阶段九：可视化、论文式报告和最终 Demo

### 11.1 最小展示界面（Streamlit）

仅做四个页面，不做复杂前端：

1. **Overview**：工单数量、分布、当前 backlog、流式数据更新情况。
2. **Survival & Prediction**：KM 曲线、输入新工单并显示预测和模型版本。
3. **Seasonality & Capacity**：天气关联、激增事件、机构容量假设表。
4. **Scalability**：1/2/4/8 Workers、1×/10×/100× 图及每组实际运行状态。

页面应读取 `artifacts/` 和 PostgreSQL 的**真实输出**。没有数据时显示 “Not run / No data available”，禁止硬编码演示数字。

### 11.2 最终报告（建议英文）

`reports/final_report.md` 建议结构：

1. Abstract and Business Question
2. Dataset, Provenance, Schema and Limitations
3. System Architecture and Streaming Semantics
4. Data Quality and Preparation
5. Survival Analysis and Right Censoring
6. Prediction-at-Creation Model and Temporal Validation
7. Seasonality / Weather Associations
8. Capacity Scenario Analysis and Assumptions
9. Scale-out & Scale-up Experiments
10. Failures, Limitations, Ethical Considerations and Future Work
11. Conclusion
12. References

至少附上真实运行产生的图。每个实验必须写：数据量、起止日期、机器条件、方法、指标、限制。

### 11.3 演示脚本（5–8 分钟建议）

- 0:00–1:00：问题背景、官方数据、架构。
- 1:00–2:30：生产者产生真实重构事件，Spark Consumer 处理，演示状态更新。
- 2:30–3:15：插入重复/乱序并重启 Consumer，验证幂等。
- 3:15–4:30：KM 曲线及 Cox 模型新工单预测。
- 4:30–5:30：季节性/天气和容量 what-if 情景。
- 5:30–7:00：1/2/4/8 Workers 与 1×/10×/100× 的实际曲线。
- 7:00–8:00：局限、结论和可复现命令。

---

## 12. 两人分工和优先级

| 子系统 | 建议负责人 A | 建议负责人 B | 集成点 |
|---|---|---|---|
| 项目/环境/数据接口 | 主责 | 参与 | 统一 schema、日期和配置 |
| Spark Batch ETL | 主责 | 测试/审阅 | `data/silver/` |
| 流式重放/状态/恢复 | 主责 | 测试/审阅 | Postgres tables + `ticket_current` |
| Kaplan–Meier + Cox | 提供数据 | 主责 | `artifacts/models` + 预测 API |
| 天气/季节性/容量 | 提供 Spark 聚合 | 主责 | `artifacts/metrics` |
| Worker 实验 | 主责 | 作图/解读 | `artifacts/benchmark` |
| Dashboard/报告 | 后端联调 | 主责 | 所有真实产物 |
| 全流程 QA/演示 | 共同 | 共同 | `make smoke` / `make report` |

### 三层优先级

**P0（必须先通）：** 官方真实数据、小样本 ETL、目录流、状态幂等、KM + Cox、最小端到端预测、测试和 README。

**P1（Proposal 必须交）：** 天气/季节性、容量情景、1/2/4/8 Workers、10×/100× 基准、完整图表与报告。

**P2（有余力才做）：** Kafka、实时 API 服务、交互式地图、自动定时重训、复杂模型、云端多节点部署。

### 推荐工作顺序

- **里程碑 M1：** `setup + smoke + fetch-sample + etl` 都成功。
- **M2：** `stream-demo` 成功，重复/乱序/重启恢复测试通过。
- **M3：** KM 图和 Cox 测试报告成功产生，创建事件有预测。
- **M4：** 天气、季节、容量情景报告产生。
- **M5：** 完成真实 benchmark，Dashboard 和 final report 引用实际数据。

禁止未通过 M1 就一次性生成所有统计图和报告；这容易导致图表完全建立在随机数或虚构数据上。

---

## 13. 最终验收清单

- [ ] 能在新机器按 README 重建可运行环境，列出外部依赖。
- [ ] 真实数据来源可追溯、下载可恢复，并有本次数据快照 manifest。
- [ ] Spark 在真实数据上运行 ETL，Parquet 可查询，有数据质量报告。
- [ ] Producer 以确定性方式重放历史 CREATED/CLOSED 事件。
- [ ] Structured Streaming 使用 checkpoint；Postgres 幂等写入。
- [ ] 重复、乱序、迟到、故障重启场景有自动测试。
- [ ] 右删失标签计算正确；KM 按机构/类型/行政区绘图。
- [ ] Cox 只使用创建时特征，基于时间切分测试，报告 C-index 和 baseline。
- [ ] 创建事件发生时能写出模型版本和预测。
- [ ] 有季节性图和天气关联分析，不能把相关性说成因果。
- [ ] 有容量情景模拟，所有假设、校准、无法估计的情况清晰披露。
- [ ] 真实跑出 1/2/4/8 Workers 的结果；如果环境不支持，如实记录未完成配置。
- [ ] 真实跑出 1×/10×/100× 结果；合成数据与真实分析彻底隔离。
- [ ] `make test`、`make smoke`、`make report` 可执行且文档清楚。
- [ ] 报告中的每个数值都有对应的 CSV/JSON/日志来源，未运行项目显示为未运行。
- [ ] Git 未包含 `.env`、密钥、大型 raw 数据和 checkpoint。

---

## 14. 直接复制给 Codex 的总 Prompt

> **使用方法：** 先在 Codex 工作目录新建项目文件夹，把**本 Markdown 文件**放进项目根目录，并确保 Codex 能读取它；然后复制下面 `Codex Master Prompt` 的完整内容作为任务。建议打开能运行终端命令、可编辑文件的 Codex 模式。Codex 能写代码，但下载公网数据、安装 Docker、启动服务、跑 8 Workers 仍取决于实际环境和权限。

### Codex Master Prompt（从下一段开始复制）

```text
你是我的 Senior Data Engineer + Machine Learning Engineer，负责在当前工作目录独立实现课程项目：
NYC 311 Streaming Analytics, Resolution-Time Prediction & Capacity Planning。

请先完整阅读本仓库中的：
NYC311_BigData_Codex_Implementation_Plan.md
它是唯一的项目实施规格说明。原始 proposal 的核心要求：
(1) 311 streaming pipeline，包含更新、乱序、迟到、故障恢复；
(2) Spark 上的 1/2/4/8 Worker 扩展实验以及 10x/100x 数据压力测试；
(3) Kaplan–Meier + Cox 或 gradient-boosted survival，处理右删失；
(4) 创建时解决时间预测，并在最近的时间段上测试；
(5) 天气/季节性激增；
(6) 政府机构容量分配 what-if 模拟。

执行原则：
1. 不要停留在写计划或伪代码；直接创建完整仓库、真实源代码、配置、测试、Makefile、Docker Compose、文档与可复现的运行流程。
2. 分里程碑 M1→M2→M3→M4→M5 实现。每个里程碑都应实际运行对应测试；确认通过后继续做下一个，不需要每个阶段都等待我确认。
3. 优先让 P0 形成真实端到端闭环，再做 P1。不要为了漂亮架构堆 Kafka/Kubernetes/Airflow，也不要做缺乏后端的炫酷前端。
4. 数据源是 NYC Open Data 数据集 erm2-nwe9。先验证 API 和字段；当前官网 UI 的 Problem 字段在 API 中仍叫 complaint_type。真实 311 开放数据每日更新，不能假称它是真正的 CDC 实时事件流。流式输入请重构真实 created/closed 事件，并清楚标记 synthetic update、duplicate 和 out-of-order 测试数据。
5. 对网络失败、App Token 缺失、Docker 不可用、计算资源不足，应使用可复现的本地 fixtures 开发并继续做能完成的部分；记录 BLOCKER 和可执行的人工步骤。不得用合成数据冒充官方真实数据。
6. 绝不虚构 benchmark 结果、模型分数、图表结论、截图或成功执行记录。只能从实际运行输出 CSV/JSON 中构建报告。失败配置记录 Failed 或 Not Run。
7. 处理 open ticket 必须采用 right censoring，固定 as_of。模型特征只允许创建时可获得的信息，时间切分训练/测试，防止数据泄漏。
8. foreachBatch + PostgreSQL 要设计端到端幂等，覆盖重复输入、乱序、故障重启、数据库提交后重放。
9. 机构容量改善只能报告基于显式假设且做过校准的情景模拟，不得写成增员的因果效应。
10. 将下载、预处理、模型、分析、基准实验、可视化拆成可独立运行的模块。核心代码使用 Python/Spark，单测使用 pytest。使用固定依赖版本，配置放 YAML/环境变量，密钥不入库。
11. 项目全程可观测：数据规模、过滤数、训练样本、删失率、运行时间、失败原因均写入机器可读的日志/指标产物。
12. 创建 AGENTS.md，写明本项目的约束；保持 README 和测试与代码同步。最终英文报告在 reports/final_report.md，并且引用真实产物及来源。

请依次执行：
A. 探测当前工作目录、Python、Java、Docker、网络及资源是否可用，明确可运行模式。
B. 创建仓库结构和 make setup/test/smoke/fetch-sample/etl/analyze/stream-demo/bench/report/dashboard 命令。
C. 以极小离线 fixture 跑通 Spark ETL、Producer、Consumer、Postgres、模型预测数据格式，验证恢复与幂等。
D. 若外网可用，分日期窗口下载真实 NYC 311 样本，生成 manifest 和 silver Parquet，运行数据质量检查。
E. 实现 Kaplan–Meier、CoxPH、创建时预测、时间 holdout 评估及必要图表，并保留 baseline。
F. 实现天气/季节性分析以及机构容量 what-if 模拟，进行基本校准，明确限制。
G. 在资源允许时实际执行 1/2/4/8 Worker、1x/10x/100x 基准；失败时记录具体配置和原因，不能编造。
H. 用实际产生的结果实现简洁的 Streamlit dashboard 和 final_report，写清可复现步骤及 demo_script。
I. 执行全部可运行的 tests/smoke，输出最终总结：已完成的文件、通过的测试、实际运行的数据量和指标、尚未运行或阻塞的部分、具体命令。

验收的首要目标：一个老师/助教拿到仓库后，能使用 README + Makefile 在可用的环境中运行最小真实流程，并可以复核所有统计、模型结果和 benchmark。

现在开始读取规格并实施，不要只回复计划。
```

### 如果 Codex 一次执行中断/上下文不够，用这个续跑 Prompt

```text
继续实现 NYC311_BigData_Codex_Implementation_Plan.md。
先检查实际 Git 状态、README、测试结果、artifacts、尚未通过的里程碑，
不要重写已正确完成的代码。列出阻塞并尽可能自行修复。
按 M1→M5 顺序推进；执行真实测试并保留运行记录；禁止虚构结果。
本轮结束时更新 README、reports/final_report.md 和 IMPLEMENTATION_STATUS.md，
记录完成/未完成项、测试命令、真实指标和下一步可执行命令。
```

---

## 15. 参考资料与引用（代码注释/报告可使用）

1. NYC Open Data, *311 Service Requests from 2020 to Present*: <https://data.cityofnewyork.us/d/erm2-nwe9>
2. NYC Open Data, *311 Service Requests Updates*: <https://www.nyc.gov/opendata/news/all-news/311-Service-Requests-Updates>
3. NYC Open Data, *Open Data APIs / SoQL*: <https://www.nyc.gov/opendata/get-started/user-guides/open-data-apis>
4. Socrata, *Using the SODA3 API*: <https://support.socrata.com/hc/en-us/articles/43491231777047-Using-the-SODA3-API>
5. Socrata, *Application Tokens*: <https://dev.socrata.com/docs/app-tokens>
6. Apache Spark, *Structured Streaming Programming Guide*: <https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html>
7. lifelines, *KaplanMeierFitter*: <https://lifelines.readthedocs.io/en/latest/fitters/univariate/KaplanMeierFitter.html>
8. lifelines, *CoxPHFitter*: <https://lifelines.readthedocs.io/en/latest/fitters/regression/CoxPHFitter.html>
9. Open-Meteo, *Historical Weather API*: <https://open-meteo.com/en/docs/historical-weather-api>

**区分来源：** 项目研究目标来自用户上传的 Proposal；技术栈、目录结构、时间切分、数据库 schema、验收测试、what-if 设计、任务分工和 Codex Prompt 是为了落实 Proposal 而补充的工程实施建议。
