# 自适应分区写并行度（adaptivePartitionWriteParallelism）设计

> **状态**：Phase 1.3 已实现——热点判定集中在 LifecycleManager（driver）侧的 `PartitionHotnessTracker`，executor 只保留写路径；SOFT/HARD_SPLIT 统一计量（worker 可用性守卫）；升档为 fillTime 比例步进。功能定名 `adaptivePartitionWriteParallelism`（自适应分区写并行度）。Commits：Phase 1 `f398d73dc`、Phase 1.1 `5342d934c`、Phase 1.2 `6edd5b06e`（分支 `parallel-partition-write`）。验证结果见 §12。本文档第二部分按实际代码修订。

# 第一部分：调研与对比

## 1. 问题与 Celeborn 现状

- 写侧：**单活跃 location**（epoch 递增覆盖）；SOFT_SPLIT 不丢数据只发 revive，HARD_SPLIT 需重推，期间所有 map task 阻塞等新 location。
- **SOFT→HARD 黄金窗口（问题本质）**：SOFT 模式下 split 升级判定为 `fileLength > splitThreshold(默认 1G) 且 < partitionSplitMaximumSize(默认 2G) → SOFT_SPLIT`；`≥ 2G → HARD_SPLIT`（同步阻塞所有写该 partition 的 map task）。多 mapper 并发写同一 partition 时，单 location 的 fileLength 由 N 个 mapper 共同推高（涨速 ×N），从 1G（SOFT）到 2G（HARD）的**窗口宽度 = 1G / 聚合写速**——写得快则窗口只有秒级，revive + 路由切换来不及完成就升 HARD，shuffle write 全局阻塞。**1:N 并行写的价值正在于此：N 个 mapper 散到 P 个 location，单 location 涨速 ÷P，窗口同比例拉宽**。HARD 模式（≥1G 直接 HARD_SPLIT）无 SOFT 预警，更需要并行写直接压低单 location 涨速。
- **读侧天然支持一个 partition 多个文件**（方案的最大利好，零改动）：
  - `reducerFileGroupsMap: partitionId -> Set[PartitionLocation]`（`LifecycleManager.scala`）；
  - reader 经 `GetReducerFileGroup` 拿整个 Set，`CelebornInputStream.nextReadableLocation()` 顺序串流；
  - 去重已有：batch 头 (mapId, attemptId, batchId)，attempt 过滤 + 跨 location (mapId,batchId) 去重；batchId per-mapTask 全局单调，并行写不撞车；
  - worker 文件名 `partitionId-epoch-mode`（`PartitionLocation.java`），多文件无冲突。
- "单活跃 epoch"假设点（改造涉及）：`latestPartitionLocation`、`ChangePartitionManager.getLatestPartition`、`ShuffleClientImpl.newerPartitionLocationExists`、`updateLatestPartitionLocations`。

## 2. CIP-20 / PR #3260 深度分析（基于 PR 实际代码）

PR #3260：作者 ErikFang，2025-05 创建（[WIP]），diff ~3800 行，2026-04 被 stale bot 关闭，**GitHub 上零代码评审**。

### 2.1 机制总览
- executor 新增 `LocationManager`（392 行）：每 partition 一个 `PartitionLocationList`（locations + `locationStatusCode` 可用性标记 + sticky `index[]` 游标 + RWLock）；路由 = `mapId % size` 静态 hash + 跳过不可用（SOFT_SPLIT 的 location 仍允许排水续写）。LocationManager **自身不做任何并行度判定**，只判紧急度（`urgent = 全部不可用 && 无在途请求`）；
- **双通道**：紧急走原有 PbRevive；非紧急走新增的 `PbPartitionSplitReport`（新 MessageType 92）；
- LM 侧每 partition 一个 `PartitionLocationMonitor` + `PartitionSplitTimeSlidingHub` 滑窗（默认 180s / 10s bucket，TimeSlidingHub 从 worker 拥塞控制包搬到 common）；`latestPartitionLocation` 单值结构被 monitor 体系整体替换。

### 2.2 速率信号的本质：事件计数 × 假定字节数
- SOFT_SPLIT 记 `1 × threshold`（默认 1G）；HARD_SPLIT 记 `3 × threshold`（**3 倍为硬编码魔数**，代码留 TODO）；hard-after-soft 补差 2×；push 失败不计入；
- `pushSpeed = 窗口内累计 MB / 窗口秒数`；
- **关键缺陷：不是实测**——10 秒写满 1G 和 10 分钟写满 1G 在窗口里留下完全相同的一笔 1G，速度只能靠窗口内事件**频率**隐式表达；180s 窗口 + 高阈值部署下，需要多个事件才能积累出速度估计。

### 2.3 并行度公式与 expectedWorkerSpeed 魔数
- `nextReserveSlotCount = max(min(maxActiveLocation, ceil(pushSpeed / expectedWorkerSpeed)) − activeCount, 0)`；`maxActiveLocation` 默认取 numMappers；
- `expectedWorkerSpeed` 默认 **10MB/s 静态配置**。评审（Mridul）指出它应是集群当前负载、异构硬件、IO 特征的函数，高度动态，集群管理员无法给值；作者承认是 best guess，生产即设 10MB/s；
- **紧急通道一次只补 1 个 location**（`targetEpoch = currentMax + 1`），升档完全依赖非紧急通道的异步预分配（构造 context=null 的合成请求）；非紧急回复的 location 列表可为空，executor 靠 50ms 轮询收敛。

### 2.4 epoch 语义重用：侵入性连锁
- epoch 从"替换序号"变成"并行度刻度"：`ReviveRequest` 删 `epoch` 加 `clientMaxEpoch`/`urgent`；`ChangePartitionRequest` 拆成 `clientMaxEpoch` + `targetEpoch`；`inBatchPartitions` 去重从"首请求赢"改为"targetEpoch 最大者赢"（Set → partitionId→maxTargetEpoch）；
- proto `PbChangeLocationPartitionInfo.partition` **单值改 `repeated`**：proto3 单元素时 wire 兼容，但旧 client 收到**多元素**响应时按 merge 语义拼接 message，有产出畸形 PartitionLocation 的风险；
- executor 重试状态机重写：不再持有 ReviveRequest 轮询 reviveStatus，改为 50ms 间隔轮询 `getLocationOrReviveAsync`。

### 2.5 评审要点与关闭原因
- GitHub reviews/comments 均为空；实质讨论在 CIP Google Doc（主要评论者 Mridul Muralidharan）：(a) 多 location 写的重复 batch——由既有 (mapId,attemptId,batchId) 去重兜底，确认可接受；(b) `expectedWorkerSpeed` 难估（主要质疑）；(c) 其生态 split 阈值 10G，split 频率信号太钝，可能与方案有效性冲突；(d) 建议加能力协商字段；
- **关闭原因：stale bot 流程性关闭**——PR 描述的 Why/How tested 始终 TBD、长期无人正式 review、作者停更。**不是技术否决**：大 partition 多 location 并行写这个 idea 在社区没有反对意见。

### 2.6 工程质量信号
- 新核心类 `LocationManager`（392 行，含并发状态机）与 ReviveManager 双通道**零单测**；`PartitionLocationMonitorSuite` 4 例（放在 spark-it 模块但实为纯单测）+ `DynamicallySplitPartitionSuite` 端到端粗粒度时间断言；
- diff 残留 `// TODO check`、`// TODO: @漠云` 等 WIP 痕迹。

### 2.7 与本方案的同构点（公平起见）
- 决策都在 LM、每 partition 一份状态（PartitionLocationMonitor ≈ 本方案的 HotState）；
- 同样复用 worker 现成 split 信号、零 worker 改动；executor 路由同样 mapId 静态 hash；同样只升不降。

## 3. 本方案 vs CIP-20

### 3.1 总览表

| 维度 | CIP-20 / PR#3260 | 本方案（已实现） |
|---|---|---|
| **判定信号** | split 事件计数 × 假定字节数（SOFT=1G / HARD=3G 魔数），非实测 | **fillTime 实测**：首报时刻 − 该 epoch 真实分配时刻，per-epoch 独立对照 |
| **速率估算** | `expectedWorkerSpeed=10MB/s` 静态魔数（评审最大质疑） | **无需估算**：hotWindow 即 SLO（单 location 写满应慢于窗口） |
| **升档公式** | `ceil(pushSpeed/expectedSpeed) − activeCount`，依赖速率估算与窗口积累 | `ceil(window / fillTime)` 比例步进，**一次判定直达**，封顶 maxLocations（默认 8） |
| **信号守卫** | push 失败不计速率 | cause ∈ {SOFT,HARD} + 旧 location worker 可用 双守卫 |
| **决策位置** | LM PartitionLocationMonitor（per-partition） | LM PartitionHotnessTracker / HotState（per-partition，同构） |
| **执行通道** | 双通道：紧急只补 1、非紧急异步预分配（回复可空 + 50ms 轮询收敛） | 单通道原生 revive；**全集回复一次收敛**；HARD_SPLIT/失败预置 SUCCESS 立即换路 |
| **epoch 语义** | 重用为并行度刻度（连锁改 ChangePartitionRequest / inBatch 去重 / proto） | 保持单值 max 语义；新 location epoch 递增分配 |
| **协议兼容** | `partition` 单值→repeated（旧 client 多元素 merge 畸形风险）+ 新 MessageType/StatusCode | proto3 additive `additionalPartitions`（field 5），双向降级安全（§10） |
| **executor 改动** | LocationManager 392 行 + 重试状态机重写（无单测） | PartitionLocationGroup 276 行薄包装 + 退休上报，重试路径复用 |
| **测试** | Monitor 4 例 + 端到端时间断言 | 23 例（PartitionLocationGroup 7 + 判定/分配 10 + tracker 6） |
| **实现规模** | ~3800 行（WIP） | **~1300 行含测试** |
| **上限/回收** | maxActiveLocation 默认 numMappers；只升不降 | maxLocations 默认 8；只升不降（同） |

### 3.2 逐项分析

**判定信号**：CIP-20 的信号是"事件 × 假定字节数"——SOFT_SPLIT 一律记 1G，无法区分 10s 写满和 10min 写满，只能靠 180s 窗口内的事件频率隐式编码速度；高阈值部署（10G）下事件稀疏，频率估算失准（评审原话）。本方案的 fillTime 是**对每个事件的独立实测**（首报时刻 − 该 epoch 真实分配时刻），一次事件即可判定，无需窗口积累。两者共同的代价：检测延迟都是"写满一个 threshold"，这是 split 驱动方案的固有局限（根治见 §14）。

**并行度决策**：CIP-20 需要 `expectedWorkerSpeed`（静态 10MB/s，评审主战场）。本方案的窗口即 SLO——"单 location 写满应慢于 60s"，`ceil(window / fillTime)` 直接给出把写满周期拉出窗口所需的路数，**不含任何速度假设**。热点定义从"比某个假定的 worker 速度快"变成"比运维设定的 SLO 快"，后者是可理解、可调优的语义。

**决策位置（共识）**：两者都把 per-partition 状态与判定放在 LM。本方案进一步把热点状态提取为独立可单测的 `PartitionHotnessTracker`（依赖注入 + 时钟注入），`ChangePartitionManager` 本体只增 ~250 行。

**executor 间一致性**：CIP-20 非紧急通道回复可为空，executor 靠 50ms 轮询收敛到最新 location 集合；本方案每次 revive 响应携带**活跃全集**（max epoch + additionalPartitions），任何 executor 一次 revive 即与全局收敛，且按 epoch 有序插入保证不同 executor 收敛到**相同顺序**，`mapId % K` 分派一致。

**协议与兼容**：CIP-20 把 `partition` 单值改 repeated，旧 client 收到多元素响应有 merge 畸形风险，且新增 MessageType 92 / StatusCode URGENT_REVIVE 对旧 LM 不可识别。本方案只新增 additive 字段 `additionalPartitions`：老 client 忽略、新 client 缺失时退化为单 location。

**executor 侵入**：CIP-20 重写重试状态机（轮询替代 reviveStatus 等待）且核心类无单测；本方案 PartitionLocationGroup 276 行薄包装（快路径与现状零差异），重试路径复用现有 reviveStatus 机制（HARD_SPLIT/失败时预置 SUCCESS 实现立即换路）。

**复杂度与规模**：CIP-20 ~3800 行 WIP；本方案 ~1300 行含测试。差异来源：不重写重试状态机、不动 epoch 语义、不引入滑窗/速率估算、不引入第二条消息通道。

**上游化叙事**：CIP-20 是**流程性死亡而非技术否决**——GitHub 零代码评审，Google Doc 的实质质疑集中在两点：`expectedWorkerSpeed` 难估、高阈值下信号钝，而这两点正是本方案消除的。本方案可以作为 CIP-20 的延续上游化。

---

# 第二部分：技术方案（按实际代码修订）

## 4. 目标与非目标

**目标（已达成）**
- 单 partition 同时写多个 PartitionLocation（默认并行度 1，热点自动升档，上限可配）；
- location 不可用（SOFT_SPLIT/HARD_SPLIT/push 失败）时写不阻塞：立即切换候选 location，后台补充；
- **热点判定集中在 LM（driver），executor 只保留写路径**；
- 读侧、worker、master、Spark/Flink 集成层零改动；
- 双向版本兼容（新老 client × 新老 LM 均不报错，功能自动降级）。

**非目标（Phase 2）**
- worker/client 侧速率统计（解决"检测延迟=写满一个 threshold"的根本手段）；
- worker 主动过载上报（SOFT_SPLIT_OVERLOAD）；
- MAP partition 类型（Flink hybrid shuffle）；
- 并行度降档。

## 5. 总体数据流（实现版）

```
mapper pushData(partitionId)
   └─ ShuffleClientImpl.pushOrMergeData
        └─ PartitionLocationGroup.currentFor(mapId) ← mapId % 可写数 选可写 location
             │   （可写 = 非退休 + SOFT_SPLIT；soft 文件在 2G 硬分裂前持续可写）
             ├─ 正常 → 走现有 push/merge 路径（PushState 按 host 分桶，天然兼容）
             ├─ SOFT_SPLIT → group.retire(epoch, SOFT_SPLIT)（保持可写继续分摊写，写不阻塞）
             │     └─ 首次退休才发原生 ReviveRequest(epoch, SOFT_SPLIT) 上报 ← 唯一 executor 义务
             ├─ HARD_SPLIT / push 失败 → retire + 预置 SUCCESS：
             │     有另一可写 location 时重推线程立即换目标（不等 LM 响应）
             └─ 全部不可写 → 现有同步 revive 路径
LM (ChangePartitionManager → PartitionHotnessTracker):
  收到带 cause 的 revive → 活跃集维护：SOFT_SPLIT 且 worker 可用 → epoch 保留（仍可写）；
    HARD_SPLIT/失败/worker 不可用 → epoch 移出活跃集；
    计量条件 (cause ∈ {SOFT_SPLIT, HARD_SPLIT} 且旧 location 的 worker 仍可用) 满足时
    HotState 判定：fillTime = 首报时刻 - allocTime(epoch)
    < hotWindow ? desired = ceil(window / fillTime)（首报去重、单调递增、上限截断）；
    push 失败类 cause 一律只退休不判定（见 §8.2 统一计量规则）
  补差分配 gap = desired - 活跃数（互不相同 worker、epoch 递增，可为 0；
    SOFT 上报不释放容量，仅 desired 增长或硬性移除后补缺）
  revive 响应一律返回【活跃 location 全集】(newLocs 放 max epoch + additionalLocs 放其余，
    含仍可写的 soft epoch) → 所有 executor 收敛到同一集合
读侧：不变（fileGroups Set + 多 location 串流 + (mapId,attemptId,batchId) 去重）
```

## 6. 协议改动

### 6.1 proto（`common/src/main/proto/TransportMessages.proto`，已实施）
唯一的新增字段：
```proto
message PbChangeLocationPartitionInfo {
  int32 partitionId = 1;
  int32 status = 2;
  PbPartitionLocation partition = 3;   // 保持：epoch 最大的 location（老 client 语义）
  bool oldAvailable = 4;
  // The remaining active locations of the partition (full active set delivery).
  repeated PbPartitionLocation additionalPartitions = 5;
}
```
- proto3 新增字段天然向后兼容：新 client→老 LM 拿不到 additionalPartitions，退化为单 location；老 client→新 LM 忽略未知字段。
- Phase 1 曾短暂引入 `PbRevivePartitionInfo.desiredLocationCount`（client 上报期望并行度），**Phase 1.1 判定移到 LM 后已回收**（未发布，删除安全）。
- **不采用** PR#3260 把 field 3 改 repeated 的做法（多元素对旧 client 有 merge 畸形风险，见 §2.4）。

### 6.2 Java 层（已实施）
- `ReviveRequest`：**无新增字段**（Phase 1.1 已删除 desiredLocationCount/urgent）——executor 上报与上游原生完全一致。
- `ChangeLocationResponse` 增加第 4 个 case class 字段 `additionalLocs: util.Map[Integer, util.List[PartitionLocation]]`（带默认值，兼容既有构造点）；toPb 写入 `additionalPartitions`，fromPb 读回。
- `RequestLocationCallContext.reply` 增加带默认值的可选参数 `additionalLocations`。
- **Revive 消息允许同 partition 多 epoch 条目（Phase 1.6）**：executor 会把同一 partition 已退休的每个 epoch 的上报都放进一条 Revive（并行列表天然支持重复 partitionId）；`handleRevive` 按 **distinct partition 数**构造 `ChangeLocationsCallContext`，同 partition 的重复回复**首条生效、后续忽略**（原来会 logError 并覆盖，且按条目数计数会导致响应永远凑不齐而超时）。
- 兼容性说明：Revive/ChangeLocationResponse 是 **executor client ↔ LM（driver）的应用内消息**，executor 与 driver 必然使用同一 celeborn client jar；proto serde 路径另有 6.1 的 wire 兼容兜底。cpp client 的 `PbPartitionSplit` 路径不动；Flink client 直接消费 `PbChangeLocationResponse` proto，新增字段对其无感。

## 7. Executor client 侧实现

### 7.0 内存开销：薄包装 + 懒加载
- `reducePartitionMap` 值类型为 `PartitionLocationGroup`（276 行，其中核心逻辑 ~120 行），初始是**薄包装**：`volatile PartitionLocation single` + `volatile ParallelState parallel = null`（只比现状多一个对象头+一个字段）；
- `ParallelState`（active 列表 / retired 表 / maxEpoch）**只在首次 SOFT_SPLIT/HARD_SPLIT/push 失败或 revive 响应携带多 location 时 inflate**（双重检查锁）；
- `currentFor(mapId)` 快路径：`parallel == null` 直接返回 `single`，与现状开销相同；膨胀后 `single` 不再同步，`active` 列表是唯一事实源；
- 内存账目（5 万 partition/executor）：薄包装增量 ≈ 1MB；ParallelState 仅热点 partition 存在（个位数~几十个）。

### 7.1 PartitionLocationGroup 行为语义
- **选择策略：可写集合均匀取模**（`Math.floorMod`）：`currentFor` 与 `anotherUsableFor` 统一委托私有方法 `pick(mapId, excludeEpoch)`——可写 = 非退休 + SOFT_SPLIT（soft 文件在达到 `partitionSplitMaximumSize` 硬分裂前持续可写），`mapId % 可写数` 在可写子集（epoch 升序）上均匀分派；全部不可写返回 null。**soft location 是一等路由目标而非兜底**：若把 soft 排除出新写路由，稳态下几乎所有槽位都处于 soft 状态，全部 map 会 bump 到最新 1~2 个非退休 location，并行度塌缩成串行 churn（线上实证，见 §12）。同一 map task 稳定写同一 location（保住 PushState 按 host 聚合语义）；不同 map task 散到不同 worker；
- **退休语义**：`retire(epoch, cause)` 返回是否首次退休（调用方据此保证每 (partition,epoch) 只上报一次 revive）；**cause 可升级不可降级**——已 SOFT_SPLIT 退休的 epoch 之后又 HARD_SPLIT/失败时升级为硬性 cause（退出可写集合，与 CIP-20 的 SOFT→HARD 升级对齐），反向不降级；
- **`anotherUsableFor(mapId, excludeEpoch)`**：HARD_SPLIT/push 失败时在其余可写 location 中挑一个立即重推；
- **全集收敛**：`mergeActiveLocations(locations, fullSet)`（`synchronized`，消除并发 revive 响应下"检查重复→插入"的竞态）以 LM 下发的活跃全集为准，按 epoch 有序插入——不同 executor 收敛到**相同顺序**的 active 列表，`mapId % 可写数` 分派一致；跳过本地已退休 epoch；单 location 响应走 `updateLatest` 保持薄包装不膨胀；`fullSet=true` 时**清理已被 LM 消化（全集中不再出现）的退休 epoch**——退休条目规模收敛到"在途退休"量级，路由空间不会被死条目稀释（非全集的 `updateLatest` 单条更新不触发清理；LM 侧 soft epoch 保留在活跃集内，会继续出现在全集中，故 soft location 不会被误清理）；
- 可写数变化时 mapId 映射偏移——不影响正确性（读侧按 (mapId,attemptId,batchId) 去重）。

### 7.2 ShuffleClientImpl 接入（实现版）
| 位置 | 实现 |
|---|---|
| `reducePartitionMap` | 值类型 `PartitionLocationGroup`；私有 `getPartitionLocationMap()`；**公开 `getPartitionLocation()` 签名不变**，内部投影 `group.latest()` |
| `pushOrMergeData` | 选 location 改为 `group.currentFor(mapId)`；全不可用且开关开启时走同步 revive 后重取 |
| SOFT_SPLIT 回调（pushData 与 mergeData 两处） | 收敛于 `handleSoftSplitRetire`：`newlyRetired = group.retire(epoch, SOFT_SPLIT)`；仅首次退休且 `!mapperEnded` 发原生 ReviveRequest（无任何附加字段）。数据已落 worker，写不阻塞，soft location 保持可写继续分摊路由。**判定逻辑零残留** |
| HARD_SPLIT / push 失败 / mergeData 重提交 | 收敛于 `retireAndPresetIfAnotherUsable`：发 ReviveRequest + `group.retire(epoch, cause)`；若 `anotherUsableFor(mapId, epoch) != null`，**预置 `reviveStatus=SUCCESS`**，重推线程立即从 `currentFor(mapId)` 取另一可写 location 重推，不等 LM 响应；否则走现有 urgent revive+重推 |
| 重推 SUCCESS 分支 | 取 `group.currentFor(mapId)`（硬性退休 epoch 被排除，soft 仍可被选中） |
| `newerPartitionLocationExists` | `group.maxEpoch() > epoch` |
| `reviveBatch` 响应处理 | 开关开启：`group.mergeActiveLocations(partition + additionalLocs, true)` 全集收敛（含退休条目清理）；关闭：`group.updateLatest(loc)`。含 `loc == null` NPE 保护 |
| mergeData 路径、`PushState` | 零改动 |

### 7.3 executor 的唯一义务：退休上报
并行写下 executor 即使已切到其他活跃 location、不需要新 location，**也必须在首次退休某 epoch 时上报 revive**——这是 LM 感知 split/失败、维护活跃集合与做热点判定的唯一通道。

**该通道是承重墙（Phase 1.6 修复）**：ReviveManager 批量发送时的两个过滤曾经会把"本地已满足"（已有更新 location / mapper 已结束）的上报整条丢弃、并把同 partition 多 epoch 折叠为 max epoch 一条——上游单 location 语义下无害，但本方案中 LM 的活跃集记账依赖**每个 (partition, epoch) 的退休 cause 都到达**（soft 保留的 epoch 只有等到同 epoch 的硬性上报才会被移除）。丢弃会导致 LM 活跃集被死 epoch 撑大、gap 分配归零、executor 全集收敛后仍无可写 location（线上实锤：`Partition location ... is NULL!`，见 §12 Phase 1.6）。修复：开关开启时所有退休上报（含已满足的）按 (partition, epoch) 去重（保留最硬 cause）后一律转发；一条 Revive 消息可携带同 partition 的多个 epoch 条目，LM 侧按 distinct partition 计数完成响应、同 partition 首条回复生效（§6.2）。

### 7.4 正确性论证
- **去重**：batchId per-mapTask 全局单调，读侧 (mapId, attemptId, batchId) 去重不依赖 batch 在文件内的顺序/连续性 → 并行写安全；
- **重推重复**：换 location 重推产生的重复 batch 由读侧去重兜底（CIP-20 评审问答确认这是既有行为，§2.5）；
- **soft-retired 续写**：SOFT_SPLIT 语义下 worker 继续接收该文件的写（直到 2G 硬分裂才拒写），in-flight 不丢，新写也可以继续分摊到 soft location；`partitionSplitMaximumSize` 是硬性兜底上限；
- **预置 SUCCESS 的安全性**：HARD_SPLIT 时 worker 已拒收该 batch，旧 location 里只有之前成功落盘的 batch；重推到另一 location 后读侧两文件合并+去重，等价于现有"revive 后重推"路径；
- **speculation/rerun/stageEnd 后重跑**：既有路径不变。

### 7.5 并发设计（审计结论）

**线程模型**：`PartitionLocationGroup` 被四类线程并发访问——DataPusher push 线程（每 map task 一个）、push 回调线程（split/失败回调）、ReviveManager 单线程调度器（批量 revive 响应应用）、以及 push 线程直连（全不可用分支的同步 `revive()` 也会应用响应）。LM 侧同一 partition 的批处理由条纹锁 + `inBatchPartitions` 去重天然串行。

**无需加锁的点及依据**：
- `pick`/`currentFor` 读路径：CopyOnWriteArrayList + ConcurrentHashMap，快照迭代安全；
- `retire()` 首报信号：CHM `compute` 按 key 原子——并发退休同一 epoch 恰好一个线程拿到 true（每 epoch 一条 revive 上报的保证）；
- `retire` 与 `mergeActiveLocations` 交错：退休不丢——merge 跳过 retired；竞态下入 active 但 retired 有标记，硬性退休照样被 pick 跳过（soft 标记不影响可选性）；清理只删"LM 不再报告的 retired"，误删后该 epoch 亦不在 active，不会再被选中；
- `HotState.activeEpochs` 全部变更点（注册/移除/读）均在 `entry.synchronized` 内；热点判定去重的 `splitReported.add` 在 `state.synchronized` 内；
- `reviveStatus` 预置：字段本身 volatile（baseline 机制）。

**修复项**：`updateLatest` 非膨胀路径的 check-then-set 原非原子（同步 revive 使响应应用可被 push 线程并发执行，乱序响应可能旧 epoch 覆盖新 epoch）——已加 `synchronized`（与 `mergeActiveLocations` 同 monitor，重入安全）。baseline 是无保护 `put`（last-writer-wins），修复后严格优于 baseline。

**良性记录在案**：(a) 全不可用场景的同步 revive 羊群——重复 RPC 由 LM 侧 `inBatchPartitions` piggyback 合并，正确性无虞；(b) `removeShuffle` 后迟到上报会重建一个 HotState 条目——生命周期末尾的一次性微泄漏；(c) `latest()` 在 active 为空时回退 stale `single`——只用作 revive oldLoc 与拥塞门控代表，无正确性影响。

## 8. LM 侧实现（热点判定集中地）

### 8.1 HotState（稀疏，per (shuffleId, partitionId)）
热点状态全部收敛在独立组件 **`PartitionHotnessTracker.scala`**（233 行，从 ChangePartitionManager 提取）：`ChangePartitionManager` 持有一个实例，把判定依赖（latestPartitionLocation 查询、worker 可用性查询）以函数注入，所有时间戳由调用方传入（可注入时钟），tracker 可脱离 LM 独立单测。内部维护 `partitionHotStates: shuffleId -> partitionId -> HotState`：
```
activeEpochs      : LinkedHashSet[Integer]   // 可写 epoch（插入序）：soft 保留，hard/失败移除
hardRetiredEpochs : Set[Integer]             // 被硬性移除过的 epoch：迟到的 SOFT 上报不得复活（Phase 1.6）
allocTimeMs       : Map[Int, Long]           // 每个 epoch 的真实分配时间
splitReported     : Set[Integer]             // 已判定过的 epoch（首报去重）
desired           : volatile Int = 1         // 期望活跃 location 总数（单调递增，封顶 max）
```
- **稀疏**：普通 partition 无条目，活跃集合推导为 `{ latestPartitionLocation.epoch }`；任何并行模式下 oldEpoch >= 0 的 revive 都会建条目（onEpochRetired 维护可写集，且新 epoch 的 allocTime 必须记录，供后续判定）；
- **allocTime 来源**（比 executor 版更准的关键）：
  - 新 epoch：`allocateGapLocations` 分配成功时记录（真实创建时间）；
  - epoch 0：`LifecycleManager.handleRegisterShuffle` 成功路径调用 `recordInitialAllocTime`（putIfAbsent，重复注册不覆盖）；
  - 未知 allocTime 的 epoch（老数据）：该次 split 保守不升档；
- shuffle 注销时清理（含 `shuffleInitialAllocTimeMs`）。

### 8.2 热点判定（`onEpochRetired`，在 revive 请求到达时触发）
```
onEpochRetired(shuffleId, partitionId, epoch, oldPartition, cause, now):
  // 活跃集维护：soft 保留可写，其余移除；移除是终态——迟到的 SOFT 上报不复活已硬性移除的 epoch
  if (cause == SOFT_SPLIT && workerAvailableByLocation(oldPartition)
      && epoch ∉ hardRetiredEpochs) activeEpochs += epoch
  else { activeEpochs -= epoch; hardRetiredEpochs += epoch }
  measureEligible = (cause ∈ {SOFT_SPLIT, HARD_SPLIT})
                    && workerAvailableByLocation(oldPartition)   // 统一计量规则
  if (!measureEligible) return                     // push 失败类 / 已知不可用 worker / null 旧 location：只退休
  if (splitReported.contains(epoch)) return        // 同 epoch 重复上报去重
  allocTime = allocTimeMs[epoch] ?? (epoch==0 ? shuffleInitialAllocTime : null)
  if (allocTime == null || now - allocTime >= windowMs) { markSplitReported; return }  // 不升档
  if (splitReported.add(epoch)) {
      target = ceil(windowMs / fillTimeMs)          // 比例步进：K 个 location 各分 1/K 流速，
                                                    // 写满周期拉长到 K×fillTime ≥ window 所需的最小 K
      newDesired = min(maxLocations, target)
      if (newDesired > desired) desired = newDesired   // 单调递增 + 上限截断，无需去抖
  }
```
- **比例步进**：初版 +1/窗口去抖爬升太慢（极热分区到上限需 ~3 个窗口）；内部实验分支的"倍增+短冷却"证明了快速爬升对护住 SOFT→HARD 窗口的价值。比例步进比"倍增+冷却"更直接：fillTime 本身就携带了需要多少并行度的信息——10s 写满（window 60s）意味着需要 6 路才能把单 location 写满周期拉出窗口，一次判定直达（封顶 max=4）；30s 写满只需 2 路，不会过度分配。desired 单调递增 + per-epoch 首报去重 + 上限三重约束，去抖/冷却参数都不需要；
- **统一计量规则**：SOFT_SPLIT 与 HARD_SPLIT 统一计量——两者都是"阈值触发的 split"，同样反映快速写满；HARD 模式（`celeborn.client.shuffle.partitionSplit.mode=HARD`）下热点判定由此激活，不再只有 SOFT 模式受益。两个守卫条件：
  - **worker 必须仍可用**（`workerStatusTracker.workerAvailableByLocation`）：HARD_SPLIT 若由 worker 故障/过载触发（worker 已被 exclude），反映的是 worker 问题而非 partition 热点，不计量；
  - **push 失败类 cause 一律不计量**（PUSH_DATA_FAIL_* / CONNECTION_EXCEPTION 等）：网络/连接问题与分区热度无关，这是原则性排除。
- **判定依据**：fillTime = 全局第一个 split 上报到达时刻 - 该 epoch 真实分配时刻 ≈ `threshold / 该 location 聚合写入速度`。`threshold=1G`、`window=60s` ⇒ 热点线 ≈ **17MB/s 单 location 聚合速度**；
- **epoch 乱序免疫**：每个 epoch 独立对照自己的 allocTime——epoch 10 先于 epoch 5 写满互不影响（executor "间隔法"在这里会误判，这也是不用间隔法的原因）；
- **偏差说明（如实）**：epoch 0 的 allocTime 是 registerShuffle 时刻，mapper 可能更晚才开始写 → fillTime 高估 → 首个 epoch 判定**偏保守**（方向安全）；split 首报时刻略晚于真实写满时刻（一次 push 间隔内），fillTime 略偏大 → 同样偏保守；
- **检测延迟不变**：仍由 split 事件驱动（写满一个 threshold 才感知），这是 split 驱动方案的共同局限，根治需要速率统计（Phase 2）。

### 8.3 分配与回复解耦（`handleRequestPartitions`）
1. **补差分配**（`allocateParallelLocations`）：desired 从 HotState 读（无条目=1），截断上限；`surviving = currentActiveEpochs`（请求到达时已完成活跃集维护——soft 保留、hard/失败移除——无需再减 change.epoch；SOFT 上报不释放容量，仅 desired 增长或硬性移除后补缺），`gap = max(0, desired - surviving.size)`；`allocateGapLocations` 逐次分配（epoch 从 max(latest, surviving)+1 递增），每轮从 candidates 排除已选 worker（best-effort 互不相同）。gap 可为 0；
2. **全集回复**（`replySuccessFullSet`）：对每个请求回复该 partition 当前可写全集（`newLocs` 放 max epoch，`additionalLocs` 放其余，**含仍可写的 soft epoch**——客户端全集收敛因此不会清理它们），本轮分配 0 个也回全集——executor 间收敛；
3. 分配成功后登记活跃 epoch 与新 epoch 的 allocTime；失败路径不变；
4. 早返回路径（本地已有更新 location）同样回复 `latestLoc + additionalLocs`；
5. `updateLatestPartitionLocations` 用 `map.merge` 保留 max epoch（非并行路径同样受益）。

### 8.4 退休 location 的 commit：维持现状
SOFT_SPLIT 语义允许 split 后续写，提前增量 commit 会使续写 push 命中已提交文件（worker 按 "already committed" 拒绝），引入 revive 风暴。维持 StageEnd 全量 commit。HARD_SPLIT 既有增量 commit 路径不动。

### 8.5 不动的部分
- Master / SlotsAllocator：不动（K 个 location 用现有 `reserveSlotsWithRetry` 逐次 reserve）；
- worker：不动（SOFT_SPLIT 阈值触发是现成信号）；
- `workerStatusTracker.excludeWorkerFromPartition`：逻辑不变。

## 9. 配置项（`CelebornConf.scala`；`docs/configuration/client.md` 已再生成）

| 配置 | 默认 | 生效侧 | 说明 |
|---|---|---|---|
| `celeborn.client.shuffle.adaptivePartitionWriteParallelism.enabled` | false | client + LM | 总开关 |
| `celeborn.client.shuffle.adaptivePartitionWriteParallelism.maxLocations` | 8 | **LM** | 单 partition 活跃 location 上限 |
| `celeborn.client.shuffle.adaptivePartitionWriteParallelism.hotWindow` | 60s | **LM** | 单 location 写满耗时的热点判定窗口；升档目标 = ceil(窗口 / 写满耗时) |

开关关闭时所有代码路径与现状等价（PartitionLocationGroup 薄包装快路径、LM 走原有 `replySuccess`、不建 HotState）。仅有的 3 处微观差异（均有意识保留、方向更安全）：`updateLatest` 丢弃低 epoch 的迟到响应（现状是无条件覆盖）；SOFT_SPLIT 上报前增加 `!mapperEnded` 前置过滤（现状靠 ReviveManager 后置过滤，效果等价）；DataPushQueue 拥塞门控取 `group.latest()` 作为 partition 的代表 location（并行写下门控粒度略粗，不影响正确性）。

### 9.1 上线观测点（日志）

**LM/driver 侧**（回答"判定与分配是否生效"）：

| 日志 | 级别 | 位置 | 内容 |
|---|---|---|---|
| 升档判定 | INFO | `PartitionHotnessTracker.onEpochRetired` | partition、fillTime、窗口、boost 后 desired |
| 补差分配 | INFO | `ChangePartitionManager.allocateParallelLocations` | partition、分配数、epochs、worker hosts；候选不足时 WARN |
| 未计量退休 | DEBUG | `PartitionHotnessTracker.onEpochRetired` | push 失败类 / worker 不可用 / 超窗，附原因 |
| stage-end 摘要 | INFO | `PartitionHotnessTracker.removeShuffle` | 每 shuffle 一行：热点 partition 数、各自 desired 与判定次数 |

**Executor 侧**（回答"写路径是否在用多 location"）：

| 日志 | 级别 | 位置 | 内容 |
|---|---|---|---|
| 并行激活 | INFO | `reviveBatch` 响应处理 | activeCount 增长且 >1：partition、K、`epoch@host` 列表 |
| 立即换路 | INFO | `retireAndPresetIfAnotherUsable` | 旧 `epoch@host`、cause、新目标 `epoch@host`（预置 SUCCESS） |
| SOFT 首报退休 | INFO | `handleSoftSplitRetire` | `epoch@host` soft-split，继续排水（每 epoch 一条，天然去重） |
| 全不可用阻塞 revive | INFO | `pushOrMergeData` | 进入同步 revive、当时 maxEpoch |
| 收敛清理 | DEBUG | `reviveBatch` 响应处理 | 清理掉 LM 不再报告的退休条目数 |

## 10. 兼容性矩阵

| 组合 | 行为 |
|---|---|
| 新 client + 新 LM | 全功能 |
| 新 client + 老 LM | 响应无 additionalPartitions → executor 的 mergeActiveLocations 只收单 location，退化为现状，无异常 |
| 老 client + 新 LM | 响应新字段被老 client 忽略；LM 的 HotState 判定照常（老 client 的原生 revive 就是判定输入），只是老 client 不会使用多 location |
| worker 任意版本 | 无感（协议未动 worker 面） |
| Flink client | 直接消费 `PbChangeLocationResponse` proto，新增字段无感；不启用并行写 |
| cpp client | PbPartitionSplit 路径不动，不启用并行写 |

## 11. 实际改动清单

Phase 1（commit `f398d73dc`）→ Phase 1.1（commit `5342d934c`，判定迁移 LM + 组类简化）→ Phase 1.2（commit `6edd5b06e`，统一计量 + tracker 提取）→ Phase 1.3（比例步进 + 定名 adaptivePartitionWriteParallelism）→ Phase 1.4（retire cause 升级 + 全集收敛清理 + 命名定版）合计：

| 文件 | 内容 | 规模 |
|---|---|---|
| `common/src/main/proto/TransportMessages.proto` | `additionalPartitions` 一个字段 | +2 |
| `common/.../message/ControlMessages.scala` | additionalLocs 字段 + 双向 serde | +25 |
| `common/.../CelebornConf.scala` + `docs/configuration/client.md` | 3 个配置 + 文档再生成 | +43 |
| `client/.../PartitionLocationGroup.java` | 薄包装 + ParallelState + 统一 pick 选择（无判定逻辑）；retire cause 升级 + 全集收敛清理 | 276 |
| `client/.../ShuffleClientImpl.java`、`ReviveManager.java` | §7.2 接入；`handleSoftSplitRetire` / `retireAndPresetIfAnotherUsable` 收敛三处重复 | ~+200 |
| `client/.../ChangePartitionManager.scala` | 补差分配 + 全集回复；热点状态全部委托给 tracker（自身 734 行） | ~+250 |
| `client/.../PartitionHotnessTracker.scala` | HotState + 统一计量判定 + 比例步进 + 依赖注入（latestEpoch / workerAvailable / 时钟） | 233 |
| `client/.../LifecycleManager.scala`、`RequestLocationCallContext.scala` | registerShuffle 记录 allocTime、additionalLocs 透传 | ~+60 |
| `client/src/test/.../PartitionLocationGroupSuiteJ.java` | 7 例 | 190 |
| `client/src/test/.../ChangePartitionManagerAdaptiveParallelismSuite.scala` | 10 例（判定逻辑 + 分配回复） | 511 |
| `client/src/test/.../PartitionHotnessTrackerSuite.scala` | 6 例：HARD+健康升档 / HARD+不可用不升档 / push 失败不升档 / HARD 与 SOFT 等价 / 比例步进直达上限 / 慢速后续不降级 | 145 |

## 12. 验证

**Phase 1（executor 判定版，commit f398d73dc）已验证**：
- 目标单测全绿：LocationGroupSuiteJ 5 + HotTrackerSuiteJ 8 + ChangePartitionManager 套件 4；
- client 全量回归全绿（JUnit 43 + ScalaTest 37，32 分钟）；`./dev/reformat` 通过。

**Phase 1.1（LM 判定版，commit 5342d934c）验证**：
- `ChangePartitionManagerAdaptiveParallelismSuite` 重写为 10 例：fillTime 升/不升档、同 epoch 首报去重、窗口去抖、上限截断、epoch 乱序（epoch 10 先于 epoch 5 互不影响）、allocTime 未知保守不升、epoch 0 用注册时间、补差分配不同 worker、分配 0 仍回全集、desired 在失败 revive 后保持；
- `LocationGroupSuiteJ` 适配新构造器后保留 5 例；`HotTrackerSuiteJ` 删除（逻辑迁移）；
- 定向套件全绿（LocationGroupSuiteJ 5/5 + ScalaTest 43/43）；
- LocationGroup 简化（pick 合并、synchronized mergeAll）经行为等价审查。

**Phase 1.2（统一计量规则 + PartitionHotnessTracker 提取，commit 6edd5b06e）验证**：
- 计量条件统一为 `(cause ∈ {SOFT_SPLIT, HARD_SPLIT}) && workerAvailable(oldPartition)`，HARD 模式热点判定激活；push 失败类原则性不计量；零 worker / 零 proto 改动（曾评估 worker 侧文件长度上报方案，被否决：不改 worker）；
- ShuffleClientImpl 提取 `handleSoftSplitRetire` / `retireAndPresetIfAnotherActive`，收敛三处 SOFT_SPLIT 重复块与两处退休+预置重复块（+80/−73，纯重构无行为变化）；
- 热点状态提取为 `PartitionHotnessTracker.scala`，ChangePartitionManager 863→734 行纯委托；
- 新增 `PartitionHotnessTrackerSuite` 4 例；HARD_SPLIT 例改为"worker 不可用不升档"语义；
- 定向套件 47/47 全绿；`./dev/reformat` 通过；
- **client 全量回归全绿**（offline，33.5 分钟，BUILD SUCCESS）。

**Phase 1.3（比例步进 + 定名）验证**：
- 升档从"+1/窗口去抖"改为 `desired = min(max, ceil(window / fillTime))`，删除 `lastBoostTimeMs` 去抖字段；三重约束（per-epoch 首报去重、单调递增、上限截断）替代去抖；
- `PartitionHotnessTrackerSuite` 4→6 例：新增"比例步进直达上限"（30s→2、25s→3、10s→封顶 4）与"慢速后续不降级"两例；
- `ChangePartitionManagerAdaptiveParallelismSuite` 适配比例步进语义（"debounce"例重写为"比例步进无去抖直达上限"，"上限截断"例改为 40s→2 / 25s→3 / 15s→4 / 10s→封顶）；
- 功能定名 `adaptivePartitionWriteParallelism`，配置 key 全量替换，编译 + spotless + 配置文档 golden 检查通过；
- **定向回归全绿**（rename + R4 后）：LocationGroupSuiteJ 5/5（JUnit）+ ScalaTest 49/49（含 tracker 新增 2 例）。

**Phase 1.4（retire 语义修正 + 收敛清理 + 命名定版）验证**：
- 实证复审发现两个 executor 侧缺陷并修复：(a) `retire()` 原用 `putIfAbsent`，SOFT_SPLIT 退休的 epoch 之后再 hard-fail 时 cause 不升级、永远充当兜底写目标——改为 `compute` 实现"可升级（SOFT→硬性）不降级"，与 CIP-20 的 SOFT→HARD 升级语义对齐；(b) 退休条目原只增不删，`mapId % size` 哈希槽被死条目稀释导致路由不均——`mergeActiveLocations(locations, fullSet)` 在全集回复时清理"已退休且 LM 不再报告"的 epoch（LM 已消化该退休），非全集的 `updateLatest` 不触发清理；
- 命名定版：`LocationGroup` → `PartitionLocationGroup`（补 partition 锚点）、`anotherActiveFor` → `anotherUsableFor`（pass-1 会返回 soft-retired，"Active" 名不副实）、`updateSingle` → `updateLatest`、`isInflated` → `hasParallelState`、`mergeAll` → `mergeActiveLocations`；LM 侧命名不动；
- `PartitionLocationGroupSuiteJ` 5→7 例：新增 `testRetireCauseUpgrade`（SOFT→HARD 升级后不再兜底、硬性 cause 不降级）与 `testFullSetMergeEvictsProcessedRetiredEpochs`（LM 未消化时保留、消化后清理、非全集更新不清理）；
- **定向回归全绿**：PartitionLocationGroupSuiteJ 7/7（JUnit）+ ScalaTest 49/49；`spotless:check` 通过。
- 后续调参：`maxLocations` 默认值 4 → 8（`client.md` 已再生成）；3 处依赖旧默认值的封顶断言测试改为显式设置 `maxLocations=4`，与产品默认值解耦。复跑定向回归全绿（JUnit 7/7 + ScalaTest 49/49）。

**Phase 1.5（SOFT_SPLIT 改为可写路由目标——并行度塌缩修复）**：
- **线上实证（问题发现）**：重度倾斜作业（单热点 partition，69 路上限）下观测到并行度塌缩——split 按 epoch 顺序串行发生（连续 epoch、间隔 ~300ms、落在不同 worker），fillTime 恒为 ~200ms 与路数无关，单 map 对单 worker 的 push 过半数收到 softSplit 响应。根因：`pick()` 把 soft-retired 排除出新写路由（只做兜底），稳态下 69 个槽位几乎全部处于 soft 状态，所有 map 的 `mapId % 69` 起点落在 retired 槽位后向前 bump 到同一个最新 location，1000+ 并发 map 瞬间灌满它 → 串行 churn 自我维持，有效并行度塌缩为 1~2 路，queueStall 不消失；
- **修复**：SOFT_SPLIT location 改为一等可写路由目标（worker 在 2G 硬分裂前持续接收写），`pick()` 改为在可写集合（非退休 + soft）上均匀取模；LM 侧 `onEpochRetired` 对"SOFT_SPLIT 且 worker 可用"的 epoch 保留在活跃集（其余 cause 移除），`allocateParallelLocations` 的 surviving 不再减 change.epoch（soft 上报不释放容量，仅 desired 增长或硬性移除后补缺）；全集回复包含 soft epoch，客户端收敛不会误清理；
- **连带修复**：(a) `pick()`/`latest()` 的 size→get TOCTOU 竞态（fullSet 收敛 removeIf 并发缩容导致 ArrayIndexOutOfBoundsException，线上实锤）——改为 toArray 快照迭代；(b) re-push 成功日志按 batch 刷屏——按 firstRetire 去重；(c) 阻塞等新 location 的耗时记录（`revive()` 同步路径 + retry 等待）；
- 测试适配：`PartitionLocationGroupSuiteJ` 的 `testRetireSwitchesCurrent` 重写为 `testSoftSplitStaysWritableHardSplitExcluded`（soft 参与路由、hard 排除、升级后排除）；`PartitionHotnessTrackerSuite` 新增 3 例活跃集保留/移除断言；`ChangePartitionManagerAdaptiveParallelismSuite` 分配数与全集回复期望适配新语义（SOFT 首报 surviving 含原 epoch、gap 减 1、回复含 soft epoch）。

**Phase 1.6（退休上报通道修复——线上 NULL location 事故）**：
- **线上实证（问题发现）**：Phase 1.5 上线后重度倾斜作业失败：`CelebornIOException: Partition location for shuffle 0 partition 868 is NULL!`。driver 侧同一 epoch 被反复以 PUSH_DATA_FAIL 上报且永不分配新 location。根因是上报通道两级丢弃 + LM 侧无防复活：(a) ReviveManager 批量发送时把"本地已满足"（`newerPartitionLocationExists` 或 mapperEnded）的请求整条丢弃——executor 对 soft-retained epoch 的后续 HARD/失败上报几乎总被该过滤吞掉（客户端 maxEpoch 已被全集回复推高）；(b) 同 partition 同批次只保留 max epoch 一条，低 epoch 的退休上报被折叠丢弃；(c) 跨 executor 乱序时迟到的 SOFT 上报会把已硬性移除的 epoch 重新加回活跃集。后果链：LM 活跃集被死 epoch 撑大（远超 desired）→ `surviving.size >= desired` → gap 恒为 0 → 不再分配新 location → executor 把所有已知 location 硬性退休后全集收敛仍无可写 → NULL 异常 → task 失败；
- **修复**：(a) ReviveManager 开关开启时把所有退休上报（含已满足的）按 (partition, epoch) 去重（保留最硬 cause，非 SOFT 覆盖 SOFT）后一律转发，一条 Revive 可携带同 partition 多个 epoch；(b) `handleRevive`/`ChangeLocationsCallContext` 按 distinct partition 计数完成响应、同 partition 首条回复生效；(c) `PartitionHotnessTracker` 新增 `hardRetiredEpochs`，硬性移除为终态，迟到 SOFT 不复活；
- **行为变化说明**：老 epoch 的硬性移除现在即时生效，但补缺分配仍由"前沿 epoch 事件"（无可写 location 的 revive / split 首报）触发——热点 partition 前沿事件持续发生，活跃宽度在下一次前沿分配时补回 desired，不会塌缩；
- 测试：`PartitionHotnessTrackerSuite` 新增"迟到 SOFT 不复活"例；新增 `RequestLocationCallContextSuite`（同 partition 重复回复忽略、按 distinct 数完成）。

**待办（上生产前必须做）**：
- 集成测试（`tests/spark-it`）：`partitionSplit.threshold=10m`、重倾斜 Spark 作业——(a) reducer 数据与原生 shuffle 对拍一致；(b) 该 partition 最终有 >1 个 committed location 且数据无重复无丢失；(c) 写阶段无 revive 长尾（对比开关前后耗时）；
- 故障注入：写中 kill 一台 worker → 写不中断，数据无丢无重；
- 真实集群灰度：小流量开 `adaptivePartitionWriteParallelism.enabled`，观察 worker slot 占用与 StageEnd commit 时长。

## 13. 风险与开放问题
1. **检测延迟**：split 事件驱动，首次判定要等"写满一个 threshold"（边界 ~60s，10G 阈值部署 ×10）；滞后期间等价于现状（单 worker 写），不会更差。根治：worker/client 速率统计（Phase 2）；
2. ~~爬升延迟~~（已解决）：比例步进一次判定直达目标并行度（极热分区首个 split 即封顶），不再有 +1/窗口 的爬坡期；残留的"爬升"只有新 location 的 reserveSlots RPC 时延；
3. **epoch 0 判定偏保守**：allocTime=registerShuffle 时刻早于实际开写，fillTime 高估 → target 低估（方向安全）；
4. **HARD_SPLIT 统一计量的残余误判（已如实评估）**：worker 仍可用但 HARD_SPLIT 并非热度所致的两个子情形——(a) 该 partition 数据已被 commit（mapper 已结束后的迟到写）；(b) worker 内存紧张触发的整体 HARD_SPLIT。两者都罕见（(a) 需要迟到写恰好越过阈值；(b) 内存紧张通常先走向 exclude worker），且误判后果有上限：desired 封顶 maxLocations，仅多占少量 slot，不继续升。接受该残余误判换取 HARD 模式的热点检测能力；
5. **worker 占用放大**：热点 partition 占 K 台 worker；比例步进下极热分区**一次判定即占满 K 台上限**——maxLocations 是唯一刹车，生产上线前建议按集群规模谨慎设值（默认 8）；
6. **单 partition 磁盘占用上界（Phase 1.5 后）**：soft location 可写至 2G 硬上限，单 partition 磁盘占用上界 = K × partitionSplitMaximumSize（如 69 路 ≈ 138G 散布集群）。soft 文件持续接收新写但 worker 在 soft 状态下不上报拥塞（PushDataHandler 既有行为），拥塞感知存在缺口，后续可单独处理；
7. **AQE skew read 交互**：`splitSkewedPartitionLocations` 多文件场景理论兼容，必须 IT 回归；
8. **replicate 模式**：K 个 location = 2K 个 slot，需压测 reserve 开销；
9. **StageEnd commit 列表变长**：超大 shuffle 需观察；
10. **LM 单点复杂度**：HotState 稀疏且无滑窗，且已提取为独立的 PartitionHotnessTracker（可单测、依赖注入），ChangePartitionManager 本体只增 ~250 行——CIP-20 评审对 LM 复杂度的警惕值得在上游化时预案（本实现比滑窗方案轻一个量级是主要论据）。

## 14. Phase 2 展望（不在本期）
- worker/client 侧速率统计替代 split 事件驱动（检测延迟 → 10~20s，且与 split 阈值解耦）；
- worker 写入速率监控 + `SOFT_SPLIT_OVERLOAD` 主动上报（CIP-20 Further Optimization 的方向）；
- 并行度降档与热点消散回收；
- LM 滑窗估算（可参考 PR#3260 `PartitionLocationMonitor`，仅当速率统计证明必要）。
