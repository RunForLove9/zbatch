# BatchQueryManager 架构设计

## 概述

`BatchQueryManager` 是一个高性能的并发批量查询管理器，通过将多个独立的查询合并为一次批量操作来优化数据库或外部 API 调用。

## 架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BatchQueryManager                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────┐    ┌─────────────────────────────────────────┐    │
│  │   Context   │    │          配置                          │    │
│  │             │    │  - MaxBatchSize  (10-1000)       │    │
│  │   (ctx)     │    │  - MaxWaitTime   (10ms-5s)      │    │
│  └─────────────┘    │  - MaxCtxTimeOut (1s-10s)       │    │
│                     └─────────────────────────────────────────┘    │
│                           │                                    │
│  ┌────────────────────────┼────────────────────────────────┐  │
│  │   互斥锁 (mu)        │                                │  │
│  └────────────────────────┼────────────────────────────────┘  │
│                           │                                │
│  ┌────────────────────────▼──────────────────────────────┐  │
│  │              executor (当前批次)              │  │
│  │  ┌─────────────────────────────────────────┐      │  │
│  │  │    BatchQueryExecutor              │      │  │
│  │  │                                    │      │  │
│  │  │  ┌────────────────────────────┐    │      │  │
│  │  │  │  executeCtx + cancelFunc │    │      │  │
│  │  │  └────────────────────────────┘    │      │  │
│  │  │                                    │      │  │
│  │  │  ┌────────────────────────────┐    │      │  │
│  │  │  │  canAdd (bool)         │    │      │  │
│  │  │  │  query (BatchQueryI)   │    │      │  │
│  │  │  │  config                │    │      │  │
│  │  │  │  err                   │    │      │  │
│  │  │  │  keys []K              │    │      │  │
│  │  │  │  results []V            │    │      │  │
│  │  │  │  resultsMap map[K][]V  │    │      │  │
│  │  │  │  wg (WaitGroup)       │    │      │  │
│  │  │  │  mu (Mutex)           │    │      │  │
│  │  │  │  once (Once)          │    │      │  │
│  │  │  └────────────────────────────┘    │      │  │
│  │  └─────────────────────────────────────────┘      │  │
│  └───────────────────────────────────────────────────┘  │
│                          │                                    │
│  ┌───────────────────┴───────────────────────────┐    │
│  │              BatchQueryI                 │    │
│  └───────────────────┬───────────────────────────┘    │
│                      │                                    │
│        ┌─────────────┴─────────────┐                     │
│        │                            │                     │
│  ┌─────▼──────┐          ┌──────▼──────┐          │
│  │  Query()   │          │SplitResults()│          │
│  └────────────┘          └─────────────┘          │
│                                                 │
└─────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│                    并发请求                         │
├──────────────────────────────────────────────────────────────────────┤
│                                                              │
│  请求1 ──┐                                            │
│  请求2 ──┼──► TryAddKey() ──► executor        │
│  请求3 ──┤                   │                        │
│  请求4 ──┘              [检查: canAdd?]              │
│              │                    │                         │
│              │              ┌─────┴─────┐               │
│              │              │           │               │
│              │         [是]       [否]               │
│              │              │           │               │
│              │         ┌────▼───┐  ┌─▼──────┐      │
│              │         │Add key │  │Create    │      │
│              │         │to batch│  │new exec  │      │
│              │         └────┬───┘  └─┬──────┘      │
│              │              │         │               │
│              │         批次已满？                   │
│              │              │                         │
│              │         [是]     [否]              │
│              │              │          │              │
│              │         ┌────▼───┐  ┌─▼────────┐     │
│              │         │StartTrick│  │继续    │     │
│              │         │(定时器)   │  │等待    │     │
│              │         └────┬────┘  └───────────┘     │
│              │              │                            │
│              └──────────────┼────────────────────────────┤
│                             │                        │
│                        ┌────▼─────┐                │
│                        │ DoQuery() │                │
│                        │(实际   │                │
│                        │ 批次)   │                │
│                        └────┬─────┘                │
│                             │                        │
│                ┌────────────┴────────────┐          │
│                │                         │          │
│           ┌────▼────┐             ┌──▼──────┐   │
│           │ Query()  │             │超时  │   │
│           │(DB/API) │             │错误    │   │
│           └────┬────┘             └──────────┘   │
│                │                                 │
│           ┌────▼─────┐                             │
│           │ SplitResults│                             │
│           │           │                             │
│           └────┬─────┘                             │
│                │                                  │
│           ┌────▼─────────────┐                      │
│           │ resultsMap      │                      │
│           │ (map[K][]V)    │                      │
│           └────┬───────────┘                      │
│                │                                  │
│     ┌──────────┴──────────┐                     │
│     │                     │                     │
│  ┌──▼────┐          ┌────▼─────┐           │
│  │结果1 │          │ 结果2  │           │
│  └────────┘          └──────────┘           │
│                                                 │
└─────────────────────────────────────────────┘
```

## 核心组件

### 1. BatchQueryManager
**职责**：管理批量查询的生命周期并协调并发请求。

**主要字段**：
- `ctx`：所有操作的父上下文
- `config`：批处理配置（MaxBatchSize、MaxWaitTime、MaxCtxTimeOut）
- `queryer`：查询接口实现
- `executor`：当前批次执行器（初始为 nil）
- `mu`：保护 executor 访问的互斥锁

### 2. BatchQueryExecutor
**职责**：执行单个批次的查询并管理批次生命周期。

**主要字段**：
- `executeCtx`：此批次的带超时上下文
- `canAdd`：指示是否可以添加更多 key 的标志
- `query`：查询接口
- `config`：批处理配置
- `err`：查询执行错误
- `keys`：此批次累积的 keys
- `results`：查询结果
- `resultsMap`：映射结果（key → values）
- `wg`：批次完成的等待组
- `mu`：保护内部状态的互斥锁
- `once`：确保查询只执行一次

### 3. BatchQueryI（接口）
**职责**：定义批量查询操作的契约。

**方法**：
- `Query(ctx, keys)`：执行批量查询，返回所有结果
- `SplitResults(ctx, results)`：将结果映射回原始 keys

## 工作原理

### 1. 请求流程

```
客户端请求
    │
    ▼
BatchQueryManager.Get(ctx, key) 或 GetByKeys(ctx, keys)
    │
    ▼
锁定管理器互斥锁
    │
    ▼
检查 executor 是否存在？
    │
    ├── 否 ──► 创建新的 BatchQueryExecutor
    │          - 添加 wg.Add(1)
    │          - 尝试添加 key(s)
    │          - 启动定时器（StartTrick）
    │
    └── 是 ──► 尝试将 key(s) 添加到当前 executor
    │              (能否添加？检查 executor.canAdd)
    │
    ├── 无法添加 ──► 创建新的 executor（同上）
    │
    └── 添加成功 ──► 检查批次是否已满
    │                              (len(keys) >= MaxBatchSize)
    │
    ├── 是 ──► 设置 canAdd = false
    │              在 goroutine 中立即启动 DoQuery
    │
    └── 否 ──► 继续等待（定时器稍后触发）
```

### 2. 批次执行触发

批次查询有两种触发方式：

**A. 基于大小的触发**（立即执行）：
```
TryAddKey(s) 添加 keys
    │
    ▼
检查：len(keys) >= MaxBatchSize？
    │
    ├── 是 ──► canAdd = false
    │            立即启动 DoQuery goroutine
    │
    └── 否 ──► 继续等待
```

**B. 基于时间的触发**（延迟执行）：
```
StartTrick() 被调用
    │
    ▼
启动 MaxWaitTime 定时器
    │
    ▼
等待定时器 OR 上下文超时
    │
    ├── 定时器过期 ──► canAdd = false
    │                    启动 DoQuery goroutine
    │
    └── 上下文超时 ──► canAdd = false
    │                         设置错误 = context.DeadlineExceeded
    │                         wg.Done()
```

### 3. 查询执行

```
DoQuery() goroutine
    │
    ▼
once.Do（确保只执行一次）
    │
    ▼
Query.Query(ctx, keys)
    │
    ├── 成功 ──► Query.SplitResults(ctx, results)
    │                    存储到 executor.resultsMap
    │                    wg.Done()
    │
    └── 错误 ──► 存储到 executor.err
    │                  wg.Done()
```

### 4. 结果获取

```
等待批次完成
    │
    ▼
executor.wg.Wait()
    │
    ▼
检查 executor.err
    │
    ├── 有错误 ──► 返回 (nil, false, err)
    │
    └── 无错误 ──► 返回 (results, true, nil)
                       来自 executor.resultsMap[key]
```

## 并发模型

### 线程安全

1. **BatchQueryManager.mu**：保护 executor 字段赋值
2. **BatchQueryExecutor.mu**：保护 canAdd、keys、err 字段
3. **BatchQueryExecutor.once**：确保 DoQuery 只执行一次
4. **BatchQueryExecutor.wg**：协调批次完成

### Goroutine 管理

```
管理器线程（Get/GetByKeys 调用者）
    │
    ├── 锁定 manager.mu
    ├── 获取/创建 executor
    ├── 解锁 manager.mu
    ├── 等待 executor.wg
    └── 返回结果

执行器线程（DoQuery goroutine）
    │
    ├── 执行 Query.Query()
    ├── 执行 Query.SplitResults()
    ├── 调用 wg.Done()
    └── 退出

定时器线程（StartTrick goroutine）
    │
    ├── 等待 MaxWaitTime
    ├── 检查 canAdd
    ├── 必要时设置 canAdd = false
    ├── 必要时启动 DoQuery
    └── 退出
```

## 性能特性

### 优化建议

1. **调整 MaxBatchSize**：更大的批次 = 更少的查询，但每批次延迟更高
2. **调整 MaxWaitTime**：更短的等待 = 更好的延迟，但可能创建更小的批次
3. **使用 GetByKeys**：当需要多个值时，将它们一起批处理
4. **重用管理器**：为每个用例创建一个管理器，不要为每个请求创建

## 错误处理

| 错误类型 | 来源 | 处理方式 |
|----------|------|---------|
| 查询错误 | Query.Query() | 批次中的所有请求收到相同错误 |
| 上下文超时 | executeCtx deadline | 返回 context.DeadlineExceeded |
| Panic | DoQuery | 恢复并作为错误返回 |
| Key 未找到 | SplitResults() | Result exists=false，values=[] |

## 核心优势

1. **减少 API/数据库调用**：将 N 个独立查询批处理为 ~N/MaxBatchSize 个实际查询
2. **自动并发**：线程安全，可与任意数量的并发调用者一起工作
3. **灵活触发**：基于大小和基于时间的触发器
4. **简洁 API**：简单的 Get/GetByKeys 接口
5. **可配置**：为您的特定用例进行调整

## TODO list

1. BatchQueryExecutor 对象持有无法释放: 若 BatchQueryManager 持有的 BatchQueryExecutor 对象刚好攒批执行(keys数量&等待时间), Manager 持有的 Executor 对象会一直持有返回值 result (内存持有), 直到有新的请求达到, Manager 才会新建新的 Executor 对象, 从而产生内存泄漏的可能. (正在改进中)

