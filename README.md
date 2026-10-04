# DRR 字节公平队列与额度结转回放后端

使用 TypeScript + Node.js + SQLite（`node:sqlite`，零原生依赖）实现的
Deficit Round Robin（DRR）字节公平调度模拟器。只提供 API 与 CLI，
只计算服务顺序与字节账目：不发送网络包、不提供消息代理协议、
不模拟带宽或真实墙钟时间。

## 核心语义

- 每个流有唯一编号、固定正整数 quantum 和 FIFO 包队列；每包有唯一编号与正整数长度。
- 活动流组成有序环（初始顺序由首次激活事件决定）。每次访问当前流：
  先累加一个 quantum，再从队首连续发送长度不超过当前额度的包，逐包扣减。
- 亏欠额度跨轮结转：队首包放不下时停止本次访问，剩余额度保留到下次访问继续累加。
  不跳过大包发小包，不对非空队列清零额度（否则大于 quantum 的包永远无法发送）。
- 队列在访问后变空：额度归零并移出活动环；空流再次入队时在环尾重新激活，
  不继承空闲前的剩余额度。入队已活跃流不改变其环位置。
- 每次访问结束转到原环中的下一活动流；删除当前节点或只剩一个节点时不跳过、
  不重复加额度。空环推进返回 `NO_ACTIVE_STREAMS`，不产生访问记录。
- 流参数（quantum）在首次调度后冻结；改变 quantum 需创建新运行。
  同一事件序列产生完全相同的服务次序与账目（确定性回放）。
- 所有量为安全整数；包长、quantum、总入队包数、单次推进次数均有上限（见 `src/errors.ts` 的 `LIMITS`）。

## 运行要求

- Windows 本地，Node.js 不低于 22.5（建议 24+），无需 Docker / WSL / 外部服务。

## 命令

```powershell
npm install        # 安装开发依赖（typescript / tsx / @types/node）
npm run build      # 编译到 dist/
npm test           # 运行全部测试（node:test + tsx）
npm start          # 启动 API 服务（默认端口 0 = 自动选择空闲端口；PORT=8080 指定）
npm run demo       # 运行内置演示（内存数据库）
node dist/cli.js   # CLI（见下方用法）
```

数据目录默认为项目内 `./data/drr.db`，可用环境变量 `DRR_DATA_DIR` 覆盖。
SQLite 以 WAL 模式在事务中原子保存当前指针、额度、队列与服务事件；
进程重启后继续推进，不会从第一个流重新开始。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/runs` | 创建运行 `{name?}` |
| GET | `/runs` | 列出运行 |
| POST | `/runs/:id/streams` | 登记流 `{streamId, quantum}`（首次调度后冻结） |
| POST | `/runs/:id/enqueue` | 入队 `{streamId, packetId, length}` |
| POST | `/runs/:id/advance` | 推进 `{count?}` 次访问，返回访问事件 |
| GET | `/runs/:id/ring` | 查看活动环、指针、各流额度/累计服务/积压 |
| GET | `/runs/:id/events?streamId=` | 访问历史 |
| GET | `/runs/:id/explain/:visit` | 用中文解释某次访问 |
| GET | `/runs/:id/packets/:streamId/:packetId` | 查询包状态 |
| GET | `/runs/:id/export` | 导出运行完整 JSON |

错误响应为 `{"error": 错误码, "message": 中文描述}`。错误码：
`INVALID_ARGUMENT`、`UNKNOWN_RUN`、`UNKNOWN_STREAM`、`UNKNOWN_PACKET`、
`DUPLICATE_STREAM`、`DUPLICATE_PACKET`、`LIMIT_EXCEEDED`、
`NO_ACTIVE_STREAMS`、`STREAMS_FROZEN`、`NOT_FOUND`。
所有失败操作在事务中回滚，不消耗额度、不留下已发送标记。

## CLI

```
node dist/cli.js create-run [name]
node dist/cli.js add-stream <runId> <streamId> <quantum>
node dist/cli.js enqueue <runId> <streamId> <packetId> <length>
node dist/cli.js advance <runId> [count]
node dist/cli.js ring <runId>
node dist/cli.js events <runId> [streamId]
node dist/cli.js explain <runId> <visit>
node dist/cli.js packet <runId> <streamId> <packetId>
node dist/cli.js export <runId>
```

## 演示内容（npm run demo）

1. quantum=2 的流发送长度 3 的包：第一次访问发不出、保留 2 字节额度，
   第二次累加到 4 后发送。
2. 两个流不同 quantum（3 与 1）与不同包长的交错访问。
3. 流清空后移出环、再入队重新激活且不继承旧额度。

## 测试覆盖（npm test，13 个用例）

单流环、连续发多包、剩余额度不足结转、环头尾移除、空环推进报错、
入队活跃流顺序不变、清空再激活不继承额度、字节守恒（入队 = 已服务 + 积压）、
FIFO 不越过队首、失败回滚、重启续算一致、流参数冻结、长期字节份额趋近
quantum 比例、多运行命名空间独立、explain 文本。

## 限制与说明

- 字节份额只是长期趋势：短序列偏差与包长量化有关（包不可切分），
  本模拟器不承诺有限任意时间窗口内的精确带宽比例，也不以包数量代替字节公平性。
- 调度器不模拟真实时间与发送速率，"一次访问"是逻辑步进。
- `node:sqlite` 在 Node 25 仍标记为实验特性（启动时有 ExperimentalWarning），
  功能稳定可用；如需消除警告可换用 better-sqlite3。
- 单进程服务，无鉴权，面向本地回放与教学用途。
