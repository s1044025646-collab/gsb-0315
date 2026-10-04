# DRR 字节公平队列与额度结转回放后端

使用 TypeScript + Node.js + SQLite（`node:sqlite` 内置模块，零外部运行时依赖）实现的
Deficit Round Robin（亏欠轮转）字节公平调度模拟器。只提供 HTTP API 与 CLI，
不发送网络包、不提供消息代理协议、不模拟带宽或真实墙钟时间。

## 核心语义

- 每个流有唯一编号、固定正整数 `quantum` 和一个 FIFO 包队列；每包有唯一编号与正整数长度。
- 活动流形成有序环（顺序由首次激活事件决定）。每次访问当前流：
  1. 先给额度追加一个 `quantum`；
  2. 从队首连续发送长度不超过当前额度的包，每发一包扣去其长度；
  3. 遇到放不下的队首即停止本次访问，**剩余额度保留到下次访问继续累加**（亏欠结转）。
- 不能跳过队首大包发送后面的小包；非空队列的额度不在每轮清零，
  因此大于 `quantum` 的包最终一定能发送。
- 队列在访问后变空：额度归零并移出活动环；空流再次入队时在**环尾**重新激活，
  不继承空闲前的剩余额度。入队已活跃流不改变其环中位置。
- 每次访问结束转到原环中的下一活动流；删除当前节点或只剩一个节点时不跳过、不重复加额度。
- 空环推进返回"无可服务流"，不产生访问记录。
- 流参数（quantum）在运行首次推进后冻结；需要不同 quantum 请创建新运行。
  同一事件序列产生完全相同的服务次序与账目（确定性回放）。

## 环境要求

- Windows 本地，Node.js >= 22（使用内置 `node:sqlite`，有 ExperimentalWarning 属正常）
- 不需要 Docker / WSL / 付费服务 / 外部在线 API

## 构建、测试、启动、演示

```powershell
npm install      # 仅安装开发依赖 typescript / @types/node
npm run build    # 编译 TypeScript 到 dist/
npm test         # 构建并运行全部单元测试（node:test）
npm start        # 启动 HTTP API（默认自动选择空闲端口）
npm run demo     # 运行内置演示（亏欠结转 / 交错访问 / 清空再激活 / 长期份额）
node dist/src/cli.js serve --port 8080   # 指定端口
```

数据保存在项目内 `data/drr.db`（可用环境变量 `DRR_DATA_DIR`、`DRR_DB_PATH` 覆盖）。
服务重启后从 SQLite 恢复当前指针、额度、队列与事件，**不会重新从第一个流开始**。

## CLI 命令

```
node dist/src/cli.js create-run [name]
node dist/src/cli.js add-stream <runId> <streamId> <quantum>
node dist/src/cli.js enqueue <runId> <streamId> <packetId> <length>
node dist/src/cli.js advance <runId> [count]
node dist/src/cli.js ring <runId>          # 活动环与当前指针
node dist/src/cli.js events <runId> [streamId]
node dist/src/cli.js explain <runId> <seq> # 中文解释某次访问
node dist/src/cli.js packet <runId> <packetId>
node dist/src/cli.js history <runId>       # 每流统计与守恒校验
node dist/src/cli.js export <runId>        # 完整 JSON 导出
node dist/src/cli.js serve [--port N]
node dist/src/cli.js demo
```

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/runs` | 创建运行 `{name?}` |
| POST | `/runs/:id/streams` | 登记流 `{streamId, quantum}` |
| POST | `/runs/:id/enqueue` | 入队 `{streamId, packetId, length}` |
| POST | `/runs/:id/advance` | 推进 `{count?}`，返回访问事件数组 |
| GET | `/runs/:id/ring` | 活动环与当前指针 |
| GET | `/runs/:id/events?streamId=` | 访问历史（可按流过滤） |
| GET | `/runs/:id/explain/:seq` | 中文解释某次访问事件 |
| GET | `/runs/:id/packets/:packetId` | 包状态（queued/sent、发送事件号） |
| GET | `/runs/:id/history` | 每流累计服务字节、积压、守恒校验 |
| GET | `/runs/:id/export` | 运行完整 JSON 导出 |

每次访问事件记录：访问前后额度、追加量、发出的包及字节数、被阻塞的队首、下一流。

## 错误码

统一返回 `{ "error": { "code", "message" } }`：

- `INVALID_ARGUMENT`（400）：零长度、负数、非整数、非法键名等
- `RUN_NOT_FOUND` / `STREAM_NOT_FOUND` / `PACKET_NOT_FOUND` / `EVENT_NOT_FOUND`（404）
- `DUPLICATE_STREAM` / `DUPLICATE_PACKET`（409）：编号在**本运行命名空间**内重复
- `RUN_SEALED`（409）：运行已推进，流参数冻结
- `LIMIT_EXCEEDED`（400）：超过包长 / quantum / 包数 / 访问预算 / 额度安全上限

所有写操作在 SQLite 事务中原子提交；失败整体回滚，不消耗额度、不留已发送标记。
多个运行使用独立的流与包命名空间。

## 参数上限（src/config.ts）

`quantum`、`length` ≤ 1,000,000；单运行包数 ≤ 100,000；单次推进 ≤ 10,000 次；
单运行流数 ≤ 10,000；单流额度 ≤ 2^40。所有量保持安全整数范围。

## 关于"公平性"的说明

DRR 保证的是**字节**维度的长期公平：始终有积压的流，其长期服务字节之比趋近
quantum 之比（见 `npm run demo` 第 4 段与对应测试）。短序列中的偏差来自包长粒度
与队首阻塞（大包需积累多轮额度），这是算法固有特性；本模拟器不以包数量替代字节
公平性，也不宣称在任意有限时间窗内达到精确带宽比例。

## 已知限制

- `node:sqlite` 在 Node 25 仍带 Experimental 标记，未来版本 API 可能微调。
- 单进程服务；未实现鉴权与并发多写者，定位为本地回放/教学工具。
- 事件与包记录只增不删，超长运行需自行清理数据目录。
