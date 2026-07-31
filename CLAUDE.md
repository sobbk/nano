# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目简介

Nano 是一个轻量级、高性能的 Go 游戏服务器网络库，支持单机和分布式集群模式，基于 WebSocket/TCP，使用 gRPC 进行节点间通信。

## 常用命令

```bash
# 运行所有测试
make test
# 或
go test -v ./...

# 运行单个测试
go test -v ./cluster/ -run TestXxx

# 构建所有包
go build ./...

# 重新生成 protobuf 代码（需要安装 protoc）
make proto
# 等价于：cd ./cluster/clusterpb/proto/ && protoc --go_out=plugins=grpc:../ *.proto

# 带覆盖率的测试
go test -v -cover ./...
```

## 架构概览

### 核心三层结构

```
Component（组件）→ Service（服务）→ Handler（处理器）
```

- **Component**：应用的基本构建单元，实现 `component.Component` 接口（`Init/AfterInit/BeforeShutdown/Shutdown`）
- **Service**：通过反射从 Component 中提取 Handler 方法，自动注册
- **Handler 方法签名**：`func (c *Comp) Handler(s *session.Session, data *T) error`，`data` 可为指针类型（自动反序列化）或 `[]byte`（原始字节）

### 网络分层

```
TCP/WebSocket → Packet（包层）→ Message（消息层）→ Handler（处理层）
```

- **Packet**（`internal/packet`）：底层网络帧，格式 `Type(1B) + Length(3B) + Data`
- **Message**（`internal/message`）：应用层消息，含类型（Request/Notify/Response/Push）、路由字符串、payload
- **路由压缩**：可通过 `WithDictionary` 将路由字符串映射为 uint16 减少带宽

### 集群模式（`cluster/`）

三种运行模式通过 `Listen` 选项组合决定：

| 模式 | 选项 | 说明 |
|------|------|------|
| 单机 | 无 Master/AdvertiseAddr | 所有服务在一个进程 |
| Master 节点 | `WithMaster()` | 管理服务注册、心跳、拓扑广播 |
| Backend 节点 | `WithAdvertiseAddr(masterAddr, retryInterval)` | 向 Master 注册，处理业务逻辑 |

Backend 节点收到本地无法处理的消息时，通过 gRPC 转发到对应的远程节点（`cluster/connpool.go` 维护连接池）。

### 关键抽象

- **Session**（`session/session.go`）：每个客户端连接的上下文，含 Snowflake ID、绑定 UID、KV 数据存储
- **Group**（`group.go`）：会话容器，用于 `Broadcast`（全量广播）和 `Multicast`（条件广播）
- **Pipeline**（`pipeline/pipeline.go`）：入站/出站消息中间件链，`Inbound` 在 Handler 前执行，`Outbound` 在响应前执行
- **Scheduler**（`scheduler/`）：全局任务队列；组件可通过实现 `LocalScheduler` 接口获得独立 goroutine

### 入口点

`interface.go` 的 `nano.Listen(addr, ...Option)` 是唯一公开入口，初始化环境、创建 `cluster.Node` 并监听 OS 信号（SIGINT/SIGQUIT/SIGTERM）优雅关闭。

## 重要约定

- Handler 必须运行在调度器 goroutine 中，**不要在 Handler 内启动新 goroutine 直接访问 Session**，使用 `scheduler.PushTask` 调度
- `nano.Listen` 的 `WithComponents` 选项接受 `*component.Components`，通过 `hub.go` 统一管理生命周期
- 序列化器默认为 JSON，生产环境可通过 `WithSerializer(protobuf.NewSerializer())` 切换
- Session 的 `Bind(uid)` 绑定用户 ID 后，Group 的 `Members()` 返回的是 UID 列表而非 Session ID

## 示例代码

`examples/` 目录包含三个参考实现：
- `examples/demo/chat`：最简单的聊天室（约 100 行，适合快速了解 API）
- `examples/demo/tadpole`：完整游戏示例
- `examples/cluster`：Master + Gate + Backend 分布式架构示例
