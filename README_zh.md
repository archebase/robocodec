# Robocodec

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)

[English](README.md) | [简体中文](README_zh.md)

**Robocodec** 是一个高性能机器人数据格式库，用于读写 MCAP 和 ROS bag 文件。它提供了以格式为核心的架构，具有并行处理能力、高效的内存管理以及对多种消息编码的支持。

## 功能特性

- **多格式支持**：读写 MCAP 和 ROS1 bag 文件
- **消息编解码**：CDR（ROS1/ROS2）、Protobuf 和 JSON 编码/解码
- **模式解析**：解析 ROS `.msg` 文件、ROS2 IDL 和 OMG IDL 格式
- **高性能 I/O**：支持内存映射的并行和顺序读取策略
- **以格式为核心的架构**：每种格式都有独立的模块，包含读取器、写入器和高级 API
- **数据转换**：内置支持主题重命名、类型归一化和格式转换
- **内存高效**：Arena 分配和零拷贝操作
- **Python 绑定**：通过 PyO3 提供全功能 Python API（可选功能）

## 安装

### 前置要求

- Rust 1.70 或更高版本
- Python 3.11+（用于 Python 绑定）
- maturin（用于构建 Python 包）

### 从源码构建

```bash
# 克隆仓库
git clone https://github.com/archebase/robocodec.git
cd robocodec

# 构建库
cargo build --release

# 运行测试
cargo test

# 构建 Python 包（可选）
pip install maturin
maturin develop
```

### 作为 Rust 依赖使用

在 `Cargo.toml` 中添加：

```toml
[dependencies]
robocodec = "0.1"
```

根据需要启用可选功能：

```toml
robocodec = { version = "0.1", features = ["python", "jemalloc"] }
```

### 可选功能

| 功能 | 描述 |
|---------|-------------|
| `python` | 通过 PyO3 提供 Python 绑定 |
| `jemalloc` | 使用 jemalloc 分配器（仅 Linux） |

## 快速开始

### 读取 MCAP 文件

```rust
use robocodec::io::formats::mcap::reader::McapReader;

let reader = McapReader::open("data.mcap")?;
for result in reader.decode_messages()? {
    let (decoded, channel) = result?;
    println!("Topic: {}, Fields: {:?}", channel.topic, decoded);
}
```

### 写入 MCAP 文件

```rust
use robocodec::io::formats::mcap::writer::ParallelMcapWriter;

let writer = ParallelMcapWriter::create("output.mcap")?;
writer.add_channel(...)?;
writer.write_chunk(...)?;
writer.finish()?;
```

### 使用自动检测重写

```rust
use robocodec::rewriter::RoboRewriter;

// 格式从文件扩展名自动检测
let mut rewriter = RoboRewriter::open("input.mcap")?;
rewriter.rewrite("output.mcap")?;
```

### Python API

```python
from robocodec import McapReader

# 读取 MCAP 文件
reader = McapReader("data.mcap")
for message, channel in reader:
    print(f"Topic: {channel.topic}, Data: {message}")
```

## 架构

Robocodec 是一个**以格式为核心**的库，每个机器人数据格式都有独立的模块，包含所有相关功能（读取器、写入器、高级 API）。

### 分层架构

```
┌─────────────────────────────────────────────┐
│  用户层 (lib.rs 重新导出)                    │
│  - McapReader, BagWriter, 等                │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  高级 API 层                                │
│  - io/formats/mcap/reader.rs (自动解码)     │
│  - io/formats/mcap/writer.rs (自定义)       │
│  - io/formats/bag/writer.rs (高级)          │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  底层 I/O 层                                │
│  - io/formats/mcap/parallel.rs, reader.rs   │
│  - io/formats/bag/parallel.rs, reader.rs    │
│  - io/ (统一 trait)                         │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  基础层                                     │
│  - core/ (错误、类型)                       │
│  - encoding/ (编解码器)                     │
│  - schema/ (解析)                           │
└─────────────────────────────────────────────┘
```

### 核心原则

1. **以格式为核心的组织**：每个格式（MCAP、ROS1 bag）都有独立的模块，包含底层 I/O 操作、特定格式的读取器和写入器以及高级便利 API。

2. **分层架构**：用户 API、高级操作、底层 I/O 和基础层之间有清晰的分离。

3. **统一的重写器接口**：从文件扩展名自动检测格式并委托给特定格式的重写器。

更多详情请参阅 [ARCHITECTURE.md](ARCHITECTURE.md)。

## 支持的格式

| 格式 | 读取 | 写入 | 备注 |
|--------|------|-------|-------|
| MCAP | ✅ | ✅ | 针对追加优化的通用数据格式 |
| ROS1 Bag | ✅ | ✅ | ROS1 rosbag 格式 |
| CDR | ✅ | ✅ | 通用数据表示（ROS1/ROS2） |
| Protobuf | ✅ | ✅ | Protocol Buffers |
| JSON | ✅ | ✅ | JSON 序列化 |

## 模式支持

- ROS `.msg` 文件（ROS1）
- ROS2 IDL（接口定义语言）
- OMG IDL（对象管理组织）

## 开发

### 构建

```bash
# 构建库
cargo build --release

# 运行测试
cargo test

# 使用特定功能构建
cargo build --features python
```

### 运行示例

```bash
# 读取 MCAP 文件
cargo run --example read_mcap -- data.mcap

# 在格式之间转换
cargo run --example convert -- input.bag output.mcap
```

## 贡献

我们欢迎贡献！请参阅 [CONTRIBUTING.md](CONTRIBUTING.md) 了解指南。

## 许可证

本项目在 MulanPSL v2 下许可 - 详见 [LICENSE](LICENSE) 文件。

## 相关项目

- [Roboflow](https://github.com/archebase/roboflow) - 基于 robocodec 构建的高级流水线和转换工具
- [MCAP](https://mcap.dev/) - 针对机器人社区追加优化的通用数据格式
- [ROS](https://www.ros.org/) - 机器人操作系统

## 文档

- [架构](ARCHITECTURE.md) - 高层系统设计

## 链接

- [问题追踪器](https://github.com/archebase/robocodec/issues)
- [行为准则](CODE_OF_CONDUCT_zh.md)
- [安全策略](SECURITY_zh.md)
