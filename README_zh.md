# Robocodec

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![codecov](https://codecov.io/gh/archebase/robocodec/branch/main/graph/badge.svg)](https://codecov.io/gh/archebase/robocodec)

[English](README.md) | [简体中文](README_zh.md)

**Robocodec** 是一个机器人数据格式库，用于读取、写入和转换 MCAP 和 ROS1 bag 文件。它提供统一的 API，支持自动格式检测、并行处理，以及多种消息编码（CDR、Protobuf、JSON）和模式类型（ROS .msg、ROS2 IDL、OMG IDL）。

## 为什么选择 Robocodec？

- **简洁的 API** - 仅在顶层暴露 `RoboReader`、`RoboWriter`、`RoboRewriter`
- **自动检测** - 从文件扩展名或 URL scheme 自动检测格式
- **高性能** - 使用 rayon 并行处理，零拷贝内存映射文件
- **原生 S3 支持** - 对 `s3://` URL 的一流支持（AWS S3、MinIO、阿里云 OSS 等）
- **数据转换** - 内置主题/类型重命名和文件重写功能

## 快速开始

### Rust

```toml
# Cargo.toml
[dependencies]
robocodec = "0.1"
```

```rust
use robocodec::RoboReader;

// 格式从扩展名自动检测
let reader = RoboReader::open("data.mcap")?;
println!("找到 {} 个通道", reader.channels().len());
```

### Python（从源码构建）

Python 绑定可用，但需要从源码构建：

```bash
git clone https://github.com/archebase/robocodec.git
cd robocodec
make build-python-dev
```

```python
from robocodec import RoboReader

reader = RoboReader("data.mcap")
print(f"找到 {len(reader.channels)} 个通道")
```

> **注意：** PyPI 发布即将推出。目前请按照上述说明从源码构建。

## 常见任务

### 读取文件中的消息

```rust
use robocodec::RoboReader;

let reader = RoboReader::open("file.mcap")?;

// 列出所有通道
for channel in reader.channels() {
    println!("{}: {} 条消息", channel.topic, channel.message_count);
}

// 获取消息总数
println!("总消息数: {}", reader.message_count());
```

### 写入消息到文件

```rust
use robocodec::RoboWriter;

let mut writer = RoboWriter::create("output.mcap")?;
let channel_id = writer.add_channel("/topic", "MessageType", "cdr", None)?;
// ... 写入消息 ...
writer.finish()?;
```

### 读取解码后的消息

```rust
use robocodec::RoboReader;

let reader = RoboReader::open("file.mcap")?;

for result in reader.decoded()? {
    let msg = result?;
    println!("主题: {}", msg.topic());
    println!("数据: {:?}", msg.message);
    println!("日志时间: {:?}", msg.log_time);
}
```

### 从 S3 读取

Robocodec 支持直接从兼容 S3 的存储使用 `s3://` URL 读取数据：

```rust
use robocodec::RoboReader;

// 格式和 S3 访问自动检测
let reader = RoboReader::open("s3://my-bucket/path/to/data.mcap")?;
println!("找到 {} 个通道", reader.channels().len());
```

**兼容 S3 的存储服务**（AWS S3、阿里云 OSS、MinIO 等）需要通过环境变量配置凭证：

```bash
# AWS S3
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"  # 可选，默认为 us-east-1

# 对于阿里云 OSS、MinIO 或其他兼容 S3 的服务
export AWS_ACCESS_KEY_ID="your-oss-access-key"
export AWS_SECRET_ACCESS_KEY="your-oss-secret-key"
```

> **注意：** 虽然我们使用 AWS 标准的环境变量名称以确保兼容性，但 robocodec 可与任何兼容 S3 的存储服务配合使用。

### 自定义 S3 端点

对于具有自定义端点的兼容 S3 服务：

**方式 1：环境变量**（全局）
```bash
export S3_ENDPOINT="http://localhost:9000"  # MinIO
export S3_ENDPOINT="https://oss-cn-hangzhou.aliyuncs.com"  # 阿里云 OSS
```

**方式 2：URL 查询参数**（按请求）
```rust
use robocodec::RoboReader;

// 本地运行的 MinIO
let reader = RoboReader::open("s3://bucket/data.mcap?endpoint=http://localhost:9000")?;

// 阿里云 OSS（杭州区域）
let reader = RoboReader::open(
    "s3://bucket/data.mcap?endpoint=https://oss-cn-hangzhou.aliyuncs.com"
)?;
```

### 使用转换重写文件

重写器以相同格式处理文件，可选择应用主题和类型转换：

```rust
use robocodec::{RoboRewriter, TransformBuilder, RewriteOptions};

let transform = TransformBuilder::new()
    .with_topic_rename("/old/topic", "/new/topic")
    .build();

let options = RewriteOptions::default().with_transforms(transform);
let rewriter = RoboRewriter::with_options("input.mcap", options)?;
rewriter.rewrite("output.mcap")?;
```

**注意：** 重写器保持相同的格式。目前不支持跨格式转换。

## 安装

### Rust 用户

添加到 `Cargo.toml`：

```toml
[dependencies]
robocodec = "0.1"
```

可选功能：

```toml
robocodec = { version = "0.1", features = ["jemalloc"] }
```

| 功能 | 描述 | 默认 |
|---------|-------------|---------|
| `s3` | 支持 S3 兼容存储（AWS S3、MinIO 等） | ✅ 是 |
| `python` | Python 绑定 | ❌ 否 |
| `jemalloc` | 使用 jemalloc 分配器（仅 Linux） | ❌ 否 |

### Python 用户

从源码构建（PyPI 发布即将推出）：

```bash
git clone https://github.com/archebase/robocodec.git
cd robocodec
make build-python-dev
```

## 支持的格式

| 格式 | 读取 | 写入 |
|--------|-----|-------|
| MCAP | ✅ | ✅ |
| ROS1 Bag | ✅ | ✅ |
| RRF2 (Rerun) | ✅ | ✅ |

> **注意：** RRF2 支持兼容 Rerun **0.27+** 版本。更早版本使用不同的格式，暂不支持。

## 消息编码

| 编码 | 描述 |
|---------|-------------|
| CDR | 通用数据表示（ROS1/ROS2） |
| Protobuf | Protocol Buffers |
| JSON | JSON 编码 |

## 模式支持

- ROS `.msg` 文件（ROS1）
- ROS2 IDL（接口定义语言）
- OMG IDL（对象管理组织）

## 许可证

MulanPSL v2 - 详见 [LICENSE](LICENSE)

## 链接

- [问题追踪器](https://github.com/archebase/robocodec/issues)
- [安全策略](SECURITY.md)
