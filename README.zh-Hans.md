# pglogrepl

PostgreSQL 逻辑复制的 Go 语言实现，支持全量和增量数据同步。

[English Documentation](./README.md)

## 功能特性

- **全量同步**：使用 PostgreSQL 一致性快照进行初始数据同步
- **增量同步**：通过逻辑复制实时跟踪数据变化
- **自动恢复**：LSN 持久化支持崩溃恢复和断点续传
- **事件驱动**：基于回调的数据变更事件架构
- **线程安全**：全程并发安全操作
- **模式过滤**：支持指定 schema 和表进行复制
- **自动管理**：自动创建和管理 publication 和 replication slot

## 安装

```bash
go get github.com/hupeh/pglogrepl
```

## 前置要求

### PostgreSQL 配置

PostgreSQL 服务器必须配置为支持逻辑复制：

1. **检查是否支持逻辑复制：**

   ```go
   package main

   import (
       "context"
       "log"

       "github.com/hupeh/pglogrepl"
   )

   func main() {
       dsn := "host=localhost port=5432 user=postgres password=secret dbname=mydb"
       result, err := pglogrepl.CheckLogicalReplication(context.Background(), dsn)
       if err != nil {
           log.Fatal(err)
       }

       log.Println(result.String())

       if !result.Supported {
           log.Fatal("逻辑复制未正确配置")
       }
   }
   ```

2. **如果未配置，编辑 `postgresql.conf`：**

   ```ini
   wal_level = logical
   max_replication_slots = 4
   max_wal_senders = 4
   ```

3. 重启 PostgreSQL 使配置生效

4. 授予用户复制权限：

   ```sql
   ALTER USER your_user WITH REPLICATION;
   ```

### 支持的 PostgreSQL 版本

- PostgreSQL 10+
- 推荐：PostgreSQL 14+（支持大事务流式传输）

## 快速开始

```go
package main

import (
    "context"
    "log"

    "github.com/hupeh/pglogrepl"
)

func main() {
    // 配置连接
    config := pglogrepl.Config{
        Host:     "localhost",
        Port:     5432,
        Username: "postgres",
        Password: "password",
        Database: "mydb",
        Schema:   "public",
        Tables:   []string{"users", "orders"}, // 为空表示所有表
        LSNFile:  "/var/lib/myapp/lsn.dat",
    }

    // 创建复制实例
    repl := pglogrepl.New(config)

    // 注册事件回调
    repl.SetCallback(pglogrepl.EventInsert, func(table pglogrepl.Table, data map[string]any) {
        log.Printf("[INSERT] %s: %v", table.Name(), data)
    })

    repl.SetCallback(pglogrepl.EventUpdate, func(table pglogrepl.Table, data map[string]any) {
        log.Printf("[UPDATE] %s: %v", table.Name(), data)
    })

    repl.SetCallback(pglogrepl.EventDelete, func(table pglogrepl.Table, data map[string]any) {
        log.Printf("[DELETE] %s: %v", table.Name(), data)
    })

    // 启动复制
    ctx := context.Background()
    if err := repl.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer repl.Stop()

    // 阻塞等待（或等待信号）
    select {}
}
```

## 工作原理

### 初次启动（无 LSN）

1. 使用 `REPEATABLE READ` 隔离级别创建一致性数据库快照
2. 读取配置表中的所有现有行
3. 为每个现有行触发 `EventInsert` 事件
4. 创建 replication slot 和 publication
5. 切换到增量同步模式
6. 从快照位置开始消费 WAL 变更

### 恢复启动（有 LSN）

1. 从文件加载保存的 LSN
2. 验证 LSN 与当前配置是否匹配
3. 连接到现有的 replication slot
4. 从保存的位置继续消费 WAL 变更
5. 为每个数据修改触发对应事件

### LSN 持久化

日志序列号（LSN）跟踪 PostgreSQL Write-Ahead Log 中的当前位置：

- **写入**：每次变更时写入文件（缓冲）
- **同步**：每分钟自动同步到磁盘
- **格式**：`checksum/LSN_string`（例如：`1234567890/0/16B2E58`）
- **恢复**：防止崩溃后重复处理数据

## 配置说明

### Config 字段

| 字段       | 类型       | 说明                                              | 默认值                |
| ---------- | ---------- | ------------------------------------------------- | --------------------- |
| `Host`     | `string`   | PostgreSQL 服务器主机名                           | `localhost`           |
| `Port`     | `int`      | PostgreSQL 服务器端口                             | `5432`                |
| `Username` | `string`   | 数据库用户（需要 REPLICATION 权限）               | -                     |
| `Password` | `string`   | 数据库密码                                        | -                     |
| `Database` | `string`   | 目标数据库名称                                    | -                     |
| `SSLMode`  | `string`   | SSL 模式（disable/require/verify-ca/verify-full） | `disable`             |
| `Tables`   | `[]string` | 要复制的表（空 = 所有表）                         | `[]`（所有表）        |
| `Schema`   | `string`   | PostgreSQL schema 名称                            | `public`              |
| `PubName`  | `string`   | Publication 名称                                  | `pglogrepl_demo`      |
| `SlotName` | `string`   | Replication slot 名称                             | `{PubName}_sync_slot` |
| `LSNFile`  | `string`   | LSN 持久化文件路径                                | -                     |
| `Logger`   | `Logger`   | 自定义日志实现                                    | stdout logger         |

### 自定义日志

实现 `Logger` 接口进行自定义日志记录：

```go
type MyLogger struct{}

func (l *MyLogger) LogInfo(format string, args ...any) {
    log.Printf("[INFO] "+format, args...)
}

func (l *MyLogger) LogError(format string, args ...any) {
    log.Printf("[ERROR] "+format, args...)
}

func (l *MyLogger) LogWarn(format string, args ...any) {
    log.Printf("[WARN] "+format, args...)
}

config.Logger = &MyLogger{}
```

或使用函数适配器：

```go
config.Logger = pglogrepl.LoggerFunc(func(level, format string, args ...any) {
    msg := fmt.Sprintf(format, args...)
    log.Printf("[%s] %s", level, msg)
})
```

## 事件处理

### 事件类型

- `EventInsert`：插入新行时触发
- `EventUpdate`：修改行时触发
- `EventDelete`：删除行时触发
- `EventTruncate`：截断表时触发（未完全实现）

### 回调数据

回调函数接收：

- `table`：限定表名（包含 schema）
- `data`：列名到值的映射

```go
repl.SetCallback(pglogrepl.EventUpdate, func(table pglogrepl.Table, data map[string]any) {
    schema := table.Schema() // 例如 "public"
    name := table.Name()     // 例如 "users"

    if userID, ok := data["id"].(int64); ok {
        log.Printf("用户 %d 在 %s.%s 中更新", userID, schema, name)
    }
})
```

### 关于 DELETE 事件的说明

要在 `DELETE` 事件中包含旧行数据，必须设置表的副本标识：

```sql
-- 选项 1：完整行（所有列）
ALTER TABLE users REPLICA IDENTITY FULL;

-- 选项 2：使用主键（默认，仅主键列）
ALTER TABLE users REPLICA IDENTITY DEFAULT;

-- 选项 3：使用唯一索引
ALTER TABLE users REPLICA IDENTITY USING INDEX users_email_key;
```

## 状态监控

### 状态代码

- `StatusStopped`：复制未运行
- `StatusStarting`：正在初始化连接和 slot
- `StatusSyncing`：正在执行全量同步
- `StatusListening`：增量复制活动中
- `StatusStopping`：正在优雅关闭

### 检查状态

```go
status := repl.Status()
statusName := pglogrepl.StatusName(status)
log.Printf("复制状态: %s", statusName)

if err := repl.Err(); err != nil {
    log.Printf("复制错误: %v", err)
}
```

## 优雅关闭

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/hupeh/pglogrepl"
)

func main() {
    config := pglogrepl.Config{
        // ... 配置
    }

    repl := pglogrepl.New(config)

    // 设置信号处理
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

    // 启动复制
    ctx := context.Background()
    if err := repl.Start(ctx); err != nil {
        log.Fatal(err)
    }

    // 等待信号
    <-sigChan
    log.Println("正在关闭...")

    // 优雅停止
    if err := repl.Stop(); err != nil {
        log.Printf("关闭时出错: %v", err)
    }
}
```

## 高级用法

### 过滤特定表

```go
config := pglogrepl.Config{
    // ... 其他字段
    Tables: []string{"users", "orders", "products"},
}
```

### 复制所有表

```go
config := pglogrepl.Config{
    // ... 其他字段
    Tables: []string{}, // 空 = schema 中的所有表
}
```

### 自定义 LSN 同步间隔

包默认每分钟将 LSN 同步到磁盘一次。同步在后台 goroutine 中自动进行。如果需要手动控制，可以直接访问 LSN 实例调用 `lsn.Sync()`。

### 处理大事务

PostgreSQL 14+ 支持流式传输大事务。包自动处理：

- `StreamStartMessageV2`：开始流式传输大事务
- `StreamCommitMessageV2`：提交流式传输的事务
- `StreamAbortMessageV2`：中止流式传输的事务

## 故障排除

### Replication Slot 已存在

如果看到关于现有 replication slot 的错误：

```sql
-- 列出所有 replication slot
SELECT * FROM pg_replication_slots;

-- 删除特定 slot
SELECT pg_drop_replication_slot('slot_name');
```

### Publication 已存在

```sql
-- 列出所有 publication
SELECT * FROM pg_publication;

-- 删除特定 publication
DROP PUBLICATION publication_name;
```

### LSN 校验和不匹配

当配置（数据库、schema、表等）更改但 LSN 文件仍然存在时会出现此错误。解决方案：

1. 删除 LSN 文件以强制全量重新同步
2. 调用 `lsn.Reset()` 清除 LSN
3. 确保配置与之前的运行匹配

### 权限被拒绝

确保 PostgreSQL 用户具有所需权限：

```sql
-- 授予复制权限
ALTER USER your_user WITH REPLICATION;

-- 授予必要的表权限
GRANT SELECT ON ALL TABLES IN SCHEMA public TO your_user;
```

## 性能考虑

1. **LSN 同步频率**：LSN 每分钟同步到磁盘一次。更频繁的同步会增加 I/O，但减少潜在的数据丢失窗口。

2. **事件回调**：保持回调轻量级。繁重的处理应该异步完成：

   ```go
   eventChan := make(chan Event, 1000)

   repl.SetCallback(pglogrepl.EventInsert, func(table pglogrepl.Table, data map[string]any) {
       eventChan <- Event{Table: table, Data: data}
   })

   // 在单独的 goroutine 中处理事件
   go processEvents(eventChan)
   ```

3. **网络延迟**：为复制使用专用连接或连接池，以避免阻塞其他操作。

4. **WAL 保留**：监控 WAL 磁盘使用情况。不活动的 replication slot 会阻止 WAL 清理。

## 许可证

此包是 go-slim 框架的一部分。

## 贡献

欢迎贡献！请确保：

- 代码遵循 Go 规范
- 所有测试通过
- 文档已更新
- 提交消息清晰

## 支持

问题和咨询：

- GitHub Issues：[pglogrepl issues](https://github.com/hupeh/pglogrepl/issues)
- 文档：[Go 包文档](https://pkg.go.dev/github.com/hupeh/pglogrepl)
