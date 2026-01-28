# AWS SDK for Go v1 → v2 迁移文档（状态跟踪版）

**目标**：将项目中使用的 `github.com/aws/aws-sdk-go`（v1）迁移到 `github.com/aws/aws-sdk-go-v2/*`（v2），统一 SDK 版本并完成安全合规升级。

**状态**：✅ 已完成

---

## 1. 背景与范围

- v1 与 v2 **不兼容**，迁移必然涉及代码修改。
- 迁移后可减少维护负担并更好地支持后续安全修复。

---

## 2. 现状盘点（仓库内 v1 使用点）

**必改文件（v1 SDK 直接引用）：** ✅ 已全部迁移
- `cmd/admin/migrate.go`
- `internal/aws/*`
- `internal/s3/*`
- `internal/storage/blobstorage/s3/*`
- `internal/dlq/sqs/*`
- `internal/storage/metastorage/dynamodb/*`
- 相关测试与 mocks：
  - `internal/s3/mocks/*`
  - `internal/storage/metastorage/dynamodb/mocks/*`

**已使用 v2 的文件：**
- `cmd/admin/db_init.go`（`config` + `secretsmanager`）

---

## 3. 迁移原则（摘要）

- **Session → Config**：`session.NewSession()` → `config.LoadDefaultConfig(ctx)`
- **API 调用方式变化**：`WithContext` / `Request` 版本消失，统一为 `Operation(ctx, input)`
- **类型与指针变化**：v2 中大量入参不再是指针
- **attributevalue 替代**：`dynamodbattribute` → `feature/dynamodb/attributevalue`
- **Mock 方式变更**：v1 的 `*iface` 包不存在，需要自定义接口 + 重新生成 mocks

---

## 4. 推荐迁移顺序（阶段化推进）

### 阶段 1：基础设施与公共模块 ✅
- [x] `internal/aws/*`（统一 config / retry / session 逻辑）
  - 删除 `session.go`，重写 `config.go` 和 `retryer.go`
  - 使用 `config.LoadDefaultConfig` 替代 `session.NewSession`
  - 添加 DataDog tracing 中间件 `awstrace.AppendMiddleware`

### 阶段 2：DynamoDB ✅
- [x] `internal/storage/metastorage/dynamodb/*`
  - 更新所有文件使用 v2 API
  - `dynamodbattribute` → `attributevalue`
  - `types.AttributeValue` 替代 `*dynamodb.AttributeValue`
- [x] `cmd/admin/migrate.go`（含 DynamoDB 操作）
  - `dynamodb.New(session)` → `dynamodb.NewFromConfig(cfg)`
  - `QueryWithContext` → `Query`

### 阶段 3：S3 ✅
- [x] `internal/s3/*`
  - `Downloader` 接口使用 `io.WriterAt`
  - `manager.NewUploader/Downloader` 使用 v2 API
- [x] `internal/storage/blobstorage/s3/*`
  - ACL 使用 `types.ObjectCannedACLBucketOwnerFullControl`

### 阶段 4：SQS ✅
- [x] `internal/dlq/sqs/*`
  - `sqs.New(session)` → `sqs.NewFromConfig(cfg)`
  - `DelaySecs` 和 `VisibilityTimeoutSecs` 使用 `int32` 类型

### 阶段 5：测试与 Mock ✅
- [x] `internal/s3/mocks/*` - 重新生成
- [x] `internal/storage/metastorage/dynamodb/mocks/*` - 重新生成
- [x] 相关 unit tests / integration tests - 已更新

---

## 5. 迁移细节 Checklist

### 依赖调整（go.mod） ✅
- [x] 删除 `github.com/aws/aws-sdk-go`
- [x] 添加 v2 依赖（按服务）：
  - [x] `github.com/aws/aws-sdk-go-v2/config`
  - [x] `github.com/aws/aws-sdk-go-v2/service/s3`
  - [x] `github.com/aws/aws-sdk-go-v2/feature/s3/manager`
  - [x] `github.com/aws/aws-sdk-go-v2/service/sqs`
  - [x] `github.com/aws/aws-sdk-go-v2/service/dynamodb`
  - [x] `github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue`
  - [x] `github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression`

### API 调用转换 ✅
- [x] `NewSession()` → `LoadDefaultConfig(ctx)`
- [x] `New(sess)` → `NewFromConfig(cfg)`
- [x] `OperationWithContext` → `Operation(ctx, input)`
- [x] `dynamodbattribute` → `attributevalue`

### Retry / Middleware ✅
- [x] 将 v1 handler/Retryer 重写为 v2 `retry.Standard` 或自定义 `aws.Retryer`

### Mock / 接口 ✅
- [x] 自定义接口封装 client 方法
- [x] 重新生成 mocks（`gomock` / `mockgen`）

---

## 6. 验证与回归

**编译** ✅
- [x] `go build ./...`
- [x] `go mod tidy`

**功能验证**
- [x] S3 上传/下载 - 单元测试通过
- [x] DynamoDB 读写 + condition expression - 单元测试通过
- [x] SQS 发送/接收 - 代码已更新
- [x] 管理命令 `cmd/admin/*` - 编译通过

---

## 7. 风险与回滚

**风险**
- v2 类型签名变化导致编译失败 → 已解决
- mocks 失效导致测试失败 → 已重新生成

**回滚**
- 回滚 `go.mod` / `go.sum` 与相关迁移代码
- 保留迁移分支用于增量修复

---

## 8. 附：CVE-2020-8911 处置说明

- `go mod why github.com/aws/aws-sdk-go/service/s3/s3crypto` 显示 **未使用 s3crypto**
- CVE-2020-8911 对本项目 **不可达 / not exploitable**
- 通过迁移到 v2 SDK，彻底消除了该安全隐患
- 记录该结论供合规审计

---

## 9. 进度记录

| 日期 | 阶段 | 负责人 | 备注 |
|------|------|--------|------|
| 2026-01-28 | 阶段 1-5 | Claude | 完成全部迁移，v1 SDK 已从直接依赖中移除 |

---

## 10. 主要代码变更摘要

### internal/aws/config.go
```go
// v1
sess, err := session.NewSession(&aws.Config{...})

// v2
cfg, err := config.LoadDefaultConfig(ctx,
    config.WithRegion(params.Config.AWS.Region),
    config.WithRetryer(newCustomRetryer),
)
awstrace.AppendMiddleware(&cfg)
```

### internal/storage/metastorage/dynamodb/*
```go
// v1
dynamodbattribute.MarshalMap(item)
map[string]*dynamodb.AttributeValue

// v2
attributevalue.MarshalMap(item)
map[string]types.AttributeValue
```

### internal/s3/client.go
```go
// v1 Downloader interface
DownloadWithContext(ctx, w, input) (int64, error)

// v2 Downloader interface
Download(ctx, w io.WriterAt, input *s3.GetObjectInput, ...) (int64, error)
```

### internal/dlq/sqs/*
```go
// v1
DelaySecs: aws.Int64(q.config.AWS.DLQ.DelaySecs)

// v2
DelaySeconds: int32(q.config.AWS.DLQ.DelaySecs)
```
