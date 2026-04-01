# 模块依赖管理说明 (Workspace模式)

## 当前结构

本项目采用Go Workspace多模块结构，包含以下子模块：
- `api` - API定义和协议缓冲区
- `audit` - 审计功能
- `pagination` - 分页功能
- `viewer` - 视图相关功能
- `entgo` - Ent框架集成
- `gorm` - GORM框架集成
- `cassandra` - Cassandra数据库集成
- `clickhouse` - ClickHouse数据库集成
- `elasticsearch` - Elasticsearch集成
- `influxdb` - InfluxDB集成
- `mongodb` - MongoDB集成

## 依赖管理 (Workspace模式)

在Go Workspace模式下，模块间的依赖管理如下：

### go.mod文件
- 各模块的go.mod文件保留必要的require语句
- **移除**了所有内部模块间的replace指令
- 各模块可以相互引用，Go工具会自动使用workspace中的本地副本

### go.work文件
- 统一管理所有模块：使用`use`指令注册所有模块
- 外部依赖的replace指令集中在此文件中：

```go
replace github.com/chnxq/x-utils => ../x-utils
replace github.com/chnxq/x-utils/id => ../x-utils/id
replace github.com/chnxq/x-utils/mapper => ../x-utils/mapper
```

## 构建和测试

使用工作区模式进行开发：

```bash
# 同步工作区依赖
go work sync

# 构建所有模块
go build all

# 测试所有模块
go test all
```

## 优势

1. **集中管理**：所有replace指令集中在go.work文件中
2. **简化模块**：各go.mod文件更简洁，无冗余replace指令
3. **统一依赖**：workspace自动处理内部模块引用
4. **开发友好**：修改任一模块，其他依赖模块自动使用最新版本

## 注意事项

1. 需要Go 1.18+版本支持workspace功能
2. 内部模块依赖仍需在require中声明，但无需replace指令
3. 外部依赖（如x-utils）的路径映射在go.work中统一管理