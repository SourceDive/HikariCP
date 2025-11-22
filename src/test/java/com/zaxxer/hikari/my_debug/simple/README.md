# HikariCP 源码阅读入口

## SimpleTest.java

这是最简单的 HikariCP 测试程序，作为源码阅读的入口点。

### 特点

- ✅ 使用 Mock 数据源（StubDataSource），无需真实数据库连接
- ✅ 代码简洁，易于理解
- ✅ 包含详细的步骤说明和断点提示
- ✅ 演示了连接池的完整生命周期

### 运行方式

#### 方式1：使用 Maven 运行（推荐）

```bash
mvn test-compile exec:java -Dexec.mainClass="com.zaxxer.hikari.my_debug.SimpleTest" -Dexec.classpathScope=test
```

#### 方式2：在 IDE 中运行

1. 打开 `SimpleTest.java`
2. 右键点击 `main` 方法
3. 选择 "Run 'SimpleTest.main()'"

#### 方式3：使用 Java 命令运行

```bash
# 先编译
mvn test-compile

# 运行
java -cp "target/test-classes:target/classes:$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q)" \
  com.zaxxer.hikari.my_debug.SimpleTest
```

### 源码阅读建议

#### 关键断点位置

1. **HikariConfig 构造函数**
   - 了解配置的默认值
   - 文件：`src/main/java/com/zaxxer/hikari/HikariConfig.java:56`

2. **HikariDataSource 构造函数**
   - 连接池的创建入口
   - 文件：`src/main/java/com/zaxxer/hikari/HikariDataSource.java:46`

3. **HikariPool 构造函数**
   - 连接池核心初始化逻辑
   - 文件：`src/main/java/com/zaxxer/hikari/HikariPool.java`

4. **HikariPool.getConnection()**
   - 从连接池获取连接的逻辑
   - 文件：`src/main/java/com/zaxxer/hikari/HikariPool.java`

5. **ConnectionProxy.close()**
   - 连接归还到连接池的逻辑
   - 文件：`src/main/java/com/zaxxer/hikari/proxy/ConnectionProxy.java`

#### 核心类说明

- **HikariConfig**: 连接池配置类，包含所有可配置参数
- **HikariDataSource**: 数据源入口，实现 JDBC DataSource 接口
- **HikariPool**: 连接池核心实现，管理连接的创建、获取、归还
- **ConnectionProxy**: 连接代理类，包装真实连接，实现连接池功能
- **ProxyFactory**: 代理工厂，负责创建连接代理对象

#### 执行流程

```
1. 创建 HikariConfig 配置对象
   ↓
2. 创建 HikariDataSource（内部创建 HikariPool）
   ↓
3. 调用 dataSource.getConnection()
   ↓
4. HikariPool.getConnection() 获取连接
   ↓
5. 创建 ConnectionProxy 包装真实连接
   ↓
6. 使用连接执行 SQL
   ↓
7. 调用 connection.close()
   ↓
8. ConnectionProxy.close() 归还连接到池中
```

### 调试技巧

1. **设置条件断点**：在关键方法入口设置断点，观察参数值
2. **查看调用栈**：理解方法调用链
3. **观察变量值**：关注连接池状态（总连接数、空闲连接数等）
4. **单步执行**：逐步跟踪代码执行路径

### 扩展学习

- 修改连接池参数，观察行为变化
- 尝试多线程场景，理解并发控制
- 查看连接泄漏检测机制
- 研究连接池的监控和管理功能

