package com.zaxxer.hikari.my_debug.simple;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 最简单的 HikariCP 测试程序
 * 作为源码阅读的入口点
 * 
 * 使用 StubDataSource（Mock 数据源），无需真实数据库连接
 * 适合用于：
 * 1. 理解 HikariCP 的基本使用流程
 * 2. 调试和跟踪源码执行路径
 * 3. 学习连接池的工作原理
 * 
 * 使用说明：
 * 1. 在 IDE 中设置断点，逐步跟踪代码执行
 * 2. 重点关注以下类的执行流程：
 *    - HikariConfig: 配置类
 *    - HikariDataSource: 数据源入口
 *    - HikariPool: 连接池核心实现
 *    - ConnectionProxy: 连接代理类
 */
public class SimpleTest {
    
    public static void main(String[] args) throws SQLException {
        System.out.println("=== HikariCP 源码阅读入口测试 ===");
        
        // 1. 创建配置对象
        System.out.println("\n[步骤1] 创建 HikariConfig 配置对象...");
        HikariConfig config = getHikariConfig();

        // 2. 创建数据源（这里会创建连接池）
        // 断点位置：HikariDataSource 构造函数 -> HikariPool 构造函数
        System.out.println("\n[步骤2] 创建 HikariDataSource（会初始化连接池）...");
        DataSource dataSource = new HikariDataSource(config);
        System.out.println("数据源创建成功！");
        
        // 打印连接池状态（通过反射访问 pool 字段）
        printPoolStatus(dataSource, "创建数据源后");
        
        // 3. 从连接池获取连接
        // 断点位置：HikariPool.getConnection()
        System.out.println("\n[步骤3] 从连接池获取连接...");
        Connection connection = dataSource.getConnection();
        System.out.println("获取到连接: " + connection.getClass().getName());
        System.out.println("连接是否为代理对象: " + (connection.getClass().getName().contains("Proxy")));
        
        printPoolStatus(dataSource, "获取连接后");
        
        // 4. 使用连接执行 SQL（Mock 操作）
        // 断点位置：ConnectionProxy 的各种方法
        System.out.println("\n[步骤4] 使用连接执行操作...");
        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM test WHERE id=?");
        stmt.setInt(1, 1);
        ResultSet rs = stmt.executeQuery();
        
        if (rs.next()) {
            System.out.println("查询结果: " + rs.getString(1));
        }
        
        rs.close();
        stmt.close();
        
        // 5. 归还连接到连接池
        // 断点位置：ConnectionProxy.close()
        System.out.println("\n[步骤5] 归还连接到连接池...");
        connection.close();
        
        printPoolStatus(dataSource, "归还连接后");
        
        // 6. 再次获取连接（应该复用之前的连接）
        System.out.println("\n[步骤6] 再次获取连接（测试连接复用）...");
        Connection connection2 = dataSource.getConnection();
        System.out.println("获取到连接: " + connection2.getClass().getName());
        System.out.println("是否为同一个连接对象: " + (connection == connection2));
        
        connection2.close();
        
        System.out.println("\n=== 测试完成 ===");
        System.out.println("\n提示：");
        System.out.println("1. 在 IDE 中设置断点，重新运行此程序");
        System.out.println("2. 逐步跟踪代码执行，理解 HikariCP 的工作流程");
        System.out.println("3. 重点关注连接池的创建、连接的获取和归还过程");
    }

    private static HikariConfig getHikariConfig() {
        HikariConfig config = new HikariConfig();

        // 配置连接池参数
        config.setMinimumPoolSize(1);      // 最小连接数
        config.setMaximumPoolSize(2);      // 最大连接数
        config.setAcquireIncrement(1);     // 每次获取连接的增量
        config.setConnectionTestQuery("VALUES 1");  // 连接测试查询

        // 使用 Mock 数据源（不需要真实数据库）
        config.setDataSourceClassName("com.zaxxer.hikari.mocks.StubDataSource");

        System.out.println("配置完成:");
        System.out.println("  - 最小连接数: " + config.getMinimumPoolSize());
        System.out.println("  - 最大连接数: " + config.getMaximumPoolSize());
        System.out.println("  - 数据源类: " + config.getDataSourceClassName());
        return config;
    }

    /**
     * 打印连接池状态信息（通过反射访问包级别的 pool 字段）
     */
    private static void printPoolStatus(DataSource dataSource, String stage) {
        try {
            // 通过反射访问包级别的 pool 字段
            Field poolField = HikariDataSource.class.getDeclaredField("pool");
            poolField.setAccessible(true);
            Object pool = poolField.get(dataSource);
            
            // 通过反射调用 HikariPool 的方法
            Class<?> poolClass = pool.getClass();
            java.lang.reflect.Method getTotalConnections = poolClass.getMethod("getTotalConnections");
            java.lang.reflect.Method getIdleConnections = poolClass.getMethod("getIdleConnections");
            
            int total = (Integer) getTotalConnections.invoke(pool);
            int idle = (Integer) getIdleConnections.invoke(pool);
            int active = total - idle;
            
            System.out.println(String.format("[%s] 连接池状态:", stage));
            System.out.println("  - 总连接数: " + total);
            System.out.println("  - 空闲连接数: " + idle);
            System.out.println("  - 活跃连接数: " + active);
        } catch (Exception e) {
            System.out.println(String.format("[%s] 无法获取连接池状态: %s", stage, e.getMessage()));
        }
    }
}

