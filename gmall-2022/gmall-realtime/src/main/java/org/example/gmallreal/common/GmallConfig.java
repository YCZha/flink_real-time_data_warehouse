package org.example.gmallreal.common;

//项目配置的常量类
public class GmallConfig {
    //hbase命名空间
    public static final String HBASE_SCHEMA = "GMALL0622_REALTIME";
    //phoenix连接的服务器地址
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    //ClickHouse连接的服务器地址
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";

}
