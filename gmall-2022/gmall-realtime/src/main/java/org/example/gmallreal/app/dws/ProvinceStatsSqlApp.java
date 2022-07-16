package org.example.gmallreal.app.dws;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.gmallreal.bean.ProvinceStats;
import org.example.gmallreal.utils.ClickHouseUtil;
import org.example.gmallreal.utils.MyKafkaUtil;

//使用flinkSQL 对地区主题进行统计
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1. 基本环境准备
        //配置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setParallelism(3);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/checkpoint/provincestatsapp"));
        env.setRestartStrategy(RestartStrategies.noRestart());
        System.setProperty("HADOOP_USER_NAME", "zyc");

        //定义table流环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //TODO 2. 把数据源定义为动态表
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (province_id BIGINT, " +
                        "province_name STRING,province_area_code STRING," +
                        "province_iso_code STRING,province_3166_2_code STRING,order_id STRING, " +
                        "total_amount DOUBLE,create_time STRING,rowtime AS TO_TIMESTAMP(create_time) ," +
                        "WATERMARK FOR rowtime AS rowtime)" +
                        " WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");


        //TODO 3. 聚合计算
        Table provinceStateTable = tableEnv.sqlQuery("select " +
                        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                " province_id,province_name,province_area_code," +
                        "province_iso_code,province_3166_2_code," +
                        "COUNT( DISTINCT order_id) order_count, sum(total_amount) order_amount," +
                        "UNIX_TIMESTAMP()*1000 ts "+
                        " from ORDER_WIDE group by TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                        " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");

        //TODO 4. 将动态表转化为流
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);

        provinceStatsDS.print("finalDS>>>>>>>>>>>>>>>>>>");
        //TODO 5. 将流中数据保存到ClickHouse
        provinceStatsDS.addSink(ClickHouseUtil.getJdbcSink("insert into province_stats_2022 values(?,?,?,?,?,?,?,?,?,?)"));
        env.execute();
    }


}
