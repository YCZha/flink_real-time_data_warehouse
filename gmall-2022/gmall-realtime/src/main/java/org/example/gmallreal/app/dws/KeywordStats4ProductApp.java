package org.example.gmallreal.app.dws;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.gmallreal.app.func.KeywordProduct2RUDTF;
import org.example.gmallreal.app.func.KeywordUDTF;
import org.example.gmallreal.bean.KeywordStats;
import org.example.gmallreal.utils.ClickHouseUtil;
import org.example.gmallreal.utils.MyKafkaUtil;

public class KeywordStats4ProductApp {
    public static void main(String[] args) throws Exception {
        //TODO 1. 基本环境准备
        //配置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setParallelism(3);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/checkpoint/keywordstats4app"));
        env.setRestartStrategy(RestartStrategies.noRestart());
        System.setProperty("HADOOP_USER_NAME", "zyc");

        //定义table流环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //TODO 2. 注册自定义函数
        tableEnv.createTemporarySystemFunction("keywordProduct2R", KeywordProduct2RUDTF.class);
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //TODO 3. 将数据源定义为动态表
        String groupId = "keyword_stats_app";
        String productStatsSourceTopic = "dws_product_stats";

        tableEnv.executeSql("CREATE TABLE product_stats (spu_name STRING," +
                "click_ct BIGINT," +
                "cart_ct BIGINT," +
                "order_ct BIGINT," +
                "stt STRING, edt STRING)" +
                "WITH (" + MyKafkaUtil.getKafkaDDL(productStatsSourceTopic,groupId) + ")");

        //TODO 4. 聚合计算
        Table keywordStatsProduct = tableEnv.sqlQuery("select keyword,ct,source, " +
                "DATE_FORMAT(stt,'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(edt,'yyyy-MM-dd HH:mm:ss') as edt, " +
                "UNIX_TIMESTAMP()*1000 ts from product_stats , " +
                "LATERAL TABLE(ik_analyze(spu_name)) as T(keyword) ," +
                "LATERAL TABLE(keywordProduct2R( click_ct,cart_ct,order_ct)) as T2(ct,source)");

        //TODO 5. 转换为数据流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.<KeywordStats>toAppendStream(keywordStatsProduct, KeywordStats.class);
        keywordStatsDataStream.print("keyword>>>>>>>>>>>>>>");

        //TODO 6. 写入clickHouse
        keywordStatsDataStream.addSink(
                ClickHouseUtil.<KeywordStats>getJdbcSink(
                        "insert into keyword_stats_2022(keyword,ct,source,stt,edt,ts) " +
                                "values(?,?,?,?,?,?)"
                )
        );
        env.execute();
    }
}
