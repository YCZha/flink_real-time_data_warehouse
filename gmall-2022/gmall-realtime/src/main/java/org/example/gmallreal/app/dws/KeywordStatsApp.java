package org.example.gmallreal.app.dws;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.gmallreal.app.func.KeywordUDTF;
import org.example.gmallreal.bean.KeywordStats;
import org.example.gmallreal.common.GmallConstant;
import org.example.gmallreal.utils.ClickHouseUtil;
import org.example.gmallreal.utils.MyKafkaUtil;

//搜索关键词主题宽表统计
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {

        //TODO 1. 基本环境准备
        //配置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setParallelism(3);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/checkpoint/keywordstatsapp"));
        env.setRestartStrategy(RestartStrategies.noRestart());
        System.setProperty("HADOOP_USER_NAME", "zyc");

        //定义table流环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //TODO 2. 注册自定义函数
        tableEnv.createTemporarySystemFunction("ika_analyze", KeywordUDTF.class);
        //TODO 3. 创建动态表
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic ="dwd_page_log";
        tableEnv.executeSql("CREATE TABLE page_view " +
                "(common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>,ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                "WITH ("+ MyKafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId)+")");

        //TODO 4. 从动态表中查询数据
        Table fullwordView = tableEnv.sqlQuery("select page['item'] fullword ," +
                "rowtime from page_view " +
                "where page['page_id']='good_list' " +
                "and page['item'] IS NOT NULL ");

        //TODO 5. 利用自定义函数对搜索关键词进行拆分
        //使用内连接，将关键字和分词出来的数据进行一个关联
        Table keywordView = tableEnv.sqlQuery("select keyword,rowtime from " + fullwordView + " ," +
                " LATERAL TABLE(ika_analyze(fullword)) as T(keyword)");


        //TODO 6， 分组、开窗、聚合
        Table keywordStatsSearch = tableEnv.sqlQuery("select keyword,count(*) ct, '"
                        + GmallConstant.KEYWORD_SEARCH + "' source ," +
                        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from "+keywordView
                        + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword");


        //TODO 7. 动态表转换为流

        DataStream<KeywordStats> keywordStatsSearchDataStream =
                tableEnv.<KeywordStats>toAppendStream(keywordStatsSearch, KeywordStats.class);

        keywordStatsSearchDataStream.print("keyword>>>>>>>>>>>>>>>");
        //TODO 8. 写入到clickHouse
        keywordStatsSearchDataStream.addSink(
                ClickHouseUtil.<KeywordStats>getJdbcSink(
                        "insert into keyword_stats_2022(keyword,ct,source,stt,edt,ts) " + " values(?,?,?,?,?,?)")
        );

        env.execute();

    }
}
