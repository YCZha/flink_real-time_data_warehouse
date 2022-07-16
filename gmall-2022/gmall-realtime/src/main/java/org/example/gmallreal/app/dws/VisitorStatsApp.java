package org.example.gmallreal.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.gmallreal.bean.VisitStats;
import org.example.gmallreal.utils.ClickHouseUtil;
import org.example.gmallreal.utils.MyKafkaUtil;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

//访客主题统计
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1. 基本环境准备
        //配置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setParallelism(3);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/checkpoint/visitstatsapp"));
        env.setRestartStrategy(RestartStrategies.noRestart());
        System.setProperty("HADOOP_USER_NAME", "zyc");

        //TODO 2. 从kafka主题中读取数据
        //2.1 定义主题和消费者主
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        String groupId = "visitor_stats_app";

        //2.2 从kafka中读取数据
        DataStreamSource<String> pvJsonStrDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvJsonStrDS = env.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> userJumpJsonStrDS = env.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId));

        //2.3 输出各个流的数据
        pvJsonStrDS.print("pv>>>>>>>>>>>>>>>>>");
        uvJsonStrDS.print("uv>>>>>>>>>>>>>>>");
        userJumpJsonStrDS.print("userJump>>>>>>>>>>>>>>>>>>");

        //TODO 3. 统一格式，合并，将三条流转换为四条流
        //3.1 转换pv 对各个流的数据进行结构转换，jsonStr->VisitStats
        SingleOutputStreamOperator<VisitStats> pvStatsDS = pvJsonStrDS.map(
                new MapFunction<String, VisitStats>() {
                    @Override
                    public VisitStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        VisitStats visitStats = new VisitStats(
                                "",
                                "",
                                jsonObj.getJSONObject("common").getString("vc"),
                                jsonObj.getJSONObject("common").getString("ch"),
                                jsonObj.getJSONObject("common").getString("ar"),
                                jsonObj.getJSONObject("common").getString("is_new"),
                                0L,
                                1L,
                                0L,
                                0L,
                                jsonObj.getJSONObject("page").getLong("during_time"),
                                jsonObj.getLong("ts")
                        );
                        return visitStats;
                    }
                }
        );
        //3.2 转换uv
        SingleOutputStreamOperator<VisitStats> uvStatsDS = uvJsonStrDS.map(
                new MapFunction<String, VisitStats>() {
                    @Override
                    public VisitStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        VisitStats visitStats = new VisitStats(
                                "",
                                "",
                                jsonObj.getJSONObject("common").getString("vc"),
                                jsonObj.getJSONObject("common").getString("ch"),
                                jsonObj.getJSONObject("common").getString("ar"),
                                jsonObj.getJSONObject("common").getString("is_new"),
                                1L,
                                0L,
                                0L,
                                0L,
                                0L,
                                jsonObj.getLong("ts")
                        );
                        return visitStats;
                    }
                }
        );
        //3.3 转换sv流（session_count），会话次数，从page_log里读，过滤last_page_id不为空的数据
        SingleOutputStreamOperator<VisitStats> svStatsDS = pvJsonStrDS.process(
                new ProcessFunction<String, VisitStats>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, VisitStats>.Context context, Collector<VisitStats> collector) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String last_page_id = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (last_page_id == null || last_page_id.length() == 0) {
                            VisitStats visitStats = new VisitStats(
                                    "",
                                    "",
                                    jsonObj.getJSONObject("common").getString("vc"),
                                    jsonObj.getJSONObject("common").getString("ch"),
                                    jsonObj.getJSONObject("common").getString("ar"),
                                    jsonObj.getJSONObject("common").getString("is_new"),
                                    0L,
                                    0L,
                                    1L,
                                    0L,
                                    0L,
                                    jsonObj.getLong("ts")
                            );
                            collector.collect(visitStats);
                        }
                    }
                }
        );
        //3.4 转换跳出流
        SingleOutputStreamOperator<VisitStats> userJumpStatsDS = userJumpJsonStrDS.map(
                new MapFunction<String, VisitStats>() {
                    @Override
                    public VisitStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        VisitStats visitStats = new VisitStats(
                                "",
                                "",
                                jsonObj.getJSONObject("common").getString("vc"),
                                jsonObj.getJSONObject("common").getString("ch"),
                                jsonObj.getJSONObject("common").getString("ar"),
                                jsonObj.getJSONObject("common").getString("is_new"),
                                0L,
                                0L,
                                0L,
                                1L,
                                0L,
                                jsonObj.getLong("ts")
                        );
                        return visitStats;
                    }
                }
        );
        //3.5 将四条流合并到一起,要求合并的流的结构必须相同
        DataStream<VisitStats> unionDS = pvStatsDS.union(uvStatsDS, svStatsDS, userJumpStatsDS);

        //TODO 4。分组、开窗、聚合
        //4.1 设置waterMark，提取事件时间
        SingleOutputStreamOperator<VisitStats> visitorStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<VisitStats>() {
                                    @Override
                                    public long extractTimestamp(VisitStats visitStats, long l) {
                                        return visitStats.getTs();
                                    }
                                }
                        )
        );
        //4.2 分组 按照地区、渠道、版本、新老访客维度进行分组，因为有四个维度，所以将它们封装为一个Tuple4
        KeyedStream<VisitStats, Tuple4<String, String, String, String>> keyedDS = visitorStatsWithWatermarkDS.keyBy(
                new KeySelector<VisitStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitStats visitStats) throws Exception {
                        return Tuple4.of(
                                visitStats.getAr(),
                                visitStats.getCh(),
                                visitStats.getVc(),
                                visitStats.getIs_new()
                        );
                    }
                }
        );

        //4.3 开窗
        WindowedStream<VisitStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDS.window(
                //设置滚动窗口
                TumblingEventTimeWindows.of(Time.seconds(10))
        );

        //4.4 对窗口中的数据进行聚合，聚合结束之后，需要补充统计的起止时间
        SingleOutputStreamOperator<VisitStats> reduceDS = windowDS.reduce(
                //指定聚合方法
                new ReduceFunction<VisitStats>() {
                    @Override
                    public VisitStats reduce(VisitStats stats1, VisitStats stats2) throws Exception {
                        stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                        stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                        stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                        stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                        return stats1;
                    }
                },
                new ProcessWindowFunction<VisitStats, VisitStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> tuple4, ProcessWindowFunction<VisitStats, VisitStats, Tuple4<String, String, String, String>, TimeWindow>.Context context, Iterable<VisitStats> elements, Collector<VisitStats> collector) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                        for (VisitStats visitStats : elements) {
                            String startDate = sdf.format(new Date(context.window().getStart()));
                            String endDate = sdf.format(new Date(context.window().getEnd()));
                            visitStats.setStt(startDate);
                            visitStats.setEdt(endDate);
                            visitStats.setTs(new Date().getTime());
                            collector.collect(visitStats);
                        }
                    }
                }
        );
        reduceDS.print("reduce>>>>>>>>>>>>>>>>>>>");
        //TODO 5. 将数据写道OLAP数据库中，大数据量存储，存储时间长，不适合在kafka，易于查询

        reduceDS.addSink(ClickHouseUtil.getJdbcSink("insert into visitor_stats_2022 values(?,?,?,?,?,?,?,?,?,?,?,?)"));
        env.execute();

    }
}
