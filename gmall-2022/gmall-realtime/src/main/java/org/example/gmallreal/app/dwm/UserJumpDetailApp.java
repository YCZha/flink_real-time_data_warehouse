package org.example.gmallreal.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.gmallreal.utils.MyKafkaUtil;

import java.util.List;
import java.util.Map;

//用户跳出行为过滤
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.配置执行环境，设置并行度，检查点
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(3);
        env.setParallelism(1);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/checkpoint/userjumpDetailapp"));
        env.setRestartStrategy(RestartStrategies.noRestart());
        System.setProperty("HADOOP_USER_NAME", "zyc");

        //TODO 2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_group";
        String sinkTopic = "dwm_user_jump_detail";

        DataStreamSource<String> kafkaSourceDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

//        DataStream<String> dataStream = env
//                .fromElements(
//
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"home\"},\"ts\":15000} ",
//
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"detail\"},\"ts\":21000} "
//                );
//        dataStream.print("in json:");
        //TODO 3.将StringDS->jsonDS
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSourceDS.map(jsonStr -> JSON.parseObject(jsonStr));
        jsonObjDS.print("json>>>>>>>>>>>>>>>>");

        //从Flink1.12开始时间语义就是事件事件，不需要额外指定，如果是之前的版本，需要。
        //TODO 4。指定事件时间字段，设置waterMark
        SingleOutputStreamOperator<JSONObject> jsonObjWithTSDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObj, long l) {
                                return jsonObj.getLong("ts"); //指定事件事件为时间戳字段
                            }
                        }
                ));

        //TODO 5.按照mid进行分组
        KeyedStream<JSONObject, String> keyedByMidDS = jsonObjWithTSDS.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );


        /**
         * 计算页面跳出明细需要满足两个条件
         * 1、是首次访问的页面
         * 2、距离首次访问结束指定时间（1分钟）内没有进行访问
         */
        //TODO 6.配置CEP表达式
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(
                        //条件一：首次访问页面
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObj) throws Exception {
                                //获取last_page_id
                                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                                //如果是首次访问，则赋true
                                if (lastPageId == null || lastPageId.length() == 0) {
                                    return true;
                                }
                                return false;
                            }
                        }
                ).next("next") //严格连续，即从第一条件执行之后，会执行第二个条件（首次访问页面，且一分钟内没有访问其他页面，跳出）
                .where(
                        //条件2：，没有对其他页面进行访问
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObj) throws Exception {
                                //获取当前页面的id
                                String pageId = jsonObj.getJSONObject("page").getString("page_id");
                                //判断当前访问的页面id是否为空
                                if (pageId != null && pageId.length() > 0) {
                                    return true;
                                }
                                return false;
                            }
                        }
                ).within(Time.milliseconds(10000));//两个模式之前会存在一个超时时间，如果两个模式之间发生的时间在一分钟之外，则会被丢弃

        //TODO 7.将流和CEP表达式建立关系，从而筛选数据
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedByMidDS, pattern);

        //TODO 8.从筛选的流中提取数据，将超时的数据放到侧输出流中
        OutputTag<String> timeOutTag = new OutputTag<String>("timeout"){};
        SingleOutputStreamOperator<String> filterDS = patternStream.flatSelect(
                timeOutTag,
                //处理超时数据
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long l, Collector<String> collector) throws Exception {
                        //获取所有符合第一个条件的json对象数据
                        List<JSONObject> jsonObjList = pattern.get("first"); //在超时的方法中只能获取第一个模式
                        //注意：在timeout方法中，都会被参数一中的标签标记，即可有获取到timeoutTag
                        for (JSONObject jsonObject : jsonObjList) {
                            collector.collect(jsonObject.toJSONString()); //自带第一个标签tag，所以可以用collect
                        }
                    }
                },
                //处理没有超时数据
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> collector) throws Exception {
                        //没有超时的数据不在统计范围内，所以不需要代码
                          //List<JSONObject> jsonObjectList = pattern.get("next") //两个模式都通过，且属于第一个模式的事件
//                        List<JSONObject> jsonObjectList = pattern.get("next"); //两个模式都通过且属于第二个模式的事件。
//                        for (JSONObject jsonObject : jsonObjectList) {
//                            collector.collect(jsonObject.toJSONString());
//                        }

                    }
                }
        );

        //TODO 9，从侧输出流中获取超时数据
        DataStream<String> jumpDS = filterDS.getSideOutput(timeOutTag);
        jumpDS.print("jump>>>>>>>>>>>>>>>>>>>>>>>>>");
        filterDS.print("filter>>>>>>>>>>>>>>>>>");

        //TODO 10.将当前数据放入kafka
        jumpDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        env.execute();

    }
}
