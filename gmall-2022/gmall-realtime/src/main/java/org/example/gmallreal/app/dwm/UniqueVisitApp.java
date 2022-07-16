package org.example.gmallreal.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.example.gmallreal.utils.MyKafkaUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

//完成独立访客uv的计算
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.基本环境准备
        //1.1准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
//        env.setParallelism(3);
        //1.3 设置检查点
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/checkpoint/uniquevisitapp"));
        env.setRestartStrategy(RestartStrategies.noRestart());
        System.setProperty("HADOOP_USER_NAME","zyc");

        //TODO 2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "uv_group";
        String sinkTopic = "dwm_unique_visit";
        FlinkKafkaConsumer<String> pageSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStrDS = env.addSource(pageSource);

        //TODO 3.对读取到的数据进行结构的转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));
//        jsonObjDS.print("pageJson>>>>>>>>>>>>>>");

        //TODO 4.按照mid进行分组
        KeyedStream<JSONObject, String> keyByWithMidDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 5.过滤页面信息，获得uv独立访客
        SingleOutputStreamOperator<JSONObject> filteredDS = keyByWithMidDS.filter(
                new RichFilterFunction<JSONObject>() {
                    //定义状态
                    ValueState<String> lastVisitDataState = null;
                    //定义日期工具类
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化状态和日期工具类
                        sdf = new SimpleDateFormat("yyyyMMdd");
                        //初始化状态
                        ValueStateDescriptor<String> lastVisitDataStateDes = new ValueStateDescriptor<>("lastVisitDataState", String.class);
                        //因为统计的日活，所以昨天的状态是没用的状态，所以可以设置失效时间:1天
                        lastVisitDataStateDes.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        this.lastVisitDataState = getRuntimeContext().getState(lastVisitDataStateDes);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        //首先判断当前页面是否是首次访问，还是从其他页面跳转
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        //已经访问过的页面
                        if (lastPageId != null && lastPageId.length() > 0) {
                            return false; //把从其他页面跳转过来的页面进行过滤
                        }

                        //获取当前访问时间
                        Long ts = jsonObj.getLong("ts");
                        //将当前访问时间戳转化为日期字符串
                        String logData = sdf.format(new Date(ts));
                        //获取状态日期
                        String lastVisitData = lastVisitDataState.value();
                        //用当前页面的访问时间和状态时间进行对比
                        if (lastVisitData != null && lastVisitData.length() > 0 && lastVisitData.equals(logData)) { //曾经访问过
                            System.out.println("已访问，上次访问时间：lastVisitData" + lastVisitData);
                            return false;
                        } else {
                            //没访问过，或者一天后再访问，状态更新
                            lastVisitDataState.update(logData);
                            return true;
                        }
                    }
                }
        );
        filteredDS.print("filtered>>>>>>>>>>>>>>>>>>>");

        //TODO 6.向kafka中写回，需要将json转为String
        SingleOutputStreamOperator<String> kafkaDS = filteredDS.map(jsonObj -> jsonObj.toJSONString());
        //将流输出到kafka的dwm层
        kafkaDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        env.execute();
    }
}
