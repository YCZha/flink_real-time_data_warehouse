package org.example.gmallreal.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.data.Json;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hdfs.web.resources.StoragePolicyParam;
import org.example.gmallreal.utils.MyKafkaUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

//准备用户行为日志的DWD层
public class BaseLogApp {
    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";
    private static final String TOPIC_PAGE = "dwd_page_log";

    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        //1.1创建flink流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度,和kafka分区有关，因为从分区中读数据
//        env.setParallelism(3);
        env.setParallelism(1);
        //1.3 设置checkpoint,实现精准一次性，每五秒钟开启一次
//        env.enableCheckpointing(5000);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //设置状态后端,路径是存放checkpoint的路径
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/checkpoint/baselogApp"));
        //因为我们的windows用户没有根目录的写权限，所以有两种方法，一种将根目录权限降低，二将用户改为zyc。
        System.setProperty("HADOOP_USER_NAME", "zyc");

        //TODO 2.从kafka中读数据
        //2.1 调用Kafka工具类，获取flinkkafkaConsumer
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 3. 对读取到的数据格式进行转换，String-》json
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String s) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        return jsonObject;
                    }
                }
        );
//        jsonObjDS.print("json>>>>>>");
        /*
        //TODO 4. 识别新老访客  前端也会对新老状态进行记录，可能会不准，所以再做一次确认
        //保存mid某天访问日期（将首次访问日期作为状态保存起来），等该设备再有日志过来的时候，从状态中获取日期和日志产生日期进行对比
        //如果状态不为空，且状态日期和日志日期不相等，说明是老访客，如果is_new是1，则对状态进行修复。如果为空，将其日期记录到状态
        */
        //4.1 根据mid对日志进行分组,按照mid将json文件进行分组
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(
                data -> data.getJSONObject("common").getString("mid")
        );
        //4.2 新老状态的修复  状态分为算子状态和键控状态，记录某一个设备的访问，使用键控状态比较合适
        SingleOutputStreamOperator<JSONObject> jsonDsWithFlag = midKeyedDS.map(
                //RichMapFunction有算子的生命周期
                new RichMapFunction<JSONObject, JSONObject>() {
                    //定义mid访问状态
                    private ValueState<String> firstVisitDAtaState;
                    //定义日期格式化对象
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //对状态以及日期格式进行初始化，该函数用于整个算子开始时执行的，使用valueState数据结构
                        firstVisitDAtaState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("newMidDateState", String.class)
                        );
                        sdf = new SimpleDateFormat("yyyyMMdd");

                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        //每条数据过来都要执行map`
                        //获取当前日志标记状态
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        //获取当前日志访问时间戳
                        Long ts = jsonObject.getLong("ts");
                        if ("1".equals(isNew)) {
                            //获取当前mid对应的状态
                            String stateDate = firstVisitDAtaState.value();
                            //对当前日志的日期格式进行转换
                            String curDate = sdf.format(new Date(ts));
                            //如果状态不为空，且状态日期和日志日期不相等，说明是老访客
                            if (stateDate != null && stateDate.length() != 0) {
                                //判断是否为同一天数据
                                if (!stateDate.equals(curDate)) {
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }
                            } else {
                                //如果没记录设备状态，将当前访问日期作为状态值
                                firstVisitDAtaState.update(curDate);
                            }
                        }
                        return jsonObject;
                    }
                }
        );
//        jsonDsWithFlag.print(">>>>>>>>>>>>>>>");

        //TODO 5.分流，根据日志的数据内容，将日志数据分为三类，页面日志，曝光日志，启动日志
        //页面日志输出到主流，启动日志输出到启动侧输出流，曝光日志输出到曝光侧输出流
        //侧输出流：1）接受迟到数据、2）分流
        //定义启动侧输出流标签
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        //定义启动侧输出流标签
        OutputTag<String> displayTag = new OutputTag<String >("display"){};

        SingleOutputStreamOperator<String> pageDS = jsonDsWithFlag.process(
                //输入对象是json，因为要输出到kafka,所以输出对象是string
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                        //获取启动日志的标记
                        JSONObject startJsonObj = jsonObject.getJSONObject("start");
                        //将json对象转化为String，方便像侧输出流，向kafka输出
                        String dataStr = jsonObject.toString();
                        //判断是否为启动日志
                        if (startJsonObj != null && startJsonObj.size() > 0) {
                            //如果是启动日志要输出到启动侧输出流
                            context.output(startTag, dataStr);
                        } else {
                            //如果不是启动日志，说明就是页面日志，直接输出主流
                            collector.collect(dataStr);
                            // 页面日志中包含曝光日志，继续判断来获取曝光日志
                            JSONArray displays = jsonObject.getJSONArray("displays");
                            //判断是否为曝光日志
                            if (displays != null && displays.size() > 0) {
                                //输出到侧输出流，因为曝光日志是一个数组，所以需要遍历
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject displaysJSONObject = displays.getJSONObject(i);
                                    //获取页面id
                                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                                    //将pageId放在每一条曝光日志中
                                    displaysJSONObject.put("page_id", pageId);
                                    context.output(displayTag, displaysJSONObject.toString());
                                }

                            }
//                            else {
//                                //是页面日志，此时统计的是没有曝光的页面，实际上有曝光的页面也会有页面信息，且目前这种情况取出来的页面，都会包含last_page_id，不符合
//                                collector.collect(dataStr);
//                            }
                        }
                    }
                }
        );
        //获取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        //打印
        pageDS.print("page>>>>>>>>>>>>>>>>>>");
        startDS.print("start>>>>>>>>>>>>>>>>>");
        displayDS.print("display>>>>>>>>>>>>>>");

        //TODO 6.将不同的流数据协会到Kafka的不同的主题中
        FlinkKafkaProducer<String> startSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        FlinkKafkaProducer<String> displaySink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);
        FlinkKafkaProducer<String> pageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);

        startDS.addSink(startSink);
        displayDS.addSink(displaySink);
        pageDS.addSink(pageSink);

        env.execute();

    }
}
