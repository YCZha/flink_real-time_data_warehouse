package org.example.gmallreal.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.gmallreal.app.func.DimSink;
import org.example.gmallreal.app.func.TableProcessFunction;
import org.example.gmallreal.bean.TableProcess;
import org.example.gmallreal.utils.MyKafkaUtil;

import javax.annotation.Nullable;

//准备业务数据DWD层，对数据从kafka读入，清洗，动态分流，将维度数据送入hbase，事实数据送入kafka不同主题
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备执行环境
        //1.1创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2设置并行度,和kafka分区数保持一致
//        env.setParallelism(3);
        env.setParallelism(1);
        //开启checkpoint 实现精准一次性
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/checkpoint/basedbapp"));
        //重启策略，如果没有开启ck，则不会重启，如果开启了ck，会默认帮你重启，从正确的地方开始执行，直到处理成功。
        env.setRestartStrategy(RestartStrategies.noRestart());
        //将访问hdfs的用户设置成zyc
        System.setProperty("HADOOP_USER_NAME","zyc");

        //TODO 2.从ods层读取数据
        //2.1 设置主题和消费者组
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";
        //2.2 获取FlinkKafkaSource
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //2.3 将数据源传给env
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);

        //TODO 3. 对DS中的数据进行结构的转换，String->Json
//        SingleOutputStreamOperator<JSONObject> jsonObj = jsonStrDS.map(
//                new MapFunction<String, JSONObject>() {
//                    @Override
//                    public JSONObject map(String s) throws Exception {
//                        return JSON.parseObject(s);
//                    }
//                }
//        );
//        jsonStrDS.map(
//                jsonStr->JSON.parse(jsonStr);
//        );
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(JSON::parseObject);

//        jsonObjDS.print("json>>>>>>>>>>>>>>>>>");
        //TODO 4.对数据进行ETL
        //4.1 利用filter完成过滤
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(
                jsonObj -> {
                    boolean flag = jsonObj.getString("table") != null
                            && jsonObj.getJSONObject("data") != null
                            && jsonObj.getString("data").length() >= 3;
                    //如果表名为空或者数据为空或者数据长度为空就过滤，返回一个bool值，true保留，false清洗
                    return flag;
                }
        );

//        filteredDS.print("filter>>>>>>>>>>>>>");
//        //TODO 5.对数据进行动态分流
        //如果是事实表，放在主流输出，输出到kafka，如果是维度表，通过侧输出流，写入到Hbase
        //实现侧输出流只能通过process算子，因为更底层，map实现不了。
        //5.1定义侧输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};
        //5.2 主流 事实数据写入kafka
        SingleOutputStreamOperator<JSONObject> kafkaDS = filteredDS.process(
                new TableProcessFunction(hbaseTag)
        );
        //5.3 侧输出流 维度数据写入hbase
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);

        //打印输出
        kafkaDS.print("kafka>>>>>>>>>>>>>>>>");
        hbaseDS.print("hbase>>>>>>>>>>>>>>>>");

        //TODO 6.将维度数据保存到phoenix对应的维度表中
        hbaseDS.addSink(new DimSink());

        //TODO 7.将实时数据保存到kafka对应的主题中
        FlinkKafkaProducer<JSONObject> kafkaSinkBySchema = MyKafkaUtil.getKafkaSinkBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public void open(SerializationSchema.InitializationContext context) throws Exception {
                        System.out.println("Kafka 序列化");
                    }

                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timsstamp) {
                        //可以根据需求将数据传输到指定的topic
                        String sinkTopic = jsonObj.getString("sink_table");
                        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                        return new ProducerRecord<>(sinkTopic, dataJsonObj.toString().getBytes());
                    }
                }
        );
        kafkaDS.addSink(kafkaSinkBySchema);
        env.execute();

    }
}
