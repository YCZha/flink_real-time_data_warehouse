package org.example.gmallreal.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.example.gmallreal.bean.OrderWide;
import org.example.gmallreal.bean.PaymentInfo;
import org.example.gmallreal.bean.PaymentWide;
import org.example.gmallreal.utils.DataTimeUtil;
import org.example.gmallreal.utils.MyKafkaUtil;

import java.awt.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

//支付宽表处理程序
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.配置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setParallelism(3);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/checkpoint/paymentwideapp"));
        System.setProperty("HADOOP_USER_NAME", "zyc");

        //TODO 2. 从kafka读取数据
        String dwdPaymentSourceTopic = "dwd_payment_info";
        String dwmOrderWideSourceTopic = "dwm_order_wide";
        String userGroup = "payment_wide_group";
        String paymentWideSinkTopic = "dwm_payment_wide";

        //2.1 从kafka读数据
        //支付数据
        DataStreamSource<String> paymentInfoJsonStrDS = env.addSource(MyKafkaUtil.getKafkaSource(dwdPaymentSourceTopic, userGroup));
        //订单宽表数据
        DataStreamSource<String> orderWideJsonStrDS = env.addSource(MyKafkaUtil.getKafkaSource(dwmOrderWideSourceTopic, userGroup));

        //TODO 3.对读取到的数据进行转换 JsonStr->POJO
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoJsonStrDS.map(
                jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class)
        );
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideJsonStrDS.map(
                jsonStr -> JSON.parseObject(jsonStr, OrderWide.class)
        );

//        paymentInfoDS.print("payinfo>>>>>>>>>>>>>>>>>>>>");
//        orderWideDS.print("orderwide>>>>>>>>>>>>>>>>>>");

        //TODO 4. 设置watermark，以及提取事件时间字段
        //4.1 支付流的watermark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWaterDS = paymentInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                                        //需要将字符串时间转换为ms
                                        Long ts = DataTimeUtil.toTS(paymentInfo.getCallback_time());
                                        return ts;
                                    }
                                }
                        )
        );
        //4.2 订单流的watermark
        SingleOutputStreamOperator<OrderWide> orderWideWithWaterDS = orderWideDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide orderWide, long l) {
                                        Long ts = DataTimeUtil.toTS(orderWide.getCreate_time());
                                        return ts;
                                    }
                                }
                        )
        );

        //TODO 5.对数据进行分组
        //5.1 支付流数据分组
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithWaterDS.keyBy(PaymentInfo::getOrder_id);
        //5.2 订单宽表数据分组
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithWaterDS.keyBy(OrderWide::getOrder_id);

        //TODO 6. 使用intervaljoin关联两条流
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedDS.intervalJoin(orderWideKeyedDS)
                //支付必须再下单后半小时内付款，所以向前半小时找订单数据
                .between(Time.minutes(-30), Time.minutes(0))
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context context, Collector<PaymentWide> collector) throws Exception {
                                collector.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }
                );
        paymentWideDS.print("paymentWideDS>>>>>>>>>>>>>>>>>");

        //TODO 7. 将数据写道kafka的dwm层
        //7.1 将结构转化为jsonStr
        SingleOutputStreamOperator<String> paymentWideSinkDS = paymentWideDS.map(
                paymentWide -> JSON.toJSONString(paymentWide)
        );
        paymentWideSinkDS.addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        env.execute();
    }
}
