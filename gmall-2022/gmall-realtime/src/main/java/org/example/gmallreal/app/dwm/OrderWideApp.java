package org.example.gmallreal.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.example.gmallreal.app.func.DimAsyncFunction;
import org.example.gmallreal.bean.OrderDetail;
import org.example.gmallreal.bean.OrderInfo;
import org.example.gmallreal.bean.OrderWide;
import org.example.gmallreal.utils.MyKafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

//合并订单宽表
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        //配置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setParallelism(3);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/checkpoint/orderwideapp"));
        env.setRestartStrategy(RestartStrategies.noRestart());
        System.setProperty("HADOOP_USER_NAME", "zyc");

        //TODO 2.从kafka中DWD层读取订单表和订单明细表
        //2.1 定义主题和消费者主
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        //2.2 读取订单表数据
        DataStreamSource<String> orderInfoJsonDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId));
        //2.3 读取订单明细数据
        DataStreamSource<String> orderDetailJsonDS = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId));
//        orderInfoJsonDS.print(">>>>>>>>>>>>>>");
        //TODO 3.对读取数据进行结构转换，将jsonStr->OrderInfo/OrderDetail对象
        //3.1 转换订单表数据结构
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoJsonDS.map(
                //为了将创建时间转化为时间戳，所以使用richMapFunction，在open方法中初始化SimpleDataFormat
                new RichMapFunction<String, OrderInfo>() {
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String jsonStr) throws Exception {
                        //将jsonString类型的数据转换成为orderInfo实体类对象
                        OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                        //设置时间戳，将创建事件的年月日时间转化为毫秒为单位
                        orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                }
        );
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailJsonDS.map(
                new RichMapFunction<String, OrderDetail>() {
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String jsonStr) throws Exception {
                        //将jsonString类型的数据转换成为orderDetail实体类对象
                        OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                        //设置时间戳，将创建事件的年月日时间转化为毫秒为单位
                        orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                }
        );
//        orderInfoDS.print("orderInfo>>>>>>>>>>>>>>>");
//        orderDetailDS.print("orderDetail>>>>>>>>>>>>>>>");

        //TODO 4.指定事件时间字段
        //订单表指定事件时间字段
        SingleOutputStreamOperator<OrderInfo> orderInfoWithDS = orderInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long l) {
                                return orderInfo.getCreate_ts();
                            }
                        })
        );
        //订单明细表指定事件时间字段
        SingleOutputStreamOperator<OrderDetail> orderDetailWithDS = orderDetailDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long l) {
                                return orderDetail.getCreate_ts();
                            }
                        })
        );
        //TODO 5.双流Join
        //5.1 keyby分组
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithDS.keyBy(OrderInfo::getId); //按照订单id进行分组
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithDS.keyBy(OrderDetail::getOrder_id); //按照订单id进行分组

        //5.2 使用interval join将订单和订单明细进行关联
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS
                .intervalJoin(orderDetailKeyedDS)
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(
                        new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                            @Override
                            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context context, Collector<OrderWide> collector) throws Exception {
                                collector.collect(new OrderWide(orderInfo, orderDetail));
                            }
                        }
                );
//        orderWideDS.print("join>>>>>>>>>>>>>>>>");



        //TODO 6.将事实数据和维度数据进行关联
        //6.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    //订单宽表中只需要用户的年龄和性别，所以只需要提取相关内容即可
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimJsonObj) throws Exception {
                        //获取用户生日
                        String birthday = dimJsonObj.getString("BIRTHDAY");
                        //定义日期转换的工具类
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        Date birthdayDate = sdf.parse(birthday);
                        Long birthdayTS = birthdayDate.getTime();
                        //获取当前时间的毫秒数
                        Long curTS = System.currentTimeMillis();
                        //转化成年龄
                        Long ageTS = curTS - birthdayTS;
                        Long ageLong = ageTS / 1000L / 60L / 60L / 24L / 365L;
                        Integer age = ageLong.intValue();

                        //将维度中的年龄复制到宽表中的字段
                        orderWide.setUser_age(age);
                        //将维度中的性别复制到宽表中的字段
                        orderWide.setUser_gender(dimJsonObj.getString("GENDER"));
                    }
                },
                60000, TimeUnit.MILLISECONDS);//设置超时时间不宜过短，因为如果在redis中查找失败，会去hbase中查找，查找时间很慢

        //6.2 关联省市维度数据
        SingleOutputStreamOperator<OrderWide> orderWideWithProDS = AsyncDataStream.unorderedWait(
                orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide obj, JSONObject dimJsonObj) throws Exception {
                        obj.setProvince_name(dimJsonObj.getString("NAME"));
                        obj.setProvince_3166_2_code(dimJsonObj.getString("ISO_3166_2"));
                        obj.setProvince_area_code(dimJsonObj.getString("AREA_CODE"));
                        obj.setProvince_iso_code(dimJsonObj.getString("ISO_CODE"));
                    }
                },
                600000, TimeUnit.MICROSECONDS
        );
        //6.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSKUDS = AsyncDataStream.unorderedWait(
                orderWideWithProDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide obj, JSONObject dimJsonObj) throws Exception {
//                        obj.setSku_name(dimJsonObj.getString("SKU_NAME"));//冗余设计，减少表关联，可以不设置，因为订单明细表中有
                        obj.setCategory3_id(dimJsonObj.getLong("CATEGORY3_ID"));
                        obj.setSpu_id(dimJsonObj.getLong("SPU_ID"));
                        obj.setTm_id(dimJsonObj.getLong("TM_ID"));
                    }
                },
                600000, TimeUnit.MICROSECONDS
        );
        //6.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSPUDS = AsyncDataStream.unorderedWait(
                orderWideWithSKUDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {

                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide obj, JSONObject dimJsonObj) throws Exception {
                        obj.setSpu_name(dimJsonObj.getString("SPU_NAME"));
                    }
                },
                600000, TimeUnit.MICROSECONDS
        );
        //6.5 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithSPUDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {

                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getCategory3_id().toString();
                    }

                    @Override
                    public void join(OrderWide obj, JSONObject dimJsonObj) throws Exception {
                        obj.setCategory3_name(dimJsonObj.getString("NAME"));
                    }
                },
                600000, TimeUnit.MICROSECONDS
        );
        //6.6 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithCategory3DS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {

                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getTm_id().toString();
                    }

                    @Override
                    public void join(OrderWide obj, JSONObject dimJsonObj) throws Exception {
                        obj.setTm_name(dimJsonObj.getString("TM_NAME"));
                    }
                },
                600000, TimeUnit.MICROSECONDS
        );
        //TODO 7.将流写入kafka
        //先将OrderWide对象转化为JsonStr，再传输到kafka
        orderWideWithTmDS.map(
                orderWide -> JSON.toJSONString(orderWide)
        ).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));
//        orderWideWithSKUDS.print("kafkaSink>>>>>>>>>>>>>>>>");
        env.execute();
    }

}
